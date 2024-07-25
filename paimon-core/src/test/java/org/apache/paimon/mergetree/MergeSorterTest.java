/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.SizedReaderSupplier;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IteratorRecordReader;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MergeSorter}. */
@ExtendWith(ParameterizedTestExtension.class)
public class MergeSorterTest {

    private static final int MEMORY_SIZE = 1024 * 1024 * 32;

    private final RowType keyType = RowType.builder().field("k", DataTypes.INT()).build();

    private final RowType valueType = RowType.builder().field("v", DataTypes.INT()).build();

    private final SortEngine sortEngine;

    @TempDir Path tempDir;

    private IOManager ioManager;
    private MergeSorter sorter;
    private int totalPages;

    public MergeSorterTest(SortEngine sortEngine) {
        this.sortEngine = sortEngine;
    }

    @SuppressWarnings("unused")
    @Parameters(name = "{0}")
    public static List<SortEngine> getVarSeg() {
        return Arrays.asList(SortEngine.LOSER_TREE, SortEngine.MIN_HEAP);
    }

    @BeforeEach
    public void beforeTest() {
        ioManager = IOManager.create(tempDir.toString());
        Options options = new Options();
        options.set(CoreOptions.SORT_SPILL_BUFFER_SIZE, new MemorySize(MEMORY_SIZE));
        options.set(CoreOptions.SORT_ENGINE, sortEngine);
        sorter = new MergeSorter(new CoreOptions(options), keyType, valueType, ioManager);
        totalPages = sorter.memoryPool().freePages();
    }

    @AfterEach
    public void afterTest() throws Exception {
        assertThat(sorter.memoryPool().freePages()).isEqualTo(totalPages);
        List<File> files =
                Files.walk(tempDir)
                        .map(Path::toFile)
                        .filter(f -> !f.isDirectory())
                        .collect(Collectors.toList());
        assertThat(files).isEmpty();
        this.ioManager.close();
    }

    @TestTemplate
    public void testSortAndMerge() throws Exception {
        innerTest(null);
    }

    @TestTemplate
    public void testWithUserDefineSequence() throws Exception {
        innerTest(
                new FieldsComparator() {
                    @Override
                    public int[] compareFields() {
                        return new int[] {0};
                    }

                    @Override
                    public int compare(InternalRow o1, InternalRow o2) {
                        return Integer.compare(o1.getInt(0), o2.getInt(0));
                    }
                });
    }

    private void innerTest(FieldsComparator userDefinedSeqComparator) throws Exception {
        Comparator<KeyValue> comparator =
                Comparator.comparingInt((KeyValue o) -> o.key().getInt(0));
        if (userDefinedSeqComparator != null) {
            comparator =
                    comparator.thenComparing(
                            (o1, o2) -> userDefinedSeqComparator.compare(o1.value(), o2.value()));
        }
        comparator = comparator.thenComparingLong(KeyValue::sequenceNumber);

        List<SizedReaderSupplier<KeyValue>> readers = new ArrayList<>();
        Random rnd = new Random();
        List<KeyValue> expectedKvs = new ArrayList<>();
        Set<Long> distinctSeq = new HashSet<>();
        for (int i = 0; i < rnd.nextInt(20) + 3; i++) {
            List<KeyValue> kvs = new ArrayList<>();
            Set<Integer> distinctKeys = new HashSet<>();
            for (int j = 0; j < 100; j++) {
                while (true) {
                    int key = rnd.nextInt(1000);
                    if (distinctKeys.contains(key)) {
                        continue;
                    }

                    long seq = rnd.nextLong();
                    while (distinctSeq.contains(seq)) {
                        seq = rnd.nextLong();
                    }
                    distinctSeq.add(seq);
                    kvs.add(
                            new KeyValue()
                                    .replace(
                                            GenericRow.of(key),
                                            seq,
                                            RowKind.fromByteValue((byte) rnd.nextInt(4)),
                                            GenericRow.of(rnd.nextInt(1000)))
                                    .setLevel(rnd.nextInt(100)));
                    distinctKeys.add(key);
                    break;
                }
            }
            expectedKvs.addAll(kvs);
            kvs.sort(comparator);
            readers.add(
                    new SizedReaderSupplier<KeyValue>() {
                        @Override
                        public long estimateSize() {
                            return kvs.size();
                        }

                        @Override
                        public RecordReader<KeyValue> get() {
                            return new IteratorRecordReader<>(kvs.iterator());
                        }
                    });
        }

        expectedKvs.sort(comparator);

        TestMergeFunctionWrapper collectFunc = new TestMergeFunctionWrapper();
        List<KeyValue> all = new ArrayList<>();
        sorter.mergeSort(
                        readers,
                        Comparator.comparingInt(o -> o.getInt(0)),
                        userDefinedSeqComparator,
                        collectFunc)
                .forEachRemaining(all::addAll);

        assertThat(toString(all)).containsExactlyElementsOf(toString(expectedKvs));
    }

    private List<String> toString(List<KeyValue> kvs) {
        return kvs.stream().map(kv -> kv.toString(keyType, valueType)).collect(Collectors.toList());
    }

    private static class TestMergeFunctionWrapper implements MergeFunctionWrapper<List<KeyValue>> {

        private List<KeyValue> result;

        @Override
        public void reset() {
            result = new ArrayList<>();
        }

        @Override
        public void add(KeyValue kv) {
            result.add(kv);
        }

        @Nullable
        @Override
        public List<KeyValue> getResult() {
            return result;
        }
    }
}
