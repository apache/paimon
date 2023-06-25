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
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.mergetree.compact.ConcatRecordReader.ReaderSupplier;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MergeSorter}. */
public class MergeSorterTest {

    private static final int MEMORY_SIZE = 1024 * 1024 * 32;

    private final RowType keyType = RowType.builder().field("k", DataTypes.INT()).build();

    private final RowType valueType = RowType.builder().field("v", DataTypes.INT()).build();

    @TempDir Path tempDir;

    private IOManager ioManager;
    private MergeSorter sorter;
    private int totalPages;

    @BeforeEach
    public void beforeTest() {
        ioManager = IOManager.create(tempDir.toString());
        Options options = new Options();
        options.set(CoreOptions.READ_SPILL_BUFFER_SIZE, new MemorySize(MEMORY_SIZE));
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

    @Test
    public void testSortAndMerge() throws Exception {
        List<ReaderSupplier<KeyValue>> readers = new ArrayList<>();
        Random rnd = new Random();
        List<KeyValue> expectedKvs = new ArrayList<>();
        Set<Long> distinctSeq = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            List<KeyValue> kvs = new ArrayList<>();
            for (int j = 0; j < 100; j++) {
                long seq = rnd.nextLong();
                while (distinctSeq.contains(seq)) {
                    rnd.nextLong();
                }
                distinctSeq.add(seq);
                kvs.add(
                        new KeyValue()
                                .replace(
                                        GenericRow.of(rnd.nextInt(100)),
                                        seq,
                                        RowKind.fromByteValue((byte) rnd.nextInt(4)),
                                        GenericRow.of(rnd.nextInt()))
                                .setLevel(rnd.nextInt(100)));
            }
            expectedKvs.addAll(kvs);
            readers.add(() -> new IteratorRecordReader<>(kvs.iterator()));
        }

        expectedKvs.sort(
                Comparator.comparingInt((KeyValue o) -> o.key().getInt(0))
                        .thenComparingLong(KeyValue::sequenceNumber));

        MergeFunctionWrapper<List<KeyValue>> collectFunc =
                new MergeFunctionWrapper<List<KeyValue>>() {

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
                };
        List<KeyValue> all = new ArrayList<>();
        sorter.mergeSort(readers, Comparator.comparingInt(o -> o.getInt(0)), collectFunc)
                .forEachRemaining(all::addAll);

        assertThat(toString(all)).containsExactlyElementsOf(toString(expectedKvs));
    }

    private List<String> toString(List<KeyValue> kvs) {
        return kvs.stream().map(kv -> kv.toString(keyType, valueType)).collect(Collectors.toList());
    }
}
