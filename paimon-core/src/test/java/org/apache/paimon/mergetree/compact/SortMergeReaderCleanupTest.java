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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for merge state cleanup when a {@link SortMergeReader} is closed. */
class SortMergeReaderCleanupTest {

    @TempDir Path tempDir;

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    void testCloseReleasesLookupSpills(SortEngine sortEngine) throws Exception {
        Options rawOptions = new Options();
        rawOptions.set(CoreOptions.LOOKUP_MERGE_RECORDS_THRESHOLD, 0);
        rawOptions.set(CoreOptions.LOOKUP_MERGE_BUFFER_SIZE, MemorySize.parse("4 kb"));
        rawOptions.set(CoreOptions.PAGE_SIZE, MemorySize.parse("4 kb"));
        CoreOptions options = new CoreOptions(rawOptions);
        RowType keyType = RowType.builder().field("k", DataTypes.INT()).build();
        RowType valueType = RowType.builder().field("v", DataTypes.STRING()).build();
        char[] chars = new char[1000];
        Arrays.fill(chars, 'x');
        BinaryString value = BinaryString.fromString(new String(chars));
        try (IOManager ioManager = IOManager.create(tempDir.toString())) {
            MergeFunction<KeyValue> merge =
                    new LookupMergeFunction(
                            DeduplicateMergeFunction.factory().create(),
                            options,
                            keyType,
                            valueType,
                            ioManager);
            List<RecordReader<KeyValue>> inputs = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                KeyValue kv =
                        new KeyValue()
                                .replace(GenericRow.of(1), i, RowKind.INSERT, GenericRow.of(value))
                                .setLevel(0);
                inputs.add(new IteratorRecordReader<>(Collections.singletonList(kv).iterator()));
            }
            KeyValue result;
            try (SortMergeReader<KeyValue> reader =
                    SortMergeReader.createSortMergeReader(
                            inputs,
                            Comparator.comparingInt(row -> row.getInt(0)),
                            null,
                            new ReducerMergeFunctionWrapper(merge),
                            sortEngine)) {
                RecordReader.RecordIterator<KeyValue> batch = reader.readBatch();
                assertThat(batch).isNotNull();
                try {
                    result = batch.next();
                    assertThat(result).isNotNull();
                    assertThat(result.sequenceNumber()).isEqualTo(15);
                    assertThat(spillFileCount()).isPositive();
                } finally {
                    batch.releaseBatch();
                }
            }
            assertThat(spillFileCount()).isZero();
            assertThat(result.value().getString(0)).isEqualTo(value);
        }
    }

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    void testCloseStillResetsMergeWhenInputCloseFails(SortEngine sortEngine) {
        IOException inputFailure = new IOException("input close failed");
        RuntimeException resetFailure = new IllegalStateException("merge reset failed");
        RecordReader<KeyValue> input =
                new RecordReader<KeyValue>() {
                    @Override
                    public RecordIterator<KeyValue> readBatch() {
                        return null;
                    }

                    @Override
                    public void close() throws IOException {
                        throw inputFailure;
                    }
                };
        AtomicInteger resets = new AtomicInteger();
        MergeFunctionWrapper<KeyValue> merge =
                new ReducerMergeFunctionWrapper(DeduplicateMergeFunction.factory().create()) {
                    @Override
                    public void reset() {
                        resets.incrementAndGet();
                        throw resetFailure;
                    }
                };
        SortMergeReader<KeyValue> reader =
                SortMergeReader.createSortMergeReader(
                        Collections.singletonList(input),
                        Comparator.comparingInt(row -> row.getInt(0)),
                        null,
                        merge,
                        sortEngine);
        assertThatThrownBy(reader::close).isSameAs(inputFailure);
        assertThat(inputFailure.getSuppressed()).containsExactly(resetFailure);
        assertThat(resets.get()).isEqualTo(1);
    }

    private long spillFileCount() throws IOException {
        try (Stream<Path> paths = Files.walk(tempDir)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }
}
