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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.DeleteOrderingGuard;
import org.apache.paimon.index.DeleteOrderingGuardSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.SizedReaderSupplier;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Tests capture on sorted, TTL-filtered compaction inputs, before merging and delete pruning. */
class DeleteOrderingGuardMergeFunctionWrapperTest {

    private static final RowType KEY_TYPE = RowType.of(DataTypes.INT());
    private static final RowType VALUE_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT())
                    .field("seqA", DataTypes.INT())
                    .field("a", DataTypes.STRING())
                    .field("seqB", DataTypes.INT())
                    .field("b", DataTypes.STRING())
                    .field("payload", DataTypes.STRING())
                    .field("eventTime", DataTypes.BIGINT())
                    .build();

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({"MIN_HEAP,false", "MIN_HEAP,true", "LOSER_TREE,false", "LOSER_TREE,true"})
    void testCaptureBeforeOrdinaryDeletePruning(SortEngine engine, boolean spill) throws Exception {
        Options options = options(engine);
        options.set(CoreOptions.SEQUENCE_FIELD, "seqA");
        List<DeleteOrderingGuard> guards = new ArrayList<>();

        // Both merge sort implementations order the stale insert before the explicit delete.
        List<KeyValue> result =
                merge(
                        options,
                        spill,
                        guards,
                        Arrays.asList(
                                Collections.singletonList(row(1, 4, RowKind.DELETE, 4, null, null)),
                                Collections.singletonList(
                                        row(1, 9, RowKind.INSERT, 2, null, "stale"))),
                        null);

        assertThat(result).isEmpty();
        assertThat(guards).hasSize(1);
        assertThat(guards.get(0).ordering().getInt(0)).isEqualTo(4);
        assertThat(guards.get(0).sequenceNumber()).isEqualTo(4);
        assertThat(guards.get(0).level()).isEqualTo(2);
    }

    @ParameterizedTest
    @CsvSource({"MIN_HEAP,false", "MIN_HEAP,true", "LOSER_TREE,false", "LOSER_TREE,true"})
    void testCapturesStaleDeleteEvenWhenMergedResultIsLive(SortEngine engine, boolean spill)
            throws Exception {
        Options options = options(engine);
        options.set(CoreOptions.SEQUENCE_FIELD, "seqA");
        KeyValue delete = row(1, 4, RowKind.DELETE, 4, 7, "delete-payload");
        KeyValue update = row(1, 8, RowKind.INSERT, 5, 8, "fresh");
        List<DeleteOrderingGuard> guards = new ArrayList<>();
        List<KeyValue> result =
                merge(
                        options,
                        spill,
                        guards,
                        Arrays.asList(
                                Collections.singletonList(delete),
                                Collections.singletonList(update)),
                        null);

        assertThat(result).hasSize(1);
        InternalRow value = result.get(0).value();
        assertThat(value.getInt(1)).isEqualTo(5);
        assertThat(value.getString(2)).isEqualTo(BinaryString.fromString("fresh"));
        assertThat(value.getInt(3)).isEqualTo(8);
        assertThat(value.getString(4)).isEqualTo(BinaryString.fromString("fresh"));
        assertThat(value.getString(5)).isEqualTo(BinaryString.fromString("fresh"));
        assertThat(guards).hasSize(1);
        assertThat(guards.get(0).ordering().getInt(0)).isEqualTo(4);
        assertThat(delete.value().getString(2))
                .isEqualTo(BinaryString.fromString("delete-payload"));
        assertThat(delete.valueKind()).isEqualTo(RowKind.DELETE);
    }

    @ParameterizedTest
    @CsvSource({"MIN_HEAP,false", "MIN_HEAP,true", "LOSER_TREE,false", "LOSER_TREE,true"})
    void testExpiredRowsAndDeletesProduceNoGuards(SortEngine engine, boolean spill)
            throws Exception {
        Options options = options(engine);
        options.set(CoreOptions.SEQUENCE_FIELD, "seqA");
        options.set(CoreOptions.RECORD_LEVEL_EXPIRE_TIME, Duration.ofSeconds(10));
        options.set(CoreOptions.RECORD_LEVEL_TIME_FIELD, "eventTime");
        TableSchema schema =
                new TableSchema(
                        0,
                        VALUE_TYPE.getFields(),
                        6,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        options.toMap(),
                        null);
        RecordLevelExpire ttl =
                RecordLevelExpire.create(
                        new CoreOptions(options), schema, mock(SchemaManager.class));
        KeyValue expiredDelete = row(1, 4, RowKind.DELETE, 4, null, null);
        ((GenericRow) expiredDelete.value()).setField(6, 1L);
        KeyValue expiredInsert = row(2, 5, RowKind.INSERT, 5, null, null);
        ((GenericRow) expiredInsert.value()).setField(6, 1L);
        KeyValue activeDelete = row(3, 7, RowKind.DELETE, 7, null, null);
        List<DeleteOrderingGuard> guards = new ArrayList<>();
        List<KeyValue> result =
                merge(
                        options,
                        spill,
                        guards,
                        Arrays.asList(
                                Arrays.asList(expiredDelete, expiredInsert),
                                Collections.singletonList(activeDelete)),
                        ttl);

        assertThat(result).isEmpty();
        assertThat(guards).hasSize(1);
        assertThat(guards.get(0).key().getInt(0)).isEqualTo(3);
        assertThat(guards.get(0).ordering().getInt(0)).isEqualTo(7);
    }

    @Test
    void testCollectorFailureStopsMerge() {
        @SuppressWarnings("unchecked")
        MergeFunctionWrapper<KeyValue> wrapped = mock(MergeFunctionWrapper.class);
        IOException failure = new IOException("injected segment write failure");
        DeleteOrderingGuardMergeFunctionWrapper<KeyValue> capture =
                new DeleteOrderingGuardMergeFunctionWrapper<>(
                        wrapped,
                        new DeleteOrderingGuardSerializer(
                                0, KEY_TYPE, VALUE_TYPE, new CoreOptions(new Options())),
                        guard -> {
                            throw failure;
                        });
        KeyValue delete = row(1, 4, RowKind.DELETE, 4, null, null);

        assertThatThrownBy(() -> capture.add(delete))
                .isInstanceOf(UncheckedIOException.class)
                .hasCause(failure);
        verify(wrapped, never()).add(delete);
    }

    private List<KeyValue> merge(
            Options options,
            boolean spill,
            List<DeleteOrderingGuard> guards,
            List<List<KeyValue>> runs,
            RecordLevelExpire ttl)
            throws Exception {
        CoreOptions coreOptions = new CoreOptions(options);
        MergeFunctionFactory<KeyValue> factory = DeduplicateMergeFunction.factory(options);
        MergeFunctionWrapper<KeyValue> wrapper =
                new DeleteOrderingGuardMergeFunctionWrapper<>(
                        new ReducerMergeFunctionWrapper(factory.create()),
                        new DeleteOrderingGuardSerializer(0, KEY_TYPE, VALUE_TYPE, coreOptions),
                        guards::add);
        List<SizedReaderSupplier<KeyValue>> inputs = new ArrayList<>();
        List<List<KeyValue>> inputRuns = new ArrayList<>(runs);
        // With a threshold of two, three inputs force the spill path when an IOManager is present.
        while (inputRuns.size() < 3) {
            inputRuns.add(Collections.emptyList());
        }
        for (List<KeyValue> run : inputRuns) {
            FileReaderFactory<KeyValue> raw = file -> new IteratorRecordReader<>(run.iterator());
            FileReaderFactory<KeyValue> filtered = ttl == null ? raw : ttl.wrap(raw);
            inputs.add(
                    new SizedReaderSupplier<KeyValue>() {
                        @Override
                        public long estimateSize() {
                            return 100;
                        }

                        @Override
                        public RecordReader<KeyValue> get() throws IOException {
                            return filtered.createRecordReader(mock(DataFileMeta.class));
                        }
                    });
        }
        try (IOManager ioManager = IOManager.create(tempDir.toString())) {
            MergeSorter sorter =
                    new MergeSorter(coreOptions, KEY_TYPE, VALUE_TYPE, spill ? ioManager : null);
            List<KeyValue> result = new ArrayList<>();
            try (RecordReader<KeyValue> reader =
                    new DropDeleteReader(
                            sorter.mergeSort(
                                    inputs,
                                    Comparator.comparingInt(key -> key.getInt(0)),
                                    coreOptions.sequenceField().isEmpty()
                                            ? null
                                            : UserDefinedSeqComparator.create(
                                                    VALUE_TYPE, coreOptions),
                                    wrapper))) {
                reader.forEachRemaining(
                        record ->
                                result.add(
                                        record.copy(
                                                new InternalRowSerializer(KEY_TYPE),
                                                new InternalRowSerializer(VALUE_TYPE))));
            }
            return result;
        }
    }

    private static Options options(SortEngine engine) {
        Options options = new Options();
        options.set(CoreOptions.SORT_ENGINE, engine);
        options.set(CoreOptions.SORT_SPILL_THRESHOLD, 2);
        return options;
    }

    private static KeyValue row(
            int key, long sequence, RowKind kind, Integer seqA, Integer seqB, String payload) {
        BinaryString text = payload == null ? null : BinaryString.fromString(payload);
        return new KeyValue()
                .replace(
                        GenericRow.of(key),
                        sequence,
                        kind,
                        GenericRow.of(
                                key,
                                seqA,
                                text,
                                seqB,
                                text,
                                text,
                                System.currentTimeMillis() / 1000L + 3600))
                .setLevel(2);
    }
}
