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
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.SizedReaderSupplier;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Exercises persisted bounds through native merge ordering, spilling and group visibility. */
class DeleteOrderingGuardReaderTest {

    private static final RowType KEY_TYPE = RowType.of(DataTypes.INT());
    private static final RowType VALUE_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT().notNull())
                    .field("seqA", DataTypes.INT())
                    .field("a", DataTypes.STRING())
                    .field("seqB", DataTypes.INT())
                    .field("b", DataTypes.STRING())
                    .field("ungrouped", DataTypes.STRING().notNull())
                    .build();
    private static final Comparator<InternalRow> KEYS = Comparator.comparingInt(r -> r.getInt(0));

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({"MIN_HEAP,false", "MIN_HEAP,true", "LOSER_TREE,false", "LOSER_TREE,true"})
    void testSourceOrderingAndInternalTiesAcrossSegments(SortEngine engine, boolean spill)
            throws Exception {
        Options options = options(engine);
        options.set(CoreOptions.SEQUENCE_FIELD, "seqA");
        List<List<KeyValue>> bounds =
                Arrays.asList(
                        Arrays.asList(guard(1, 10, 4, null), guard(3, 10, 4, null)),
                        Arrays.asList(
                                guard(1, 8, 3, null),
                                guard(2, 10, 4, null),
                                guard(4, 10, 4, null),
                                guard(9, 10, 4, null)));
        List<DeleteOrderingGuard> captured = new ArrayList<>();
        List<KeyValue> result =
                merge(
                        options,
                        spill,
                        bounds,
                        captured,
                        Arrays.asList(
                                Arrays.asList(
                                        row(1, 100, 2, null, "stale"),
                                        row(2, 9, 4, null, "old-tie")),
                                Arrays.asList(
                                        row(3, 10, 4, null, "equal-tie"),
                                        row(4, 11, 4, null, "new-tie")),
                                Collections.singletonList(row(5, 0, 1, null, "unprotected"))));
        assertThat(result).extracting(kv -> kv.key().getInt(0)).containsExactly(3, 4, 5);
        assertThat(captured).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"MIN_HEAP,false", "MIN_HEAP,true", "LOSER_TREE,false", "LOSER_TREE,true"})
    void testIndependentPartialGroupsAndUngroupedPayload(SortEngine engine, boolean spill)
            throws Exception {
        Options options = partialOptions(engine);
        List<DeleteOrderingGuard> captured = new ArrayList<>();
        List<KeyValue> result =
                merge(
                        options,
                        spill,
                        Arrays.asList(
                                Collections.singletonList(guard(1, 1, 4, null)),
                                Collections.singletonList(guard(1, 2, null, 7))),
                        captured,
                        Arrays.asList(
                                Collections.singletonList(row(1, 10, 2, 8, "accepted")),
                                Collections.emptyList(),
                                Collections.emptyList()));
        assertThat(result)
                .singleElement()
                .satisfies(
                        kv -> {
                            assertThat(kv.value().getInt(1)).isEqualTo(4);
                            assertThat(kv.value().isNullAt(2)).isTrue();
                            assertThat(kv.value().getInt(3)).isEqualTo(8);
                            assertThat(kv.value().getString(4).toString()).isEqualTo("accepted");
                            assertThat(kv.value().getString(5).toString()).isEqualTo("accepted");
                        });
        assertThat(captured).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"MIN_HEAP,false", "MIN_HEAP,true", "LOSER_TREE,false", "LOSER_TREE,true"})
    void testWholeRowVisibilityFollowsDesignatedGroup(SortEngine engine, boolean spill)
            throws Exception {
        Options options = partialOptions(engine);
        options.set(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP, "seqA");
        List<KeyValue> result =
                merge(
                        options,
                        spill,
                        Collections.singletonList(
                                Arrays.asList(guard(1, 1, 4, 7), guard(2, 1, 4, 7))),
                        new ArrayList<>(),
                        Arrays.asList(
                                Collections.singletonList(row(1, 10, 2, 8, "hidden")),
                                Collections.singletonList(row(2, 10, 5, 2, "live")),
                                Collections.emptyList()));
        assertThat(result)
                .singleElement()
                .satisfies(
                        kv -> {
                            assertThat(kv.key().getInt(0)).isEqualTo(2);
                            assertThat(kv.value().getString(2).toString()).isEqualTo("live");
                            assertThat(kv.value().isNullAt(4)).isTrue();
                        });
    }

    @Test
    void testDescendingCompositeSourceOrderAndNulls() throws Exception {
        Options options = options(SortEngine.MIN_HEAP);
        options.set(CoreOptions.SEQUENCE_FIELD, "seqA,seqB");
        options.set(CoreOptions.SEQUENCE_FIELD_SORT_ORDER, CoreOptions.SortOrder.DESCENDING);
        List<KeyValue> result =
                merge(
                        options,
                        false,
                        Collections.singletonList(
                                Arrays.asList(
                                        guard(1, 1, 4, 7), guard(2, 1, 4, 7), guard(3, 1, 4, 7))),
                        new ArrayList<>(),
                        Arrays.asList(
                                Collections.singletonList(row(1, 9, 4, 8, "stale")),
                                Collections.singletonList(row(2, 9, 4, 6, "fresh")),
                                Collections.singletonList(row(3, 9, null, null, "null"))));
        // Descending source comparison uses the native null ordering as well.
        assertThat(result).extracting(kv -> kv.key().getInt(0)).containsExactly(2);
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                "ASCENDING,4,8,true", "DESCENDING,4,8,false",
                "ASCENDING,4,6,false", "DESCENDING,4,6,true",
                "ASCENDING,NULL,NULL,false", "DESCENDING,NULL,NULL,false"
            },
            nullValues = "NULL")
    void testCompositeGroupDirectionAndNullHandling(
            String direction, Integer a, Integer b, boolean accepted) throws Exception {
        Options options = options(SortEngine.MIN_HEAP);
        options.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.PARTIAL_UPDATE);
        options.setString("fields.seqA,seqB.sequence-group", "a,b");
        options.setString("fields.seqA,seqB.sequence-group.sort-order", direction);
        List<KeyValue> result =
                merge(
                        options,
                        false,
                        Collections.singletonList(Collections.singletonList(guard(1, 1, 4, 7))),
                        new ArrayList<>(),
                        Arrays.asList(
                                Collections.singletonList(row(1, 10, a, b, "candidate")),
                                Collections.emptyList(),
                                Collections.emptyList()));
        assertThat(result)
                .singleElement()
                .satisfies(
                        kv -> {
                            assertThat(kv.value().isNullAt(2)).isEqualTo(!accepted);
                            assertThat(kv.value().isNullAt(4)).isEqualTo(!accepted);
                            assertThat(kv.value().getString(5).toString()).isEqualTo("candidate");
                        });
    }

    @Test
    void testWholeRowDeleteWithoutGroups() throws Exception {
        Options options = options(SortEngine.MIN_HEAP);
        options.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.PARTIAL_UPDATE);
        options.set(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE, true);
        options.set(CoreOptions.SEQUENCE_FIELD, "seqA");
        List<KeyValue> result =
                merge(
                        options,
                        false,
                        Collections.singletonList(
                                Arrays.asList(guard(1, 1, 4, null), guard(2, 1, 4, null))),
                        new ArrayList<>(),
                        Arrays.asList(
                                Collections.singletonList(row(1, 10, 2, null, "stale")),
                                Collections.singletonList(row(2, 10, 5, null, "fresh")),
                                Collections.emptyList()));
        assertThat(result)
                .singleElement()
                .satisfies(kv -> assertThat(kv.key().getInt(0)).isEqualTo(2));
    }

    @ParameterizedTest
    @ValueSource(strings = {"lookup", "full"})
    void testGuardedChangelogBeforeImageUsesResolvedPartialGroups(String producer)
            throws Exception {
        Options options = partialOptions(SortEngine.MIN_HEAP);
        MergeFunctionFactory<KeyValue> factory =
                PartialUpdateMergeFunction.factory(
                        options, VALUE_TYPE, Collections.singletonList("id"));
        MergeFunctionWrapper<ChangelogResult> nativeWrapper =
                changelogWrapper(producer, factory, options);
        try (DeleteOrderingGuardReader guards =
                new DeleteOrderingGuardReader(
                        Collections.singletonList(
                                new IteratorRecordReader<>(
                                        Collections.singletonList(guard(1, 1, 4, 7)).iterator())),
                        KEYS)) {
            MergeFunctionWrapper<ChangelogResult> resolver =
                    guards.wrap(nativeWrapper, factory, null, null);
            resolver.reset();
            resolver.add(row(1, 0, 2, 8, "before").setLevel(2));
            resolver.add(row(1, 10, 5, 8, "after"));
            ChangelogResult result = resolver.getResult();
            assertThat(result.changelogs())
                    .extracting(KeyValue::valueKind)
                    .containsExactly(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER);
            assertThat(result.changelogs().get(0).value().isNullAt(2)).isTrue();
            assertThat(result.changelogs().get(0).value().getString(4).toString())
                    .isEqualTo("before");
            assertThat(result.changelogs().get(1).value().getString(2).toString())
                    .isEqualTo("after");
        }
    }

    @ParameterizedTest
    @CsvSource({"lookup,2", "lookup,5", "full,2", "full,5"})
    void testHiddenBeforeImageDoesNotProducePhantomChangelog(String producer, int source)
            throws Exception {
        Options options = options(SortEngine.MIN_HEAP);
        options.set(CoreOptions.SEQUENCE_FIELD, "seqA");
        CoreOptions core = new CoreOptions(options);
        MergeFunctionFactory<KeyValue> factory = DeduplicateMergeFunction.factory(options);
        MergeFunctionWrapper<ChangelogResult> nativeWrapper =
                changelogWrapper(producer, factory, options);
        try (DeleteOrderingGuardReader guards =
                new DeleteOrderingGuardReader(
                        Collections.singletonList(
                                new IteratorRecordReader<>(
                                        Collections.singletonList(guard(1, 1, 4, null))
                                                .iterator())),
                        KEYS)) {
            MergeFunctionWrapper<ChangelogResult> resolver =
                    guards.wrap(
                            nativeWrapper,
                            factory,
                            null,
                            UserDefinedSeqComparator.create(VALUE_TYPE, core));
            resolver.reset();
            resolver.add(row(1, 0, 1, null, "hidden").setLevel(2));
            resolver.add(row(1, 10, source, null, "candidate"));
            ChangelogResult result = resolver.getResult();
            if (source == 2) {
                assertThat(result.changelogs()).isEmpty();
                assertThat(result.result()).isNull();
            } else {
                assertThat(result.changelogs())
                        .extracting(KeyValue::valueKind)
                        .containsExactly(RowKind.INSERT);
                assertThat(result.result().value().getString(2).toString()).isEqualTo("candidate");
            }
        }
    }

    private MergeFunctionWrapper<ChangelogResult> changelogWrapper(
            String producer, MergeFunctionFactory<KeyValue> factory, Options options) {
        if (producer.equals("full")) {
            return new FullChangelogMergeFunctionWrapper(factory.create(), 2, null);
        }
        CoreOptions core = new CoreOptions(options);
        return new LookupChangelogMergeFunctionWrapper<>(
                LookupMergeFunction.wrap(factory, core, KEY_TYPE, VALUE_TYPE),
                key -> null,
                null,
                LookupStrategy.from(false, true, false, false),
                null,
                UserDefinedSeqComparator.create(VALUE_TYPE, core));
    }

    @Test
    void testAllNullGroupDoesNotRetractAndRealDeletesStillCaptured() throws Exception {
        Options options = partialOptions(SortEngine.MIN_HEAP);
        KeyValue delete = row(1, 9, 5, null, "real-delete");
        delete.replace(delete.key(), delete.sequenceNumber(), RowKind.DELETE, delete.value());
        List<DeleteOrderingGuard> captured = new ArrayList<>();
        List<KeyValue> result =
                merge(
                        options,
                        false,
                        Collections.singletonList(
                                Collections.singletonList(guard(1, 1, null, null))),
                        captured,
                        Arrays.asList(
                                Collections.singletonList(row(1, 8, 4, 8, "old")),
                                Collections.singletonList(delete),
                                Collections.emptyList()));
        assertThat(result)
                .singleElement()
                .satisfies(
                        kv -> {
                            assertThat(kv.value().isNullAt(2)).isTrue();
                            assertThat(kv.value().getString(4).toString()).isEqualTo("old");
                            assertThat(kv.value().getString(5).toString()).isEqualTo("old");
                        });
        assertThat(captured).hasSize(1);
        assertThat(captured.get(0).ordering().getInt(0)).isEqualTo(5);
    }

    @Test
    void testClosesAllResourcesWhenDataCloseFails() throws Exception {
        RecordReader<KeyValue> segment = mock(RecordReader.class);
        when(segment.readBatch()).thenReturn(null);
        RecordReader<KeyValue> data = mock(RecordReader.class);
        doThrow(new IOException("data close")).when(data).close();
        DeleteOrderingGuardReader guards =
                new DeleteOrderingGuardReader(Collections.singletonList(segment), KEYS);
        assertThatThrownBy(() -> guards.withDataReader(data).close())
                .isInstanceOf(IOException.class)
                .hasMessage("data close");
        verify(segment).close();
    }

    @Test
    void testOpeningFailureReleasesBatchesAndClosesUninitializedSegments() throws Exception {
        RecordReader<KeyValue> first = mock(RecordReader.class);
        RecordReader.RecordIterator<KeyValue> batch = mock(RecordReader.RecordIterator.class);
        when(first.readBatch()).thenReturn(batch);
        when(batch.next()).thenReturn(guard(1, 1, 4, 7));
        RecordReader<KeyValue> broken = mock(RecordReader.class);
        when(broken.readBatch()).thenThrow(new IOException("guard read"));
        RecordReader<KeyValue> unopened = mock(RecordReader.class);
        assertThatThrownBy(
                        () ->
                                new DeleteOrderingGuardReader(
                                        Arrays.asList(first, broken, unopened), KEYS))
                .isInstanceOf(IOException.class)
                .hasMessage("guard read");
        verify(batch).releaseBatch();
        verify(first).close();
        verify(broken).close();
        verify(unopened).close();
    }

    private List<KeyValue> merge(
            Options options,
            boolean spill,
            List<List<KeyValue>> segments,
            List<DeleteOrderingGuard> captured,
            List<List<KeyValue>> runs)
            throws Exception {
        CoreOptions core = new CoreOptions(options);
        MergeFunctionFactory<KeyValue> factory =
                core.mergeEngine() == CoreOptions.MergeEngine.PARTIAL_UPDATE
                        ? PartialUpdateMergeFunction.factory(
                                options, VALUE_TYPE, Collections.singletonList("id"))
                        : DeduplicateMergeFunction.factory(options);
        List<RecordReader<KeyValue>> guardReaders = new ArrayList<>();
        segments.forEach(
                segment -> guardReaders.add(new IteratorRecordReader<>(segment.iterator())));
        try (IOManager io = IOManager.create(tempDir.toString());
                DeleteOrderingGuardReader guards =
                        new DeleteOrderingGuardReader(guardReaders, KEYS)) {
            MergeFunctionWrapper<KeyValue> wrapper =
                    guards.wrap(
                            new DeleteOrderingGuardMergeFunctionWrapper<>(
                                    new ReducerMergeFunctionWrapper(factory.create()),
                                    new DeleteOrderingGuardSerializer(
                                            0, KEY_TYPE, VALUE_TYPE, core),
                                    captured::add),
                            factory,
                            null,
                            UserDefinedSeqComparator.create(VALUE_TYPE, core));
            List<SizedReaderSupplier<KeyValue>> inputs = new ArrayList<>();
            for (List<KeyValue> run : runs) {
                inputs.add(
                        new SizedReaderSupplier<KeyValue>() {
                            @Override
                            public long estimateSize() {
                                return 100;
                            }

                            @Override
                            public RecordReader<KeyValue> get() {
                                return new IteratorRecordReader<>(run.iterator());
                            }
                        });
            }
            MergeSorter sorter = new MergeSorter(core, KEY_TYPE, VALUE_TYPE, spill ? io : null);
            List<KeyValue> result = new ArrayList<>();
            try (RecordReader<KeyValue> reader =
                    new DropDeleteReader(
                            sorter.mergeSort(
                                    inputs,
                                    KEYS,
                                    UserDefinedSeqComparator.create(VALUE_TYPE, core),
                                    wrapper))) {
                reader.forEachRemaining(
                        kv ->
                                result.add(
                                        kv.copy(
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

    private static Options partialOptions(SortEngine engine) {
        Options options = options(engine);
        options.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.PARTIAL_UPDATE);
        options.setString("fields.seqA.sequence-group", "a");
        options.setString("fields.seqB.sequence-group", "b");
        return options;
    }

    private static KeyValue guard(int key, long internal, Integer a, Integer b) {
        return new KeyValue()
                .replace(
                        GenericRow.of(key),
                        internal,
                        RowKind.DELETE,
                        GenericRow.of(null, a, null, b, null, null))
                .setLevel(2);
    }

    private static KeyValue row(int key, long internal, Integer a, Integer b, String payload) {
        BinaryString text = BinaryString.fromString(payload);
        return new KeyValue()
                .replace(
                        GenericRow.of(key),
                        internal,
                        RowKind.INSERT,
                        GenericRow.of(key, a, text, b, text, text))
                .setLevel(0);
    }
}
