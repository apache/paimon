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

package org.apache.paimon.operation.commit;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.globalindex.CentroidIndexMeta;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.ThrowingConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.manifest.FileKind.ADD;
import static org.apache.paimon.manifest.FileKind.DELETE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Whole conflict-check comparisons; successful streaming checks must not collect legacy bases. */
class StreamingDataEvolutionConflictTest {

    @TempDir Path temporary;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCentroidSourceChangesMatchOrdinaryCheck(boolean indexedColumn) throws Exception {
        Fixture fixture = new Fixture(list(file("base", ADD, 0L, 10, 0, -1)), 10, false);
        TableSchema schema =
                TableSchema.create(
                        0,
                        Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.INT())
                                .build());
        when(fixture.scanner.schema(0)).thenReturn(schema);
        DataFileMeta update =
                file("updated", ADD, 0L, 10, 0, -1)
                        .file()
                        .withWriteCols(Collections.singletonList(indexedColumn ? "a" : "b"));
        when(fixture.scanner.readIncrementalEntries(
                        fixture.snapshot, Collections.singletonList(EMPTY_ROW)))
                .thenReturn(
                        Collections.singletonList(
                                ManifestEntry.create(ADD, EMPTY_ROW, 0, -1, update)));
        byte[] meta =
                CentroidIndexMeta.router(new ResultEntry("model", 0, null), "model", 2).meta();
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "ivf-pq",
                        "router",
                        100,
                        0,
                        null,
                        null,
                        new GlobalIndexMeta(
                                0,
                                9,
                                0,
                                null,
                                meta,
                                new DataEvolutionIndexSourceMeta(1).serialize()));
        List<IndexManifestEntry> changes =
                Collections.singletonList(new IndexManifestEntry(ADD, EMPTY_ROW, 0, indexFile));
        Optional<RuntimeException> ordinary =
                fixture.detection.checkConflicts(
                        fixture.snapshot,
                        fixture.base,
                        Collections.emptyList(),
                        changes,
                        null,
                        CommitKind.APPEND);
        assertThat(ordinary.isPresent()).isEqualTo(indexedColumn);
        try (IOManager io = IOManager.create(temporary.toString())) {
            Optional<RuntimeException> streaming =
                    fixture.detection.checkConflictsStreaming(
                            options(),
                            io,
                            fixture.snapshot,
                            Collections.singletonList(EMPTY_ROW),
                            Collections.emptyList(),
                            changes,
                            null,
                            CommitKind.APPEND);
            assertThat(streaming.isPresent()).isEqualTo(indexedColumn);
            if (indexedColumn) {
                assertThat(streaming.get()).hasMessage(ordinary.get().getMessage());
                assertThat(streaming.get()).hasMessageContaining("source rows changed");
            }
            fixture.assertNoLegacyBaseCollection();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void testMatchesOriginal(
            String name,
            List<ManifestEntry> base,
            List<ManifestEntry> delta,
            CommitKind kind,
            long nextRowId,
            boolean checkRanges,
            boolean valid)
            throws Exception {
        Fixture fixture = new Fixture(base, nextRowId, checkRanges);
        Outcome expected = fixture.original(delta, kind);
        assertThat(expected.error == null).as(name).isEqualTo(valid);
        try (IOManager io = IOManager.create(temporary.toString())) {
            Outcome actual =
                    outcome(
                            () ->
                                    fixture.detection.checkConflictsStreaming(
                                            options(),
                                            io,
                                            fixture.snapshot,
                                            Collections.singletonList(EMPTY_ROW),
                                            delta,
                                            Collections.emptyList(),
                                            null,
                                            kind));
            assertThat(actual.thrown).as(name).isEqualTo(expected.thrown);
            assertThat(actual.error == null).as(name).isEqualTo(valid);
            if (!valid) {
                assertThat(actual.error.getClass()).as(name).isEqualTo(expected.error.getClass());
                assertThat(actual.error.getMessage())
                        .as(name)
                        .isEqualTo(expected.error.getMessage());
            } else {
                fixture.assertNoLegacyBaseCollection();
            }
        }
        try (Stream<Path> files = Files.walk(temporary)) {
            assertThat(files.filter(Files::isRegularFile).count()).isZero();
        }
    }

    @Test
    void testSuccessfulUpdateActuallySpillsWithoutLegacyCollection() throws Exception {
        List<ManifestEntry> base = new ArrayList<>();
        List<ManifestEntry> delta = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            base.add(file("base-" + i, ADD, i * 2L, 2, 0, -1));
            delta.add(file("update-" + i, ADD, i * 2L, 2, 0, -1));
        }
        Fixture fixture = new Fixture(base, 20000, true);
        try (ObservedIO io = new ObservedIO(temporary.toString())) {
            assertThat(
                            fixture.detection.checkConflictsStreaming(
                                    options(),
                                    io,
                                    fixture.snapshot,
                                    Collections.singletonList(EMPTY_ROW),
                                    delta,
                                    Collections.emptyList(),
                                    null,
                                    CommitKind.APPEND))
                    .isEmpty();
            assertThat(io.spills.get()).isPositive();
            fixture.assertNoLegacyBaseCollection();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testGlobalIndexUsesBaseDedicatedFilesAndBucketScope(boolean wrongBucket) throws Exception {
        Fixture fixture = new Fixture(list(file("base.blob", ADD, 0L, 10, 0, -1)), 10, false);
        GlobalIndexMeta global = mock(GlobalIndexMeta.class);
        when(global.rowRange()).thenReturn(new Range(0, 9));
        IndexFileMeta indexFile = mock(IndexFileMeta.class);
        when(indexFile.globalIndexMeta()).thenReturn(global);
        when(indexFile.fileName()).thenReturn("index");
        IndexManifestEntry index = mock(IndexManifestEntry.class);
        when(index.kind()).thenReturn(ADD);
        when(index.partition()).thenReturn(EMPTY_ROW);
        when(index.bucket()).thenReturn(wrongBucket ? 1 : 0);
        when(index.indexFile()).thenReturn(indexFile);
        List<IndexManifestEntry> changes = Collections.singletonList(index);
        Outcome expected =
                outcome(
                        () ->
                                fixture.detection.checkConflicts(
                                        fixture.snapshot,
                                        fixture.base,
                                        Collections.emptyList(),
                                        changes,
                                        null,
                                        CommitKind.APPEND));
        try (IOManager io = IOManager.create(temporary.toString())) {
            Outcome actual =
                    outcome(
                            () ->
                                    fixture.detection.checkConflictsStreaming(
                                            options(),
                                            io,
                                            fixture.snapshot,
                                            Collections.singletonList(EMPTY_ROW),
                                            Collections.emptyList(),
                                            changes,
                                            null,
                                            CommitKind.APPEND));
            assertThat(actual.thrown).isEqualTo(expected.thrown);
            assertThat(actual.error == null).isEqualTo(!wrongBucket);
            if (wrongBucket) {
                assertThat(actual.error.getClass()).isEqualTo(expected.error.getClass());
                assertThat(actual.error.getMessage()).isEqualTo(expected.error.getMessage());
            } else {
                fixture.assertNoLegacyBaseCollection();
            }
        }
    }

    @Test
    void testHistoricalFieldConflictStillRunsAfterStreamingChecks() throws Exception {
        Fixture fixture = new Fixture(list(file("f", ADD, 0L, 10, 0, -1)), 10, true);
        List<ManifestEntry> delta = list(file("g", ADD, 0L, 10, 0, -1));
        when(fixture.scanner.readIncrementalEntries(any(), anyList()))
                .thenReturn(list(file("concurrent", ADD, 0L, 10, 0, -1)));
        RowIdConflictChecker checker = mock(RowIdConflictChecker.class);
        when(checker.isEmpty()).thenReturn(false);
        when(checker.conflictsWith(any())).thenReturn(true);
        Outcome expected =
                outcome(
                        () ->
                                fixture.detection.checkConflicts(
                                        fixture.snapshot,
                                        fixture.base,
                                        SimpleFileEntry.from(delta),
                                        Collections.emptyList(),
                                        checker,
                                        CommitKind.APPEND));
        assertThat(expected.error).isNotNull();
        try (IOManager io = IOManager.create(temporary.toString())) {
            Outcome actual =
                    outcome(
                            () ->
                                    fixture.detection.checkConflictsStreaming(
                                            options(),
                                            io,
                                            fixture.snapshot,
                                            Collections.singletonList(EMPTY_ROW),
                                            delta,
                                            Collections.emptyList(),
                                            checker,
                                            CommitKind.APPEND));
            assertThat(actual.thrown).isEqualTo(expected.thrown);
            assertThat(actual.error).isNotNull();
            assertThat(actual.error.getClass()).isEqualTo(expected.error.getClass());
            assertThat(actual.error.getMessage()).isEqualTo(expected.error.getMessage());
            fixture.assertNoLegacyBaseCollection();
        }
    }

    private static Stream<Arguments> cases() {
        ManifestEntry f = file("f", ADD, 0L, 10, 0, -1);
        ManifestEntry z = file("z", ADD, 10L, 10, 0, -1);
        ManifestEntry deleteF = file("f", DELETE, 0L, 10, 0, -1);
        ManifestEntry deleteZ = file("z", DELETE, 10L, 10, 0, -1);
        ManifestEntry g = file("g", ADD, 0L, 10, 0, -1);
        return Stream.of(
                example("partial update", list(f), list(g), CommitKind.APPEND, 10, true, true),
                example(
                        "new IDs",
                        list(f),
                        list(file("g", ADD, 10L, 10, 0, -1)),
                        CommitKind.APPEND,
                        10,
                        true,
                        true),
                example(
                        "ordinary subrange",
                        list(f),
                        list(file("g", ADD, 2L, 4, 0, -1)),
                        CommitKind.APPEND,
                        10,
                        true,
                        false),
                example(
                        "stale DELETE",
                        list(file("f", ADD, 100L, 10, 0, -1)),
                        list(deleteF),
                        CommitKind.APPEND,
                        110,
                        true,
                        false),
                example(
                        "different overlapping ranges",
                        list(f),
                        list(file("g", ADD, 0L, 20, 0, -1)),
                        CommitKind.APPEND,
                        0,
                        true,
                        false),
                example(
                        "dedicated containment",
                        list(f),
                        list(file("g.blob", ADD, 2L, 4, 0, -1)),
                        CommitKind.APPEND,
                        10,
                        true,
                        true),
                example(
                        "dedicated spans adjacent ranges",
                        list(f, z),
                        list(file("g.blob", ADD, 0L, 20, 0, -1)),
                        CommitKind.APPEND,
                        0,
                        true,
                        false),
                example(
                        "compact adjacent coverage",
                        list(f, z),
                        list(deleteF, deleteZ, file("g", ADD, 0L, 20, 0, -1)),
                        CommitKind.COMPACT,
                        20,
                        true,
                        true),
                example(
                        "compact bucket scope",
                        list(f),
                        list(deleteF, file("g", ADD, 0L, 10, 1, -1)),
                        CommitKind.COMPACT,
                        10,
                        true,
                        false),
                example(
                        "union exact",
                        list(f, file("z", ADD, 5L, 10, 0, -1)),
                        list(file("g", ADD, 0L, 15, 0, -1)),
                        CommitKind.APPEND,
                        15,
                        false,
                        true),
                example(
                        "union subrange rejected",
                        list(f, file("z", ADD, 5L, 10, 0, -1)),
                        list(g),
                        CommitKind.APPEND,
                        15,
                        false,
                        false),
                example(
                        "delta duplicate throws",
                        list(f),
                        list(g, g),
                        CommitKind.APPEND,
                        10,
                        true,
                        false),
                example(
                        "bucket conflict precedes delta duplicate",
                        list(file("f", ADD, 0L, 10, 0, 1)),
                        list(file("g", ADD, 0L, 10, 0, 2), file("g", ADD, 0L, 10, 0, 2)),
                        CommitKind.APPEND,
                        10,
                        true,
                        false),
                example(
                        "unassigned new IDs",
                        Collections.emptyList(),
                        list(file("g", ADD, null, 10, 0, -1)),
                        CommitKind.APPEND,
                        0,
                        true,
                        true),
                example(
                        "64 bit row IDs",
                        list(file("f", ADD, 1L << 40, 10, 0, -1)),
                        list(file("g", ADD, 1L << 40, 10, 0, -1)),
                        CommitKind.APPEND,
                        (1L << 40) + 10,
                        true,
                        true));
    }

    private static Arguments example(
            String name,
            List<ManifestEntry> base,
            List<ManifestEntry> delta,
            CommitKind kind,
            long next,
            boolean ranges,
            boolean valid) {
        return Arguments.of(name, base, delta, kind, next, ranges, valid);
    }

    private static List<ManifestEntry> list(ManifestEntry... entries) {
        return Arrays.asList(entries);
    }

    private static ManifestEntry file(
            String name, FileKind kind, Long first, long count, int bucket, int totalBuckets) {
        return ManifestEntry.create(
                kind,
                EMPTY_ROW,
                bucket,
                totalBuckets,
                DataFileMeta.forAppend(
                        name,
                        100,
                        count,
                        SimpleStats.EMPTY_STATS,
                        0,
                        0,
                        0,
                        Collections.emptyList(),
                        null,
                        FileSource.APPEND,
                        Collections.singletonList("a"),
                        null,
                        first,
                        Collections.singletonList("a")));
    }

    private static CoreOptions options() {
        Options options = new Options();
        options.set(CoreOptions.SORT_SPILL_BUFFER_SIZE, MemorySize.ofKibiBytes(256));
        options.set(CoreOptions.PAGE_SIZE, MemorySize.ofKibiBytes(4));
        return new CoreOptions(options);
    }

    private static final class Fixture {
        private final CommitScanner scanner = mock(CommitScanner.class);
        private final Snapshot snapshot = mock(Snapshot.class);
        private final DataEvolutionConflictDetection detection;
        private final List<SimpleFileEntry> base;

        private Fixture(List<ManifestEntry> entries, long next, boolean ranges) throws Exception {
            base = SimpleFileEntry.from(entries);
            when(snapshot.nextRowId()).thenReturn(next);
            when(snapshot.commitUser()).thenReturn("base-writer");
            when(snapshot.id()).thenReturn(2L);
            when(snapshot.commitKind()).thenReturn(CommitKind.APPEND);
            SnapshotManager snapshots = mock(SnapshotManager.class);
            Snapshot start = mock(Snapshot.class);
            when(start.nextRowId()).thenReturn(next);
            when(snapshots.snapshot(1L)).thenReturn(start);
            when(snapshots.snapshot(2L)).thenReturn(snapshot);
            FileStorePathFactory paths = mock(FileStorePathFactory.class);
            when(paths.getPartitionString(any())).thenReturn("");
            detection =
                    new DataEvolutionConflictDetection(
                            "table",
                            "writer",
                            RowType.of(),
                            paths,
                            BucketMode.BUCKET_UNAWARE,
                            false,
                            false,
                            null,
                            snapshots,
                            scanner);
            if (ranges) {
                detection.setRowIdCheckFromSnapshot(1L);
            }
            when(scanner.readAllEntriesFromChangedRowRanges(any(), anyList(), anyList()))
                    .thenReturn(base);
            when(scanner.readAllEntriesFromDataFiles(any(), anyList(), anySet()))
                    .thenReturn(Collections.emptyList());
            doAnswer(
                            invocation -> {
                                ThrowingConsumer<SimpleFileEntry, Exception> consumer =
                                        invocation.getArgument(3);
                                for (SimpleFileEntry entry : base) {
                                    consumer.accept(entry);
                                }
                                return null;
                            })
                    .when(scanner)
                    .forEachEntryFromChangedRowRanges(any(), anyList(), anyList(), any());
        }

        private Outcome original(List<ManifestEntry> delta, CommitKind kind) {
            return outcome(
                    () ->
                            detection.checkConflicts(
                                    snapshot,
                                    base,
                                    SimpleFileEntry.from(delta),
                                    Collections.emptyList(),
                                    null,
                                    kind));
        }

        private void assertNoLegacyBaseCollection() {
            verify(scanner, never())
                    .readAllEntriesFromChangedRowRanges(any(), anyList(), anyList());
            verify(scanner, never()).readAllEntriesFromDataFiles(any(), anyList(), anySet());
            verify(scanner, never()).readAllEntriesFromChangedPartitions(any(), anyList());
        }
    }

    private static Outcome outcome(Supplier<Optional<RuntimeException>> supplier) {
        try {
            return new Outcome(supplier.get().orElse(null), false);
        } catch (RuntimeException failure) {
            return new Outcome(failure, true);
        }
    }

    private static final class Outcome {
        private final RuntimeException error;
        private final boolean thrown;

        private Outcome(RuntimeException error, boolean thrown) {
            this.error = error;
            this.thrown = thrown;
        }
    }

    private static final class ObservedIO extends IOManagerImpl {
        private final AtomicInteger spills = new AtomicInteger();

        private ObservedIO(String directory) {
            super(directory);
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channel)
                throws IOException {
            spills.incrementAndGet();
            return super.createBufferFileWriter(channel);
        }
    }
}
