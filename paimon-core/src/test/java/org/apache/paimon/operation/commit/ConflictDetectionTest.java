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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.manifest.SimpleFileEntryWithDV;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.SchemaEvolutionTableTestBase.TestingSchemaManager;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.manifest.FileKind.ADD;
import static org.apache.paimon.manifest.FileKind.DELETE;
import static org.apache.paimon.operation.commit.ConflictDetection.buildBaseEntriesWithDV;
import static org.apache.paimon.operation.commit.ConflictDetection.buildDeltaEntriesWithDV;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConflictDetectionTest {

    @Test
    public void testBuildBaseEntriesWithDV() {
        {
            // Scene 1
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntry("f1", ADD));
            baseEntries.add(createFileEntry("f2", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", ADD, Arrays.asList("f2")));

            assertThat(buildBaseEntriesWithDV(baseEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", ADD, null),
                            createFileEntryWithDV("f2", ADD, "dv1"));
        }

        {
            // Scene 2: skip delete dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntry("f1", ADD));
            baseEntries.add(createFileEntry("f2", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f2")));

            assertThat(buildBaseEntriesWithDV(baseEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", ADD, null),
                            createFileEntryWithDV("f2", ADD, null));
        }
    }

    @Test
    void testSimpleFileEntryFromManifestEntryPreservesSchemaAndWriteCols() {
        ManifestEntry manifestEntry =
                ManifestEntry.create(
                        ADD,
                        EMPTY_ROW,
                        0,
                        1,
                        createDataFileMeta("f1", 0L, 1L, 1L, Arrays.asList("b")));

        SimpleFileEntry entry = SimpleFileEntry.from(manifestEntry);
        assertThat(entry.schemaId()).isEqualTo(1L);
        assertThat(entry.writeCols()).containsExactly("b");

        SimpleFileEntry delete = entry.toDelete();
        assertThat(delete.schemaId()).isEqualTo(1L);
        assertThat(delete.writeCols()).containsExactly("b");
    }

    @Test
    void testRowIdConflictAllowsDisjointWriteColumns() {
        Optional<RuntimeException> conflict =
                checkRowIdConflict(Arrays.asList("b"), 0L, Arrays.asList("c"), 0L);

        assertThat(conflict).isEmpty();
    }

    @Test
    void testRowIdConflictDetectsSameWriteColumns() {
        Optional<RuntimeException> conflict =
                checkRowIdConflict(Arrays.asList("b"), 0L, Arrays.asList("b"), 0L);

        assertThat(conflict).isPresent();
        assertThat(conflict.get().getMessage())
                .contains("multiple 'MERGE INTO' operations have encountered conflicts");
    }

    @Test
    void testRowIdConflictRequiresWriteColumns() {
        assertThatThrownBy(() -> checkRowIdConflict(null, 0L, Arrays.asList("b"), 0L))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Write columns of row-id file");
    }

    @Test
    void testRowIdConflictUsesFieldIdAcrossRename() {
        Optional<RuntimeException> conflict =
                checkRowIdConflict(Arrays.asList("b_renamed"), 1L, Arrays.asList("b"), 0L);

        assertThat(conflict).isPresent();
    }

    @Test
    void testRowIdConflictIndexMergesOverlappedDeltaRanges() {
        RowIdColumnConflictChecker checker =
                RowIdColumnConflictChecker.fromDeltaEntries(
                        createSchemaManager(),
                        Arrays.asList(
                                createFileEntry("current-b", ADD, 0L, 11L, 0L, Arrays.asList("b")),
                                createFileEntry(
                                        "current-c", ADD, 5L, 11L, 0L, Arrays.asList("c"))));

        ManifestEntry historicalB =
                ManifestEntry.create(
                        ADD,
                        EMPTY_ROW,
                        0,
                        1,
                        createDataFileMeta("historical-b", 12L, 1L, 0L, Arrays.asList("b")));
        ManifestEntry historicalC =
                ManifestEntry.create(
                        ADD,
                        EMPTY_ROW,
                        0,
                        1,
                        createDataFileMeta("historical-c", 12L, 1L, 0L, Arrays.asList("c")));

        assertThat(checker.conflictsWith(historicalB)).isTrue();
        assertThat(checker.conflictsWith(historicalC)).isTrue();
    }

    @Test
    void testRowIdConflictIndexScansAllOverlappedRanges() {
        RowIdColumnConflictChecker checker =
                RowIdColumnConflictChecker.fromDeltaEntries(
                        createSchemaManager(),
                        Arrays.asList(
                                createFileEntry("current-b", ADD, 0L, 5L, 0L, Arrays.asList("b")),
                                createFileEntry(
                                        "current-c", ADD, 10L, 5L, 0L, Arrays.asList("c"))));

        ManifestEntry historical =
                ManifestEntry.create(
                        ADD,
                        EMPTY_ROW,
                        0,
                        1,
                        createDataFileMeta("historical", 3L, 10L, 0L, Arrays.asList("c")));

        assertThat(checker.conflictsWith(historical)).isTrue();
    }

    @Test
    public void testBuildDeltaEntriesWithDV() {
        {
            // Scene 1: update f2's dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f2", ADD, null));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f2_new", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f2_new")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f2", DELETE, null),
                            createFileEntryWithDV("f2_new", ADD, "dv2"));
        }

        {
            // Scene 2: update f2 and merge f1's dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f2", ADD, null));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f2_new", ADD));
            deltaEntries.add(createFileEntry("f3", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f1")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f1", "f2_new")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv1"),
                            createFileEntryWithDV("f1", ADD, "dv2"),
                            createFileEntryWithDV("f2", DELETE, null),
                            createFileEntryWithDV("f2_new", ADD, "dv2"),
                            createFileEntryWithDV("f3", ADD, null));
        }

        {
            // Scene 3: update f2 (with dv) and merge f1's dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f2", ADD, "dv2"));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f2_new", ADD));
            deltaEntries.add(createFileEntry("f3", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f1")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", DELETE, Arrays.asList("f2")));
            deltaIndexEntries.add(createDvIndexEntry("dv3", ADD, Arrays.asList("f1", "f2_new")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv1"),
                            createFileEntryWithDV("f1", ADD, "dv3"),
                            createFileEntryWithDV("f2", DELETE, "dv2"),
                            createFileEntryWithDV("f2_new", ADD, "dv3"),
                            createFileEntryWithDV("f3", ADD, null));
        }

        {
            // Scene 4: full compact
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, null));
            baseEntries.add(createFileEntryWithDV("f2", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f3", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f4", ADD, "dv2"));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f1", DELETE));
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f3", DELETE));
            deltaEntries.add(createFileEntry("f4", DELETE));
            deltaEntries.add(createFileEntry("f5_compact", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f2", "f3")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", DELETE, Arrays.asList("f4")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, null),
                            createFileEntryWithDV("f2", DELETE, "dv1"),
                            createFileEntryWithDV("f3", DELETE, "dv1"),
                            createFileEntryWithDV("f4", DELETE, "dv2"),
                            createFileEntryWithDV("f5_compact", ADD, null));
        }

        {
            // Scene 5: merge into with update, delete and insert
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, null));
            baseEntries.add(createFileEntryWithDV("f2", ADD, null));
            baseEntries.add(createFileEntryWithDV("f3", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f4", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f5", ADD, "dv2"));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f3", DELETE));
            deltaEntries.add(createFileEntry("f3_new", ADD));
            deltaEntries.add(createFileEntry("f7", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f3", "f4")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", DELETE, Arrays.asList("f5")));
            deltaIndexEntries.add(createDvIndexEntry("dv3", ADD, Arrays.asList("f1", "f4", "f5")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, null),
                            createFileEntryWithDV("f1", ADD, "dv3"),
                            createFileEntryWithDV("f2", DELETE, null),
                            createFileEntryWithDV("f3", DELETE, "dv1"),
                            createFileEntryWithDV("f3_new", ADD, null),
                            createFileEntryWithDV("f4", DELETE, "dv1"),
                            createFileEntryWithDV("f4", ADD, "dv3"),
                            createFileEntryWithDV("f5", DELETE, "dv2"),
                            createFileEntryWithDV("f5", ADD, "dv3"),
                            createFileEntryWithDV("f7", ADD, null));
        }
    }

    @Test
    public void testConflictDeletionWithDV() {
        {
            // Scene 1: base -------------> update2 (conflict)
            //           f1          ^         <f1, +dv2>
            //                       |
            //                  update1 (finished)
            //                    <f1, +dv1>
            List<SimpleFileEntry> update1Entries = new ArrayList<>();
            update1Entries.add(createFileEntryWithDV("f1", ADD, "dv1"));

            List<SimpleFileEntry> update2DeltaEntries = new ArrayList<>();

            List<IndexManifestEntry> update2DeltaIndexEntries = new ArrayList<>();
            update2DeltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f1")));

            List<SimpleFileEntry> update2DeltaEntriesWithDV =
                    buildDeltaEntriesWithDV(
                            update1Entries, update2DeltaEntries, update2DeltaIndexEntries);
            assertThat(update2DeltaEntriesWithDV)
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, null),
                            createFileEntryWithDV("f1", ADD, "dv2"));
            assertConflict(update1Entries, update2DeltaEntriesWithDV);
        }

        {
            // Scene 2: base -------------> update2 (conflict)
            //         <f1, dv0>     ^        <f1, +dv2>
            //                       |
            //                  update1 (finished)
            //                    <f1, +dv1>
            List<SimpleFileEntry> update1Entries = new ArrayList<>();
            update1Entries.add(createFileEntryWithDV("f1", ADD, "dv1"));

            List<SimpleFileEntry> update2DeltaEntries = new ArrayList<>();

            List<IndexManifestEntry> update2DeltaIndexEntries = new ArrayList<>();
            update2DeltaIndexEntries.add(createDvIndexEntry("dv0", DELETE, Arrays.asList("f1")));
            update2DeltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f1")));

            List<SimpleFileEntry> update2DeltaEntriesWithDV =
                    buildDeltaEntriesWithDV(
                            update1Entries, update2DeltaEntries, update2DeltaIndexEntries);
            assertThat(update2DeltaEntriesWithDV)
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv0"),
                            createFileEntryWithDV("f1", ADD, "dv2"));
            assertConflict(update1Entries, update2DeltaEntriesWithDV);
        }

        {
            // Scene 3: base -------------> update2 (conflict)
            //         <f1, dv0>      ^     <-f1, -dv0>, <+f3, null>
            //                        |
            //                  update1 (finished)
            //                 <-f1, -dv0>, <+f2, dv1>
            List<SimpleFileEntry> update1Entries = new ArrayList<>();
            update1Entries.add(createFileEntryWithDV("f2", ADD, "dv1"));

            List<SimpleFileEntry> update2DeltaEntries = new ArrayList<>();
            update2DeltaEntries.add(createFileEntry("f1", DELETE));
            update2DeltaEntries.add(createFileEntry("f3", ADD));

            List<IndexManifestEntry> update2DeltaIndexEntries = new ArrayList<>();
            update2DeltaIndexEntries.add(createDvIndexEntry("dv0", DELETE, Arrays.asList("f1")));

            List<SimpleFileEntry> update2DeltaEntriesWithDV =
                    buildDeltaEntriesWithDV(
                            update1Entries, update2DeltaEntries, update2DeltaIndexEntries);
            assertThat(update2DeltaEntriesWithDV)
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv0"),
                            createFileEntryWithDV("f3", ADD, null));
            assertConflict(update1Entries, update2DeltaEntriesWithDV);
        }
    }

    private SimpleFileEntry createFileEntry(String fileName, FileKind kind) {
        return new SimpleFileEntry(
                kind,
                EMPTY_ROW,
                0,
                1,
                0,
                fileName,
                Collections.emptyList(),
                null,
                EMPTY_ROW,
                EMPTY_ROW,
                null,
                0L,
                null,
                0L,
                null);
    }

    private SimpleFileEntryWithDV createFileEntryWithDV(
            String fileName, FileKind kind, @Nullable String dvFileName) {
        return new SimpleFileEntryWithDV(createFileEntry(fileName, kind), dvFileName);
    }

    private IndexManifestEntry createDvIndexEntry(
            String fileName, FileKind kind, List<String> fileNames) {
        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        for (String name : fileNames) {
            dvRanges.put(name, new DeletionVectorMeta(name, 1, 1, 1L));
        }
        return new IndexManifestEntry(
                kind,
                EMPTY_ROW,
                0,
                new IndexFileMeta(
                        DELETION_VECTORS_INDEX, fileName, 11, dvRanges.size(), dvRanges, null));
    }

    private void assertConflict(
            List<SimpleFileEntry> baseEntries, List<SimpleFileEntry> deltaEntries) {
        ArrayList<SimpleFileEntry> simpleFileEntryWithDVS = new ArrayList<>(baseEntries);
        simpleFileEntryWithDVS.addAll(deltaEntries);
        Collection<SimpleFileEntry> merged = FileEntry.mergeEntries(simpleFileEntryWithDVS);
        int deleteCount = 0;
        for (SimpleFileEntry simpleFileEntryWithDV : merged) {
            if (simpleFileEntryWithDV.kind().equals(FileKind.DELETE)) {
                deleteCount++;
            }
        }
        assert (deleteCount > 0);
    }

    @Test
    void testShouldBeOverwriteCommit() {
        ConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> addOnlyEntries = new ArrayList<>();
        addOnlyEntries.add(createFileEntry("f1", ADD));
        addOnlyEntries.add(createFileEntry("f2", ADD));
        assertThat(detection.shouldBeOverwriteCommit(addOnlyEntries, Collections.emptyList()))
                .isFalse();

        assertThat(
                        detection.shouldBeOverwriteCommit(
                                Collections.emptyList(), Collections.emptyList()))
                .isFalse();

        List<SimpleFileEntry> deleteEntries = new ArrayList<>();
        deleteEntries.add(createFileEntry("f1", DELETE));
        deleteEntries.add(createFileEntry("f2", ADD));
        assertThat(detection.shouldBeOverwriteCommit(deleteEntries, Collections.emptyList()))
                .isTrue();

        List<IndexManifestEntry> dvIndexFiles = new ArrayList<>();
        dvIndexFiles.add(createDvIndexEntry("dv1", ADD, Arrays.asList("f1")));
        assertThat(detection.shouldBeOverwriteCommit(Collections.emptyList(), dvIndexFiles))
                .isTrue();

        detection.setRowIdCheckFromSnapshot(1L);
        assertThat(detection.shouldBeOverwriteCommit(addOnlyEntries, Collections.emptyList()))
                .isFalse();
    }

    private ConflictDetection createConflictDetection() {
        return new ConflictDetection(
                "test-table",
                "test-user",
                RowType.of(),
                createSchemaManager(),
                null,
                null,
                BucketMode.HASH_FIXED,
                false,
                true,
                false,
                null,
                null,
                null);
    }

    private Optional<RuntimeException> checkRowIdConflict(
            @Nullable List<String> currentWriteCols,
            long currentSchemaId,
            @Nullable List<String> historicalWriteCols,
            long historicalSchemaId) {
        Snapshot baseSnapshot = snapshot(1L, CommitKind.APPEND, 10L);
        Snapshot latestSnapshot = snapshot(2L, CommitKind.APPEND, 10L);

        ManifestEntry historicalEntry =
                ManifestEntry.create(
                        ADD,
                        EMPTY_ROW,
                        0,
                        1,
                        createDataFileMeta(
                                "historical", 0L, 10L, historicalSchemaId, historicalWriteCols));

        SnapshotManager snapshotManager = new TestingSnapshotManager(baseSnapshot, latestSnapshot);
        CommitScanner commitScanner =
                new TestingCommitScanner(
                        latestSnapshot.id(), Collections.singletonList(historicalEntry));

        ConflictDetection detection =
                new ConflictDetection(
                        "test-table",
                        "test-user",
                        RowType.of(),
                        createSchemaManager(),
                        null,
                        null,
                        BucketMode.HASH_FIXED,
                        false,
                        true,
                        false,
                        null,
                        snapshotManager,
                        commitScanner);
        detection.setRowIdCheckFromSnapshot(1L);

        return detection.checkConflicts(
                latestSnapshot,
                Collections.emptyList(),
                Collections.singletonList(
                        createFileEntry(
                                "current", ADD, 0L, 10L, currentSchemaId, currentWriteCols)),
                Collections.emptyList(),
                CommitKind.APPEND);
    }

    private SimpleFileEntry createFileEntry(
            String fileName,
            FileKind kind,
            @Nullable Long firstRowId,
            long rowCount,
            long schemaId,
            @Nullable List<String> writeCols) {
        return new SimpleFileEntry(
                kind,
                EMPTY_ROW,
                0,
                1,
                0,
                fileName,
                Collections.emptyList(),
                null,
                EMPTY_ROW,
                EMPTY_ROW,
                null,
                rowCount,
                firstRowId,
                schemaId,
                writeCols);
    }

    private DataFileMeta createDataFileMeta(
            String fileName,
            @Nullable Long firstRowId,
            long rowCount,
            long schemaId,
            @Nullable List<String> writeCols) {
        return DataFileMeta.create(
                fileName,
                0L,
                rowCount,
                EMPTY_ROW,
                EMPTY_ROW,
                EMPTY_STATS,
                EMPTY_STATS,
                0L,
                0L,
                schemaId,
                0,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                firstRowId,
                writeCols);
    }

    private SchemaManager createSchemaManager() {
        Map<Long, org.apache.paimon.schema.TableSchema> schemas = new HashMap<>();
        schemas.put(
                0L,
                org.apache.paimon.schema.TableSchema.create(
                        0L,
                        new Schema(
                                Arrays.asList(
                                        new DataField(0, "id", DataTypes.INT()),
                                        new DataField(1, "b", DataTypes.INT()),
                                        new DataField(2, "c", DataTypes.INT())),
                                Collections.emptyList(),
                                Collections.singletonList("id"),
                                Collections.emptyMap(),
                                "")));
        schemas.put(
                1L,
                org.apache.paimon.schema.TableSchema.create(
                        1L,
                        new Schema(
                                Arrays.asList(
                                        new DataField(0, "id", DataTypes.INT()),
                                        new DataField(1, "b_renamed", DataTypes.INT()),
                                        new DataField(2, "c", DataTypes.INT())),
                                Collections.emptyList(),
                                Collections.singletonList("id"),
                                Collections.emptyMap(),
                                "")));
        return new TestingSchemaManager(new Path("/tmp/conflict-detection-test"), schemas);
    }

    private Snapshot snapshot(long id, CommitKind commitKind, @Nullable Long nextRowId) {
        return new Snapshot(
                id,
                0L,
                "base",
                0L,
                "delta",
                0L,
                null,
                null,
                null,
                "user",
                id,
                commitKind,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                nextRowId);
    }

    private static class TestingSnapshotManager extends SnapshotManager {

        private final Map<Long, Snapshot> snapshots = new HashMap<>();

        private TestingSnapshotManager(Snapshot... snapshots) {
            super(
                    LocalFileIO.create(),
                    new Path("/tmp/conflict-detection-snapshot-test"),
                    null,
                    null,
                    null);
            for (Snapshot snapshot : snapshots) {
                this.snapshots.put(snapshot.id(), snapshot);
            }
        }

        @Override
        public Snapshot snapshot(long snapshotId) {
            return snapshots.get(snapshotId);
        }
    }

    private static class TestingCommitScanner extends CommitScanner {

        private final long snapshotId;
        private final List<ManifestEntry> entries;

        private TestingCommitScanner(long snapshotId, List<ManifestEntry> entries) {
            super(() -> null, null, new CoreOptions(new Options()));
            this.snapshotId = snapshotId;
            this.entries = entries;
        }

        @Override
        public List<ManifestEntry> readIncrementalEntries(
                Snapshot snapshot, List<org.apache.paimon.data.BinaryRow> changedPartitions) {
            return snapshot.id() == snapshotId ? entries : Collections.emptyList();
        }
    }
}
