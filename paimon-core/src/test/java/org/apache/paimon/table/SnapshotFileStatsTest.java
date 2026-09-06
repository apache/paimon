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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link Snapshot#numFiles()} and {@link Snapshot#totalFileSizeInBytes()}, which are
 * folded incrementally at commit time.
 *
 * <p>Two things are checked: the values agree with a full manifest scan whenever they are present,
 * and tables whose snapshots predate the fields keep reading and writing exactly as before, with
 * the values reported as unknown rather than as a wrong number.
 */
public class SnapshotFileStatsTest extends TableTestBase {

    private static final String commitUser = "test-commit-user";

    // ------------------------------------------------------------------------------------------
    // the statistics agree with a full manifest scan
    // ------------------------------------------------------------------------------------------

    @Test
    public void testAppendTable() throws Exception {
        FileStoreTable table = createTable("append_table", false);

        write(table, row(1, "a"), row(2, "b"));
        assertStatsMatchManifests(table);

        write(table, row(3, "c"));
        assertStatsMatchManifests(table);

        write(table, row(4, "d"), row(5, "e"), row(6, "f"));
        assertStatsMatchManifests(table);

        assertThat(latest(table).numFiles()).isGreaterThan(0L);
    }

    @Test
    public void testPrimaryKeyTableAcrossCompaction() throws Exception {
        FileStoreTable table = createTable("pk_table", true);

        // enough commits to trigger compaction, which deletes files as well as adding them
        for (int i = 0; i < 12; i++) {
            write(table, row(i % 3, "v" + i));
            assertStatsMatchManifests(table);
        }

        // guard the premise: without a COMPACT snapshot this test only covers appends
        assertThat(commitKindCount(table, Snapshot.CommitKind.COMPACT)).isGreaterThan(0);
    }

    @Test
    public void testOverwrite() throws Exception {
        FileStoreTable table = createTable("overwrite_table", false);

        write(table, row(1, "a"), row(2, "b"));
        assertThat(latest(table).numFiles()).isGreaterThan(0L);

        overwrite(table, row(9, "z"));
        assertStatsMatchManifests(table);
    }

    @Test
    public void testStatsSurviveTagAndRollback() throws Exception {
        FileStoreTable table = createTable("tag_rollback", false);

        write(table, row(1, "a"));
        long targetId = latest(table).id();
        Long targetNumFiles = latest(table).numFiles();
        table.createTag("t1");

        write(table, row(2, "b"));
        assertStatsMatchManifests(table);

        // a tag carries the statistics of the snapshot it was taken from
        Tag tag = table.tagManager().get("t1").orElseThrow(IllegalStateException::new);
        assertThat(tag.numFiles()).isEqualTo(targetNumFiles);

        // rolling back restores the target snapshot's file set, so its statistics come along
        table.rollbackTo(targetId);
        table = reload("tag_rollback");
        assertStatsMatchManifests(table);
    }

    // ------------------------------------------------------------------------------------------
    // tables written before these fields existed
    // ------------------------------------------------------------------------------------------

    @Test
    public void testLegacySnapshotOmitsTheFieldsEntirely() throws Exception {
        FileStoreTable table = createTable("legacy_json", false);
        write(table, row(1, "a"));

        String json = legacyJsonOf(latest(table));
        assertThat(json).doesNotContain("numFiles").doesNotContain("totalFileSizeInBytes");

        Snapshot parsed = Snapshot.fromJson(json);
        assertThat(parsed.numFiles()).isNull();
        assertThat(parsed.totalFileSizeInBytes()).isNull();
    }

    @Test
    public void testReadLegacyTable() throws Exception {
        FileStoreTable table = createTable("legacy_read", false);
        write(table, row(1, "a"), row(2, "b"));
        write(table, row(3, "c"));
        List<String> before = readRows(table);

        table = degradeToLegacy("legacy_read");

        assertThat(latest(table).numFiles()).isNull();
        assertThat(latest(table).totalFileSizeInBytes()).isNull();
        assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(before);
    }

    @Test
    public void testWriteToLegacyTableStaysUnknownAndNeverWrong() throws Exception {
        FileStoreTable table = createTable("legacy_write", false);
        write(table, row(1, "a"), row(2, "b"));

        table = degradeToLegacy("legacy_write");

        // writing on top of a snapshot without statistics must succeed, and must report unknown
        // rather than a count derived from a zero baseline
        for (int i = 0; i < 3; i++) {
            write(table, row(10 + i, "n" + i));
            Snapshot snapshot = latest(table);
            assertThat(snapshot.numFiles())
                    .as("snapshot %s must stay unknown once the chain is broken", snapshot.id())
                    .isNull();
            assertThat(snapshot.totalFileSizeInBytes()).isNull();
        }

        assertThat(readRows(table))
                .containsExactlyInAnyOrder("1:a", "2:b", "10:n0", "11:n1", "12:n2");

        // a full scan still answers the question the statistics would have answered
        assertThat(scannedNumFiles(table)).isGreaterThan(0L);
    }

    @Test
    public void testLegacyPrimaryKeyTableCompactsAndReadsCorrectly() throws Exception {
        FileStoreTable table = createTable("legacy_pk", true);
        for (int i = 0; i < 4; i++) {
            write(table, row(i % 2, "v" + i));
        }

        table = degradeToLegacy("legacy_pk");

        for (int i = 4; i < 16; i++) {
            write(table, row(i % 2, "v" + i));
            assertThat(latest(table).numFiles()).isNull();
        }

        assertThat(commitKindCount(table, Snapshot.CommitKind.COMPACT)).isGreaterThan(0);
        // the merge engine still resolves to the last value written per key
        assertThat(readRows(table)).containsExactlyInAnyOrder("0:v14", "1:v15");
    }

    @Test
    public void testOverwriteLegacyTable() throws Exception {
        FileStoreTable table = createTable("legacy_overwrite", false);
        write(table, row(1, "a"), row(2, "b"));

        table = degradeToLegacy("legacy_overwrite");

        overwrite(table, row(9, "z"));
        assertThat(latest(table).numFiles()).isNull();
        assertThat(readRows(table)).containsExactly("9:z");
    }

    @Test
    public void testTagAndRollbackOnLegacyTable() throws Exception {
        FileStoreTable table = createTable("legacy_tag", false);
        write(table, row(1, "a"));
        write(table, row(2, "b"));

        table = degradeToLegacy("legacy_tag");
        long targetId = table.snapshotManager().earliestSnapshotId();

        assertThatCode(() -> reload("legacy_tag").createTag("legacy_t1"))
                .doesNotThrowAnyException();
        Tag tag = table.tagManager().get("legacy_t1").orElseThrow(IllegalStateException::new);
        assertThat(tag.numFiles()).isNull();
        assertThat(tag.totalFileSizeInBytes()).isNull();

        table.rollbackTo(targetId);
        table = reload("legacy_tag");
        assertThat(latest(table).numFiles()).isNull();
        assertThat(readRows(table)).containsExactly("1:a");
    }

    @Test
    public void testSnapshotsSystemTableOnLegacyTable() throws Exception {
        FileStoreTable table = createTable("legacy_sys", false);
        write(table, row(1, "a"));
        write(table, row(2, "b"));

        // a table that still carries the statistics reports them in the two new columns
        Table systemTable = catalog.getTable(identifier("legacy_sys$snapshots"));
        List<InternalRow> rows = read(systemTable);
        assertThat(rows).hasSize(2);
        for (InternalRow row : rows) {
            assertThat(row.isNullAt(16)).isFalse();
            assertThat(row.isNullAt(17)).isFalse();
        }

        degradeToLegacy("legacy_sys");

        // after degrading, the same query must still work and report the columns as null
        systemTable = catalog.getTable(identifier("legacy_sys$snapshots"));
        rows = read(systemTable);
        assertThat(rows).hasSize(2);
        for (InternalRow row : rows) {
            assertThat(row.isNullAt(16)).isTrue();
            assertThat(row.isNullAt(17)).isTrue();
        }
    }

    @Test
    public void testMixedLegacyAndCurrentSnapshotsAreReadable() throws Exception {
        FileStoreTable table = createTable("legacy_mixed", false);
        write(table, row(1, "a"));
        write(table, row(2, "b"));

        // degrade only the first snapshot, as an upgraded table looks: old snapshots without the
        // fields still in the retained history, newer ones with them
        degradeSnapshot("legacy_mixed", table.snapshotManager().earliestSnapshotId());
        table = reload("legacy_mixed");

        SnapshotManager manager = table.snapshotManager();
        assertThat(manager.snapshot(manager.earliestSnapshotId()).numFiles()).isNull();
        assertThat(manager.snapshot(manager.latestSnapshotId()).numFiles()).isNotNull();

        // time travel into the degraded part of the history keeps working
        assertThat(
                        readRows(
                                table.copy(
                                        java.util.Collections.singletonMap(
                                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                                String.valueOf(manager.earliestSnapshotId())))))
                .containsExactly("1:a");
        assertThat(readRows(table)).containsExactlyInAnyOrder("1:a", "2:b");
    }

    @Test
    public void testManifestCompactionKeepsStats() throws Exception {
        FileStoreTable table = createTable("manifest_compact", false);
        for (int i = 0; i < 6; i++) {
            write(table, row(i, "v" + i));
        }
        Long filesBefore = latest(table).numFiles();
        Long sizeBefore = latest(table).totalFileSizeInBytes();

        compactManifests(table);
        table = reload("manifest_compact");

        // compacting manifests only rewrites metadata, so the data file statistics carry over
        assertThat(latest(table).numFiles()).isEqualTo(filesBefore);
        assertThat(latest(table).totalFileSizeInBytes()).isEqualTo(sizeBefore);
        assertStatsMatchManifests(table);
    }

    @Test
    public void testManifestCompactionOnLegacyTableStaysUnknown() throws Exception {
        FileStoreTable table = createTable("manifest_compact_legacy", false);
        for (int i = 0; i < 6; i++) {
            write(table, row(i, "v" + i));
        }

        table = degradeToLegacy("manifest_compact_legacy");
        compactManifests(table);
        table = reload("manifest_compact_legacy");

        assertThat(latest(table).numFiles()).isNull();
        assertThat(readRows(table))
                .containsExactlyInAnyOrder("0:v0", "1:v1", "2:v2", "3:v3", "4:v4", "5:v5");
    }

    @Test
    public void testExpireSnapshotsOnLegacyTable() throws Exception {
        FileStoreTable table = createTable("legacy_expire", false);
        for (int i = 0; i < 5; i++) {
            write(table, row(i, "v" + i));
        }

        table = degradeToLegacy("legacy_expire");
        table.newExpireSnapshots()
                .config(
                        ExpireConfig.builder()
                                .snapshotMaxDeletes(Integer.MAX_VALUE)
                                .snapshotRetainMax(2)
                                .snapshotRetainMin(1)
                                .build())
                .expire();
        table = reload("legacy_expire");

        assertThat(latest(table).numFiles()).isNull();
        assertThat(readRows(table))
                .containsExactlyInAnyOrder("0:v0", "1:v1", "2:v2", "3:v3", "4:v4");

        // writing after expiration on a legacy table still works
        write(table, row(9, "z"));
        assertThat(latest(table).numFiles()).isNull();
        assertThat(readRows(table)).hasSize(6);
    }

    @Test
    public void testReplaceManifestListReportsUnknown() throws Exception {
        FileStoreTable table = createTable("replace_manifests", false);
        write(table, row(1, "a"), row(2, "b"));
        write(table, row(3, "c"));
        List<String> before = readRows(table);
        assertThat(latest(table).numFiles()).isNotNull();

        replaceManifestListWithSameLayout(table);
        table = reload("replace_manifests");

        // this path swaps the manifest layout wholesale, so the counters cannot be carried over.
        // Reporting unknown is the only honest answer; reporting the previous numbers would be
        // wrong for callers such as RemoveUnexistingManifestsAction, which drops files.
        assertThat(latest(table).numFiles()).isNull();
        assertThat(latest(table).totalFileSizeInBytes()).isNull();

        // the data itself is untouched, and the table keeps taking writes
        assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(before);
        write(table, row(4, "d"));
        assertThat(latest(table).numFiles()).isNull();
        assertThat(readRows(table)).containsExactlyInAnyOrder("1:a", "2:b", "3:c", "4:d");
    }

    @Test
    public void testReplaceManifestListThatDropsFiles() throws Exception {
        // keep every commit in its own manifest so one of them can be dropped below
        FileStoreTable table =
                createTable(
                        "replace_manifests_drop",
                        false,
                        CoreOptions.MANIFEST_MERGE_MIN_COUNT.key(),
                        "100");
        write(table, row(1, "a"));
        write(table, row(2, "b"));
        write(table, row(3, "c"));

        long filesBefore = latest(table).numFiles();
        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> manifests = manifestList.readDataManifests(latest(table));
        assertThat(manifests.size()).isGreaterThan(1);

        // drop one manifest, the way a repair operation removes manifests whose files are gone
        Snapshot latest = latest(table);
        List<ManifestFileMeta> kept = new ArrayList<>(manifests.subList(1, manifests.size()));
        Pair<String, Long> base = manifestList.write(kept);
        Pair<String, Long> delta = manifestList.write(Collections.emptyList());
        try (FileStoreCommitImpl commit =
                (FileStoreCommitImpl) table.store().newCommit(commitUser, table)) {
            assertThat(
                            commit.replaceManifestList(
                                    latest,
                                    latest.totalRecordCount(),
                                    base,
                                    delta,
                                    latest.indexManifest(),
                                    latest.nextRowId()))
                    .isTrue();
        }
        table = reload("replace_manifests_drop");

        // the live file set really did shrink, so carrying the previous counters over would have
        // published a number that is simply wrong; unknown is the correct answer
        long filesAfter = scannedNumFiles(table);
        assertThat(filesAfter).isLessThan(filesBefore);
        assertThat(latest(table).numFiles()).isNull();
        assertThat(latest(table).totalFileSizeInBytes()).isNull();

        // and the table stays usable afterwards
        write(table, row(4, "d"));
        assertThat(latest(table).numFiles()).isNull();
        assertThat(readRows(table)).contains("4:d");
    }

    @Test
    public void testUnknownJsonFieldsAreIgnored() {
        // an older reader must tolerate a snapshot written by a newer writer; the same mechanism
        // that lets it ignore numFiles is exercised here with an unknown field
        Snapshot parsed =
                Snapshot.fromJson(
                        "{\n"
                                + "  \"version\" : 3,\n"
                                + "  \"id\" : 5,\n"
                                + "  \"schemaId\" : 0,\n"
                                + "  \"baseManifestList\" : \"base\",\n"
                                + "  \"deltaManifestList\" : \"delta\",\n"
                                + "  \"commitUser\" : \"user\",\n"
                                + "  \"commitIdentifier\" : 0,\n"
                                + "  \"commitKind\" : \"APPEND\",\n"
                                + "  \"timeMillis\" : 1000,\n"
                                + "  \"totalRecordCount\" : 10,\n"
                                + "  \"deltaRecordCount\" : 10,\n"
                                + "  \"numFiles\" : 7,\n"
                                + "  \"totalFileSizeInBytes\" : 4096,\n"
                                + "  \"someFieldFromTheFuture\" : \"whatever\"\n"
                                + "}");
        assertThat(parsed.numFiles()).isEqualTo(7L);
        assertThat(parsed.totalFileSizeInBytes()).isEqualTo(4096L);
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private FileStoreTable createTable(String name, boolean primaryKey, String... options)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "1");
        for (int i = 0; i < options.length; i += 2) {
            builder.option(options[i], options[i + 1]);
        }
        if (primaryKey) {
            builder.primaryKey("k");
        } else {
            builder.option(CoreOptions.BUCKET_KEY.key(), "k");
        }
        Identifier identifier = identifier(name);
        catalog.createTable(identifier, builder.build(), false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private FileStoreTable reload(String name) throws Exception {
        return (FileStoreTable) catalog.getTable(identifier(name));
    }

    private static GenericRow row(int k, String v) {
        return GenericRow.of(k, BinaryString.fromString(v));
    }

    /**
     * Rewrites the snapshot with exactly the manifest layout it already has, which is the shape
     * metadata-repair operations commit through {@code replaceManifestList}.
     */
    private void replaceManifestListWithSameLayout(FileStoreTable table) throws Exception {
        Snapshot latest = latest(table);
        ManifestList manifestList = table.store().manifestListFactory().create();
        Pair<String, Long> base = manifestList.write(manifestList.readDataManifests(latest));
        Pair<String, Long> delta = manifestList.write(Collections.emptyList());
        try (FileStoreCommitImpl commit =
                (FileStoreCommitImpl) table.store().newCommit(commitUser, table)) {
            assertThat(
                            commit.replaceManifestList(
                                    latest,
                                    latest.totalRecordCount(),
                                    base,
                                    delta,
                                    latest.indexManifest(),
                                    latest.nextRowId()))
                    .isTrue();
        }
    }

    private void compactManifests(FileStoreTable table) throws Exception {
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            commit.compactManifests();
        }
    }

    private void overwrite(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder().withOverwrite();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private List<String> readRows(Table table) throws Exception {
        return read(table).stream()
                .map(row -> row.getInt(0) + ":" + row.getString(1))
                .collect(Collectors.toList());
    }

    private static Snapshot latest(FileStoreTable table) {
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot).isNotNull();
        return snapshot;
    }

    private static long scannedNumFiles(FileStoreTable table) {
        return table.newScan().listPartitionEntries().stream()
                .mapToLong(PartitionEntry::fileCount)
                .sum();
    }

    private void assertStatsMatchManifests(FileStoreTable table) {
        Snapshot snapshot = latest(table);
        List<PartitionEntry> entries = table.newScan().listPartitionEntries();
        long expectedFiles = entries.stream().mapToLong(PartitionEntry::fileCount).sum();
        long expectedSize = entries.stream().mapToLong(PartitionEntry::fileSizeInBytes).sum();

        assertThat(snapshot.numFiles())
                .as("num files of snapshot %s", snapshot.id())
                .isEqualTo(expectedFiles);
        assertThat(snapshot.totalFileSizeInBytes())
                .as("total file size of snapshot %s", snapshot.id())
                .isEqualTo(expectedSize);
    }

    private int commitKindCount(FileStoreTable table, Snapshot.CommitKind kind) {
        SnapshotManager manager = table.snapshotManager();
        int count = 0;
        for (long id = manager.earliestSnapshotId(); id <= manager.latestSnapshotId(); id++) {
            if (manager.snapshot(id).commitKind() == kind) {
                count++;
            }
        }
        return count;
    }

    /** Rewrites every snapshot file the way a Paimon without these fields would have written it. */
    private FileStoreTable degradeToLegacy(String name) throws Exception {
        FileStoreTable table = reload(name);
        SnapshotManager manager = table.snapshotManager();
        List<Long> ids = new ArrayList<>();
        for (long id = manager.earliestSnapshotId(); id <= manager.latestSnapshotId(); id++) {
            ids.add(id);
        }
        for (long id : ids) {
            degradeSnapshot(name, id);
        }
        return reload(name);
    }

    private void degradeSnapshot(String name, long snapshotId) throws Exception {
        FileStoreTable table = reload(name);
        Path path = table.snapshotManager().snapshotPath(snapshotId);
        Snapshot snapshot = Snapshot.fromJson(table.fileIO().readFileUtf8(path));
        table.fileIO().overwriteFileUtf8(path, legacyJsonOf(snapshot));
        // the snapshot object and the table itself are cached, drop both so the rewritten file is
        // the one that gets read back
        table.snapshotManager().invalidateCache();
        catalog.invalidateTable(identifier(name));
    }

    /**
     * Serializes through the constructor that predates the two fields, so the payload looks exactly
     * like one written by an older version: the keys are absent, not null.
     */
    private static String legacyJsonOf(Snapshot s) {
        return new Snapshot(
                        s.version(),
                        s.uuid(),
                        s.id(),
                        s.schemaId(),
                        s.baseManifestList(),
                        s.baseManifestListSize(),
                        s.deltaManifestList(),
                        s.deltaManifestListSize(),
                        s.changelogManifestList(),
                        s.changelogManifestListSize(),
                        s.indexManifest(),
                        s.commitUser(),
                        s.writerVersion(),
                        s.commitIdentifier(),
                        s.commitKind(),
                        s.timeMillis(),
                        s.totalRecordCount(),
                        s.deltaRecordCount(),
                        s.changelogRecordCount(),
                        s.watermark(),
                        s.statistics(),
                        s.properties(),
                        s.nextRowId(),
                        s.operation())
                .toJson();
    }
}
