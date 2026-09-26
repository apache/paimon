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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** IT cases for rollback_to_as_latest procedure. */
public class RollbackToAsLatestProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testRollbackToSnapshotAsLatest() throws Exception {
        sql("CREATE TABLE T (id INT, name STRING)");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        commitRow(table, 1, "a");
        commitRow(table, 2, "b");
        commitRow(table, 3, "c");
        assertEquals(3, snapshotManager.latestSnapshotId());

        assertThat(sql("CALL sys.rollback_to_as_latest(`table` => 'default.T', snapshot_id => 1)"))
                .containsExactly(Row.of(3L, 1L, 4L));

        assertEquals(4, snapshotManager.latestSnapshotId());
        assertRollbackDelta(table, 4, 0, 2, -2L);
        assertTrue(snapshotManager.snapshotExists(2));
        assertTrue(snapshotManager.snapshotExists(3));
        assertThat(sql("SELECT * FROM T")).containsExactly(Row.of(1, "a"));

        assertThat(sql("CALL sys.rollback_to_as_latest(`table` => 'default.T', snapshot_id => 3)"))
                .containsExactly(Row.of(4L, 3L, 5L));

        assertEquals(5, snapshotManager.latestSnapshotId());
        assertRollbackDelta(table, 5, 2, 0, 2L);
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, "a"), Row.of(2, "b"), Row.of(3, "c"));

        commitRow(table, 4, "d");
        assertEquals(6, snapshotManager.latestSnapshotId());
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "a"), Row.of(2, "b"), Row.of(3, "c"), Row.of(4, "d"));
    }

    @Test
    public void testRollbackToTagAsLatest() throws Exception {
        sql("CREATE TABLE T (id INT, name STRING)");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        commitRow(table, 1, "a");
        commitRow(table, 2, "b");
        commitRow(table, 3, "c");
        assertEquals(3, snapshotManager.latestSnapshotId());

        sql("CALL sys.create_tag(`table` => 'default.T', tag => 'tag-1', snapshot_id => 1)");

        assertThat(sql("CALL sys.rollback_to_as_latest(`table` => 'default.T', tag => 'tag-1')"))
                .containsExactly(Row.of(3L, 1L, 4L));

        assertEquals(4, snapshotManager.latestSnapshotId());
        assertRollbackDelta(table, 4, 0, 2, -2L);
        assertTrue(snapshotManager.snapshotExists(2));
        assertTrue(snapshotManager.snapshotExists(3));
        assertThat(sql("SELECT * FROM T")).containsExactly(Row.of(1, "a"));
        assertThat(tagsOfSnapshot(table.tagManager(), 1))
                .extracting(Pair::getRight)
                .containsExactly("tag-1");
    }

    @Test
    public void testRollbackCreatesNonExpiringProtectionTag() throws Exception {
        sql("CREATE TABLE T (id INT, name STRING)");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        commitRow(table, 1, "a");
        commitRow(table, 2, "b");
        commitRow(table, 3, "c");
        assertEquals(3, snapshotManager.latestSnapshotId());

        Snapshot target = snapshotManager.snapshot(1);
        TagManager tagManager = table.tagManager();
        tagManager.createTag(
                target, "expiring-target", Duration.ZERO, Collections.emptyList(), false);

        assertThat(sql("CALL sys.rollback_to_as_latest(`table` => 'default.T', snapshot_id => 1)"))
                .containsExactly(Row.of(3L, 1L, 4L));

        List<Pair<Tag, String>> targetTags = tagsOfSnapshot(tagManager, target.id());
        assertThat(targetTags)
                .anyMatch(
                        tag ->
                                tag.getRight().equals("expiring-target")
                                        && Duration.ZERO.equals(
                                                tag.getLeft().getTagTimeRetained()));
        assertThat(targetTags)
                .anyMatch(
                        tag ->
                                tag.getRight()
                                                .startsWith(
                                                        "rollback-to-as-latest-"
                                                                + target.id()
                                                                + "-")
                                        && tag.getLeft().getTagTimeRetained() == null);
    }

    @Test
    public void testRollbackDoesNotExpireKeptSnapshots() throws Exception {
        sql("CREATE TABLE T (id INT, name STRING)");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        commitRow(table, 1, "a");
        commitRow(table, 2, "b");
        commitRow(table, 3, "c");
        assertEquals(3, snapshotManager.latestSnapshotId());

        // Configure aggressive expiration that would otherwise drop every snapshot but the latest.
        sql(
                "ALTER TABLE T SET ("
                        + "'snapshot.num-retained.min' = '1', "
                        + "'snapshot.num-retained.max' = '1')");

        assertThat(sql("CALL sys.rollback_to_as_latest(`table` => 'default.T', snapshot_id => 1)"))
                .containsExactly(Row.of(3L, 1L, 4L));

        // rollback_to_as_latest must not delete snapshots whose id is larger than the target one,
        // even with snapshot.num-retained.max = 1. The rollback path skips automatic expiration.
        assertEquals(4, snapshotManager.latestSnapshotId());
        assertTrue(snapshotManager.snapshotExists(1));
        assertTrue(snapshotManager.snapshotExists(2));
        assertTrue(snapshotManager.snapshotExists(3));
    }

    @Test
    public void testRollbackKeepsRowIdMonotonic() throws Exception {
        sql("CREATE TABLE RT (id INT, name STRING) WITH ('row-tracking.enabled' = 'true')");

        FileStoreTable table = paimonTable("RT");
        SnapshotManager snapshotManager = table.snapshotManager();

        commitRow(table, 1, "a");
        commitRow(table, 2, "b");
        commitRow(table, 3, "c");
        assertEquals(3, snapshotManager.latestSnapshotId());

        Long latestNextRowId = snapshotManager.latestSnapshot().nextRowId();
        assertThat(latestNextRowId).isNotNull();

        // Roll back to the first snapshot, whose own nextRowId is smaller than the current latest.
        assertThat(sql("CALL sys.rollback_to_as_latest(`table` => 'default.RT', snapshot_id => 1)"))
                .containsExactly(Row.of(3L, 1L, 4L));

        // nextRowId must not move backwards: otherwise new appends would reuse row ids already
        // assigned by snapshots 2 and 3, breaking the global uniqueness of _ROW_ID. The rollback
        // snapshot keeps the larger of the previous latest and the target nextRowId.
        Snapshot rolledBack = table.snapshot(4);
        assertThat(rolledBack.nextRowId()).isEqualTo(latestNextRowId);
    }

    @Test
    public void testRollbackTriggersCommitCallback() throws Exception {
        sql(
                "CREATE TABLE T (id INT, name STRING) WITH ("
                        + "'metadata.iceberg.storage' = 'table-location')");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        commitRow(table, 1, "a");
        commitRow(table, 2, "b");
        commitRow(table, 3, "c");
        assertEquals(3, snapshotManager.latestSnapshotId());

        assertThat(sql("CALL sys.rollback_to_as_latest(`table` => 'default.T', snapshot_id => 1)"))
                .containsExactly(Row.of(3L, 1L, 4L));
        assertEquals(4, snapshotManager.latestSnapshotId());

        // The rollback must trigger the commit callbacks like a regular commit, so external views
        // stay in sync with the rolled-back state. With Iceberg compatibility enabled, that means
        // Iceberg metadata is generated for the rollback snapshot.
        Path icebergMetadata = new Path(table.location(), "metadata/v4.metadata.json");
        assertTrue(table.fileIO().exists(icebergMetadata));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRollbackKeepsProtectionTagAfterCallbackFailure(boolean concurrentCommit)
            throws Exception {
        sql(
                "CREATE TABLE T (id INT, name STRING) WITH ('commit.callbacks' = '%s')",
                FailingRollbackCallback.class.getName());
        FileStoreTable table = paimonTable("T");
        commitRow(table, 1, "original");
        // The restored file must not also belong to the snapshots retained before the rollback.
        BatchWriteBuilder overwrite = table.newBatchWriteBuilder().withOverwrite();
        try (BatchTableWrite write = overwrite.newWrite();
                BatchTableCommit commit = overwrite.newCommit()) {
            write.write(GenericRow.of(2, BinaryString.fromString("replacement")));
            commit.commit(write.prepareCommit());
        }
        commitRow(table, 3, "retained");

        FailingRollbackCallback.concurrentCommit = concurrentCommit;
        FailingRollbackCallback.fail = true;
        try {
            assertThatThrownBy(
                            () ->
                                    sql(
                                            "CALL sys.rollback_to_as_latest(`table` => 'default.T', snapshot_id => 1)"))
                    .hasStackTraceContaining("Injected post-commit callback failure");
        } finally {
            FailingRollbackCallback.fail = false;
            FailingRollbackCallback.concurrentCommit = false;
        }

        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.id()).isEqualTo(concurrentCommit ? 5L : 4L);
        assertThat(latest.commitUser()).startsWith("rollback-to-as-latest-");
        assertThat(tagsOfSnapshot(table.tagManager(), 1)).hasSize(1);
        assertThat(sql("SELECT * FROM T")).containsExactly(Row.of(1, "original"));
        ((ExpireSnapshotsImpl) table.newExpireSnapshots()).expireUntil(1, latest.id());
        assertThat(sql("SELECT * FROM T")).containsExactly(Row.of(1, "original"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRollbackDeletesProtectionTagWhenNotCommitted(boolean commitCreated)
            throws Exception {
        sql("CREATE TABLE T (id INT, name STRING)");
        FileStoreTable table = paimonTable("T");
        commitRow(table, 1, "original");
        FileStoreTable failingTable = mock(FileStoreTable.class);
        doReturn(table.store()).when(failingTable).store();
        if (commitCreated) {
            TableCommitImpl commit = mock(TableCommitImpl.class);
            when(commit.rollbackToAsLatest(any(Tag.class))).thenReturn(false);
            when(failingTable.newCommit(anyString())).thenReturn(commit);
        } else {
            when(failingTable.newCommit(anyString()))
                    .thenThrow(new RuntimeException("Injected commit creation failure"));
        }
        Catalog catalog = mock(Catalog.class);
        when(catalog.getTable(Identifier.fromString("default.T"))).thenReturn(failingTable);
        RollbackToAsLatestProcedure procedure = new RollbackToAsLatestProcedure();
        procedure.withCatalog(catalog);

        assertThatThrownBy(() -> procedure.call(null, "default.T", null, 1L))
                .hasMessageContaining("Failed to roll back to snapshot 1 as latest");
        assertThat(table.tagManager().tags()).isEmpty();
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
    }

    /** Injects a post-commit failure, optionally after another writer advances the latest ID. */
    public static class FailingRollbackCallback implements CommitCallback {

        private static boolean fail;
        private static boolean concurrentCommit;

        @Override
        public void setTable(FileStoreTable table) {
            if (fail && concurrentCommit) {
                // The procedure already read latest; core rollback has not read it yet.
                concurrentCommit = false;
                fail = false;
                try {
                    commitRow(table, 4, "concurrent");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    fail = true;
                }
            }
        }

        @Override
        public void call(Context context) {
            if (fail && context.snapshot.commitUser().startsWith("rollback-to-as-latest-")) {
                throw new RuntimeException("Injected post-commit callback failure");
            }
        }

        @Override
        public void retry(ManifestCommittable committable) {}

        @Override
        public void close() {}
    }

    private void assertRollbackDelta(
            FileStoreTable table,
            long snapshotId,
            long expectedNumAddedFiles,
            long expectedNumDeletedFiles,
            long expectedDeltaRecordCount) {
        Snapshot snapshot = table.snapshot(snapshotId);
        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> deltaManifests = manifestList.readDeltaManifests(snapshot);

        assertThat(deltaManifests).hasSize(1);
        assertThat(deltaManifests.get(0).numAddedFiles()).isEqualTo(expectedNumAddedFiles);
        assertThat(deltaManifests.get(0).numDeletedFiles()).isEqualTo(expectedNumDeletedFiles);
        assertThat(snapshot.deltaRecordCount()).isEqualTo(expectedDeltaRecordCount);
    }

    private List<Pair<Tag, String>> tagsOfSnapshot(TagManager tagManager, long snapshotId) {
        return tagManager.tagObjects().stream()
                .filter(tag -> tag.getLeft().id() == snapshotId)
                .collect(Collectors.toList());
    }

    private static void commitRow(FileStoreTable table, int id, String name) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString(name)));
            commit.commit(write.prepareCommit());
        }
    }
}
