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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildDdl;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.sEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Data Evolution IT cases for the unified {@link DeleteAction}. */
public class DeleteActionDataEvolutionITCase extends ActionITCaseBase {

    private static final String TABLE = "T";

    @BeforeEach
    public void setUp() {
        init(warehouse);
    }

    @Test
    public void testDeleteAcrossPartitionsAndMergeExistingDeletionVectors() throws Exception {
        createDataEvolutionTable(TABLE, false, true);
        insertInto(TABLE, "(1, 'one', 'A')", "(2, 'two', 'A')", "(3, 'three', 'A')");
        insertInto(TABLE, "(4, 'four', 'B')", "(5, 'five', 'B')", "(6, 'six', 'B')");

        FileStoreTable table = getFileStoreTable(TABLE);
        List<String> originalFiles = plannedFiles(table);

        action(TABLE, "id IN (2, 4)", 2).run();

        testBatchRead(
                "SELECT id, name, dt FROM T ORDER BY id",
                Arrays.asList(
                        changelogRow("+I", 1, "one", "A"),
                        changelogRow("+I", 3, "three", "A"),
                        changelogRow("+I", 5, "five", "B"),
                        changelogRow("+I", 6, "six", "B")));
        assertDeleteSnapshotAndFiles(table, originalFiles, 2L);

        // A second action must merge with the existing DV instead of losing the first deletion.
        action(TABLE, "id IN (2, 3)", 2).run();

        testBatchRead(
                "SELECT id, name, dt FROM T ORDER BY id",
                Arrays.asList(
                        changelogRow("+I", 1, "one", "A"),
                        changelogRow("+I", 5, "five", "B"),
                        changelogRow("+I", 6, "six", "B")));
        assertDeleteSnapshotAndFiles(table, originalFiles, 3L);
    }

    @Test
    public void testDeleteBlobRowWithoutRewritingDataOrBlobFiles() throws Exception {
        createDataEvolutionTable("BLOB_T", true, true);
        insertInto("BLOB_T", "(1, 'one', 'A', X'48656C6C6F')", "(2, 'two', 'A', X'5041494D4F4E')");

        FileStoreTable table = getFileStoreTable("BLOB_T");
        List<String> originalFiles = plannedFiles(table);

        action("BLOB_T", "id = 1", 1).run();

        testBatchRead(
                "SELECT id, name, picture FROM BLOB_T",
                Collections.singletonList(
                        changelogRow("+I", 2, "two", new byte[] {80, 65, 73, 77, 79, 78})));
        assertDeleteSnapshotAndFiles(table, originalFiles, 1L);
    }

    @Test
    public void testNoMatchedRowsDoesNotCreateSnapshot() throws Exception {
        createDataEvolutionTable(TABLE, false, true);
        insertInto(TABLE, "(1, 'one', 'A')");

        FileStoreTable table = getFileStoreTable(TABLE);
        long snapshotId = table.latestSnapshot().get().id();

        action(TABLE, "id = 999", 1).run();

        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshotId);
        assertThat(deletionVectorCardinality(table)).isZero();
    }

    @Test
    public void testDeleteWithBoundedExternalCandidates() throws Exception {
        createDataEvolutionTable(TABLE, false, true);
        insertInto(TABLE, "(1, 'one', 'A')", "(2, 'two', 'A')", "(3, 'three', 'A')");

        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                changelogRow("+I", 2),
                                changelogRow("+I", 2),
                                changelogRow("+I", 999)));
        String createCandidates =
                String.format(
                        "CREATE TEMPORARY TABLE deletion_candidate_source (id INT) "
                                + "WITH ('connector' = 'values', 'bounded' = 'true', "
                                + "'data-id' = '%s')",
                        dataId);
        String createCandidateView =
                "CREATE TEMPORARY VIEW deletion_candidates AS "
                        + "SELECT id FROM deletion_candidate_source";

        actionWithSourceSqls(
                        TABLE,
                        "id IN (SELECT id FROM deletion_candidates)",
                        2,
                        createCandidates,
                        createCandidateView)
                .run();

        testBatchRead(
                "SELECT id, name, dt FROM T ORDER BY id",
                Arrays.asList(
                        changelogRow("+I", 1, "one", "A"), changelogRow("+I", 3, "three", "A")));
        assertThat(deletionVectorCardinality(getFileStoreTable(TABLE))).isEqualTo(1L);
    }

    @Test
    public void testDeleteWithFilteredBoundedExternalTable() throws Exception {
        createUnpartitionedUrlTable("URL_T");
        insertInto(
                "URL_T",
                "('cold-url', 'cold')",
                "('hot-url', 'hot')",
                "('old-never-requested-url', 'old-never-requested')",
                "('fresh-never-requested-url', 'fresh-never-requested')",
                "('url-without-access-state', 'no-access-state')");

        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                changelogRow(
                                        "+I",
                                        "cold-url",
                                        LocalDateTime.parse("2026-06-01T00:00:00"),
                                        LocalDateTime.parse("2026-06-15T00:00:00")),
                                changelogRow(
                                        "+I",
                                        "hot-url",
                                        LocalDateTime.parse("2026-06-01T00:00:00"),
                                        LocalDateTime.parse("2026-07-15T00:00:00")),
                                changelogRow(
                                        "+I",
                                        "old-never-requested-url",
                                        LocalDateTime.parse("2026-06-01T00:00:00"),
                                        null),
                                changelogRow(
                                        "+I",
                                        "fresh-never-requested-url",
                                        LocalDateTime.parse("2026-07-15T00:00:00"),
                                        null),
                                changelogRow(
                                        "+I",
                                        "missing-target-url",
                                        LocalDateTime.parse("2026-06-01T00:00:00"),
                                        null)));
        String createAccessState =
                String.format(
                        "CREATE TEMPORARY TABLE external_access_state ("
                                + "url STRING, last_ingest_time TIMESTAMP(3), "
                                + "last_request_time TIMESTAMP(3)) "
                                + "WITH ('connector' = 'values', 'bounded' = 'true', "
                                + "'data-id' = '%s')",
                        dataId);

        action(
                        "URL_T",
                        "url IN (SELECT url FROM external_access_state "
                                + "WHERE last_ingest_time < TIMESTAMP '2026-07-01 00:00:00' "
                                + "AND (last_request_time IS NULL "
                                + "OR last_request_time < TIMESTAMP '2026-07-01 00:00:00'))",
                        2)
                .withSourceSqls(createAccessState)
                .run();

        testBatchRead(
                "SELECT url, payload FROM URL_T ORDER BY url",
                Arrays.asList(
                        changelogRow("+I", "fresh-never-requested-url", "fresh-never-requested"),
                        changelogRow("+I", "hot-url", "hot"),
                        changelogRow("+I", "url-without-access-state", "no-access-state")));
        assertThat(deletionVectorCardinality(getFileStoreTable("URL_T"))).isEqualTo(2L);
    }

    @Test
    public void testSourceSqlFailureDoesNotLeakSqlOrCreateSnapshot() throws Exception {
        createDataEvolutionTable(TABLE, false, true);
        insertInto(TABLE, "(1, 'one', 'A')");

        FileStoreTable table = getFileStoreTable(TABLE);
        long snapshotId = table.latestSnapshot().get().id();
        String sensitiveMarker = "password_should_not_be_logged";

        assertThatThrownBy(
                        () ->
                                action(TABLE, "id = 1", 1)
                                        .withSourceSqls("INVALID SQL " + sensitiveMarker)
                                        .run())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("source SQL statement 1")
                .hasMessageNotContaining(sensitiveMarker);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshotId);
        assertThat(deletionVectorCardinality(table)).isZero();
    }

    @Test
    public void testUnpartitionedTableWithParallelRewriteGroups() throws Exception {
        createUnpartitionedDataEvolutionTable(TABLE);
        for (int id = 1; id <= 12; id++) {
            insertInto(TABLE, String.format("(%d, 'value_%d')", id, id));
        }

        FileStoreTable table = getFileStoreTable(TABLE);
        List<String> originalFiles = plannedFiles(table);

        action(TABLE, "MOD(id, 2) = 1", 4).run();

        testBatchRead(
                "SELECT id, name FROM T ORDER BY id",
                Arrays.asList(
                        changelogRow("+I", 2, "value_2"),
                        changelogRow("+I", 4, "value_4"),
                        changelogRow("+I", 6, "value_6"),
                        changelogRow("+I", 8, "value_8"),
                        changelogRow("+I", 10, "value_10"),
                        changelogRow("+I", 12, "value_12")));
        assertDeleteSnapshotAndFiles(table, originalFiles, 6L);

        // The second action must safely merge updates which may be backed by several old DV
        // index files created by the first parallel action.
        action(TABLE, "id IN (2, 6, 10)", 4).run();

        testBatchRead(
                "SELECT id, name FROM T ORDER BY id",
                Arrays.asList(
                        changelogRow("+I", 4, "value_4"),
                        changelogRow("+I", 8, "value_8"),
                        changelogRow("+I", 12, "value_12")));
        assertDeleteSnapshotAndFiles(table, originalFiles, 9L);
    }

    @Test
    public void testRewriteGroupOwnership() {
        String bucketPath = "dt=20260719/bucket-0";
        assertThat(DataEvolutionDelete.rewriteGroup(bucketPath, "old-dv-index", "anchor-a", 4))
                .isEqualTo(
                        DataEvolutionDelete.rewriteGroup(
                                bucketPath, "old-dv-index", "anchor-b", 4));
        assertThat(DataEvolutionDelete.rewriteGroup(bucketPath, "old-dv-index-a", "anchor-a", 4))
                .isNotEqualTo(
                        DataEvolutionDelete.rewriteGroup(
                                bucketPath, "old-dv-index-b", "anchor-a", 4));
        assertThat(DataEvolutionDelete.rewriteGroup(bucketPath, null, "anchor-a", 4))
                .startsWith(bucketPath + "\u0000new\u0000");
    }

    @Test
    public void testActionsFromSameSnapshotConflictInsteadOfBeingFiltered() throws Exception {
        createDataEvolutionTable(TABLE, false, true);
        insertInto(TABLE, "(1, 'one', 'A')", "(2, 'two', 'A')", "(3, 'three', 'A')");

        // Both actions intentionally capture the same base snapshot. The second action must use a
        // different commit user so that strict mode detects the first DELETE snapshot instead of
        // treating the second action as an already committed retry.
        DeleteAction first = action(TABLE, "id = 1", 1);
        DeleteAction stale = action(TABLE, "id = 2", 1);

        first.run();

        assertThatThrownBy(stale::run)
                .hasStackTraceContaining(CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key());
        testBatchRead(
                "SELECT id, name, dt FROM T ORDER BY id",
                Arrays.asList(
                        changelogRow("+I", 2, "two", "A"), changelogRow("+I", 3, "three", "A")));
    }

    @Test
    public void testRejectTableWithoutDeletionVectors() throws Exception {
        createDataEvolutionTable(TABLE, false, false);
        insertInto(TABLE, "(1, 'one', 'A')");

        assertThatThrownBy(() -> action(TABLE, "id = 1", 1))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("deletion-vectors.enabled");
    }

    @Test
    public void testFactoryRequiresWhere() {
        assertThatThrownBy(
                        () ->
                                createAction(
                                        DeleteAction.class,
                                        "delete",
                                        "--warehouse",
                                        warehouse,
                                        "--database",
                                        database,
                                        "--table",
                                        TABLE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deletion filter");

        assertThatThrownBy(
                        () ->
                                createAction(
                                        DeleteAction.class,
                                        "delete",
                                        "--warehouse",
                                        warehouse,
                                        "--database",
                                        database,
                                        "--table",
                                        TABLE,
                                        "--where",
                                        "   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deletion filter");
    }

    private void createDataEvolutionTable(
            String tableName, boolean withBlob, boolean deletionVectors) {
        List<String> fields =
                withBlob
                        ? Arrays.asList("id INT", "name STRING", "dt STRING", "picture BYTES")
                        : Arrays.asList("id INT", "name STRING", "dt STRING");

        Map<String, String> options =
                new java.util.HashMap<String, String>() {
                    {
                        put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
                        put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
                        put(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(),
                                String.valueOf(deletionVectors));
                        put(CoreOptions.BUCKET.key(), "-1");
                        if (withBlob) {
                            put("blob-field", "picture");
                        }
                    }
                };

        sEnv.executeSql(
                buildDdl(
                        tableName,
                        fields,
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        options));
    }

    private void createUnpartitionedDataEvolutionTable(String tableName) {
        Map<String, String> options =
                new java.util.HashMap<String, String>() {
                    {
                        put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
                        put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
                        put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
                        put(CoreOptions.BUCKET.key(), "-1");
                    }
                };
        sEnv.executeSql(
                buildDdl(
                        tableName,
                        Arrays.asList("id INT", "name STRING"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options));
    }

    private void createUnpartitionedUrlTable(String tableName) {
        Map<String, String> options =
                new java.util.HashMap<String, String>() {
                    {
                        put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
                        put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
                        put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
                        put(CoreOptions.BUCKET.key(), "-1");
                    }
                };
        sEnv.executeSql(
                buildDdl(
                        tableName,
                        Arrays.asList("url STRING", "payload STRING"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options));
    }

    private DeleteAction action(String tableName, String filter, int sinkParallelism) {
        return createAction(
                DeleteAction.class,
                "delete",
                "--warehouse",
                warehouse,
                "--database",
                database,
                "--table",
                tableName,
                "--where",
                filter,
                "--sink_parallelism",
                String.valueOf(sinkParallelism));
    }

    private DeleteAction actionWithSourceSqls(
            String tableName,
            String filter,
            int sinkParallelism,
            String firstSourceSql,
            String secondSourceSql) {
        return createAction(
                DeleteAction.class,
                "delete",
                "--warehouse",
                warehouse,
                "--database",
                database,
                "--table",
                tableName,
                "--source_sql",
                firstSourceSql,
                "--source_sql",
                secondSourceSql,
                "--where",
                filter,
                "--sink_parallelism",
                String.valueOf(sinkParallelism));
    }

    private static List<String> plannedFiles(FileStoreTable table) {
        return table.store().newScan().plan().files().stream()
                .map(entry -> entry.file().fileName())
                .sorted()
                .collect(Collectors.toList());
    }

    private static long deletionVectorCardinality(FileStoreTable table) {
        Snapshot snapshot = table.latestSnapshot().get();
        return table.store().newIndexFileHandler().scan(snapshot, DELETION_VECTORS_INDEX).stream()
                .map(IndexManifestEntry::indexFile)
                .filter(index -> index.dvRanges() != null)
                .flatMap(index -> index.dvRanges().values().stream())
                .mapToLong(DeletionVectorMeta::cardinality)
                .sum();
    }

    private static void assertDeleteSnapshotAndFiles(
            FileStoreTable table, List<String> originalFiles, long expectedDeletedRows) {
        assertThat(table.latestSnapshot().get().operation()).isEqualTo(Snapshot.Operation.DELETE);
        // This list contains both normal data files and dedicated BLOB files.
        assertThat(plannedFiles(table)).containsExactlyElementsOf(originalFiles);
        assertThat(deletionVectorCardinality(table)).isEqualTo(expectedDeletedRows);
    }
}
