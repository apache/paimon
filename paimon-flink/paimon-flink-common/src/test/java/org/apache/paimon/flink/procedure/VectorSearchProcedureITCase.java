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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexMultiColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexer;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PostponeUtils;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.IndexVectorSearchSplit;
import org.apache.paimon.table.source.RawVectorSearchSplit;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TAG_NAME;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_VERSION;
import static org.apache.paimon.CoreOptions.SCAN_WATERMARK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link VectorSearchProcedure}. */
public class VectorSearchProcedureITCase extends CatalogITCaseBase {

    private static final String VECTOR_FIELD = "vec";
    private static final int DIMENSION = 2;

    @Test
    public void testPrimaryKeyVectorSearch() throws Exception {
        createPrimaryKeyVectorTable("PK_T");

        sql(
                "INSERT INTO PK_T VALUES "
                        + "(1, ARRAY[CAST(3.0 AS FLOAT), CAST(0.0 AS FLOAT)]), "
                        + "(2, ARRAY[CAST(1.0 AS FLOAT), CAST(0.0 AS FLOAT)]), "
                        + "(3, ARRAY[CAST(2.0 AS FLOAT), CAST(0.0 AS FLOAT)])");

        List<Row> result = searchPrimaryKeyVectorTable("PK_T", 2, "id");

        assertThat(result)
                .extracting(row -> row.getField(0).toString())
                .containsExactlyInAnyOrder("{\"id\":\"2\"}", "{\"id\":\"3\"}");
    }

    @Test
    public void testEmptyTableVectorSearch() {
        createVectorTable("EMPTY_T");
        createPrimaryKeyVectorTable("EMPTY_PK_T");

        assertThat(search("EMPTY_T", "0.0,0.0", 1, "id", null)).isEmpty();
        assertThat(searchPrimaryKeyVectorTable("EMPTY_PK_T", 1, "id")).isEmpty();
    }

    @Test
    public void testResolveAndPinSnapshotNormalizesTimeTravelOptions() throws Exception {
        createVectorTable("TIME_TRAVEL_T");
        FileStoreTable table = paimonTable("TIME_TRAVEL_T");
        writeVectors(table, new float[][] {{1.0f, 0.0f}});
        Snapshot snapshot = table.latestSnapshot().get();

        List<Map<String, String>> selectors =
                Arrays.asList(
                        Collections.singletonMap(SCAN_VERSION.key(), String.valueOf(snapshot.id())),
                        Collections.singletonMap(
                                SCAN_TIMESTAMP_MILLIS.key(),
                                String.valueOf(snapshot.timeMillis() + 1)));
        for (Map<String, String> selector : selectors) {
            FileStoreTable timeTravelTable = table.copy(selector);
            FileStoreTable pinned = VectorSearchProcedure.resolveAndPinSnapshot(timeTravelTable);

            assertThat(pinned).isNotNull();
            CoreOptions pinnedOptions = pinned.coreOptions();
            assertThat(pinnedOptions.scanSnapshotId()).isEqualTo(snapshot.id());
            assertThat(pinnedOptions.startupMode())
                    .isEqualTo(CoreOptions.StartupMode.FROM_SNAPSHOT);
            assertThat(pinnedOptions.toConfiguration().contains(SCAN_VERSION)).isFalse();
            assertThat(pinnedOptions.toConfiguration().contains(SCAN_TAG_NAME)).isFalse();
            assertThat(pinnedOptions.toConfiguration().contains(SCAN_WATERMARK)).isFalse();
            assertThat(pinnedOptions.toConfiguration().contains(SCAN_TIMESTAMP)).isFalse();
            assertThat(pinnedOptions.toConfiguration().contains(SCAN_TIMESTAMP_MILLIS)).isFalse();
            assertThat(pinnedOptions.toConfiguration().contains(SCAN_SNAPSHOT_ID)).isTrue();
        }
    }

    @Test
    public void testVectorSearchKeepsResolvedSchemaWhenPinningSnapshot() throws Exception {
        createVectorTable("SCHEMA_EVOLUTION_T", "'global-index.search-mode' = 'full'");
        FileStoreTable table = paimonTable("SCHEMA_EVOLUTION_T");
        float[][] vectors = {{0.0f, 0.0f}, {1.0f, 0.0f}};
        writeVectors(table, vectors);
        buildAndCommitVectorIndex(table, vectors);

        sql("ALTER TABLE SCHEMA_EVOLUTION_T ADD payload STRING FIRST");
        FileStoreTable evolvedTable = paimonTable("SCHEMA_EVOLUTION_T");
        assertThat(evolvedTable.schema().id())
                .isNotEqualTo(evolvedTable.latestSnapshot().get().schemaId());

        FileStoreTable pinnedTable = VectorSearchProcedure.resolveAndPinSnapshot(evolvedTable);
        assertThat(pinnedTable).isNotNull();
        assertThat(pinnedTable.schema().id()).isEqualTo(evolvedTable.schema().id());

        List<String> defaultProjection =
                sql(
                                "CALL sys.vector_search("
                                        + "`table` => 'default.SCHEMA_EVOLUTION_T', "
                                        + "vector_column => 'vec', "
                                        + "query_vector => '0.0,0.0', "
                                        + "top_k => 1)")
                        .stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());
        assertThat(defaultProjection).hasSize(1);
        assertThat(defaultProjection.get(0))
                .contains("\"payload\":null", "\"id\":\"0\"", "\"vec\"");

        assertThat(search("SCHEMA_EVOLUTION_T", "0.0,0.0", 1, "payload,id", null))
                .extracting(row -> row.getField(0).toString())
                .containsExactly("{\"payload\":null,\"id\":\"0\"}");

        List<String> filtered =
                sql(
                                "CALL sys.vector_search("
                                        + "`table` => 'default.SCHEMA_EVOLUTION_T', "
                                        + "vector_column => 'vec', "
                                        + "query_vector => '0.0,0.0', "
                                        + "top_k => 1, "
                                        + "projection => 'payload,id', "
                                        + "`where` => 'payload IS NULL AND id = 1')")
                        .stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());
        assertThat(filtered).containsExactly("{\"payload\":null,\"id\":\"1\"}");
    }

    @Test
    public void testDistributedPrimaryKeyVectorSearch() {
        createPrimaryKeyVectorTable(
                "DISTRIBUTED_PK_T",
                4,
                "'vector-search.distribute.enabled' = 'true', "
                        + "'global-index.thread-num' = '2'");
        String values =
                IntStream.range(0, 32)
                        .mapToObj(
                                i ->
                                        String.format(
                                                "(%d, ARRAY[CAST(%d.0 AS FLOAT), CAST(0.0 AS FLOAT)])",
                                                i, i))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO DISTRIBUTED_PK_T VALUES " + values);
        assertThat(sql("SELECT DISTINCT bucket FROM `DISTRIBUTED_PK_T$files`")).hasSize(4);

        List<String> rows =
                searchPrimaryKeyVectorTable("DISTRIBUTED_PK_T", 3, "id,__paimon_search_score")
                        .stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0)).contains("\"id\":\"0\"");
        assertThat(rows.get(1)).contains("\"id\":\"1\"");
        assertThat(rows.get(2)).contains("\"id\":\"2\"");
        assertThat(rows).allMatch(row -> row.contains("\"__paimon_search_score\""));

        List<String> scoreOnlyRows =
                searchPrimaryKeyVectorTable("DISTRIBUTED_PK_T", 2, "__paimon_search_score").stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());
        assertThat(scoreOnlyRows).hasSize(2);
        assertThat(scoreOnlyRows)
                .allMatch(
                        row ->
                                row.startsWith("{\"__paimon_search_score\":")
                                        && row.endsWith("}")
                                        && !row.contains("\"id\"")
                                        && !row.contains("\"vec\""));
    }

    @Test
    public void testDistributedPrimaryKeyVectorSearchWithResidualFilter() {
        createPrimaryKeyVectorTableWithPayload(
                "DISTRIBUTED_FILTERED_PK_T",
                4,
                "'vector-search.distribute.enabled' = 'true', "
                        + "'global-index.thread-num' = '2'");
        String values =
                IntStream.rangeClosed(1, 16)
                        .mapToObj(
                                i ->
                                        String.format(
                                                "(%d, '%s', ARRAY[CAST(%d.0 AS FLOAT), CAST(0.0 AS FLOAT)])",
                                                i, i == 1 ? "drop" : "keep", i))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO DISTRIBUTED_FILTERED_PK_T VALUES " + values);
        assertThat(sql("SELECT DISTINCT bucket FROM `DISTRIBUTED_FILTERED_PK_T$files`")).hasSize(4);

        List<String> rows =
                sql(
                                "CALL sys.vector_search("
                                        + "`table` => 'default.DISTRIBUTED_FILTERED_PK_T', "
                                        + "vector_column => 'vec', "
                                        + "query_vector => '0.0,0.0', "
                                        + "top_k => 2, "
                                        + "projection => 'id', "
                                        + "`where` => 'payload = ''keep''')")
                        .stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());

        assertThat(rows).containsExactly("{\"id\":\"2\"}", "{\"id\":\"3\"}");
    }

    @Test
    public void testPostponeBucketBuildsVectorIndexDuringCompact() throws Exception {
        createPostponePrimaryKeyVectorTable("POSTPONE_PK_T");

        sql(
                "INSERT INTO POSTPONE_PK_T VALUES "
                        + "(1, ARRAY[CAST(3.0 AS FLOAT), CAST(0.0 AS FLOAT)]), "
                        + "(2, ARRAY[CAST(1.0 AS FLOAT), CAST(0.0 AS FLOAT)]), "
                        + "(3, ARRAY[CAST(2.0 AS FLOAT), CAST(0.0 AS FLOAT)])");

        assertThat(sql("SELECT * FROM POSTPONE_PK_T")).isEmpty();
        assertThat(sql("SELECT bucket FROM `POSTPONE_PK_T$buckets`"))
                .allMatch(row -> !Integer.valueOf(-2).equals(row.getField(0)));
        assertThat(sql("SELECT file_path FROM `POSTPONE_PK_T$files` WHERE level = 0")).isNotEmpty();
        FileStoreTable table = paimonTable("POSTPONE_PK_T");
        long snapshotId = table.latestSnapshot().get().id();
        assertThat(PostponeUtils.getLevel0Buckets(table, snapshotId)).isNotEmpty();

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql("CALL sys.compact(`table` => 'default.POSTPONE_PK_T')");

        assertThat(paimonTable("POSTPONE_PK_T").latestSnapshot().get().id())
                .isGreaterThan(snapshotId);
        assertThat(sql("SELECT id FROM POSTPONE_PK_T"))
                .extracting(row -> row.getField(0))
                .containsExactlyInAnyOrder(1, 2, 3);
        assertThat(searchPrimaryKeyVectorTable("POSTPONE_PK_T", 2, "id"))
                .extracting(row -> row.getField(0).toString())
                .containsExactlyInAnyOrder("{\"id\":\"2\"}", "{\"id\":\"3\"}");

        sql(
                "INSERT INTO POSTPONE_PK_T VALUES "
                        + "(4, ARRAY[CAST(0.5 AS FLOAT), CAST(0.0 AS FLOAT)]), "
                        + "(6, ARRAY[CAST(6.0 AS FLOAT), CAST(0.0 AS FLOAT)])");
        sql(
                "INSERT INTO POSTPONE_PK_T /*+ OPTIONS('postpone.batch-write-fixed-bucket' = 'false') */ VALUES "
                        + "(5, ARRAY[CAST(1.5 AS FLOAT), CAST(0.0 AS FLOAT)]), "
                        + "(7, ARRAY[CAST(7.0 AS FLOAT), CAST(0.0 AS FLOAT)])");
        assertThat(sql("SELECT id FROM POSTPONE_PK_T"))
                .extracting(row -> row.getField(0))
                .containsExactlyInAnyOrder(1, 2, 3);
        assertThat(sql("SELECT bucket FROM `POSTPONE_PK_T$buckets`"))
                .extracting(row -> row.getField(0))
                .contains(-2);
        assertThat(sql("SELECT file_path FROM `POSTPONE_PK_T$files` WHERE level = 0")).isNotEmpty();

        sql("CALL sys.compact(`table` => 'default.POSTPONE_PK_T')");

        assertThat(sql("SELECT id FROM POSTPONE_PK_T"))
                .extracting(row -> row.getField(0))
                .containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7);
        assertThat(sql("SELECT bucket FROM `POSTPONE_PK_T$buckets`"))
                .allMatch(row -> !Integer.valueOf(-2).equals(row.getField(0)));
        assertThat(sql("SELECT file_path FROM `POSTPONE_PK_T$files` WHERE level = 0")).isEmpty();
        assertThat(searchPrimaryKeyVectorTable("POSTPONE_PK_T", 2, "id"))
                .extracting(row -> row.getField(0).toString())
                .containsExactlyInAnyOrder("{\"id\":\"4\"}", "{\"id\":\"2\"}");
    }

    @Test
    public void testPrimaryKeyVectorSearchAfterUpdateAndDelete() throws Exception {
        createPrimaryKeyVectorTable("PK_UPDATE_T");

        sql(
                "INSERT INTO PK_UPDATE_T VALUES "
                        + "(1, ARRAY[CAST(3.0 AS FLOAT), CAST(0.0 AS FLOAT)]), "
                        + "(2, ARRAY[CAST(1.0 AS FLOAT), CAST(0.0 AS FLOAT)])");
        sql(
                "INSERT INTO PK_UPDATE_T VALUES "
                        + "(1, ARRAY[CAST(0.5 AS FLOAT), CAST(0.0 AS FLOAT)])");

        List<Row> updated = searchPrimaryKeyVectorTable("PK_UPDATE_T", 1, "id");
        assertThat(updated)
                .extracting(row -> row.getField(0).toString())
                .containsExactly("{\"id\":\"1\"}");

        sql("DELETE FROM PK_UPDATE_T WHERE id = 1");

        List<Row> afterDelete = searchPrimaryKeyVectorTable("PK_UPDATE_T", 1, "id");
        assertThat(afterDelete)
                .extracting(row -> row.getField(0).toString())
                .containsExactly("{\"id\":\"2\"}");
    }

    @Test
    public void testPartialUpdatePrimaryKeyVectorSearch() throws Exception {
        createPartialUpdatePrimaryKeyVectorTable("PK_PARTIAL_T");

        sql(
                "INSERT INTO PK_PARTIAL_T VALUES "
                        + "(1, 'keep', ARRAY[CAST(3.0 AS FLOAT), CAST(0.0 AS FLOAT)]), "
                        + "(2, 'other', ARRAY[CAST(1.0 AS FLOAT), CAST(0.0 AS FLOAT)])");
        sql(
                "INSERT INTO PK_PARTIAL_T (id, vec) VALUES "
                        + "(1, ARRAY[CAST(0.5 AS FLOAT), CAST(0.0 AS FLOAT)])");

        List<Row> result = searchPrimaryKeyVectorTable("PK_PARTIAL_T", 1, "id,payload");

        assertThat(result)
                .extracting(row -> row.getField(0).toString())
                .containsExactly("{\"id\":\"1\",\"payload\":\"keep\"}");
    }

    @Test
    public void testVectorSearchBasic() throws Exception {
        createVectorTable("T");
        FileStoreTable table = paimonTable("T");

        float[][] vectors = {
            {1.0f, 0.0f}, // row 0
            {0.95f, 0.1f}, // row 1
            {0.1f, 0.95f}, // row 2
            {0.98f, 0.05f}, // row 3
            {0.0f, 1.0f}, // row 4
            {0.05f, 0.98f} // row 5
        };

        writeVectors(table, vectors);
        buildAndCommitVectorIndex(table, vectors);

        // Search for vectors close to (1.0, 0.0)
        List<Row> result =
                sql(
                        "CALL sys.vector_search("
                                + "`table` => 'default.T', "
                                + "vector_column => 'vec', "
                                + "query_vector => '1.0 ,0.0', "
                                + "top_k => 3)");

        assertThat(result).isNotEmpty();
        assertThat(result.size()).isLessThanOrEqualTo(3);

        // Verify results contain JSON strings
        for (Row row : result) {
            String json = row.getField(0).toString();
            assertThat(json).contains("\"id\"");
            assertThat(json).contains("\"vec\"");
        }
    }

    @Test
    public void testVectorSearchWithProjection() throws Exception {
        createVectorTable("T2");
        FileStoreTable table = paimonTable("T2");

        float[][] vectors = {
            {1.0f, 0.0f}, // row 0
            {0.0f, 1.0f}, // row 1
        };

        writeVectors(table, vectors);
        buildAndCommitVectorIndex(table, vectors);

        List<Row> result =
                sql(
                        "CALL sys.vector_search("
                                + "`table` => 'default.T2', "
                                + "vector_column => 'vec', "
                                + "query_vector => '1.0,0.0', "
                                + "top_k => 2, "
                                + "projection => 'id')");

        assertThat(result).isNotEmpty();
        assertThat(result.size()).isLessThanOrEqualTo(2);

        for (Row row : result) {
            String json = row.getField(0).toString();
            assertThat(json).contains("\"id\"");
            // projection only selects 'id', so 'vec' should not appear
            assertThat(json).doesNotContain("\"vec\"");
        }
    }

    @Test
    public void testVectorSearchTopK() throws Exception {
        createVectorTable("T3");
        FileStoreTable table = paimonTable("T3");

        float[][] vectors = new float[10][];
        for (int i = 0; i < 10; i++) {
            vectors[i] = new float[] {(float) Math.cos(i * 0.3), (float) Math.sin(i * 0.3)};
        }

        writeVectors(table, vectors);
        buildAndCommitVectorIndex(table, vectors);

        List<Row> result =
                sql(
                        "CALL sys.vector_search("
                                + "`table` => 'default.T3', "
                                + "vector_column => 'vec', "
                                + "query_vector => '1.0,0.0', "
                                + "top_k => 3)");

        assertThat(result.size()).isLessThanOrEqualTo(3);
    }

    @Test
    public void testVectorSearchWithOptions() throws Exception {
        createVectorTable(
                "T4",
                "'"
                        + TestVectorGlobalIndexer.OPT_REQUIRED_OPTION_KEY
                        + "' = 'ivf.nprobe', "
                        + "'"
                        + TestVectorGlobalIndexer.OPT_REQUIRED_OPTION_VALUE
                        + "' = '16'");
        FileStoreTable table = paimonTable("T4");

        float[][] vectors = {
            {1.0f, 0.0f}, // row 0
            {0.0f, 1.0f}, // row 1
        };

        writeVectors(table, vectors);
        buildAndCommitVectorIndex(table, vectors);

        List<Row> result =
                sql(
                        "CALL sys.vector_search("
                                + "`table` => 'default.T4', "
                                + "vector_column => 'vec', "
                                + "query_vector => '1.0,0.0', "
                                + "top_k => 2, "
                                + "options => 'ivf.nprobe=16')");

        assertThat(result).isNotEmpty();
        assertThat(result.size()).isLessThanOrEqualTo(2);
    }

    @Test
    public void testDistributedVectorSearchWithFilterAndScore() throws Exception {
        createVectorTable(
                "DISTRIBUTED_T",
                "'vector-search.distribute.enabled' = 'true', "
                        + "'global-index.thread-num' = '2', "
                        + "'global-index.search-mode' = 'full'");
        FileStoreTable table = paimonTable("DISTRIBUTED_T");
        float[][] vectors = {
            {0.0f, 0.0f},
            {1.0f, 0.0f},
            {2.0f, 0.0f},
            {3.0f, 0.0f},
            {4.0f, 0.0f},
            {5.0f, 0.0f}
        };
        writeVectors(table, vectors);

        List<String> rows =
                sql(
                                "CALL sys.vector_search("
                                        + "`table` => 'default.DISTRIBUTED_T', "
                                        + "vector_column => 'vec', "
                                        + "query_vector => '0.0,0.0', "
                                        + "top_k => 2, "
                                        + "projection => 'vec,__paimon_search_score', "
                                        + "`where` => 'id >= 3')")
                        .stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0)).contains("3.0");
        assertThat(rows.get(1)).contains("4.0");
        assertThat(rows)
                .allMatch(
                        row ->
                                row.contains("\"vec\"")
                                        && row.contains("\"__paimon_search_score\"")
                                        && !row.contains("\"id\""));

        List<String> scoreOnlyRows =
                search("DISTRIBUTED_T", "0.0,0.0", 2, "__paimon_search_score", null).stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());
        assertThat(scoreOnlyRows).hasSize(2);
        assertThat(scoreOnlyRows)
                .allMatch(
                        row ->
                                row.startsWith("{\"__paimon_search_score\":")
                                        && row.endsWith("}")
                                        && !row.contains("\"id\"")
                                        && !row.contains("\"vec\""));
    }

    @Test
    public void testDistributedMultiFieldVectorIndex() throws Exception {
        createVectorTable(
                "DISTRIBUTED_MULTI_FIELD_T",
                "'vector-search.distribute.enabled' = 'true', "
                        + "'global-index.thread-num' = '2', "
                        + "'global-index.search-mode' = 'full'");
        FileStoreTable table = paimonTable("DISTRIBUTED_MULTI_FIELD_T");
        float[][] vectors = {
            {0.0f, 0.0f},
            {1.0f, 0.0f},
            {2.0f, 0.0f},
            {3.0f, 0.0f},
            {4.0f, 0.0f},
            {5.0f, 0.0f},
            {6.0f, 0.0f},
            {7.0f, 0.0f}
        };
        writeVectors(table, vectors);
        for (int start = 0; start < vectors.length; start += 2) {
            buildAndCommitMultiFieldVectorIndex(
                    table,
                    new float[][] {vectors[start], vectors[start + 1]},
                    new Range(start, start + 1));
        }

        VectorScan.Plan plan =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD)
                        .newVectorScan()
                        .scan();
        List<IndexVectorSearchSplit> indexSplits =
                plan.splits().stream()
                        .filter(IndexVectorSearchSplit.class::isInstance)
                        .map(IndexVectorSearchSplit.class::cast)
                        .collect(Collectors.toList());
        assertThat(indexSplits).hasSize(4);
        assertThat(indexSplits).allMatch(split -> split.vectorIndexFiles().size() == 2);

        List<String> rows =
                sql(
                                "CALL sys.vector_search("
                                        + "`table` => 'default.DISTRIBUTED_MULTI_FIELD_T', "
                                        + "vector_column => 'vec', "
                                        + "query_vector => '0.0,0.0', "
                                        + "top_k => 2, "
                                        + "projection => 'id', "
                                        + "`where` => 'id >= 5')")
                        .stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());

        assertThat(rows).containsExactly("{\"id\":\"5\"}", "{\"id\":\"6\"}");
    }

    @Test
    public void testDistributedMixedIndexedAndRawVectorSearch() throws Exception {
        createVectorTable(
                "DISTRIBUTED_MIXED_T",
                "'vector-search.distribute.enabled' = 'true', "
                        + "'global-index.thread-num' = '2', "
                        + "'global-index.search-mode' = 'full'");
        FileStoreTable table = paimonTable("DISTRIBUTED_MIXED_T");
        float[][] vectors = {
            {20.0f, 0.0f},
            {21.0f, 0.0f},
            {22.0f, 0.0f},
            {23.0f, 0.0f},
            {24.0f, 0.0f},
            {25.0f, 0.0f},
            {26.0f, 0.0f},
            {27.0f, 0.0f},
            {0.0f, 0.0f},
            {1.0f, 0.0f},
            {2.0f, 0.0f},
            {3.0f, 0.0f}
        };
        writeVectors(table, vectors);
        for (int start = 0; start < 8; start += 2) {
            buildAndCommitVectorIndex(
                    table,
                    new float[][] {vectors[start], vectors[start + 1]},
                    new Range(start, start + 1));
        }

        VectorScan.Plan plan =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.0f, 0.0f})
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD)
                        .newVectorScan()
                        .scan();
        assertThat(plan.splits().stream().filter(IndexVectorSearchSplit.class::isInstance))
                .hasSize(4);
        assertThat(plan.splits().stream().filter(RawVectorSearchSplit.class::isInstance))
                .hasSize(1);

        List<String> rows =
                search("DISTRIBUTED_MIXED_T", "0.0,0.0", 3, "id", null).stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());

        assertThat(rows).containsExactly("{\"id\":\"8\"}", "{\"id\":\"9\"}", "{\"id\":\"10\"}");
    }

    @Test
    public void testDistributedMixedIndexedAndRawVectorSearchWithFilter() throws Exception {
        createVectorTable(
                "DISTRIBUTED_FILTERED_MIXED_T",
                "'vector-search.distribute.enabled' = 'true', "
                        + "'global-index.thread-num' = '2', "
                        + "'global-index.search-mode' = 'full'");
        FileStoreTable table = paimonTable("DISTRIBUTED_FILTERED_MIXED_T");
        float[][] vectors = {
            {0.0f, 0.0f},
            {0.1f, 0.0f},
            {0.2f, 0.0f},
            {0.3f, 0.0f},
            {0.4f, 0.0f},
            {1.0f, 0.0f},
            {4.0f, 0.0f},
            {5.0f, 0.0f},
            {0.5f, 0.0f},
            {2.0f, 0.0f},
            {0.05f, 0.0f},
            {0.15f, 0.0f}
        };
        writeVectors(table, vectors);
        for (int start = 0; start < 8; start += 2) {
            buildAndCommitMultiFieldVectorIndex(
                    table,
                    new float[][] {vectors[start], vectors[start + 1]},
                    new Range(start, start + 1));
        }

        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        Predicate filter =
                PredicateBuilder.and(
                        predicateBuilder.greaterOrEqual(0, 5), predicateBuilder.lessThan(0, 10));
        VectorScan.Plan plan =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.0f, 0.0f})
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD)
                        .withFilter(filter)
                        .newVectorScan()
                        .scan();
        List<IndexVectorSearchSplit> indexSplits =
                plan.splits().stream()
                        .filter(IndexVectorSearchSplit.class::isInstance)
                        .map(IndexVectorSearchSplit.class::cast)
                        .collect(Collectors.toList());
        assertThat(indexSplits).hasSize(4);
        assertThat(indexSplits)
                .allSatisfy(
                        split -> {
                            assertThat(split.vectorIndexFiles()).hasSize(2);
                            assertThat(split.scalarIndexFiles())
                                    .containsExactlyElementsOf(split.vectorIndexFiles());
                        });
        List<RawVectorSearchSplit> rawSplits =
                plan.splits().stream()
                        .filter(RawVectorSearchSplit.class::isInstance)
                        .map(RawVectorSearchSplit.class::cast)
                        .collect(Collectors.toList());
        assertThat(rawSplits).hasSize(1);
        assertThat(rawSplits.get(0).rowRanges()).containsExactly(new Range(8, 11));
        assertThat(rawSplits.get(0).scalarIndexFiles()).isEmpty();

        List<String> rows =
                sql(
                                "CALL sys.vector_search("
                                        + "`table` => 'default.DISTRIBUTED_FILTERED_MIXED_T', "
                                        + "vector_column => 'vec', "
                                        + "query_vector => '0.0,0.0', "
                                        + "top_k => 3, "
                                        + "projection => 'id', "
                                        + "`where` => 'id >= 5 AND id < 10')")
                        .stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());

        assertThat(rows).containsExactly("{\"id\":\"8\"}", "{\"id\":\"5\"}", "{\"id\":\"9\"}");
    }

    @Test
    public void testVectorSearchWithPartitions() throws Exception {
        createPartitionedVectorTable("PARTITIONED_T");
        FileStoreTable table = paimonTable("PARTITIONED_T");
        writePartitionedVectors(table);

        List<Row> result = searchWithFilters("PARTITIONED_T", 1, null, "pt=b");

        assertThat(result)
                .extracting(row -> row.getField(0).toString())
                .containsExactly("{\"id\":\"2\"}");

        assertThat(searchWithFilters("PARTITIONED_T", 1, "pt = 'a'", "pt=b")).isEmpty();
        assertThat(searchWithFilters("PARTITIONED_T", 1, "pt = 'b'", "pt=b"))
                .extracting(row -> row.getField(0).toString())
                .containsExactly("{\"id\":\"2\"}");
    }

    @Test
    public void testVectorSearchRejectsInvalidPartitions() {
        createPartitionedVectorTable("INVALID_PARTITIONS_T");

        assertThat(searchWithFilters("INVALID_PARTITIONS_T", 1, null, "   ")).isEmpty();
        assertThat(searchWithFilters("INVALID_PARTITIONS_T", 1, null, "pt=")).isEmpty();
        String separator = ";";
        for (String partitions :
                Arrays.asList(
                        separator,
                        separator + separator,
                        "pt=a" + separator,
                        separator + "pt=a",
                        "pt=a" + separator + separator + "pt=b")) {
            assertThatThrownBy(() -> searchWithFilters("INVALID_PARTITIONS_T", 1, null, partitions))
                    .hasStackTraceContaining("Partition spec must not be blank");
        }
        assertThatThrownBy(() -> searchWithFilters("INVALID_PARTITIONS_T", 1, null, "pt=a,pt=b"))
                .hasStackTraceContaining("Duplicate partition key 'pt'");
    }

    @Test
    public void testFirstRowPrimaryKeyVectorSearchWhereBoundary() {
        createFirstRowPartitionedPrimaryKeyVectorTable("FIRST_ROW_PK_T");
        sql(
                "INSERT INTO FIRST_ROW_PK_T VALUES "
                        + "(1, 'a', ARRAY[CAST(1.0 AS FLOAT), CAST(0.0 AS FLOAT)]), "
                        + "(2, 'b', ARRAY[CAST(2.0 AS FLOAT), CAST(0.0 AS FLOAT)]), "
                        + "(3, 'b', ARRAY[CAST(3.0 AS FLOAT), CAST(0.0 AS FLOAT)])");

        assertThat(searchWithFilters("FIRST_ROW_PK_T", 1, "pt = 'b'", null))
                .extracting(row -> row.getField(0).toString())
                .containsExactly("{\"id\":\"2\"}");
        assertThatThrownBy(() -> searchWithFilters("FIRST_ROW_PK_T", 1, "id >= 2", null))
                .hasStackTraceContaining(
                        "Primary-key vector where filter on non-partition columns requires")
                .hasStackTraceContaining("deletion-vectors.enabled = true");
    }

    @Test
    public void testVectorSearchValidation() {
        createVectorTable("VALIDATION_T");
        createVectorTable("QUERY_AUTH_T", "'query-auth.enabled' = 'true'");
        createVectorTable(
                "DISTRIBUTED_QUERY_AUTH_T",
                "'query-auth.enabled' = 'true', " + "'vector-search.distribute.enabled' = 'true'");

        assertThatThrownBy(() -> search("VALIDATION_T", "0.0,0.0", 0, "id", null))
                .hasStackTraceContaining("top_k must be positive");
        assertThatThrownBy(() -> search("VALIDATION_T", "NaN,0.0", 1, "id", null))
                .hasStackTraceContaining("query_vector values must be finite");
        assertThatThrownBy(() -> search("VALIDATION_T", "0.0,0.0", 1, "missing", null))
                .hasStackTraceContaining("Unknown projection column");
        assertThatThrownBy(() -> search("VALIDATION_T", "0.0,0.0", 1, "id,id", null))
                .hasStackTraceContaining("Duplicate projection column");
        assertThatThrownBy(
                        () ->
                                search(
                                        "VALIDATION_T",
                                        "0.0,0.0",
                                        1,
                                        "id",
                                        "query-auth.enabled=false"))
                .hasStackTraceContaining("Option 'query-auth.enabled' is not allowed");
        assertThatThrownBy(() -> search("QUERY_AUTH_T", "0.0,0.0", 1, "id", null))
                .hasStackTraceContaining(
                        "Vector search does not support tables with query auth enabled");
        assertThatThrownBy(() -> search("DISTRIBUTED_QUERY_AUTH_T", "0.0,0.0", 1, "id", null))
                .hasStackTraceContaining(
                        "Vector search does not support tables with query auth enabled");
    }

    private void createVectorTable(String tableName) {
        createVectorTable(tableName, "");
    }

    private void createVectorTable(String tableName, String extraOptions) {
        String formattedExtraOptions = extraOptions.isEmpty() ? "" : ", " + extraOptions;
        sql(
                "CREATE TABLE %s ("
                        + "id INT, "
                        + "vec ARRAY<FLOAT>"
                        + ") WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'test.vector.dimension' = '%d', "
                        + "'test.vector.metric' = 'l2'"
                        + "%s"
                        + ")",
                tableName, DIMENSION, formattedExtraOptions);
    }

    private void createPrimaryKeyVectorTable(String tableName) {
        createPrimaryKeyVectorTable(tableName, 2, "");
    }

    private void createPrimaryKeyVectorTable(String tableName, int bucket, String extraOptions) {
        String formattedExtraOptions = extraOptions.isEmpty() ? "" : ", " + extraOptions;
        sql(
                "CREATE TABLE %s ("
                        + "id INT, "
                        + "vec ARRAY<FLOAT>, "
                        + "PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + "'bucket' = '%d', "
                        + "'file.format' = 'json', "
                        + "'file.compression' = 'none', "
                        + "'deletion-vectors.enabled' = 'true', "
                        + "'vector-field' = 'vec', "
                        + "'field.vec.vector-dim' = '%d', "
                        + "'pk-vector.index.columns' = 'vec', "
                        + "'fields.vec.pk-vector.index.type' = '%s', "
                        + "'fields.vec.pk-vector.distance.metric' = 'l2', "
                        + "'test.vector.dimension' = '%d', "
                        + "'test.vector.metric' = 'l2'"
                        + "%s"
                        + ")",
                tableName,
                bucket,
                DIMENSION,
                TestVectorGlobalIndexerFactory.IDENTIFIER,
                DIMENSION,
                formattedExtraOptions);
    }

    private void createPrimaryKeyVectorTableWithPayload(
            String tableName, int bucket, String extraOptions) {
        String formattedExtraOptions = extraOptions.isEmpty() ? "" : ", " + extraOptions;
        sql(
                "CREATE TABLE %s ("
                        + "id INT, "
                        + "payload STRING, "
                        + "vec ARRAY<FLOAT>, "
                        + "PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + "'bucket' = '%d', "
                        + "'file.format' = 'json', "
                        + "'file.compression' = 'none', "
                        + "'deletion-vectors.enabled' = 'true', "
                        + "'vector-field' = 'vec', "
                        + "'field.vec.vector-dim' = '%d', "
                        + "'pk-vector.index.columns' = 'vec', "
                        + "'fields.vec.pk-vector.index.type' = '%s', "
                        + "'fields.vec.pk-vector.distance.metric' = 'l2', "
                        + "'test.vector.dimension' = '%d', "
                        + "'test.vector.metric' = 'l2'"
                        + "%s"
                        + ")",
                tableName,
                bucket,
                DIMENSION,
                TestVectorGlobalIndexerFactory.IDENTIFIER,
                DIMENSION,
                formattedExtraOptions);
    }

    private void createFirstRowPartitionedPrimaryKeyVectorTable(String tableName) {
        sql(
                "CREATE TABLE %s ("
                        + "id INT, "
                        + "pt STRING, "
                        + "vec ARRAY<FLOAT>, "
                        + "PRIMARY KEY (id, pt) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + "'bucket' = '2', "
                        + "'merge-engine' = 'first-row', "
                        + "'file.format' = 'json', "
                        + "'file.compression' = 'none', "
                        + "'vector-field' = 'vec', "
                        + "'field.vec.vector-dim' = '%d', "
                        + "'pk-vector.index.columns' = 'vec', "
                        + "'fields.vec.pk-vector.index.type' = '%s', "
                        + "'fields.vec.pk-vector.distance.metric' = 'l2', "
                        + "'test.vector.dimension' = '%d', "
                        + "'test.vector.metric' = 'l2'"
                        + ")",
                tableName, DIMENSION, TestVectorGlobalIndexerFactory.IDENTIFIER, DIMENSION);
    }

    private void createPartitionedVectorTable(String tableName) {
        sql(
                "CREATE TABLE %s ("
                        + "id INT, "
                        + "pt STRING, "
                        + "vec ARRAY<FLOAT>"
                        + ") PARTITIONED BY (pt) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'global-index.search-mode' = 'full', "
                        + "'test.vector.dimension' = '%d', "
                        + "'test.vector.metric' = 'l2'"
                        + ")",
                tableName, DIMENSION);
    }

    private void createPostponePrimaryKeyVectorTable(String tableName) {
        sql(
                "CREATE TABLE %s ("
                        + "id INT, "
                        + "vec ARRAY<FLOAT>, "
                        + "PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + "'bucket' = '-2', "
                        + "'write-only' = 'true', "
                        + "'postpone.batch-write-fixed-bucket' = 'true', "
                        + "'file.format' = 'json', "
                        + "'file.compression' = 'deflate', "
                        + "'deletion-vectors.enabled' = 'true', "
                        + "'deletion-vectors.merge-on-read' = 'false', "
                        + "'vector-field' = 'vec', "
                        + "'field.vec.vector-dim' = '%d', "
                        + "'pk-vector.index.columns' = 'vec', "
                        + "'fields.vec.pk-vector.index.type' = '%s', "
                        + "'fields.vec.pk-vector.distance.metric' = 'l2', "
                        + "'test.vector.dimension' = '%d', "
                        + "'test.vector.metric' = 'l2'"
                        + ")",
                tableName, DIMENSION, TestVectorGlobalIndexerFactory.IDENTIFIER, DIMENSION);
    }

    private List<Row> searchPrimaryKeyVectorTable(String tableName, int topK, String projection) {
        return sql(
                "CALL sys.vector_search("
                        + "`table` => 'default.%s', "
                        + "vector_column => 'vec', "
                        + "query_vector => '0.0,0.0', "
                        + "top_k => %d, "
                        + "projection => '%s')",
                tableName, topK, projection);
    }

    private List<Row> search(
            String tableName, String queryVector, int topK, String projection, String options) {
        String optionsArgument = options == null ? "" : ", options => '" + options + "'";
        return sql(
                "CALL sys.vector_search("
                        + "`table` => 'default.%s', "
                        + "vector_column => 'vec', "
                        + "query_vector => '%s', "
                        + "top_k => %d, "
                        + "projection => '%s'%s)",
                tableName, queryVector, topK, projection, optionsArgument);
    }

    private List<Row> searchWithFilters(
            String tableName, int topK, String where, String partitions) {
        String whereArgument =
                where == null ? "" : ", `where` => '" + where.replace("'", "''") + "'";
        String partitionsArgument =
                partitions == null ? "" : ", partitions => '" + partitions.replace("'", "''") + "'";
        return sql(
                "CALL sys.vector_search("
                        + "`table` => 'default.%s', "
                        + "vector_column => 'vec', "
                        + "query_vector => '0.0,0.0', "
                        + "top_k => %d, "
                        + "projection => 'id'%s%s)",
                tableName, topK, whereArgument, partitionsArgument);
    }

    private void createPartialUpdatePrimaryKeyVectorTable(String tableName) {
        sql(
                "CREATE TABLE %s ("
                        + "id INT, "
                        + "payload STRING, "
                        + "vec ARRAY<FLOAT>, "
                        + "PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + "'bucket' = '1', "
                        + "'file.format' = 'json', "
                        + "'file.compression' = 'none', "
                        + "'merge-engine' = 'partial-update', "
                        + "'deletion-vectors.enabled' = 'true', "
                        + "'deletion-vectors.merge-on-read' = 'false', "
                        + "'vector-field' = 'vec', "
                        + "'field.vec.vector-dim' = '%d', "
                        + "'pk-vector.index.columns' = 'vec', "
                        + "'fields.vec.pk-vector.index.type' = '%s', "
                        + "'fields.vec.pk-vector.distance.metric' = 'l2', "
                        + "'test.vector.dimension' = '%d', "
                        + "'test.vector.metric' = 'l2'"
                        + ")",
                tableName, DIMENSION, TestVectorGlobalIndexerFactory.IDENTIFIER, DIMENSION);
    }

    private void writeVectors(FileStoreTable table, float[][] vectors) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < vectors.length; i++) {
                write.write(GenericRow.of(i, new GenericArray(vectors[i])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void writePartitionedVectors(FileStoreTable table) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(
                    GenericRow.of(
                            1,
                            BinaryString.fromString("a"),
                            new GenericArray(new float[] {0.0f, 0.0f})));
            write.write(
                    GenericRow.of(
                            2,
                            BinaryString.fromString("b"),
                            new GenericArray(new float[] {1.0f, 0.0f})));
            write.write(
                    GenericRow.of(
                            3,
                            BinaryString.fromString("b"),
                            new GenericArray(new float[] {2.0f, 0.0f})));
            commit.commit(write.prepareCommit());
        }
    }

    private void buildAndCommitVectorIndex(FileStoreTable table, float[][] vectors)
            throws Exception {
        buildAndCommitVectorIndex(table, vectors, new Range(0, vectors.length - 1));
    }

    private void buildAndCommitVectorIndex(FileStoreTable table, float[][] vectors, Range rowRange)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vectorField = table.rowType().getField(VECTOR_FIELD);

        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (int i = 0; i < vectors.length; i++) {
            writer.write(vectors[i], i);
        }
        List<ResultEntry> entries = writer.finish();

        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        vectorField.id(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void buildAndCommitMultiFieldVectorIndex(
            FileStoreTable table, float[][] vectors, Range rowRange) throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vectorField = table.rowType().getField(VECTOR_FIELD);
        DataField idField = table.rowType().getField("id");

        GlobalIndexMultiColumnWriter writer =
                (GlobalIndexMultiColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                Collections.singletonList(idField),
                                options);
        for (int i = 0; i < vectors.length; i++) {
            writer.write(i, GenericRow.of(new GenericArray(vectors[i]), (int) (rowRange.from + i)));
        }
        List<ResultEntry> entries = writer.finish();

        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        Arrays.asList(vectorField, idField),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }
}
