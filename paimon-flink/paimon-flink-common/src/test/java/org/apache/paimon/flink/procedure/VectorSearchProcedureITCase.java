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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexer;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
        sql(
                "CREATE TABLE %s ("
                        + "id INT, "
                        + "vec ARRAY<FLOAT>, "
                        + "PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + "'bucket' = '2', "
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

    private void buildAndCommitVectorIndex(FileStoreTable table, float[][] vectors)
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

        Range rowRange = new Range(0, vectors.length - 1);
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
}
