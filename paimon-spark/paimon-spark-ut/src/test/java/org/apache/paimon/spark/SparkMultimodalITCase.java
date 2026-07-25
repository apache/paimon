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

package org.apache.paimon.spark;

import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.TestHiveMetastore;
import org.apache.paimon.utils.Pair;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Paimon Multimodality type support on Spark. */
public class SparkMultimodalITCase {

    private static TestHiveMetastore testHiveMetastore;
    private static int port;

    @BeforeAll
    public static void startMetastore() {
        testHiveMetastore = new TestHiveMetastore();
        testHiveMetastore.start(0);
        port = testHiveMetastore.getPort();
    }

    @AfterAll
    public static void closeMetastore() throws Exception {
        testHiveMetastore.stop();
    }

    private SparkSession.Builder createSparkSessionBuilder(Path warehousePath) {
        return SparkSession.builder()
                .config("spark.sql.warehouse.dir", warehousePath.toString())
                // with hive metastore
                .config("spark.sql.catalogImplementation", "hive")
                .config("hive.metastore.uris", "thrift://localhost:" + port)
                .config("spark.sql.catalog.spark_catalog", SparkCatalog.class.getName())
                .config("spark.sql.catalog.spark_catalog.metastore", "hive")
                .config(
                        "spark.sql.catalog.spark_catalog.hive.metastore.uris",
                        "thrift://localhost:" + port)
                .config("spark.sql.catalog.spark_catalog.format-table.enabled", "true")
                .config("spark.sql.catalog.spark_catalog.warehouse", warehousePath.toString())
                .config(
                        "spark.sql.extensions",
                        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                .master("local[2]");
    }

    @Test
    public void testVector(@TempDir java.nio.file.Path tempDir) throws IOException {
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession.Builder builder = createSparkSessionBuilder(warehousePath);
        SparkSession spark = builder.getOrCreate();
        spark.sql("CREATE DATABASE IF NOT EXISTS my_db1");
        spark.sql("USE spark_catalog.my_db1");

        spark.sql(
                "\n"
                        + "CREATE TABLE my_db1.vector_test (gid BIGINT, sid STRING, embs ARRAY<FLOAT>)"
                        + " PARTITIONED BY (`date` STRING COMMENT 'date') ROW FORMAT SERDE 'org.apache.paimon.hive.PaimonSerDe'\n"
                        + "WITH\n"
                        + "  SERDEPROPERTIES ('serialization.format' = '1') STORED AS INPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonInputFormat' OUTPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonOutputFormat' TBLPROPERTIES (\n"
                        + "    'vector.file.format'='lance',\n"
                        + "    'vector-field'='embs',\n"
                        + "    'field.embs.vector-dim'='4',\n"
                        + "    'row-tracking.enabled'='true',\n"
                        + "    'data-evolution.enabled'='true',\n"
                        + "    'global-index.enabled' = 'true'\n"
                        + ");");
        spark.close();

        spark = builder.getOrCreate();
        spark.sql(
                "insert overwrite table my_db1.vector_test VALUES \n"
                        + "(1, '1', array(cast(1.0 as float), cast(2.0 as float), cast(3.0 as float), cast(4.0 as float)), '20260420'),\n"
                        + "(2, '2', array(cast(2.0 as float), cast(3.0 as float), cast(4.0 as float), cast(5.0 as float)), '20260420'),\n"
                        + "(3, '3', array(cast(3.0 as float), cast(4.0 as float), cast(5.0 as float), cast(6.0 as float)), '20260420');");
        spark.sql(
                "insert into table my_db1.vector_test VALUES\n"
                        + "(4, '4', array(cast(4.0 as float), cast(5.0 as float), cast(6.0 as float), cast(7.0 as float)), '20260420'),\n"
                        + "(5, '5', array(cast(5.0 as float), cast(6.0 as float), cast(7.0 as float), cast(8.0 as float)), '20260420'),\n"
                        + "(6, '6', array(cast(6.0 as float), cast(7.0 as float), cast(8.0 as float), cast(9.0 as float)), '20260420');");
        spark.sql(
                "insert into table my_db1.vector_test VALUES\n"
                        + "(7, '7', array(cast(7.0 as float), cast(8.0 as float), cast(9.0 as float), cast(10.0 as float)), '20260420'),\n"
                        + "(8, '8', array(cast(8.0 as float), cast(9.0 as float), cast(10.0 as float), cast(11.0 as float)), '20260420');");
        spark.close();

        spark = builder.getOrCreate();
        spark.sql(
                "\n"
                        + "CALL sys.create_global_index(\n"
                        + "    `table` => 'my_db1.vector_test',\n"
                        + "    `partitions` => \"date='20260421'\",\n"
                        + "    `index_column` => 'embs',\n"
                        + "    `index_type` => 'lumina-vector-ann',\n"
                        + "    `options` => 'lumina.index.dimension=4'\n"
                        + ");");
        spark.sql(
                "\n"
                        + "CALL sys.create_global_index(\n"
                        + "    `table` => 'my_db1.vector_test',\n"
                        + "    `partitions` => \"date='20260420'\",\n"
                        + "    `index_column` => 'embs',\n"
                        + "    `index_type` => 'lumina-vector-ann',\n"
                        + "    `options` => 'lumina.index.dimension=4'\n"
                        + ");");
        spark.close();

        spark = builder.getOrCreate();
        List<Row> rows =
                spark.sql("select gid, sid, embs from my_db1.vector_test where date = '20260420';")
                        .collectAsList();
        assertThat(rows).hasSize(8);
        rows =
                spark.sql(
                                "select gid, sid, embs from my_db1.vector_test where date = '20260420' and embs is not null;")
                        .collectAsList();
        assertThat(rows).hasSize(8);
        Map<Long, Long> baseRowIds =
                spark.sql("select gid, _row_id from my_db1.vector_test where date = '20260420'")
                        .collectAsList().stream()
                        .collect(Collectors.toMap(row -> row.getLong(0), row -> row.getLong(1)));
        assertThat(baseRowIds).hasSize(8);
        rows =
                spark.sql(
                                "select gid, sid,  embs from vector_search('my_db1.vector_test', 'embs', array(1.0f, 2.0f, 3.0f, 4.0f), 5)  where date = '20260420'")
                        .collectAsList();
        assertThat(rows).hasSize(5);

        // **vector search with row id */
        String vectorSearchWithRowIdSql =
                "select gid, sid,  embs, _row_id AS _row_id "
                        + "from vector_search('my_db1.vector_test', 'embs', array(1.0f, 2.0f, 3.0f, 4.0f), 5) "
                        + "where date = '20260420'";
        Dataset<Row> df = spark.sql(vectorSearchWithRowIdSql);
        assertThat(df.columns()).hasSize(4);
        assertThat(df.columns()).contains("_row_id");
        rows = df.collectAsList();
        assertThat(rows).hasSize(5);
        assertThat(rows.stream().noneMatch(row -> row.isNullAt(3))).isTrue();
        assertThat(
                        rows.stream()
                                .allMatch(
                                        row ->
                                                baseRowIds
                                                        .get(row.getLong(0))
                                                        .equals(row.getLong(3))))
                .isTrue();

        // **vector search with row id and score */
        String vectorSearchWithRowIdAndScoreSql =
                "select gid, sid,  embs, _row_id AS _row_id, __paimon_search_score "
                        + "from vector_search('my_db1.vector_test', 'embs', array(1.0f, 2.0f, 3.0f, 4.0f), 5) "
                        + "where date = '20260420'";
        df = spark.sql(vectorSearchWithRowIdAndScoreSql);
        assertThat(df.columns()).hasSize(5);
        assertThat(df.columns()).contains("_row_id", "__paimon_search_score");
        rows = df.collectAsList();
        assertThat(rows).hasSize(5);
        assertThat(rows.stream().allMatch(row -> !row.isNullAt(3) && !row.isNullAt(4))).isTrue();
        assertThat(
                        rows.stream()
                                .allMatch(
                                        row ->
                                                baseRowIds
                                                        .get(row.getLong(0))
                                                        .equals(row.getLong(3))))
                .isTrue();

        // **vector search with metadata columns only */
        String vectorSearchWithMetadataColumnsOnlySql =
                "select _row_id AS _row_id, __paimon_search_score "
                        + "from vector_search('my_db1.vector_test', 'embs', array(1.0f, 2.0f, 3.0f, 4.0f), 5) "
                        + "where date = '20260420'";
        df = spark.sql(vectorSearchWithMetadataColumnsOnlySql);
        assertThat(df.columns()).hasSize(2);
        assertThat(df.columns()).contains("_row_id", "__paimon_search_score");
        rows = df.collectAsList();
        assertThat(rows).hasSize(5);
        assertThat(rows.stream().allMatch(row -> !row.isNullAt(0) && !row.isNullAt(1))).isTrue();
        assertThat(rows.stream().allMatch(row -> baseRowIds.containsValue(row.getLong(0))))
                .isTrue();
        assertThat(rows.stream().map(row -> row.getLong(0)).collect(Collectors.toSet())).hasSize(5);

        // **vector search with score */
        String vectorSearchSql =
                "select gid, sid,  embs, __paimon_search_score "
                        + "from vector_search('my_db1.vector_test', 'embs', array(1.0f, 2.0f, 3.0f, 4.0f), 5) "
                        + "where date = '20260420'";
        df = spark.sql(vectorSearchSql);
        assertThat(df.columns()).hasSize(4);
        rows = df.collectAsList();
        assertThat(rows).hasSize(5);

        // ** distribute vector search */
        spark.sql("SET `spark.paimon.vector-search.distribute.enabled`=`true`");
        spark.sql("SET `spark.paimon.global-index.thread-num`=`1`");
        List<Row> compareRows = spark.sql(vectorSearchSql).collectAsList();
        assertThat(compareRows).hasSize(5);
        assertThat(
                        compareRows.stream()
                                .map(row -> Pair.of(row.getLong(0), row.getString(1)))
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(
                        rows.stream()
                                .map(row -> Pair.of(row.getLong(0), row.getString(1)))
                                .collect(Collectors.toList()));
        spark.close();

        // ** lateral vector search */
        spark = builder.getOrCreate();
        spark.sql("SET `spark.paimon.vector-search.distribute.enabled`=`false`");
        rows =
                spark.sql(
                                "SELECT q.gid AS query_gid, q.embs AS query_embs, "
                                        + "r.gid AS result_gid, r._row_id AS result_row_id "
                                        + "FROM my_db1.vector_test AS q, "
                                        + "LATERAL (SELECT gid, _row_id "
                                        + "FROM vector_search('my_db1.vector_test', 'embs', q.embs, 5)) AS r "
                                        + "WHERE q.`date` = '20260420';")
                        .collectAsList();
        assertThat(rows).hasSize(40);
        assertThat(rows.stream().noneMatch(row -> row.isNullAt(3))).isTrue();
        assertThat(
                        rows.stream()
                                .allMatch(
                                        row ->
                                                baseRowIds
                                                        .get(row.getLong(2))
                                                        .equals(row.getLong(3))))
                .isTrue();
        assertThat(
                        rows.stream()
                                .collect(
                                        Collectors.groupingBy(
                                                row -> row.getLong(0), Collectors.counting())))
                .hasSize(8)
                .containsEntry(1L, 5L)
                .containsEntry(2L, 5L)
                .containsEntry(3L, 5L)
                .containsEntry(4L, 5L)
                .containsEntry(5L, 5L)
                .containsEntry(6L, 5L)
                .containsEntry(7L, 5L)
                .containsEntry(8L, 5L);

        // ** lateral vector search with metadata columns only in subquery */
        rows =
                spark.sql(
                                "SELECT q.gid AS query_gid, "
                                        + "r._row_id AS result_row_id, "
                                        + "r.__paimon_search_score AS result_score "
                                        + "FROM my_db1.vector_test AS q, "
                                        + "LATERAL (SELECT _row_id, __paimon_search_score "
                                        + "FROM vector_search('my_db1.vector_test', 'embs', q.embs, 5)) AS r "
                                        + "WHERE q.`date` = '20260420';")
                        .collectAsList();
        assertThat(rows).hasSize(40);
        assertThat(rows.stream().allMatch(row -> !row.isNullAt(0) && !row.isNullAt(1))).isTrue();
        assertThat(rows.stream().allMatch(row -> baseRowIds.containsValue(row.getLong(1))))
                .isTrue();
        Map<Long, java.util.Set<Long>> rowIdsPerQueryGid =
                rows.stream()
                        .collect(
                                Collectors.groupingBy(
                                        row -> row.getLong(0),
                                        Collectors.mapping(
                                                row -> row.getLong(1), Collectors.toSet())));
        assertThat(rowIdsPerQueryGid).hasSize(8);
        assertThat(rowIdsPerQueryGid.values().stream().allMatch(rowIds -> rowIds.size() == 5))
                .isTrue();
        spark.close();

        spark = builder.getOrCreate();
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`vector_test`;");
        spark.close();
    }
}
