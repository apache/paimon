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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Paimon Multimodality type support on Spark. */
public class SparkMultimodalITCase {

    private static TestHiveMetastore testHiveMetastore;
    private static final int PORT = 9092;

    @BeforeAll
    public static void startMetastore() {
        testHiveMetastore = new TestHiveMetastore();
        testHiveMetastore.start(PORT);
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
                .config("hive.metastore.uris", "thrift://localhost:" + PORT)
                .config("spark.sql.catalog.spark_catalog", SparkCatalog.class.getName())
                .config("spark.sql.catalog.spark_catalog.metastore", "hive")
                .config(
                        "spark.sql.catalog.spark_catalog.hive.metastore.uris",
                        "thrift://localhost:" + PORT)
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
                                "select gid, sid,  embs from vector_search('my_db1.vector_test', 'embs', array(1.0f, 2.0f, 3.0f, 4.0f), 5)  where date = '20260420'")
                        .collectAsList();
        assertThat(rows).hasSize(5);
        String vectorSearchSql =
                "select gid, sid,  embs, __paimon_vector_search_score "
                        + "from vector_search('my_db1.vector_test', 'embs', array(1.0f, 2.0f, 3.0f, 4.0f), 5) "
                        + "where date = '20260420'";
        Dataset<Row> df = spark.sql(vectorSearchSql);
        assertThat(df.columns()).hasSize(4);
        rows = df.collectAsList();
        assertThat(rows).hasSize(5);
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

        spark = builder.getOrCreate();
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`vector_test`;");
        spark.close();
    }
}
