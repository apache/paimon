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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for data evolution on Spark. */
public class SparkDataEvolutionITCase {

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
    public void testDataEvolution(@TempDir java.nio.file.Path tempDir) throws IOException {
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession.Builder builder =
                SparkSession.builder()
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
                        .config(
                                "spark.sql.catalog.spark_catalog.warehouse",
                                warehousePath.toString())
                        .config(
                                "spark.sql.extensions",
                                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                        .master("local[2]");
        SparkSession spark = builder.getOrCreate();
        spark.sql("CREATE DATABASE IF NOT EXISTS my_db1");
        spark.sql("USE spark_catalog.my_db1");

        /** Create table */
        spark.sql(
                "CREATE TABLE IF NOT EXISTS \n"
                        + "  `my_db1`.`data_evolution_test` (\n"
                        + "    `id` BIGINT COMMENT 'id',\n"
                        + "    `g_1_1` BIGINT COMMENT 'g_1_1',\n"
                        + "    `g_1_2` BIGINT COMMENT 'g_1_2',\n"
                        + "    `g_2_1` BIGINT COMMENT 'g_2_1',\n"
                        + "    `g_2_2` BIGINT COMMENT 'g_2_2'\n"
                        + "  ) PARTITIONED BY (`dt` STRING COMMENT 'dt') ROW FORMAT SERDE 'org.apache.paimon.hive.PaimonSerDe'\n"
                        + "WITH\n"
                        + "  SERDEPROPERTIES ('serialization.format' = '1') STORED AS INPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonInputFormat' OUTPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonOutputFormat' TBLPROPERTIES (\n"
                        + "    'file.compression' = 'snappy',\n"
                        + "    'manifest.compression' = 'snappy',\n"
                        + "    'row-tracking.enabled' = 'true',\n"
                        + "    'data-evolution.enabled' = 'true',\n"
                        + "    'data-evolution.merge-into.source-persist' = 'true',\n"
                        + "    'partition.timestamp-pattern' = '$dt',\n"
                        + "    'partition.timestamp-formatter' = 'yyyyMMdd',\n"
                        + "    'metastore.partitioned-table' = 'true'"
                        + "  )");

        spark.sql(
                "CREATE TABLE IF NOT EXISTS \n"
                        + "  `my_db1`.`data_evolution_source` (\n"
                        + "    `id` BIGINT COMMENT 'id',\n"
                        + "    `g_2_1` BIGINT COMMENT 'g_2_1',\n"
                        + "    `g_2_2` BIGINT COMMENT 'g_2_2'\n"
                        + "  ) PARTITIONED BY (`dt` STRING COMMENT 'dt') ROW FORMAT SERDE 'org.apache.paimon.hive.PaimonSerDe'\n"
                        + "WITH\n"
                        + "  SERDEPROPERTIES ('serialization.format' = '1') STORED AS INPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonInputFormat' OUTPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonOutputFormat' TBLPROPERTIES (\n"
                        + "    'file.compression' = 'snappy',\n"
                        + "    'manifest.compression' = 'snappy',\n"
                        + "    'partition.timestamp-pattern' = '$dt',\n"
                        + "    'partition.timestamp-formatter' = 'yyyyMMdd',\n"
                        + "    'metastore.partitioned-table' = 'true'"
                        + "  )");
        spark.close();

        spark = builder.getOrCreate();
        spark.sql(
                "insert  overwrite table\n"
                        + "        `my_db1`.`data_evolution_test` partition (dt='20260305')\n"
                        + "values  (1, 1, 1, null, null),\n"
                        + "        (1, 2, 1, null, null),\n"
                        + "        (2, 1, 1, null, null);");
        spark.close();

        spark = builder.getOrCreate();
        spark.sql(
                "insert  overwrite table\n"
                        + "        `my_db1`.`data_evolution_test` partition (dt='20260304')\n"
                        + "values  (10, 10, 10, null, null),\n"
                        + "        (10, 20, 10, null, null),\n"
                        + "        (20, 10, 10, null, null)");
        spark.close();

        spark = builder.getOrCreate();
        spark.sql(
                "insert  overwrite table\n"
                        + "        `my_db1`.`data_evolution_source` partition (dt='20260304')\n"
                        + "values  (10, 20, 20),\n"
                        + "        (40, 10, 10);");
        spark.close();

        spark = builder.getOrCreate();
        spark.sql(
                "insert  overwrite table\n"
                        + "        `my_db1`.`data_evolution_source` partition (dt='20260305')\n"
                        + "values  (1, 2, 2),\n"
                        + "        (4, 1, 1);");
        spark.close();

        spark = builder.getOrCreate();
        spark.sql(
                "MERGE INTO `my_db1`.`data_evolution_test` AS t\n"
                        + "USING   `my_db1`.`data_evolution_source` AS s\n"
                        + "ON      t.id = s.id\n"
                        + "AND     t.dt = s.dt\n"
                        + "AND     s.dt = '20260304'\n"
                        + "AND     t.dt = '20260304'\n"
                        + "        WHEN matched THEN\n"
                        + "UPDATE\n"
                        + "SET     t.g_2_1 = s.g_2_1,\n"
                        + "        t.g_2_2 = s.g_2_2;");
        spark.close();

        spark = builder.getOrCreate();
        spark.sql(
                "MERGE INTO `my_db1`.`data_evolution_test` AS t\n"
                        + "USING   `my_db1`.`data_evolution_source` AS s\n"
                        + "ON      t.id = s.id\n"
                        + "AND     t.dt = s.dt\n"
                        + "AND     s.dt = '20260305'\n"
                        + "AND     t.dt = '20260305'\n"
                        + "        WHEN matched THEN\n"
                        + "UPDATE\n"
                        + "SET     t.g_2_1 = s.g_2_1,\n"
                        + "        t.g_2_2 = s.g_2_2;");
        spark.close();

        spark = builder.getOrCreate();
        assertThat(
                        spark
                                .sql(
                                        "select id,g_1_1,g_2_1 from `my_db1`.`data_evolution_test` where dt='20260304'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[10,10,20]", "[10,20,20]", "[20,10,null]");

        long recordCount =
                spark.sql(
                                "select id,g_1_1,g_2_1 from `my_db1`.`data_evolution_test` where dt='20260304' and g_1_1 = 10 and g_2_1 = 10")
                        .count();
        assertThat(recordCount).isEqualTo(0);

        assertThat(
                        spark
                                .sql(
                                        "select id,g_1_1,g_2_1 from `my_db1`.`data_evolution_test` where dt='20260304' and g_1_1 = 10 and g_2_1 =20")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[10,10,20]");

        assertThat(
                        spark
                                .sql(
                                        "select id,g_1_1,g_2_1 from `my_db1`.`data_evolution_test` where dt='20260304' and (g_1_1 = 10 or g_2_1 =10)")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[10,10,20]", "[20,10,null]");

        assertThat(
                        spark
                                .sql(
                                        "select id,g_1_1,g_2_1 from `my_db1`.`data_evolution_test` where dt='20260304' and (g_1_1 = 10 or g_2_1 =20)")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[10,10,20]", "[10,20,20]", "[20,10,null]");
        spark.close();

        spark = builder.getOrCreate();
        assertThat(
                        spark
                                .sql(
                                        "select id,g_1_1,g_2_1 from `my_db1`.`data_evolution_test` where dt='20260305'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,2]", "[1,2,2]", "[2,1,null]");

        recordCount =
                spark.sql(
                                "select id,g_1_1,g_2_1 from `my_db1`.`data_evolution_test` where dt='20260305' and g_1_1 = 1 and g_2_1 =1")
                        .count();
        assertThat(recordCount).isEqualTo(0);

        assertThat(
                        spark
                                .sql(
                                        "select id,g_1_1,g_2_1 from `my_db1`.`data_evolution_test` where dt='20260305' and g_1_1 = 1 and g_2_1 =2")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,2]");
        spark.close();

        spark = builder.getOrCreate();
        spark.sql(
                "insert  overwrite table\n"
                        + "        `my_db1`.`data_evolution_test` partition (dt='20260604')\n"
                        + "values  (10, 10, 10, null, null),\n"
                        + "        (20, 10, 10, null, null)");
        spark.sql(
                "insert  overwrite table\n"
                        + "        `my_db1`.`data_evolution_source` partition (dt='20260604')\n"
                        + "values  (30, 10, 10),\n"
                        + "        (40, 10, 10);");
        spark.sql(
                "MERGE INTO `my_db1`.`data_evolution_test` AS t\n"
                        + "USING   `my_db1`.`data_evolution_source` AS s\n"
                        + "ON      t.id = s.id\n"
                        + "AND     t.dt = s.dt\n"
                        + "AND     s.dt = '20260604'\n"
                        + "AND     t.dt = '20260604'\n"
                        + "        WHEN NOT MATCHED THEN\n"
                        + "INSERT (id,g_2_1, g_2_2, dt) VALUES (s.id, s.g_2_1, s.g_2_2, s.dt);");
        assertThat(
                        spark
                                .sql(
                                        "select id,g_1_1,g_2_1 from `my_db1`.`data_evolution_test` where dt='20260604'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[10,10,null]", "[20,10,null]", "[30,null,10]", "[40,null,10]");
        spark.close();

        spark = builder.getOrCreate();
        /** Drop table */
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`data_evolution_test`;");
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`data_evolution_source`;");
        spark.close();
    }
}
