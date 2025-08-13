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
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.TestHiveMetastore;
import org.apache.paimon.table.FileStoreTableFactory;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Base tests for spark read. */
public class SparkCatalogWithHiveTest {

    private static TestHiveMetastore testHiveMetastore;
    private static final int PORT = 9087;
    @TempDir java.nio.file.Path tempDir;

    @BeforeAll
    public static void startMetastore() {
        testHiveMetastore = new TestHiveMetastore();
        testHiveMetastore.start(PORT);
    }

    @AfterAll
    public static void closeMetastore() throws Exception {
        testHiveMetastore.stop();
    }

    @Test
    public void testCreateFormatTable() throws IOException {
        SparkSession spark = createSessionBuilder().getOrCreate();
        {
            spark.sql("CREATE DATABASE IF NOT EXISTS my_db1");
            spark.sql("USE spark_catalog.my_db1");

            // test orc table

            spark.sql("CREATE TABLE IF NOT EXISTS table_orc (a INT, bb INT, c STRING) USING orc");

            assertThat(
                            spark.sql("SHOW TABLES").collectAsList().stream()
                                    .map(s -> s.get(1))
                                    .map(Object::toString))
                    .containsExactlyInAnyOrder("table_orc");

            assertThat(
                            spark.sql("EXPLAIN EXTENDED SELECT * from table_orc").collectAsList()
                                    .stream()
                                    .map(s -> s.get(0))
                                    .map(Object::toString)
                                    .filter(s -> s.contains("PaimonFormatTableScan"))
                                    .count())
                    .isGreaterThan(0);

            // todo: There are some bugs with Spark CSV table's options. In Spark 3.x, both reading
            // and
            // writing using the default delimiter value ',' even if we specific it. In Spark 4.x,
            // reading is correct, but writing is still incorrect, just skip setting it for now.
            // test csv table

            spark.sql(
                    "CREATE TABLE IF NOT EXISTS table_csv (a INT, bb INT, c STRING) USING csv OPTIONS ('csv.field-delimiter' ',')");
            spark.sql("INSERT INTO table_csv VALUES (1, 1, '1'), (2, 2, '2')").collect();
            String r = spark.sql("DESCRIBE FORMATTED table_csv").collectAsList().toString();
            assertThat(r).contains("sep=,");
            assertThat(
                            spark.sql("SELECT * FROM table_csv").collectAsList().stream()
                                    .map(Row::toString)
                                    .collect(Collectors.toList()))
                    .containsExactlyInAnyOrder("[1,1,1]", "[2,2,2]");

            // test json table

            spark.sql(
                    "CREATE TABLE IF NOT EXISTS table_json (a INT, bb INT, c STRING) USING json ");
            spark.sql("INSERT INTO table_json VALUES(1, 1, '1'), (2, 2, '2')");
            assertThat(
                            spark.sql("SELECT * FROM table_json").collectAsList().stream()
                                    .map(Row::toString)
                                    .collect(Collectors.toList()))
                    .containsExactlyInAnyOrder("[1,1,1]", "[2,2,2]");
        }
        spark.stop();
    }

    @Test
    public void testSpecifyHiveConfDirInGenericCatalog() throws IOException {
        try (SparkSession spark =
                createSessionBuilder()
                        .config("spark.sql.catalog.spark_catalog.hive-conf-dir", "nonExistentPath")
                        .config(
                                "spark.sql.catalog.spark_catalog",
                                SparkGenericCatalog.class.getName())
                        .getOrCreate()) {
            assertThatThrownBy(() -> spark.sql("CREATE DATABASE my_db"))
                    .rootCause()
                    .isInstanceOf(FileNotFoundException.class)
                    .hasMessageContaining("nonExistentPath");
        }
    }

    @Test
    public void testCreateExternalTable() throws IOException {
        try (SparkSession spark = createSessionBuilder().getOrCreate()) {
            String warehousePath = spark.sparkContext().conf().get("spark.sql.warehouse.dir");
            spark.sql("CREATE DATABASE IF NOT EXISTS test_db");
            spark.sql("USE spark_catalog.test_db");

            // create hive external table
            spark.sql("CREATE EXTERNAL TABLE external_table (a INT, bb INT, c STRING)");

            // drop hive external table
            spark.sql("DROP TABLE external_table");

            // file system table exists
            assertThatCode(
                            () ->
                                    FileStoreTableFactory.create(
                                            LocalFileIO.create(),
                                            new Path(
                                                    warehousePath,
                                                    String.format(
                                                            "%s.db/%s",
                                                            "test_db", "external_table"))))
                    .doesNotThrowAnyException();
        }
    }

    private SparkSession.Builder createSessionBuilder() {
        Path warehousePath = new Path("file:" + tempDir.toString());
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
                .config("spark.sql.catalog.spark_catalog.warehouse", warehousePath.toString())
                .config(
                        "spark.sql.extensions",
                        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                .master("local[2]");
    }

    @Test
    public void testChainTable(@TempDir java.nio.file.Path tempDir) throws IOException {
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
                        + "  `my_db1`.`chain_test` (\n"
                        + "    `t1` BIGINT COMMENT 't1',\n"
                        + "    `t2` BIGINT COMMENT 't2',\n"
                        + "    `t3` STRING COMMENT 't3'\n"
                        + "  ) PARTITIONED BY (`dt` STRING COMMENT 'dt') ROW FORMAT SERDE 'org.apache.paimon.hive.PaimonSerDe'\n"
                        + "WITH\n"
                        + "  SERDEPROPERTIES ('serialization.format' = '1') STORED AS INPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonInputFormat' OUTPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonOutputFormat' TBLPROPERTIES (\n"
                        + "    'bucket-key' = 't1',\n"
                        + "    'primary-key' = 'dt,t1',\n"
                        + "    'partition.timestamp-pattern' = '$dt',\n"
                        + "    'partition.timestamp-formatter' = 'yyyyMMdd',\n"
                        + "    'chain-table.enabled' = 'true',\n"
                        + "    'bucket' = '2',\n"
                        + "    'merge-engine' = 'deduplicate', \n"
                        + "    'sequence.field' = 't2'\n"
                        + "  )");

        /** Create branch */
        spark.sql("CALL sys.create_branch('my_db1.chain_test', 'snapshot');");
        spark.sql("CALL sys.create_branch('my_db1.chain_test', 'delta')");

        /** Set branch */
        spark.sql(
                "ALTER TABLE my_db1.chain_test SET tblproperties ("
                        + "'scan.fallback-snapshot-branch' = 'snapshot', "
                        + "'scan.fallback-delta-branch' = 'delta')");
        spark.sql(
                "ALTER TABLE `my_db1`.`chain_test$branch_snapshot` SET tblproperties ("
                        + "'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "'scan.fallback-delta-branch' = 'delta')");
        spark.sql(
                "ALTER TABLE `my_db1`.`chain_test$branch_delta` SET tblproperties ("
                        + "'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "'scan.fallback-delta-branch' = 'delta')");
        spark.close();
        spark = builder.getOrCreate();

        /** Write main branch */
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250810') values (1, 1, '1'),(2, 1, '1');");

        /** Write delta branch */
        spark.sql("set spark.paimon.branch=delta;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250809') values (1, 1, '1'),(2, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250810') values (1, 2, '1-1' ),(3, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250811') values (2, 2, '1-1' ),(4, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250812') values (3, 2, '1-1' ),(4, 2, '1-1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250813') values (5, 1, '1' ),(6, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250814') values (5, 2, '1-1' ),(6, 2, '1-1' );");

        /** Write snapshot branch */
        spark.sql("set spark.paimon.branch=snapshot;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test`  partition (dt = '20250810')  values (1, 2, '1-1'),(2, 1, '1'),(3, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250812') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250814') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1'), (5, 1, '1' ), (6, 1, '1');");

        spark.close();
        spark = builder.getOrCreate();
        /** Main read */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250810'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,1,20250810]", "[2,1,1,20250810]");

        /** Snapshot read */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250814'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,20250814]",
                        "[2,2,1-1,20250814]",
                        "[3,2,1-1,20250814]",
                        "[4,2,1-1,20250814]",
                        "[5,1,1,20250814]",
                        "[6,1,1,20250814]");

        /** Chain read */
        /** 1. non pre snapshot */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250809'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,1,20250809]", "[2,1,1,20250809]");
        /** 2. has pre snapshot */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,20250811]",
                        "[2,2,1-1,20250811]",
                        "[3,1,1,20250811]",
                        "[4,1,1,20250811]");

        /** Multi partition Read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,20250810]",
                        "[2,1,1,20250810]",
                        "[1,2,1-1,20250811]",
                        "[2,2,1-1,20250811]",
                        "[3,1,1,20250811]",
                        "[4,1,1,20250811]",
                        "[1,2,1-1,20250812]",
                        "[2,2,1-1,20250812]",
                        "[3,2,1-1,20250812]",
                        "[4,2,1-1,20250812]");

        /** Incremental read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[2,2,1-1,20250811]", "[4,1,1,20250811]");

        /** Multi partition incremental read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,20250810]",
                        "[3,1,1,20250810]",
                        "[2,2,1-1,20250811]",
                        "[4,1,1,20250811]",
                        "[3,2,1-1,20250812]",
                        "[4,2,1-1,20250812]");

        /** Hybrid read */
        assertThat(
                        spark
                                .sql(
                                        "select * from  `my_db1`.`chain_test` where dt = '20250811'\n"
                                                + "union all\n"
                                                + "select * from  `my_db1`.`chain_test$branch_delta`  where dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,20250811]",
                        "[2,2,1-1,20250811]",
                        "[3,1,1,20250811]",
                        "[4,1,1,20250811]",
                        "[2,2,1-1,20250811]",
                        "[4,1,1,20250811]");

        spark.close();
        spark = builder.getOrCreate();

        /** Drop table */
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`chain_test`;");

        spark.close();
    }

    @Test
    public void testHourlyChainTable(@TempDir java.nio.file.Path tempDir) throws IOException {
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
                        + "  `my_db1`.`chain_test` (\n"
                        + "    `t1` BIGINT COMMENT 't1',\n"
                        + "    `t2` BIGINT COMMENT 't2',\n"
                        + "    `t3` STRING COMMENT 't3'\n"
                        + "  ) PARTITIONED BY (`dt` STRING COMMENT 'dt', `hour` STRING COMMENT 'hour') ROW FORMAT SERDE 'org.apache.paimon.hive.PaimonSerDe'\n"
                        + "WITH\n"
                        + "  SERDEPROPERTIES ('serialization.format' = '1') STORED AS INPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonInputFormat' OUTPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonOutputFormat' TBLPROPERTIES (\n"
                        + "    'bucket-key' = 't1',\n"
                        + "    'primary-key' = 'dt,hour,t1',\n"
                        + "    'partition.timestamp-pattern' = '$dt $hour:00:00',\n"
                        + "    'partition.timestamp-formatter' = 'yyyyMMdd HH:mm:ss',\n"
                        + "    'chain-table.enabled' = 'true',\n"
                        + "    'bucket' = '2',\n"
                        + "    'merge-engine' = 'deduplicate', \n"
                        + "    'sequence.field' = 't2'\n"
                        + "  )");

        /** Create branch */
        spark.sql("CALL sys.create_branch('my_db1.chain_test', 'snapshot');");
        spark.sql("CALL sys.create_branch('my_db1.chain_test', 'delta')");

        /** Set branch */
        spark.sql(
                "ALTER TABLE my_db1.chain_test SET tblproperties ("
                        + "'scan.fallback-snapshot-branch' = 'snapshot', "
                        + "'scan.fallback-delta-branch' = 'delta')");
        spark.sql(
                "ALTER TABLE `my_db1`.`chain_test$branch_snapshot` SET tblproperties ("
                        + "'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "'scan.fallback-delta-branch' = 'delta')");
        spark.sql(
                "ALTER TABLE `my_db1`.`chain_test$branch_delta` SET tblproperties ("
                        + "'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "'scan.fallback-delta-branch' = 'delta')");
        spark.close();
        spark = builder.getOrCreate();

        /** Write main branch */
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250810', hour = '22') values (1, 1, '1'),(2, 1, '1');");

        /** Write delta branch */
        spark.sql("set spark.paimon.branch=delta;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250810', hour = '21') values (1, 1, '1'),(2, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250810', hour = '22') values (1, 2, '1-1' ),(3, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250810', hour = '23') values (2, 2, '1-1' ),(4, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250811', hour = '00') values (3, 2, '1-1' ),(4, 2, '1-1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250811', hour = '01') values (5, 1, '1' ),(6, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250811', hour = '02') values (5, 2, '1-1' ),(6, 2, '1-1' );");

        /** Write snapshot branch */
        spark.sql("set spark.paimon.branch=snapshot;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test`  partition (dt = '20250810', hour = '22')  values (1, 2, '1-1'),(2, 1, '1'),(3, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250811', hour = '00') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250811', hour = '02') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1'), (5, 1, '1' ), (6, 1, '1');");

        spark.close();
        spark = builder.getOrCreate();
        /** Main read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250810' and hour = '22'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,1,20250810,22]", "[2,1,1,20250810,22]");

        /** Snapshot read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250811' and hour = '02'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,20250811,02]",
                        "[2,2,1-1,20250811,02]",
                        "[3,2,1-1,20250811,02]",
                        "[4,2,1-1,20250811,02]",
                        "[5,1,1,20250811,02]",
                        "[6,1,1,20250811,02]");

        /** Chain read */
        /** 1. non pre snapshot */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250810' and hour = '21'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,1,20250810,21]", "[2,1,1,20250810,21]");
        /** 2. has pre snapshot */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250810' and  hour = '23'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,20250810,23]",
                        "[2,2,1-1,20250810,23]",
                        "[3,1,1,20250810,23]",
                        "[4,1,1,20250810,23]");

        /** Multi partition Read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250810' and hour in ('22', '23');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,20250810,22]",
                        "[2,1,1,20250810,22]",
                        "[1,2,1-1,20250810,23]",
                        "[2,2,1-1,20250810,23]",
                        "[3,1,1,20250810,23]",
                        "[4,1,1,20250810,23]");

        /** Incremental read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where dt = '20250810' and hour = '23'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[2,2,1-1,20250810,23]", "[4,1,1,20250810,23]");

        /** Multi partition incremental read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where dt = '20250810' and hour in ('22', '23');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,20250810,22]",
                        "[3,1,1,20250810,22]",
                        "[2,2,1-1,20250810,23]",
                        "[4,1,1,20250810,23]");

        /** Hybrid read */
        assertThat(
                        spark
                                .sql(
                                        "select * from  `my_db1`.`chain_test` where dt = '20250810' and hour = '23'\n"
                                                + "union all\n"
                                                + "select * from  `my_db1`.`chain_test$branch_delta`  where dt = '20250810' and hour = '23'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,20250810,23]",
                        "[2,2,1-1,20250810,23]",
                        "[3,1,1,20250810,23]",
                        "[4,1,1,20250810,23]",
                        "[2,2,1-1,20250810,23]",
                        "[4,1,1,20250810,23]");

        spark.close();
        spark = builder.getOrCreate();

        /** Drop table */
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`chain_test`;");

        spark.close();
    }
}
