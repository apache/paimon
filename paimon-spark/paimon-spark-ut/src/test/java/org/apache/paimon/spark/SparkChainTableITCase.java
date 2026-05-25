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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Base tests for spark read. */
public class SparkChainTableITCase {

    private static TestHiveMetastore testHiveMetastore;
    private static final int PORT = 9091;

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

    private void setupChainTableBranches(SparkSession spark, String tableName) {
        // Create branches
        spark.sql(String.format("CALL sys.create_branch('my_db1.%s', 'snapshot');", tableName));
        spark.sql(String.format("CALL sys.create_branch('my_db1.%s', 'delta')", tableName));

        // Set branch properties
        spark.sql(
                String.format(
                        "ALTER TABLE my_db1.%s SET tblproperties ("
                                + "'scan.fallback-snapshot-branch' = 'snapshot', "
                                + "'scan.fallback-delta-branch' = 'delta')",
                        tableName));
        spark.sql(
                String.format(
                        "ALTER TABLE `my_db1`.`%s$branch_snapshot` SET tblproperties ("
                                + "'scan.fallback-snapshot-branch' = 'snapshot',"
                                + "'scan.fallback-delta-branch' = 'delta')",
                        tableName));
        spark.sql(
                String.format(
                        "ALTER TABLE `my_db1`.`%s$branch_delta` SET tblproperties ("
                                + "'scan.fallback-snapshot-branch' = 'snapshot',"
                                + "'scan.fallback-delta-branch' = 'delta')",
                        tableName));
    }

    @Test
    public void testChainTable(@TempDir java.nio.file.Path tempDir) throws IOException {
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession.Builder builder = createSparkSessionBuilder(warehousePath);
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

        setupChainTableBranches(spark, "chain_test");
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
                "insert overwrite table  `my_db1`.`chain_test` partition (dt = '20250812') values (3, 2, '1-1' ),(4, 2, '1-1' ),(7, 1, 'd7' );");
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

        /** Chain read with filter */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250811' and t1 = 1")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,2,1-1,20250811]");
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250811' and t1 = 4")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[4,1,1,20250811]");
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250811' and t1 = 7")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .isEmpty();

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt in ('20250811', '20250812') and t1 = 1")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,2,1-1,20250811]", "[1,2,1-1,20250812]");

        /** Snapshot read with filter */
        assertThat(
                        spark.sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250812' and t1 = 7")
                                .collectAsList())
                .isEmpty();

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
                        "[4,2,1-1,20250812]",
                        "[7,1,d7,20250812]");

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
        spark.sql("set spark.paimon.branch=delta;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` values (5, 2, '1', '20250813'),(6, 2, '1', '20250814');");

        spark.close();
        spark = builder.getOrCreate();
        Dataset<Row> df =
                spark.sql(
                        "SELECT t1,t2,t3 FROM `my_db1`.`chain_test$branch_snapshot` where dt = '20250814'");
        assertThat(df.count()).isEqualTo(0);
        df =
                spark.sql(
                        "SELECT t1,t2,t3 FROM `my_db1`.`chain_test$branch_delta` where dt = '20250814'");
        assertThat(df.count()).isEqualTo(1);
        spark.close();

        spark = builder.getOrCreate();
        /** Drop table */
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`chain_test`;");
        spark.close();
    }

    @Test
    public void testHourlyChainTable(@TempDir java.nio.file.Path tempDir) throws IOException {
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession.Builder builder = createSparkSessionBuilder(warehousePath);
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

        setupChainTableBranches(spark, "chain_test");
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

        /** Chain read with non-partition filter */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250810' and hour = '23' and t1 = 1")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,2,1-1,20250810,23]");

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
        spark.sql("set spark.paimon.branch=delta;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` values (6, 2, '1', '20250811', '02');");

        spark.close();
        spark = builder.getOrCreate();
        Dataset<Row> df =
                spark.sql(
                        "SELECT t1,t2,t3 FROM `my_db1`.`chain_test$branch_snapshot` where dt = '20250811' and hour = '02'");
        assertThat(df.count()).isEqualTo(0);
        df =
                spark.sql(
                        "SELECT t1,t2,t3 FROM `my_db1`.`chain_test$branch_delta` where dt = '20250811' and hour = '02'");
        assertThat(df.count()).isEqualTo(1);
        spark.close();

        spark = builder.getOrCreate();
        /** Drop table */
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`chain_test`;");

        spark.close();
    }

    /**
     * Test chain table with partial-update merge engine.
     *
     * <p>Data layout across branches and partitions (key is primary key, seq is sequence field):
     *
     * <pre>
     * ┌──────────┬──────────┬─────────────────────────────────────────────────────────────────┐
     * │  Branch  │    dt    │                    Data (key, seq, v1, v2)                      │
     * ├──────────┼──────────┼─────────────────────────────────────────────────────────────────┤
     * │   main   │ 20250810 │ (1,1,'a','A'), (2,1,'b','B')                                    │
     * ├──────────┼──────────┼─────────────────────────────────────────────────────────────────┤
     * │  delta   │ 20250809 │ (3,1,'c','C'), (4,1,'d','D')                                    │
     * │          │ 20250810 │ (1,2,null,'A1'), (2,2,'b1',null), (3,1,'c','C')                 │
     * │          │ 20250811 │ (1,3,'a1',null), (2,3,null,'B1'), (5,1,'e','E')                 │
     * │          │ 20250812 │ (1,4,null,'A2'), (5,2,'e1',null)                                │
     * ├──────────┼──────────┼─────────────────────────────────────────────────────────────────┤
     * │ snapshot │ 20250810 │ (1,2,'a','A1'), (2,2,'b1','B'), (3,1,'c','C')                   │
     * │          │ 20250812 │ (1,4,'a1','A2'), (2,3,'b1','B1'), (3,1,'c','C'), (5,2,'e1','E') │
     * └──────────┴──────────┴─────────────────────────────────────────────────────────────────┘
     * </pre>
     *
     * <p>Expected read results (chain read merges snapshot + delta with partial-update):
     *
     * <pre>
     * ┌──────────┬───────────────────────────────────────────────────────────────────────────────┐
     * │    dt    │  Result (key, seq, v1, v2)                                                    │
     * ├──────────┼───────────────────────────────────────────────────────────────────────────────┤
     * │ 20250809 │ (3,1,'c','C'), (4,1,'d','D')             -- delta only, no pre-snapshot       │
     * │ 20250810 │ (1,1,'a','A'), (2,1,'b','B')             -- main branch data                  │
     * │ 20250811 │ (1,3,'a1','A1'), (2,3,'b1','B1'), (3,1,'c','C'), (5,1,'e','E')                │
     * │          │                                       -- snapshot[20250810] + delta[20250811] │
     * │ 20250812 │ (1,4,'a1','A2'), (2,3,'b1','B1'), (3,1,'c','C'), (5,2,'e1','E')               │
     * │          │                                          -- snapshot branch data              │
     * └──────────┴───────────────────────────────────────────────────────────────────────────────┘
     * </pre>
     */
    @Test
    public void testChainTableWithPartialUpdate(@TempDir java.nio.file.Path tempDir)
            throws IOException {
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession.Builder builder = createSparkSessionBuilder(warehousePath);
        SparkSession spark = builder.getOrCreate();
        spark.sql("CREATE DATABASE IF NOT EXISTS my_db1");
        spark.sql("USE spark_catalog.my_db1");

        // Create table with partial-update merge engine
        spark.sql(
                "CREATE TABLE IF NOT EXISTS \n"
                        + "  `my_db1`.`chain_test_partial` (\n"
                        + "    `key` BIGINT COMMENT 'key',\n"
                        + "    `seq` BIGINT COMMENT 'seq',\n"
                        + "    `v1` STRING COMMENT 'v1',\n"
                        + "    `v2` STRING COMMENT 'v2'\n"
                        + "  ) PARTITIONED BY (`dt` STRING COMMENT 'dt') ROW FORMAT SERDE 'org.apache.paimon.hive.PaimonSerDe'\n"
                        + "WITH\n"
                        + "  SERDEPROPERTIES ('serialization.format' = '1') STORED AS INPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonInputFormat' OUTPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonOutputFormat' TBLPROPERTIES (\n"
                        + "    'bucket-key' = 'key',\n"
                        + "    'primary-key' = 'dt,key',\n"
                        + "    'partition.timestamp-pattern' = '$dt',\n"
                        + "    'partition.timestamp-formatter' = 'yyyyMMdd',\n"
                        + "    'chain-table.enabled' = 'true',\n"
                        + "    'bucket' = '2',\n"
                        + "    'merge-engine' = 'partial-update', \n"
                        + "    'sequence.field' = 'seq'\n"
                        + "  )");

        setupChainTableBranches(spark, "chain_test_partial");
        spark.close();
        spark = builder.getOrCreate();

        // Write main branch
        spark.sql(
                "insert overwrite table `my_db1`.`chain_test_partial` partition (dt = '20250810') "
                        + "values (1, 1, 'a', 'A'), (2, 1, 'b', 'B');");

        // Write delta branch
        spark.sql("set spark.paimon.branch=delta;");
        spark.sql(
                "insert overwrite table `my_db1`.`chain_test_partial` partition (dt = '20250809') "
                        + "values (3, 1, 'c', 'C'), (4, 1, 'd', 'D');");
        spark.sql(
                "insert overwrite table `my_db1`.`chain_test_partial` partition (dt = '20250810') "
                        + "values (1, 2, CAST(NULL AS STRING), 'A1'), "
                        + "(2, 2, 'b1', CAST(NULL AS STRING)), "
                        + "(3, 1, 'c', 'C');");
        spark.sql(
                "insert overwrite table `my_db1`.`chain_test_partial` partition (dt = '20250811') "
                        + "values (1, 3, 'a1', CAST(NULL AS STRING)), "
                        + "(2, 3, CAST(NULL AS STRING), 'B1'), "
                        + "(5, 1, 'e', 'E');");
        spark.sql(
                "insert overwrite table `my_db1`.`chain_test_partial` partition (dt = '20250812') "
                        + "values (1, 4, CAST(NULL AS STRING), 'A2'), "
                        + "(5, 2, 'e1', CAST(NULL AS STRING));");

        // Write snapshot branch
        spark.sql("set spark.paimon.branch=snapshot;");
        spark.sql(
                "insert overwrite table `my_db1`.`chain_test_partial` partition (dt = '20250810') "
                        + "values (1, 2, 'a', 'A1'), (2, 2, 'b1', 'B'), (3, 1, 'c', 'C');");
        spark.sql(
                "insert overwrite table `my_db1`.`chain_test_partial` partition (dt = '20250812') "
                        + "values (1, 4, 'a1', 'A2'), (2, 3, 'b1', 'B1'), (3, 1, 'c', 'C'), (5, 2, 'e1', 'E');");

        spark.close();
        spark = builder.getOrCreate();

        // Main read - should return original main branch data
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test_partial` where dt = '20250810'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,a,A,20250810]", "[2,1,b,B,20250810]");

        // Snapshot read - should return snapshot branch data directly
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test_partial` where dt = '20250812'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,4,a1,A2,20250812]",
                        "[2,3,b1,B1,20250812]",
                        "[3,1,c,C,20250812]",
                        "[5,2,e1,E,20250812]");

        // Chain read
        // 1. non pre snapshot - read delta directly (20250809)
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test_partial` where dt = '20250809'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[3,1,c,C,20250809]", "[4,1,d,D,20250809]");

        // 2. has pre snapshot (20250811) - should merge snapshot(20250810) + delta(20250811)
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test_partial` where dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,3,a1,A1,20250811]",
                        "[2,3,b1,B1,20250811]",
                        "[3,1,c,C,20250811]",
                        "[5,1,e,E,20250811]");

        // Multi partition Read
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test_partial` where dt in ('20250810', '20250811', '20250812')")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,a,A,20250810]",
                        "[2,1,b,B,20250810]",
                        "[1,3,a1,A1,20250811]",
                        "[2,3,b1,B1,20250811]",
                        "[3,1,c,C,20250811]",
                        "[5,1,e,E,20250811]",
                        "[1,4,a1,A2,20250812]",
                        "[2,3,b1,B1,20250812]",
                        "[3,1,c,C,20250812]",
                        "[5,2,e1,E,20250812]");

        // Incremental read - read delta branch only with partial update data
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test_partial$branch_delta` where dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,3,a1,null,20250811]", "[2,3,null,B1,20250811]", "[5,1,e,E,20250811]");

        spark.close();
        spark = builder.getOrCreate();
        // Drop table
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`chain_test_partial`;");

        spark.close();
    }

    @Test
    public void testDropSnapshotPartition(@TempDir java.nio.file.Path tempDir) throws IOException {
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession.Builder builder = createSparkSessionBuilder(warehousePath);
        SparkSession spark = builder.getOrCreate();
        spark.sql("CREATE DATABASE IF NOT EXISTS my_db1");
        spark.sql("USE spark_catalog.my_db1");

        spark.sql(
                "CREATE TABLE IF NOT EXISTS \n"
                        + "  `my_db1`.`chain_test_drop_partition` (\n"
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

        setupChainTableBranches(spark, "chain_test_drop_partition");
        spark.close();

        spark = builder.getOrCreate();
        /** Write delta branch */
        spark.sql("set spark.paimon.branch=delta;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test_drop_partition` partition (dt = '20260101') values (1, 1, '1'),(2, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test_drop_partition` partition (dt = '20260102') values (1, 2, '1-1' ),(3, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test_drop_partition` partition (dt = '20260103') values (2, 2, '1-1' ),(4, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test_drop_partition` partition (dt = '20260104') values (3, 2, '1-1' ),(4, 2, '1-1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test_drop_partition` partition (dt = '20260105') values (5, 1, '1' ),(6, 1, '1' );");

        /** Write snapshot branch */
        spark.sql("set spark.paimon.branch=snapshot;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test_drop_partition`  partition (dt = '20260101')  values (1, 2, '1-1'),(2, 1, '1'),(3, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test_drop_partition` partition (dt = '20260103') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test_drop_partition` partition (dt = '20260105') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1'), (5, 1, '1' ), (6, 1, '1');");
        spark.close();

        final SparkSession session = builder.getOrCreate();
        assertThatNoException()
                .isThrownBy(
                        () -> {
                            session.sql(
                                    "alter table `my_db1`.`chain_test_drop_partition$branch_snapshot` drop partition (dt = '20260105');");
                        });
        assertThatThrownBy(
                () -> {
                    session.sql(
                            "alter table `my_db1`.`chain_test_drop_partition$branch_snapshot` drop partition (dt = '20260101');");
                });
        session.close();

        spark = builder.getOrCreate();
        /** Drop table */
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`chain_test_drop_partition`;");

        spark.close();
    }

    @Test
    public void testChainTableCacheInvalidation(@TempDir java.nio.file.Path tempDir)
            throws IOException {
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession.Builder builder = createSparkSessionBuilder(warehousePath);
        SparkSession spark = builder.getOrCreate();
        spark.sql("CREATE DATABASE IF NOT EXISTS my_db1");
        spark.sql("USE spark_catalog.my_db1");
        spark.sql(
                "CREATE TABLE chain_test_t ("
                        + "    `t1` string ,"
                        + "    `t2` string ,"
                        + "    `t3` string"
                        + ") PARTITIONED BY (`date` string)"
                        + "TBLPROPERTIES ("
                        + "   'chain-table.enabled' = 'true'"
                        + "  ,'primary-key' = 'date,t1'"
                        + "  ,'sequence.field' = 't2'"
                        + "  ,'bucket-key' = 't1'"
                        + "  ,'bucket' = '1'"
                        + "  ,'partition.timestamp-pattern' = '$date'"
                        + "  ,'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches(spark, "chain_test_t");
        spark.sql(
                "insert overwrite `chain_test_t$branch_delta` partition (date = '20260224') values ('1', '1', '1');");
        assertThat(
                        spark.sql("SELECT * FROM `chain_test_t`").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,1,20260224]");
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`chain_test_t`;");
        spark.close();
    }

    @Test
    public void testChainTableWithGroupPartition(@TempDir java.nio.file.Path tempDir)
            throws IOException {
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
                        + "  ) PARTITIONED BY (`region` STRING, `dt` STRING COMMENT 'dt') ROW FORMAT SERDE 'org.apache.paimon.hive.PaimonSerDe'\n"
                        + "WITH\n"
                        + "  SERDEPROPERTIES ('serialization.format' = '1') STORED AS INPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonInputFormat' OUTPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonOutputFormat' TBLPROPERTIES (\n"
                        + "    'bucket-key' = 't1',\n"
                        + "    'primary-key' = 'region,dt,t1',\n"
                        + "    'partition.timestamp-pattern' = '$dt',\n"
                        + "    'partition.timestamp-formatter' = 'yyyyMMdd',\n"
                        + "    'chain-table.enabled' = 'true',\n"
                        + "    'bucket' = '2',\n"
                        + "    'merge-engine' = 'deduplicate', \n"
                        + "    'sequence.field' = 't2',\n"
                        + "    'chain-table.chain-partition-keys' = 'dt'\n"
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
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250810') values (1, 1, '1'),(2, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250810') values (11, 1, '1'),(12, 1, '1');");

        /** Write delta branch */
        spark.sql("set spark.paimon.branch=delta;");

        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250809') values (1, 1, '1'),(2, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250810') values (1, 2, '1-1' ),(3, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250811') values (2, 2, '1-1' ),(4, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250812') values (3, 2, '1-1' ),(4, 2, '1-1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250813') values (5, 1, '1' ),(6, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250814') values (5, 2, '1-1' ),(6, 2, '1-1' );");

        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250809') values (11, 1, '1'),(12, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250810') values (11, 2, '1-1' ),(13, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250811') values (12, 2, '1-1' ),(14, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250812') values (13, 2, '1-1' ),(14, 2, '1-1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250813') values (15, 1, '1' ),(16, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250814') values (15, 2, '1-1' ),(16, 2, '1-1' );");

        /** Write snapshot branch */
        spark.sql("set spark.paimon.branch=snapshot;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test`  partition (region = 'CN', dt = '20250810')  values (1, 2, '1-1'),(2, 1, '1'),(3, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250812') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250814') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1'), (5, 1, '1' ), (6, 1, '1');");

        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test`  partition (region = 'US', dt = '20250810')  values (11, 2, '1-1'),(12, 1, '1'),(13, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250812') values (11, 2, '1-1'),(12, 2, '1-1'),(13, 2, '1-1'), (14, 2, '1-1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250814') values (11, 2, '1-1'),(12, 2, '1-1'),(13, 2, '1-1'), (14, 2, '1-1'), (15, 1, '1' ), (16, 1, '1');");

        spark.close();
        spark = builder.getOrCreate();
        /** Main read */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250810'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,20250810]",
                        "[2,1,1,CN,20250810]",
                        "[11,1,1,US,20250810]",
                        "[12,1,1,US,20250810]");

        /** Snapshot read */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250814'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250814]",
                        "[2,2,1-1,CN,20250814]",
                        "[3,2,1-1,CN,20250814]",
                        "[4,2,1-1,CN,20250814]",
                        "[5,1,1,CN,20250814]",
                        "[6,1,1,CN,20250814]",
                        "[11,2,1-1,US,20250814]",
                        "[12,2,1-1,US,20250814]",
                        "[13,2,1-1,US,20250814]",
                        "[14,2,1-1,US,20250814]",
                        "[15,1,1,US,20250814]",
                        "[16,1,1,US,20250814]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and dt = '20250814'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250814]",
                        "[2,2,1-1,CN,20250814]",
                        "[3,2,1-1,CN,20250814]",
                        "[4,2,1-1,CN,20250814]",
                        "[5,1,1,CN,20250814]",
                        "[6,1,1,CN,20250814]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt = '20250814'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,2,1-1,US,20250814]",
                        "[12,2,1-1,US,20250814]",
                        "[13,2,1-1,US,20250814]",
                        "[14,2,1-1,US,20250814]",
                        "[15,1,1,US,20250814]",
                        "[16,1,1,US,20250814]");

        /** Chain read */
        /** 1. non pre snapshot */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250809'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,20250809]",
                        "[2,1,1,CN,20250809]",
                        "[11,1,1,US,20250809]",
                        "[12,1,1,US,20250809]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and dt = '20250809'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,1,CN,20250809]", "[2,1,1,CN,20250809]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt = '20250809'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[11,1,1,US,20250809]", "[12,1,1,US,20250809]");
        /** 2. has pre snapshot */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250811]",
                        "[2,2,1-1,CN,20250811]",
                        "[3,1,1,CN,20250811]",
                        "[4,1,1,CN,20250811]",
                        "[11,2,1-1,US,20250811]",
                        "[12,2,1-1,US,20250811]",
                        "[13,1,1,US,20250811]",
                        "[14,1,1,US,20250811]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250811]",
                        "[2,2,1-1,CN,20250811]",
                        "[3,1,1,CN,20250811]",
                        "[4,1,1,CN,20250811]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,2,1-1,US,20250811]",
                        "[12,2,1-1,US,20250811]",
                        "[13,1,1,US,20250811]",
                        "[14,1,1,US,20250811]");

        /** Multi partition Read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,20250810]",
                        "[2,1,1,CN,20250810]",
                        "[1,2,1-1,CN,20250811]",
                        "[2,2,1-1,CN,20250811]",
                        "[3,1,1,CN,20250811]",
                        "[4,1,1,CN,20250811]",
                        "[1,2,1-1,CN,20250812]",
                        "[2,2,1-1,CN,20250812]",
                        "[3,2,1-1,CN,20250812]",
                        "[4,2,1-1,CN,20250812]",
                        "[11,1,1,US,20250810]",
                        "[12,1,1,US,20250810]",
                        "[11,2,1-1,US,20250811]",
                        "[12,2,1-1,US,20250811]",
                        "[13,1,1,US,20250811]",
                        "[14,1,1,US,20250811]",
                        "[11,2,1-1,US,20250812]",
                        "[12,2,1-1,US,20250812]",
                        "[13,2,1-1,US,20250812]",
                        "[14,2,1-1,US,20250812]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,20250810]",
                        "[2,1,1,CN,20250810]",
                        "[1,2,1-1,CN,20250811]",
                        "[2,2,1-1,CN,20250811]",
                        "[3,1,1,CN,20250811]",
                        "[4,1,1,CN,20250811]",
                        "[1,2,1-1,CN,20250812]",
                        "[2,2,1-1,CN,20250812]",
                        "[3,2,1-1,CN,20250812]",
                        "[4,2,1-1,CN,20250812]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,1,1,US,20250810]",
                        "[12,1,1,US,20250810]",
                        "[11,2,1-1,US,20250811]",
                        "[12,2,1-1,US,20250811]",
                        "[13,1,1,US,20250811]",
                        "[14,1,1,US,20250811]",
                        "[11,2,1-1,US,20250812]",
                        "[12,2,1-1,US,20250812]",
                        "[13,2,1-1,US,20250812]",
                        "[14,2,1-1,US,20250812]");

        /** Incremental read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[2,2,1-1,CN,20250811]",
                        "[4,1,1,CN,20250811]",
                        "[12,2,1-1,US,20250811]",
                        "[14,1,1,US,20250811]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'CN' and dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[2,2,1-1,CN,20250811]", "[4,1,1,CN,20250811]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'US' and dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[12,2,1-1,US,20250811]", "[14,1,1,US,20250811]");

        /** Multi partition incremental read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250810]",
                        "[3,1,1,CN,20250810]",
                        "[2,2,1-1,CN,20250811]",
                        "[4,1,1,CN,20250811]",
                        "[3,2,1-1,CN,20250812]",
                        "[4,2,1-1,CN,20250812]",
                        "[11,2,1-1,US,20250810]",
                        "[13,1,1,US,20250810]",
                        "[12,2,1-1,US,20250811]",
                        "[14,1,1,US,20250811]",
                        "[13,2,1-1,US,20250812]",
                        "[14,2,1-1,US,20250812]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'CN' and dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250810]",
                        "[3,1,1,CN,20250810]",
                        "[2,2,1-1,CN,20250811]",
                        "[4,1,1,CN,20250811]",
                        "[3,2,1-1,CN,20250812]",
                        "[4,2,1-1,CN,20250812]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'US' and dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,2,1-1,US,20250810]",
                        "[13,1,1,US,20250810]",
                        "[12,2,1-1,US,20250811]",
                        "[14,1,1,US,20250811]",
                        "[13,2,1-1,US,20250812]",
                        "[14,2,1-1,US,20250812]");

        spark.close();
        spark = builder.getOrCreate();
        spark.sql("set spark.paimon.branch=delta;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` values (5, 2, '1', 'CN', '20250813'),(6, 2, '1', 'CN', '20250814'), (15, 2, '1', 'US', '20250813'),(16, 2, '1', 'US', '20250814');");

        spark.close();
        spark = builder.getOrCreate();
        Dataset<Row> df =
                spark.sql(
                        "SELECT t1,t2,t3 FROM `my_db1`.`chain_test$branch_snapshot` where dt = '20250814'");
        assertThat(df.count()).isEqualTo(0);
        df =
                spark.sql(
                        "SELECT t1,t2,t3 FROM `my_db1`.`chain_test$branch_delta` where dt = '20250814'");
        assertThat(df.count()).isEqualTo(2);

        spark.close();
        spark = builder.getOrCreate();
        /** Drop table */
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`chain_test`;");

        spark.close();
    }

    @Test
    public void testHourlyChainTableWithGroupPartition(@TempDir java.nio.file.Path tempDir)
            throws IOException {
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
                        + "  ) PARTITIONED BY (`region` STRING, `dt` STRING COMMENT 'dt', `hour` STRING COMMENT 'hour') ROW FORMAT SERDE 'org.apache.paimon.hive.PaimonSerDe'\n"
                        + "WITH\n"
                        + "  SERDEPROPERTIES ('serialization.format' = '1') STORED AS INPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonInputFormat' OUTPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonOutputFormat' TBLPROPERTIES (\n"
                        + "    'bucket-key' = 't1',\n"
                        + "    'primary-key' = 'region,dt,hour,t1',\n"
                        + "    'partition.timestamp-pattern' = '$dt $hour:00:00',\n"
                        + "    'partition.timestamp-formatter' = 'yyyyMMdd HH:mm:ss',\n"
                        + "    'chain-table.enabled' = 'true',\n"
                        + "    'bucket' = '2',\n"
                        + "    'merge-engine' = 'deduplicate', \n"
                        + "    'sequence.field' = 't2',\n"
                        + "    'chain-table.chain-partition-keys' = 'dt,hour'\n"
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
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250810', hour = '22') values (1, 1, '1'),(2, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250810', hour = '22') values (11, 1, '1'),(12, 1, '1');");

        /** Write delta branch */
        spark.sql("set spark.paimon.branch=delta;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250810', hour = '21') values (1, 1, '1'),(2, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250810', hour = '22') values (1, 2, '1-1' ),(3, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250810', hour = '23') values (2, 2, '1-1' ),(4, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250811', hour = '00') values (3, 2, '1-1' ),(4, 2, '1-1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250811', hour = '01') values (5, 1, '1' ),(6, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250811', hour = '02') values (5, 2, '1-1' ),(6, 2, '1-1' );");

        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250810', hour = '21') values (11, 1, '1'),(12, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250810', hour = '22') values (11, 2, '1-1' ),(13, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250810', hour = '23') values (12, 2, '1-1' ),(14, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250811', hour = '00') values (13, 2, '1-1' ),(14, 2, '1-1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250811', hour = '01') values (15, 1, '1' ),(16, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250811', hour = '02') values (15, 2, '1-1' ),(16, 2, '1-1' );");

        /** Write snapshot branch */
        spark.sql("set spark.paimon.branch=snapshot;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250810', hour = '22')  values (1, 2, '1-1'),(2, 1, '1'),(3, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250811', hour = '00') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', dt = '20250811', hour = '02') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1'), (5, 1, '1' ), (6, 1, '1');");

        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250810', hour = '22')  values (11, 2, '1-1'),(12, 1, '1'),(13, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250811', hour = '00') values (11, 2, '1-1'),(12, 2, '1-1'),(13, 2, '1-1'), (14, 2, '1-1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', dt = '20250811', hour = '02') values (11, 2, '1-1'),(12, 2, '1-1'),(13, 2, '1-1'), (14, 2, '1-1'), (15, 1, '1' ), (16, 1, '1');");

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
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,20250810,22]",
                        "[2,1,1,CN,20250810,22]",
                        "[11,1,1,US,20250810,22]",
                        "[12,1,1,US,20250810,22]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and dt = '20250810' and hour = '22'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,1,CN,20250810,22]", "[2,1,1,CN,20250810,22]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt = '20250810' and hour = '22'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[11,1,1,US,20250810,22]", "[12,1,1,US,20250810,22]");

        /** Snapshot read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250811' and hour = '02'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250811,02]",
                        "[2,2,1-1,CN,20250811,02]",
                        "[3,2,1-1,CN,20250811,02]",
                        "[4,2,1-1,CN,20250811,02]",
                        "[5,1,1,CN,20250811,02]",
                        "[6,1,1,CN,20250811,02]",
                        "[11,2,1-1,US,20250811,02]",
                        "[12,2,1-1,US,20250811,02]",
                        "[13,2,1-1,US,20250811,02]",
                        "[14,2,1-1,US,20250811,02]",
                        "[15,1,1,US,20250811,02]",
                        "[16,1,1,US,20250811,02]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and dt = '20250811' and hour = '02'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250811,02]",
                        "[2,2,1-1,CN,20250811,02]",
                        "[3,2,1-1,CN,20250811,02]",
                        "[4,2,1-1,CN,20250811,02]",
                        "[5,1,1,CN,20250811,02]",
                        "[6,1,1,CN,20250811,02]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt = '20250811' and hour = '02'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,2,1-1,US,20250811,02]",
                        "[12,2,1-1,US,20250811,02]",
                        "[13,2,1-1,US,20250811,02]",
                        "[14,2,1-1,US,20250811,02]",
                        "[15,1,1,US,20250811,02]",
                        "[16,1,1,US,20250811,02]");

        /** Chain read */
        /** 1. non pre snapshot */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250810' and hour = '21'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,20250810,21]",
                        "[2,1,1,CN,20250810,21]",
                        "[11,1,1,US,20250810,21]",
                        "[12,1,1,US,20250810,21]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and dt = '20250810' and hour = '21'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,1,CN,20250810,21]", "[2,1,1,CN,20250810,21]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt = '20250810' and hour = '21'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[11,1,1,US,20250810,21]", "[12,1,1,US,20250810,21]");
        /** 2. has pre snapshot */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250810' and  hour = '23'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250810,23]",
                        "[2,2,1-1,CN,20250810,23]",
                        "[3,1,1,CN,20250810,23]",
                        "[4,1,1,CN,20250810,23]",
                        "[11,2,1-1,US,20250810,23]",
                        "[12,2,1-1,US,20250810,23]",
                        "[13,1,1,US,20250810,23]",
                        "[14,1,1,US,20250810,23]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and dt = '20250810' and  hour = '23'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250810,23]",
                        "[2,2,1-1,CN,20250810,23]",
                        "[3,1,1,CN,20250810,23]",
                        "[4,1,1,CN,20250810,23]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt = '20250810' and  hour = '23'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,2,1-1,US,20250810,23]",
                        "[12,2,1-1,US,20250810,23]",
                        "[13,1,1,US,20250810,23]",
                        "[14,1,1,US,20250810,23]");

        /** Multi partition Read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt = '20250810' and hour in ('22', '23');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,20250810,22]",
                        "[2,1,1,CN,20250810,22]",
                        "[1,2,1-1,CN,20250810,23]",
                        "[2,2,1-1,CN,20250810,23]",
                        "[3,1,1,CN,20250810,23]",
                        "[4,1,1,CN,20250810,23]",
                        "[11,1,1,US,20250810,22]",
                        "[12,1,1,US,20250810,22]",
                        "[11,2,1-1,US,20250810,23]",
                        "[12,2,1-1,US,20250810,23]",
                        "[13,1,1,US,20250810,23]",
                        "[14,1,1,US,20250810,23]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and dt = '20250810' and hour in ('22', '23');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,20250810,22]",
                        "[2,1,1,CN,20250810,22]",
                        "[1,2,1-1,CN,20250810,23]",
                        "[2,2,1-1,CN,20250810,23]",
                        "[3,1,1,CN,20250810,23]",
                        "[4,1,1,CN,20250810,23]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt = '20250810' and hour in ('22', '23');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,1,1,US,20250810,22]",
                        "[12,1,1,US,20250810,22]",
                        "[11,2,1-1,US,20250810,23]",
                        "[12,2,1-1,US,20250810,23]",
                        "[13,1,1,US,20250810,23]",
                        "[14,1,1,US,20250810,23]");

        /** Incremental read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where dt = '20250810' and hour = '23'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[2,2,1-1,CN,20250810,23]",
                        "[4,1,1,CN,20250810,23]",
                        "[12,2,1-1,US,20250810,23]",
                        "[14,1,1,US,20250810,23]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'CN' and dt = '20250810' and hour = '23'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[2,2,1-1,CN,20250810,23]", "[4,1,1,CN,20250810,23]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'US' and dt = '20250810' and hour = '23'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[12,2,1-1,US,20250810,23]", "[14,1,1,US,20250810,23]");

        /** Multi partition incremental read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where dt = '20250810' and hour in ('22', '23');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250810,22]",
                        "[3,1,1,CN,20250810,22]",
                        "[2,2,1-1,CN,20250810,23]",
                        "[4,1,1,CN,20250810,23]",
                        "[11,2,1-1,US,20250810,22]",
                        "[13,1,1,US,20250810,22]",
                        "[12,2,1-1,US,20250810,23]",
                        "[14,1,1,US,20250810,23]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'CN' and dt = '20250810' and hour in ('22', '23');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,20250810,22]",
                        "[3,1,1,CN,20250810,22]",
                        "[2,2,1-1,CN,20250810,23]",
                        "[4,1,1,CN,20250810,23]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'US' and dt = '20250810' and hour in ('22', '23');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,2,1-1,US,20250810,22]",
                        "[13,1,1,US,20250810,22]",
                        "[12,2,1-1,US,20250810,23]",
                        "[14,1,1,US,20250810,23]");

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
                        "[1,2,1-1,CN,20250810,23]",
                        "[2,2,1-1,CN,20250810,23]",
                        "[3,1,1,CN,20250810,23]",
                        "[4,1,1,CN,20250810,23]",
                        "[2,2,1-1,CN,20250810,23]",
                        "[4,1,1,CN,20250810,23]",
                        "[11,2,1-1,US,20250810,23]",
                        "[12,2,1-1,US,20250810,23]",
                        "[13,1,1,US,20250810,23]",
                        "[14,1,1,US,20250810,23]",
                        "[12,2,1-1,US,20250810,23]",
                        "[14,1,1,US,20250810,23]");

        spark.close();
        spark = builder.getOrCreate();
        spark.sql("set spark.paimon.branch=delta;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` values (6, 2, '1', 'CN', '20250811', '02'), (16, 2, '1', 'US', '20250811', '02');");

        spark.close();
        spark = builder.getOrCreate();
        Dataset<Row> df =
                spark.sql(
                        "SELECT t1,t2,t3 FROM `my_db1`.`chain_test$branch_snapshot` where dt = '20250811' and hour = '02'");
        assertThat(df.count()).isEqualTo(0);
        df =
                spark.sql(
                        "SELECT t1,t2,t3 FROM `my_db1`.`chain_test$branch_delta` where dt = '20250811' and hour = '02'");
        assertThat(df.count()).isEqualTo(2);

        spark.close();
        spark = builder.getOrCreate();
        /** Drop table */
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`chain_test`;");

        spark.close();
    }

    @Test
    public void testChainTableWithMultiGroupPartition(@TempDir java.nio.file.Path tempDir)
            throws IOException {
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
                        + "  ) PARTITIONED BY (`region` STRING, `biz_type` STRING COMMENT 'biz_type', `dt` STRING COMMENT 'dt') ROW FORMAT SERDE 'org.apache.paimon.hive.PaimonSerDe'\n"
                        + "WITH\n"
                        + "  SERDEPROPERTIES ('serialization.format' = '1') STORED AS INPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonInputFormat' OUTPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonOutputFormat' TBLPROPERTIES (\n"
                        + "    'bucket-key' = 't1',\n"
                        + "    'primary-key' = 'region,biz_type,dt,t1',\n"
                        + "    'partition.timestamp-pattern' = '$dt',\n"
                        + "    'partition.timestamp-formatter' = 'yyyyMMdd',\n"
                        + "    'chain-table.enabled' = 'true',\n"
                        + "    'bucket' = '2',\n"
                        + "    'merge-engine' = 'deduplicate', \n"
                        + "    'sequence.field' = 't2',\n"
                        + "    'chain-table.chain-partition-keys' = 'dt'\n"
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
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '0', dt = '20250810') values (1, 1, '1'),(2, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '1', dt = '20250810') values (1, 1, '1'),(2, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', biz_type = '1', dt = '20250810') values (11, 1, '1'),(12, 1, '1');");

        /** Write delta branch */
        spark.sql("set spark.paimon.branch=delta;");

        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '0', dt = '20250809') values (1, 1, '1'),(2, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '0', dt = '20250810') values (1, 2, '1-1' ),(3, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '0', dt = '20250811') values (2, 2, '1-1' ),(4, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '0', dt = '20250812') values (3, 2, '1-1' ),(4, 2, '1-1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '0', dt = '20250813') values (5, 1, '1' ),(6, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '0', dt = '20250814') values (5, 2, '1-1' ),(6, 2, '1-1' );");

        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '1', dt = '20250809') values (1, 1, '1'),(2, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '1', dt = '20250810') values (1, 2, '1-1' ),(3, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '1', dt = '20250811') values (2, 2, '1-1' ),(4, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '1', dt = '20250812') values (3, 2, '1-1' ),(4, 2, '1-1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '1', dt = '20250813') values (5, 1, '1' ),(6, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '1', dt = '20250814') values (5, 2, '1-1' ),(6, 2, '1-1' );");

        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', biz_type = '1', dt = '20250809') values (11, 1, '1'),(12, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', biz_type = '1', dt = '20250810') values (11, 2, '1-1' ),(13, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', biz_type = '1', dt = '20250811') values (12, 2, '1-1' ),(14, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', biz_type = '1', dt = '20250812') values (13, 2, '1-1' ),(14, 2, '1-1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', biz_type = '1', dt = '20250813') values (15, 1, '1' ),(16, 1, '1' );");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', biz_type = '1', dt = '20250814') values (15, 2, '1-1' ),(16, 2, '1-1' );");

        /** Write snapshot branch */
        spark.sql("set spark.paimon.branch=snapshot;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test`  partition (region = 'CN', biz_type = '0', dt = '20250810')  values (1, 2, '1-1'),(2, 1, '1'),(3, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '0', dt = '20250812') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '0', dt = '20250814') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1'), (5, 1, '1' ), (6, 1, '1');");

        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test`  partition (region = 'CN', biz_type = '1', dt = '20250810')  values (1, 2, '1-1'),(2, 1, '1'),(3, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '1', dt = '20250812') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'CN', biz_type = '1', dt = '20250814') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1'), (5, 1, '1' ), (6, 1, '1');");

        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test`  partition (region = 'US', biz_type = '1', dt = '20250810')  values (11, 2, '1-1'),(12, 1, '1'),(13, 1, '1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', biz_type = '1', dt = '20250812') values (11, 2, '1-1'),(12, 2, '1-1'),(13, 2, '1-1'), (14, 2, '1-1');");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` partition (region = 'US', biz_type = '1', dt = '20250814') values (11, 2, '1-1'),(12, 2, '1-1'),(13, 2, '1-1'), (14, 2, '1-1'), (15, 1, '1' ), (16, 1, '1');");

        spark.close();
        spark = builder.getOrCreate();
        /** Main read */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250810'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,0,20250810]",
                        "[2,1,1,CN,0,20250810]",
                        "[1,1,1,CN,1,20250810]",
                        "[2,1,1,CN,1,20250810]",
                        "[11,1,1,US,1,20250810]",
                        "[12,1,1,US,1,20250810]");

        /** Snapshot read */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250814'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,0,20250814]",
                        "[2,2,1-1,CN,0,20250814]",
                        "[3,2,1-1,CN,0,20250814]",
                        "[4,2,1-1,CN,0,20250814]",
                        "[5,1,1,CN,0,20250814]",
                        "[6,1,1,CN,0,20250814]",
                        "[1,2,1-1,CN,1,20250814]",
                        "[2,2,1-1,CN,1,20250814]",
                        "[3,2,1-1,CN,1,20250814]",
                        "[4,2,1-1,CN,1,20250814]",
                        "[5,1,1,CN,1,20250814]",
                        "[6,1,1,CN,1,20250814]",
                        "[11,2,1-1,US,1,20250814]",
                        "[12,2,1-1,US,1,20250814]",
                        "[13,2,1-1,US,1,20250814]",
                        "[14,2,1-1,US,1,20250814]",
                        "[15,1,1,US,1,20250814]",
                        "[16,1,1,US,1,20250814]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and biz_type  = '0' and dt = '20250814'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,0,20250814]",
                        "[2,2,1-1,CN,0,20250814]",
                        "[3,2,1-1,CN,0,20250814]",
                        "[4,2,1-1,CN,0,20250814]",
                        "[5,1,1,CN,0,20250814]",
                        "[6,1,1,CN,0,20250814]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and biz_type  = '1' and dt = '20250814'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,1,20250814]",
                        "[2,2,1-1,CN,1,20250814]",
                        "[3,2,1-1,CN,1,20250814]",
                        "[4,2,1-1,CN,1,20250814]",
                        "[5,1,1,CN,1,20250814]",
                        "[6,1,1,CN,1,20250814]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt = '20250814'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,2,1-1,US,1,20250814]",
                        "[12,2,1-1,US,1,20250814]",
                        "[13,2,1-1,US,1,20250814]",
                        "[14,2,1-1,US,1,20250814]",
                        "[15,1,1,US,1,20250814]",
                        "[16,1,1,US,1,20250814]");

        /** Chain read */
        /** 1. non pre snapshot */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250809'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,0,20250809]",
                        "[2,1,1,CN,0,20250809]",
                        "[1,1,1,CN,1,20250809]",
                        "[2,1,1,CN,1,20250809]",
                        "[11,1,1,US,1,20250809]",
                        "[12,1,1,US,1,20250809]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and biz_type  = '0' and dt = '20250809'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,1,CN,0,20250809]", "[2,1,1,CN,0,20250809]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and biz_type  = '1' and dt = '20250809'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,1,CN,1,20250809]", "[2,1,1,CN,1,20250809]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt = '20250809'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[11,1,1,US,1,20250809]", "[12,1,1,US,1,20250809]");

        /** 2. has pre snapshot */
        assertThat(
                        spark.sql("SELECT * FROM `my_db1`.`chain_test` where dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,0,20250811]",
                        "[2,2,1-1,CN,0,20250811]",
                        "[3,1,1,CN,0,20250811]",
                        "[4,1,1,CN,0,20250811]",
                        "[1,2,1-1,CN,1,20250811]",
                        "[2,2,1-1,CN,1,20250811]",
                        "[3,1,1,CN,1,20250811]",
                        "[4,1,1,CN,1,20250811]",
                        "[11,2,1-1,US,1,20250811]",
                        "[12,2,1-1,US,1,20250811]",
                        "[13,1,1,US,1,20250811]",
                        "[14,1,1,US,1,20250811]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and biz_type  = '0' and dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,0,20250811]",
                        "[2,2,1-1,CN,0,20250811]",
                        "[3,1,1,CN,0,20250811]",
                        "[4,1,1,CN,0,20250811]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and biz_type  = '1' and dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,1,20250811]",
                        "[2,2,1-1,CN,1,20250811]",
                        "[3,1,1,CN,1,20250811]",
                        "[4,1,1,CN,1,20250811]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,2,1-1,US,1,20250811]",
                        "[12,2,1-1,US,1,20250811]",
                        "[13,1,1,US,1,20250811]",
                        "[14,1,1,US,1,20250811]");

        /** Multi partition Read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,0,20250810]",
                        "[2,1,1,CN,0,20250810]",
                        "[1,2,1-1,CN,0,20250811]",
                        "[2,2,1-1,CN,0,20250811]",
                        "[3,1,1,CN,0,20250811]",
                        "[4,1,1,CN,0,20250811]",
                        "[1,2,1-1,CN,0,20250812]",
                        "[2,2,1-1,CN,0,20250812]",
                        "[3,2,1-1,CN,0,20250812]",
                        "[4,2,1-1,CN,0,20250812]",
                        "[1,1,1,CN,1,20250810]",
                        "[2,1,1,CN,1,20250810]",
                        "[1,2,1-1,CN,1,20250811]",
                        "[2,2,1-1,CN,1,20250811]",
                        "[3,1,1,CN,1,20250811]",
                        "[4,1,1,CN,1,20250811]",
                        "[1,2,1-1,CN,1,20250812]",
                        "[2,2,1-1,CN,1,20250812]",
                        "[3,2,1-1,CN,1,20250812]",
                        "[4,2,1-1,CN,1,20250812]",
                        "[11,1,1,US,1,20250810]",
                        "[12,1,1,US,1,20250810]",
                        "[11,2,1-1,US,1,20250811]",
                        "[12,2,1-1,US,1,20250811]",
                        "[13,1,1,US,1,20250811]",
                        "[14,1,1,US,1,20250811]",
                        "[11,2,1-1,US,1,20250812]",
                        "[12,2,1-1,US,1,20250812]",
                        "[13,2,1-1,US,1,20250812]",
                        "[14,2,1-1,US,1,20250812]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and biz_type  = '0' and dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,0,20250810]",
                        "[2,1,1,CN,0,20250810]",
                        "[1,2,1-1,CN,0,20250811]",
                        "[2,2,1-1,CN,0,20250811]",
                        "[3,1,1,CN,0,20250811]",
                        "[4,1,1,CN,0,20250811]",
                        "[1,2,1-1,CN,0,20250812]",
                        "[2,2,1-1,CN,0,20250812]",
                        "[3,2,1-1,CN,0,20250812]",
                        "[4,2,1-1,CN,0,20250812]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'CN' and biz_type  = '1' and dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,1,1,CN,1,20250810]",
                        "[2,1,1,CN,1,20250810]",
                        "[1,2,1-1,CN,1,20250811]",
                        "[2,2,1-1,CN,1,20250811]",
                        "[3,1,1,CN,1,20250811]",
                        "[4,1,1,CN,1,20250811]",
                        "[1,2,1-1,CN,1,20250812]",
                        "[2,2,1-1,CN,1,20250812]",
                        "[3,2,1-1,CN,1,20250812]",
                        "[4,2,1-1,CN,1,20250812]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test` where region = 'US' and dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,1,1,US,1,20250810]",
                        "[12,1,1,US,1,20250810]",
                        "[11,2,1-1,US,1,20250811]",
                        "[12,2,1-1,US,1,20250811]",
                        "[13,1,1,US,1,20250811]",
                        "[14,1,1,US,1,20250811]",
                        "[11,2,1-1,US,1,20250812]",
                        "[12,2,1-1,US,1,20250812]",
                        "[13,2,1-1,US,1,20250812]",
                        "[14,2,1-1,US,1,20250812]");

        /** Incremental read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[2,2,1-1,CN,0,20250811]",
                        "[4,1,1,CN,0,20250811]",
                        "[2,2,1-1,CN,1,20250811]",
                        "[4,1,1,CN,1,20250811]",
                        "[12,2,1-1,US,1,20250811]",
                        "[14,1,1,US,1,20250811]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'CN' and biz_type  = '0' and dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[2,2,1-1,CN,0,20250811]", "[4,1,1,CN,0,20250811]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'CN' and biz_type  = '1' and dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[2,2,1-1,CN,1,20250811]", "[4,1,1,CN,1,20250811]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'US' and dt = '20250811'")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[12,2,1-1,US,1,20250811]", "[14,1,1,US,1,20250811]");

        /** Multi partition incremental read */
        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,0,20250810]",
                        "[3,1,1,CN,0,20250810]",
                        "[2,2,1-1,CN,0,20250811]",
                        "[4,1,1,CN,0,20250811]",
                        "[3,2,1-1,CN,0,20250812]",
                        "[4,2,1-1,CN,0,20250812]",
                        "[1,2,1-1,CN,1,20250810]",
                        "[3,1,1,CN,1,20250810]",
                        "[2,2,1-1,CN,1,20250811]",
                        "[4,1,1,CN,1,20250811]",
                        "[3,2,1-1,CN,1,20250812]",
                        "[4,2,1-1,CN,1,20250812]",
                        "[11,2,1-1,US,1,20250810]",
                        "[13,1,1,US,1,20250810]",
                        "[12,2,1-1,US,1,20250811]",
                        "[14,1,1,US,1,20250811]",
                        "[13,2,1-1,US,1,20250812]",
                        "[14,2,1-1,US,1,20250812]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'CN' and biz_type  = '0' and dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,0,20250810]",
                        "[3,1,1,CN,0,20250810]",
                        "[2,2,1-1,CN,0,20250811]",
                        "[4,1,1,CN,0,20250811]",
                        "[3,2,1-1,CN,0,20250812]",
                        "[4,2,1-1,CN,0,20250812]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'CN' and biz_type  = '1' and dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,1-1,CN,1,20250810]",
                        "[3,1,1,CN,1,20250810]",
                        "[2,2,1-1,CN,1,20250811]",
                        "[4,1,1,CN,1,20250811]",
                        "[3,2,1-1,CN,1,20250812]",
                        "[4,2,1-1,CN,1,20250812]");

        assertThat(
                        spark
                                .sql(
                                        "SELECT * FROM `my_db1`.`chain_test$branch_delta` where region = 'US' and dt in ('20250810', '20250811', '20250812');")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[11,2,1-1,US,1,20250810]",
                        "[13,1,1,US,1,20250810]",
                        "[12,2,1-1,US,1,20250811]",
                        "[14,1,1,US,1,20250811]",
                        "[13,2,1-1,US,1,20250812]",
                        "[14,2,1-1,US,1,20250812]");

        spark.close();
        spark = builder.getOrCreate();
        spark.sql("set spark.paimon.branch=delta;");
        spark.sql(
                "insert overwrite table  `my_db1`.`chain_test` values (5, 2, '1', 'CN', '0', '20250813'),(6, 2, '1', 'CN', '0', '20250814'), (15, 2, '1', 'US', '1', '20250813'),(16, 2, '1', 'US', '1', '20250814');");

        spark.close();
        spark = builder.getOrCreate();
        Dataset<Row> df =
                spark.sql(
                        "SELECT t1,t2,t3 FROM `my_db1`.`chain_test$branch_snapshot` where dt = '20250814'");
        assertThat(df.count()).isEqualTo(0);
        df =
                spark.sql(
                        "SELECT t1,t2,t3 FROM `my_db1`.`chain_test$branch_delta` where dt = '20250814'");
        assertThat(df.count()).isEqualTo(2);

        spark.close();
        spark = builder.getOrCreate();
        /** Drop table */
        spark.sql("DROP TABLE IF EXISTS `my_db1`.`chain_test`;");

        spark.close();
    }
}
