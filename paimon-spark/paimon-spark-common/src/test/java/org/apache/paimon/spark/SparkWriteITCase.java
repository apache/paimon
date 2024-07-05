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
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for spark writer. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkWriteITCase {

    protected SparkSession spark = null;

    @BeforeAll
    public void startMetastoreAndSpark(@TempDir java.nio.file.Path tempDir) {
        Path warehousePath = new Path("file:///" + tempDir.toString());
        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .getOrCreate();
        spark.conf().set("spark.sql.catalog.paimon", SparkCatalog.class.getName());
        spark.conf().set("spark.sql.catalog.paimon.warehouse", warehousePath.toString());
        spark.sql("CREATE DATABASE paimon.db");
        spark.sql("USE paimon.db");
    }

    @AfterAll
    public void stopMetastoreAndSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @AfterEach
    public void afterEach() {
        spark.sql("DROP TABLE T");
    }

    @Test
    public void testWrite() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) TBLPROPERTIES"
                        + " ('primary-key'='a', 'bucket'='4', 'file.format'='avro')");
        innerSimpleWrite();
    }

    @Test
    public void testWritePartitionTable() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) PARTITIONED BY (a) TBLPROPERTIES"
                        + " ('primary-key'='a,b', 'bucket'='4', 'file.format'='avro')");
        innerSimpleWrite();
    }

    @Test
    public void testSortSpill() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) TBLPROPERTIES"
                        + " ('primary-key'='a', 'bucket'='4', 'file.format'='avro', 'sort-spill-threshold'='2')");
        innerSimpleWrite();
    }

    private void innerSimpleWrite() {
        spark.sql("INSERT INTO T VALUES (1, 2, '3')").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,2,3]]");

        spark.sql("INSERT INTO T VALUES (4, 5, '6')").collectAsList();
        spark.sql("INSERT INTO T VALUES (1, 2, '7')").collectAsList();
        spark.sql("INSERT INTO T VALUES (4, 5, '8')").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        rows.sort(Comparator.comparingInt(o -> o.getInt(0)));
        assertThat(rows.toString()).isEqualTo("[[1,2,7], [4,5,8]]");

        spark.sql("DELETE FROM T WHERE a=1").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[4,5,8]]");

        spark.sql("DELETE FROM T").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[]");
    }

    @Test
    public void testDeleteWhereNonePk() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) TBLPROPERTIES"
                        + " ('primary-key'='a', 'file.format'='avro')");
        spark.sql("INSERT INTO T VALUES (1, 11, '111'), (2, 22, '222')").collectAsList();
        spark.sql("DELETE FROM T WHERE b=11").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[2,22,222]]");
    }

    @Test
    public void testTruncateTable() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING)"
                        + " TBLPROPERTIES ('primary-key'='a', 'file.format'='avro')");
        spark.sql("INSERT INTO T VALUES (1, 11, '111'), (2, 22, '222')");
        spark.sql("TRUNCATE TABLE T");
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[]");
    }

    @Test
    public void testTruncatePartition1() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c LONG) PARTITIONED BY (c)"
                        + " TBLPROPERTIES ('primary-key'='a,c')");
        spark.sql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");

        spark.sql("TRUNCATE TABLE T PARTITION (c = 111)");
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[2,22,222]]");
    }

    @Test
    public void testTruncatePartition() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c LONG, d STRING)"
                        + " PARTITIONED BY (c,d)"
                        + " TBLPROPERTIES ('primary-key'='a,c,d')");
        spark.sql(
                "INSERT INTO T VALUES (1, 11, 111, 'a'), (2, 22, 222, 'b'), (3, 33, 333, 'b'), (4, 44, 444, 'a')");

        spark.sql("TRUNCATE TABLE T PARTITION (d = 'a')");
        List<Row> rows = spark.sql("SELECT * FROM T ORDER BY a").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[2,22,222,b], [3,33,333,b]]");
    }

    @Test
    public void testWriteDynamicBucketPartitionedTable() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) PARTITIONED BY (a) TBLPROPERTIES"
                        + " ('primary-key'='a,b', 'bucket'='-1', "
                        + "'dynamic-bucket.target-row-num'='3', 'dynamic-bucket.initial-buckets'='1')");

        spark.sql("INSERT INTO T VALUES (1, 1, '1'), (1, 2, '2')");
        List<Row> rows = spark.sql("SELECT max(bucket) FROM `T$FILES`").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[0]]");

        spark.sql("INSERT INTO T VALUES (1, 2, '22'), (1, 3, '3')");
        rows = spark.sql("SELECT max(bucket) FROM `T$FILES`").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[0]]");

        spark.sql("INSERT INTO T VALUES (1, 4, '4'), (1, 5, '5')").collectAsList();
        rows = spark.sql("SELECT max(bucket) FROM `T$FILES`").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1]]");

        spark.sql("INSERT INTO T VALUES (1, 2, '222'), (1, 6, '6'), (1, 7, '7')").collectAsList();
        rows = spark.sql("SELECT max(bucket) FROM `T$FILES`").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[2]]");

        rows = spark.sql("SELECT count(1) FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[7]]");

        rows = spark.sql("SELECT * FROM T WHERE b = 2").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,2,222]]");

        spark.sql("INSERT INTO T VALUES (2, 1, '11'), (2, 3, '33'), (1, 8, '8')").collectAsList();
        rows = spark.sql("SELECT count(1) FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[10]]");
        rows =
                spark.sql(
                                "SELECT partition, max(bucket) FROM `T$FILES` GROUP BY partition ORDER BY partition")
                        .collectAsList();
        assertThat(rows.toString()).isEqualTo("[[[1],2], [[2],0]]");
    }

    @Test
    public void testReadWriteUnawareBucketTable() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) PARTITIONED BY (a) TBLPROPERTIES"
                        + " ('bucket'='-1')");

        spark.sql("INSERT INTO T VALUES (1, 1, '1'), (1, 2, '2')");
        spark.sql("INSERT INTO T VALUES (1, 1, '1'), (1, 2, '2')");
        spark.sql("INSERT INTO T VALUES (2, 1, '1'), (2, 2, '2')");
        spark.sql("INSERT INTO T VALUES (2, 1, '1'), (2, 2, '2')");
        spark.sql("INSERT INTO T VALUES (3, 1, '1'), (3, 2, '2')");
        spark.sql("INSERT INTO T VALUES (3, 1, '1'), (3, 2, '2')");

        List<Row> rows = spark.sql("SELECT count(1) FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[12]]");

        rows = spark.sql("SELECT * FROM T WHERE b = 2 AND a = 1").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,2,2], [1,2,2]]");

        rows = spark.sql("SELECT max(bucket) FROM `T$FILES`").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[0]]");
    }
}
