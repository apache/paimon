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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for spark writer. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkWriteITCase {

    protected SparkSession spark = null;

    protected static Path warehousePath = null;

    @BeforeAll
    public void startMetastoreAndSpark(@TempDir java.nio.file.Path tempDir) {
        warehousePath = new Path("file:///" + tempDir.toString());
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

    @Test
    public void testDefaultDataFilePrefix() {
        spark.sql("CREATE TABLE T (a INT, b INT, c STRING)");

        spark.sql("INSERT INTO T VALUES (1, 1, 'aa')");
        spark.sql("INSERT INTO T VALUES (2, 2, 'bb')");
        spark.sql("INSERT INTO T VALUES (3, 3, 'cc')");

        List<Row> data = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(data.toString()).isEqualTo("[[1,1,aa], [2,2,bb], [3,3,cc]]");

        List<Row> rows = spark.sql("select file_path from `T$files`").collectAsList();
        List<String> fileNames =
                rows.stream().map(x -> x.getString(0)).collect(Collectors.toList());
        Assertions.assertEquals(3, fileNames.size());
        for (String fileName : fileNames) {
            Assertions.assertTrue(fileName.startsWith("data-"));
        }
    }

    @Test
    public void testDataFilePrefixForAppendOnlyTable() {
        spark.sql("CREATE TABLE T (a INT, b INT, c STRING)");

        spark.conf().set("spark.paimon.data-file.prefix", "test-");
        spark.sql("INSERT INTO T VALUES (1, 1, 'aa')");
        spark.sql("INSERT INTO T VALUES (2, 2, 'bb')");
        spark.sql("INSERT INTO T VALUES (3, 3, 'cc')");

        List<Row> data = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(data.toString()).isEqualTo("[[1,1,aa], [2,2,bb], [3,3,cc]]");

        List<Row> rows = spark.sql("select file_path from `T$files`").collectAsList();
        List<String> fileNames =
                rows.stream().map(x -> x.getString(0)).collect(Collectors.toList());
        Assertions.assertEquals(3, fileNames.size());
        for (String fileName : fileNames) {
            Assertions.assertTrue(fileName.startsWith("test-"));
        }
    }

    @Test
    public void testDataFilePrefixForPKTable() {
        spark.sql("CREATE TABLE T (a INT, b INT, c STRING)" + " TBLPROPERTIES ('primary-key'='a')");

        spark.conf().set("spark.paimon.data-file.prefix", "test-");
        spark.sql("INSERT INTO T VALUES (1, 1, 'aa')");
        spark.sql("INSERT INTO T VALUES (2, 2, 'bb')");
        spark.sql("INSERT INTO T VALUES (1, 3, 'cc')");

        List<Row> data = spark.sql("SELECT * FROM T order by a").collectAsList();
        assertThat(data.toString()).isEqualTo("[[1,3,cc], [2,2,bb]]");

        List<Row> rows = spark.sql("select file_path from `T$files`").collectAsList();
        List<String> fileNames =
                rows.stream().map(x -> x.getString(0)).collect(Collectors.toList());
        Assertions.assertEquals(3, fileNames.size());
        for (String fileName : fileNames) {
            Assertions.assertTrue(fileName.startsWith("test-"));
        }

        // reset config, it will affect other tests
        spark.conf().unset("spark.paimon.data-file.prefix");
    }

    @Test
    public void testChangelogFilePrefixForPkTable() throws Exception {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) TBLPROPERTIES ('primary-key'='a', 'bucket' = '1', 'changelog-producer' = 'lookup')");

        FileStoreTable table = getTable("T");
        Path tabLocation = table.location();
        FileIO fileIO = table.fileIO();

        // default prefix "changelog-"
        spark.sql("INSERT INTO T VALUES (1, 1, 'aa')");
        FileStatus[] files1 = fileIO.listStatus(new Path(tabLocation, "bucket-0"));
        Assertions.assertEquals(1, dataFileCount(files1, "changelog-"));

        // custom prefix "test-changelog"
        spark.conf().set("spark.paimon.changelog-file.prefix", "test-changelog-");
        spark.sql("INSERT INTO T VALUES (2, 2, 'bb')");
        FileStatus[] files2 = fileIO.listStatus(new Path(tabLocation, "bucket-0"));
        Assertions.assertEquals(1, dataFileCount(files2, "test-changelog-"));

        // reset config, it will affect other tests
        spark.conf().unset("spark.paimon.changelog-file.prefix");
    }

    @Test
    public void testMarkDone() throws IOException {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) PARTITIONED BY (c) TBLPROPERTIES ("
                        + "'partition.end-input-to-done' = 'true', 'partition.mark-done-action' = 'success-file')");
        spark.sql("INSERT INTO T VALUES (1, 1, 'aa')");

        FileStoreTable table = getTable("T");
        FileIO fileIO = table.fileIO();
        Path tabLocation = table.location();

        Assertions.assertTrue(fileIO.exists(new Path(tabLocation, "c=aa/_SUCCESS")));
    }

    @Test
    public void testDataFileSuffixName() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING)"
                        + " TBLPROPERTIES ("
                        + "'bucket' = '1', "
                        + "'primary-key'='a', "
                        + "'write-only' = 'true', "
                        + "'file.format' = 'parquet', "
                        + "'file.compression' = 'zstd')");

        spark.sql("INSERT INTO T VALUES (1, 1, 'aa')");
        spark.sql("INSERT INTO T VALUES (2, 2, 'bb')");
        spark.sql("INSERT INTO T VALUES (1, 3, 'cc')");

        List<Row> data = spark.sql("SELECT * FROM T order by a").collectAsList();
        assertThat(data.toString()).isEqualTo("[[1,3,cc], [2,2,bb]]");

        List<String> beforeCompactFiles =
                spark.sql("select file_path from `T$files`").collectAsList().stream()
                        .map(x -> x.getString(0))
                        .collect(Collectors.toList());
        Assertions.assertEquals(3, beforeCompactFiles.size());

        // compact
        assertThat(spark.sql("CALL sys.compact(table => 'T')").collectAsList().toString())
                .isEqualTo("[[true]]");
        List<String> afterCompactFiles =
                spark.sql("select file_path from `T$files`").collectAsList().stream()
                        .map(x -> x.getString(0))
                        .collect(Collectors.toList());
        Assertions.assertEquals(1, afterCompactFiles.size());

        // check files suffix name
        List<String> files = new ArrayList<>(beforeCompactFiles);
        String extension = "." + "zstd" + "." + "parquet";
        files.addAll(afterCompactFiles);
        for (String fileName : files) {
            Assertions.assertTrue(fileName.endsWith(extension));
        }
    }

    @Test
    public void testChangelogFileSuffixName() throws Exception {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) "
                        + "TBLPROPERTIES ("
                        + "'primary-key'='a', "
                        + "'bucket' = '1', "
                        + "'changelog-producer' = 'lookup', "
                        + "'file.format' = 'parquet', "
                        + "'file.compression' = 'zstd')");

        FileStoreTable table = getTable("T");
        Path tabLocation = table.location();
        FileIO fileIO = table.fileIO();

        spark.sql("INSERT INTO T VALUES (1, 1, 'aa')");
        FileStatus[] files = fileIO.listStatus(new Path(tabLocation, "bucket-0"));
        Assertions.assertEquals(1, dataFileCount(files, "changelog-"));

        // check files suffix name
        String extension = "." + "zstd" + "." + "parquet";
        for (FileStatus file : files) {
            Assertions.assertTrue(file.getPath().getName().endsWith(extension));
        }
    }

    protected static FileStoreTable getTable(String tableName) {
        return FileStoreTableFactory.create(
                LocalFileIO.create(),
                new Path(warehousePath, String.format("db.db/%s", tableName)));
    }

    private long dataFileCount(FileStatus[] files, String filePrefix) {
        return Arrays.stream(files)
                .filter(f -> f.getPath().getName().startsWith(filePrefix))
                .count();
    }
}
