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
import org.apache.paimon.utils.SnapshotManager;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SQLExecutionUIData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        spark.sql("DROP TABLE IF EXISTS T");
    }

    @Test
    public void testWriteWithDefaultValue() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT DEFAULT 2, c STRING DEFAULT 'my_value') TBLPROPERTIES"
                        + " ('file.format'='avro')");

        // test show create table
        List<Row> show = spark.sql("SHOW CREATE TABLE T").collectAsList();
        assertThat(show.toString())
                .contains("a INT,\n" + "  b INT DEFAULT 2,\n" + "  c STRING DEFAULT 'my_value'");

        // test partial write
        spark.sql("INSERT INTO T (a) VALUES (1), (2)").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,2,my_value], [2,2,my_value]]");

        // test write with DEFAULT
        spark.sql("INSERT INTO T VALUES (3, DEFAULT, DEFAULT)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,2,my_value], [2,2,my_value], [3,2,my_value]]");

        // test add column with DEFAULT not support
        assertThatThrownBy(() -> spark.sql("ALTER TABLE T ADD COLUMN d INT DEFAULT 5"))
                .hasMessageContaining(
                        "Unsupported table change: Cannot add column [d] with default value");

        // test alter type to default column
        spark.sql("ALTER TABLE T ALTER COLUMN b TYPE STRING").collectAsList();
        spark.sql("INSERT INTO T (a) VALUES (4)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString())
                .isEqualTo("[[1,2,my_value], [2,2,my_value], [3,2,my_value], [4,2,my_value]]");

        // test alter default column
        spark.sql("ALTER TABLE T ALTER COLUMN b SET DEFAULT '3'");
        spark.sql("INSERT INTO T (a) VALUES (5)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString())
                .isEqualTo(
                        "[[1,2,my_value], [2,2,my_value], [3,2,my_value], [4,2,my_value], [5,3,my_value]]");
    }

    @Test
    public void testWriteWithArrayDefaultValue() {
        // Test Array type default value - using Spark SQL function syntax
        spark.sql(
                "CREATE TABLE T (id INT, tags ARRAY<STRING> DEFAULT ARRAY('tag1', 'tag2'), numbers ARRAY<INT> DEFAULT ARRAY(1, 2, 3)) TBLPROPERTIES"
                        + " ('file.format'='avro')");

        // test show create table for array
        List<Row> show = spark.sql("SHOW CREATE TABLE T").collectAsList();
        assertThat(show.toString())
                .contains("tags ARRAY<STRING> DEFAULT ARRAY('tag1', 'tag2')")
                .contains("numbers ARRAY<INT> DEFAULT ARRAY(1, 2, 3)");

        // test partial write with array defaults
        spark.sql("INSERT INTO T (id) VALUES (1), (2)").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        // Support both Spark 3.x (WrappedArray) and Spark 4.x (ArraySeq) formats
        assertThat(rows.toString())
                .satisfiesAnyOf(
                        s -> assertThat(s).contains("WrappedArray('tag1', 'tag2')"),
                        s -> assertThat(s).contains("ArraySeq('tag1', 'tag2')"));
        assertThat(rows.toString())
                .satisfiesAnyOf(
                        s -> assertThat(s).contains("WrappedArray(1, 2, 3)"),
                        s -> assertThat(s).contains("ArraySeq(1, 2, 3)"));
        assertThat(rows.size()).isEqualTo(2);

        // test write with DEFAULT keyword for arrays
        spark.sql("INSERT INTO T VALUES (3, DEFAULT, DEFAULT)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString())
                .satisfiesAnyOf(
                        s -> assertThat(s).contains("WrappedArray('tag1', 'tag2')"),
                        s -> assertThat(s).contains("ArraySeq('tag1', 'tag2')"));
        assertThat(rows.size()).isEqualTo(3);

        // test empty array default value
        spark.sql("DROP TABLE IF EXISTS T");
        spark.sql(
                "CREATE TABLE T (id INT, empty_array ARRAY<STRING> DEFAULT ARRAY()) TBLPROPERTIES"
                        + " ('file.format'='avro')");

        spark.sql("INSERT INTO T (id) VALUES (1)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString())
                .satisfiesAnyOf(
                        s -> assertThat(s).contains("WrappedArray()"),
                        s -> assertThat(s).contains("ArraySeq()"));

        // test write with DEFAULT keyword for empty array
        spark.sql("INSERT INTO T VALUES (2, DEFAULT)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString())
                .satisfiesAnyOf(
                        s -> assertThat(s).contains("WrappedArray()"),
                        s -> assertThat(s).contains("ArraySeq()"));
        assertThat(rows.size()).isEqualTo(2);
    }

    @Test
    public void testWriteWithMapDefaultValue() {
        // Test Map type default value - using Spark SQL function syntax
        spark.sql(
                "CREATE TABLE T (id INT, properties MAP<STRING, STRING> DEFAULT MAP('key1', 'value1', 'key2', 'value2')) TBLPROPERTIES"
                        + " ('file.format'='avro')");

        // test show create table for map
        List<Row> show = spark.sql("SHOW CREATE TABLE T").collectAsList();
        assertThat(show.toString())
                .contains(
                        "properties MAP<STRING, STRING> DEFAULT MAP('key1', 'value1', 'key2', 'value2')");

        // test partial write with map defaults
        spark.sql("INSERT INTO T (id) VALUES (1), (2)").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).contains("'key1' -> 'value1'");
        assertThat(rows.toString()).contains("'key2' -> 'value2'");
        assertThat(rows.size()).isEqualTo(2);

        // test write with DEFAULT keyword for map
        spark.sql("INSERT INTO T VALUES (3, DEFAULT)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).contains("'key1' -> 'value1'");
        assertThat(rows.toString()).contains("'key2' -> 'value2'");
        assertThat(rows.size()).isEqualTo(3);

        // test empty map default value
        spark.sql("DROP TABLE IF EXISTS T");
        spark.sql(
                "CREATE TABLE T (id INT, empty_map MAP<STRING, INT> DEFAULT MAP()) TBLPROPERTIES"
                        + " ('file.format'='avro')");

        spark.sql("INSERT INTO T (id) VALUES (1)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).contains("Map()");

        // test write with DEFAULT keyword for empty map
        spark.sql("INSERT INTO T VALUES (2, DEFAULT)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).contains("Map()");
        assertThat(rows.size()).isEqualTo(2);
    }

    @Test
    public void testWriteWithStructDefaultValue() {
        // Test Struct/Row type default value - using Spark SQL function syntax
        spark.sql(
                "CREATE TABLE T (id INT, nested STRUCT<x: INT, y: STRING> DEFAULT STRUCT(42, 'default_value')) TBLPROPERTIES"
                        + " ('file.format'='avro')");

        // test show create table for struct
        List<Row> show = spark.sql("SHOW CREATE TABLE T").collectAsList();
        assertThat(show.toString())
                .contains("nested STRUCT<x: INT, y: STRING> DEFAULT STRUCT(42, 'default_value')");

        // test partial write with struct defaults
        spark.sql("INSERT INTO T (id) VALUES (1), (2)").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).contains("[42,'default_value']");
        assertThat(rows.size()).isEqualTo(2);

        // test write with DEFAULT keyword for struct
        spark.sql("INSERT INTO T VALUES (3, DEFAULT)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).contains("[42,'default_value']");
        assertThat(rows.size()).isEqualTo(3);

        // test complex struct with multiple types
        spark.sql("DROP TABLE IF EXISTS T");
        spark.sql(
                "CREATE TABLE T (id INT, config STRUCT<enabled: BOOLEAN, timeout: INT, name: STRING> DEFAULT STRUCT(true, 30, 'config_name')) TBLPROPERTIES"
                        + " ('file.format'='avro')");

        spark.sql("INSERT INTO T (id) VALUES (1)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).contains("[true,30,'config_name']");
        assertThat(rows.size()).isEqualTo(1);
    }

    @Test
    public void testWriteWithNestedComplexDefaultValue() {
        // Test nested complex types with default values - using Spark SQL function syntax
        spark.sql(
                "CREATE TABLE T (id INT, "
                        + "nested_array ARRAY<STRUCT<name: STRING, value: INT>> DEFAULT ARRAY(STRUCT('item1', 10), STRUCT('item2', 20)), "
                        + "map_of_arrays MAP<STRING, ARRAY<INT>> DEFAULT MAP('list1', ARRAY(1, 2), 'list2', ARRAY(3, 4))) TBLPROPERTIES"
                        + " ('file.format'='avro')");

        // test show create table for nested complex types
        List<Row> show = spark.sql("SHOW CREATE TABLE T").collectAsList();
        assertThat(show.toString())
                .contains(
                        "nested_array ARRAY<STRUCT<name: STRING, value: INT>> DEFAULT ARRAY(STRUCT('item1', 10), STRUCT('item2', 20))")
                .contains(
                        "map_of_arrays MAP<STRING, ARRAY<INT>> DEFAULT MAP('list1', ARRAY(1, 2), 'list2', ARRAY(3, 4))");

        // test partial write with nested complex type defaults
        spark.sql("INSERT INTO T (id) VALUES (1)").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.size()).isEqualTo(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);

        // test write with DEFAULT keyword for nested complex types
        spark.sql("INSERT INTO T VALUES (2, DEFAULT, DEFAULT)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.size()).isEqualTo(2);

        // test mixed simple and complex types with defaults
        spark.sql("DROP TABLE IF EXISTS T");
        spark.sql(
                "CREATE TABLE T (id INT, "
                        + "name STRING DEFAULT 'default_name', "
                        + "tags ARRAY<STRING> DEFAULT ARRAY('default_tag'), "
                        + "metadata MAP<STRING, STRING> DEFAULT MAP('created_by', 'system'), "
                        + "config STRUCT<enabled: BOOLEAN, timeout: INT> DEFAULT STRUCT(true, 30)) TBLPROPERTIES"
                        + " ('file.format'='avro')");

        // test partial write with mixed defaults
        spark.sql("INSERT INTO T (id) VALUES (1)").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).contains("default_name");
        assertThat(rows.toString())
                .satisfiesAnyOf(
                        s -> assertThat(s).contains("WrappedArray('default_tag')"),
                        s -> assertThat(s).contains("ArraySeq('default_tag')"));
        assertThat(rows.toString()).contains("Map('created_by' -> 'system')");
        assertThat(rows.toString()).contains("[true,30]");

        // test selective column insertion with mixed defaults
        spark.sql("INSERT INTO T (id, name) VALUES (2, 'custom_name')").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).contains("custom_name");
        assertThat(rows.toString())
                .satisfiesAnyOf(
                        s -> assertThat(s).contains("WrappedArray('default_tag')"),
                        s -> assertThat(s).contains("ArraySeq('default_tag')"));
        assertThat(rows.size()).isEqualTo(2);

        // test write with some DEFAULT keywords for mixed types
        spark.sql("INSERT INTO T VALUES (3, DEFAULT, ARRAY('custom_tag'), DEFAULT, DEFAULT)")
                .collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).contains("default_name");
        assertThat(rows.toString())
                .satisfiesAnyOf(
                        s -> assertThat(s).contains("WrappedArray(custom_tag)"),
                        s -> assertThat(s).contains("ArraySeq(custom_tag)"));
        assertThat(rows.size()).isEqualTo(3);
    }

    @ParameterizedTest
    @CsvSource({
        "order, true",
        "zorder, true",
        "hilbert, true",
        "order, false",
        "zorder, false",
        "hilbert, false"
    })
    public void testWriteWithClustering(String clusterStrategy, boolean useV2Write) {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            spark.conf().set("spark.paimon.write.use-v2-write", String.valueOf(useV2Write));
            spark.sql(
                    "CREATE TABLE T (a INT, b INT) TBLPROPERTIES ("
                            + "'clustering.columns'='a,b',"
                            + String.format("'clustering.strategy'='%s')", clusterStrategy));

            spark.sql("INSERT INTO T VALUES (2, 2), (1, 1), (3, 3)").collectAsList();
            scala.collection.Seq<SQLExecutionUIData> executionSeq =
                    spark.sharedState().statusStore().executionsList();

            java.util.List<SQLExecutionUIData> executionList =
                    JavaConverters.seqAsJavaList(executionSeq);

            boolean hasSort =
                    executionList.stream()
                            .anyMatch(
                                    e -> {
                                        if (e.submissionTime() <= currentTimeMillis) {
                                            return false;
                                        }
                                        String description = e.physicalPlanDescription();
                                        return description != null
                                                && description.toLowerCase().contains("sort");
                                    });
            assertThat(hasSort).isEqualTo(true);

            List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
            assertThat(rows.toString()).isEqualTo("[[1,1], [2,2], [3,3]]");
        } finally {
            spark.conf().unset("spark.paimon.write.use-v2-write");
        }
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
    public void testTruncatePartitionValueNull() {
        spark.sql("CREATE TABLE TRUNC_T (pt STRING, data STRING) PARTITIONED BY (pt) ");

        spark.sql("INSERT INTO TRUNC_T VALUES('1', 'a'), (null, 'b')");

        spark.sql("TRUNCATE TABLE TRUNC_T PARTITION (pt = null)");

        List<Row> rows = spark.sql("SELECT * FROM TRUNC_T ORDER BY pt").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,a]]");
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
        assertThat(rows.toString()).isEqualTo("[[{1},2], [{2},0]]");
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
            Assertions.assertTrue(fileName.contains("data-"));
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
            Assertions.assertTrue(fileName.contains("test-"));
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
            Assertions.assertTrue(fileName.contains("test-"));
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

        // enable file suffix
        spark.conf().set("spark.paimon.file.suffix.include.compression", true);
        spark.sql("INSERT INTO T VALUES (3, 3, 'cc')");
        spark.sql("INSERT INTO T VALUES (4, 4, 'dd')");

        List<Row> data2 = spark.sql("SELECT * FROM T order by a").collectAsList();
        assertThat(data2.toString()).isEqualTo("[[1,1,aa], [2,2,bb], [3,3,cc], [4,4,dd]]");

        // check files suffix name
        List<String> files =
                spark.sql("select file_path from `T$files`").collectAsList().stream()
                        .map(x -> x.getString(0))
                        .collect(Collectors.toList());
        Assertions.assertEquals(4, files.size());

        String defaultExtension = "." + "parquet";
        String newExtension = "." + "zst" + "." + "parquet";
        // two data files end with ".parquet", two data file end with ".zst.parquet"
        Assertions.assertEquals(
                2,
                files.stream()
                        .filter(
                                name ->
                                        name.endsWith(defaultExtension)
                                                && !name.endsWith(newExtension))
                        .count());
        Assertions.assertEquals(
                2, files.stream().filter(name -> name.endsWith(newExtension)).count());

        // reset config
        spark.conf().unset("spark.paimon.file.suffix.include.compression");
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

        spark.conf().set("spark.paimon.file.suffix.include.compression", true);
        spark.sql("INSERT INTO T VALUES (2, 2, 'bb')");

        // collect changelog files
        List<String> files =
                Arrays.stream(fileIO.listStatus(new Path(tabLocation, "bucket-0")))
                        .map(name -> name.getPath().getName())
                        .filter(name -> name.contains("changelog-"))
                        .collect(Collectors.toList());
        String defaultExtension = "." + "parquet";
        String newExtension = "." + "zst" + "." + "parquet";
        // one changelog file end with ".parquet", one changelog file end with ".zst.parquet"
        Assertions.assertEquals(
                1,
                files.stream()
                        .filter(
                                name ->
                                        name.endsWith(defaultExtension)
                                                && !name.endsWith(newExtension))
                        .count());
        Assertions.assertEquals(
                1, files.stream().filter(name -> name.endsWith(newExtension)).count());

        // reset config
        spark.conf().unset("spark.paimon.file.suffix.include.compression");
    }

    @Test
    public void testIgnoreEmptyCommitConfigurable() {
        spark.sql(
                "CREATE TABLE T (id INT, name STRING) "
                        + "TBLPROPERTIES ("
                        + "'bucket-key'='id', "
                        + "'bucket' = '1', "
                        + "'file.format' = 'avro')");

        FileStoreTable table = getTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        spark.sql("insert into T values (1, 'aa')");
        Assertions.assertEquals(1, snapshotManager.latestSnapshotId());

        spark.sql("delete from T where id = 1");
        Assertions.assertEquals(2, snapshotManager.latestSnapshotId());
        Assertions.assertEquals(
                -1, Objects.requireNonNull(snapshotManager.latestSnapshot()).deltaRecordCount());

        // in batch write, ignore.empty.commit default is true
        spark.sql("delete from T where id = 1");
        Assertions.assertEquals(2, snapshotManager.latestSnapshotId());
        Assertions.assertEquals(
                -1, Objects.requireNonNull(snapshotManager.latestSnapshot()).deltaRecordCount());

        // set false to allow commit empty snapshot
        spark.conf().set("spark.paimon.snapshot.ignore-empty-commit", "false");
        spark.sql("delete from T where id = 1");
        Assertions.assertEquals(3, snapshotManager.latestSnapshotId());
        Assertions.assertEquals(
                0, Objects.requireNonNull(snapshotManager.latestSnapshot()).deltaRecordCount());

        spark.conf().unset("spark.paimon.snapshot.ignore-empty-commit");
    }

    protected static FileStoreTable getTable(String tableName) {
        return FileStoreTableFactory.create(
                LocalFileIO.create(),
                new Path(warehousePath, String.format("db.db/%s", tableName)));
    }

    private long dataFileCount(FileStatus[] files, String filePrefix) {
        return Arrays.stream(files).filter(f -> f.getPath().getName().contains(filePrefix)).count();
    }
}
