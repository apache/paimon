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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Base tests for spark read. */
public class SparkCatalogWithHiveTest {

    private static TestHiveMetastore testHiveMetastore;

    private static final int PORT = 9087;

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
    public void testCreateFormatTable(@TempDir java.nio.file.Path tempDir) {
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession spark =
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
                        .master("local[2]")
                        .getOrCreate();

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
                                .filter(s -> s.contains("OrcScan"))
                                .count())
                .isGreaterThan(0);

        // todo: There are some bugs with Spark CSV table's options. In Spark 3.x, both reading and
        // writing using the default delimiter value ',' even if we specific it. In Spark 4.x,
        // reading is correct, but writing is still incorrect, just skip setting it for now.
        // test csv table

        spark.sql(
                "CREATE TABLE IF NOT EXISTS table_csv (a INT, bb INT, c STRING) USING csv OPTIONS ('field-delimiter' ',')");
        spark.sql("INSERT INTO table_csv VALUES (1, 1, '1'), (2, 2, '2')").collect();
        assertThat(spark.sql("DESCRIBE FORMATTED table_csv").collectAsList().toString())
                .contains("sep=,");
        assertThat(
                        spark.sql("SELECT * FROM table_csv").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,1,1]", "[2,2,2]");

        spark.close();
    }

    @Test
    public void testSpecifyHiveConfDir(@TempDir java.nio.file.Path tempDir) {
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession spark =
                SparkSession.builder()
                        .config("spark.sql.catalog.spark_catalog.hive-conf-dir", "nonExistentPath")
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        // with hive metastore
                        .config("spark.sql.catalogImplementation", "hive")
                        .config(
                                "spark.sql.catalog.spark_catalog",
                                SparkGenericCatalog.class.getName())
                        .master("local[2]")
                        .getOrCreate();

        assertThatThrownBy(() -> spark.sql("CREATE DATABASE my_db"))
                .rootCause()
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining("nonExistentPath");

        spark.close();
    }

    @Test
    public void testCreateExternalTable(@TempDir java.nio.file.Path tempDir) {
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession spark =
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
                        .config(
                                "spark.sql.catalog.spark_catalog.warehouse",
                                warehousePath.toString())
                        .master("local[2]")
                        .getOrCreate();

        spark.sql("CREATE DATABASE IF NOT EXISTS test_db");
        spark.sql("USE spark_catalog.test_db");

        // create hive external table
        spark.sql("CREATE EXTERNAL TABLE t1 (a INT, bb INT, c STRING)");

        // drop hive external table
        spark.sql("DROP TABLE t1");

        // file system table exists
        assertThatCode(
                        () ->
                                FileStoreTableFactory.create(
                                        LocalFileIO.create(),
                                        new Path(
                                                warehousePath,
                                                String.format("%s.db/%s", "test_db", "t1"))))
                .doesNotThrowAnyException();

        spark.close();
    }
}
