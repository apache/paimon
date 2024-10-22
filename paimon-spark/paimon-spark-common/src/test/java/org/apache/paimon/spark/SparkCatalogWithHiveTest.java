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

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;
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
        // firstly, we use hive metastore to creata table, and check the result.
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
        spark.sql("CREATE TABLE IF NOT EXISTS table_orc (a INT, bb INT, c STRING) USING orc");

        assertThat(
                        spark.sql("SHOW TABLES").collectAsList().stream()
                                .map(s -> s.get(1))
                                .map(Object::toString))
                .containsExactlyInAnyOrder("table_orc");
        spark.close();

        SparkSession spark1 =
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
        spark1.sql("USE spark_catalog.my_db1");
        assertThat(
                        spark1.sql("EXPLAIN EXTENDED SELECT * from table_orc").collectAsList()
                                .stream()
                                .map(s -> s.get(0))
                                .map(Object::toString)
                                .filter(s -> s.contains("OrcScan"))
                                .count())
                .isGreaterThan(0);
        spark1.close();
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
}
