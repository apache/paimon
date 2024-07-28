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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Base tests for spark read. */
public class SparkGenericCatalogWithHiveTest {
    private static TestHiveMetastore testHiveMetastore;

    @BeforeAll
    public static void startMetastore() {
        testHiveMetastore = new TestHiveMetastore();
        testHiveMetastore.start();
    }

    @AfterAll
    public static void closeMetastore() throws Exception {
        testHiveMetastore.stop();
    }

    @Test
    public void testCreateTableCaseSensitive(@TempDir java.nio.file.Path tempDir) {
        // firstly, we use hive metastore to creata table, and check the result.
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession spark =
                SparkSession.builder()
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        // with case-insensitive true
                        .config("spark.case-insensitive", "true")
                        // with hive metastore
                        .config("spark.sql.catalogImplementation", "hive")
                        .config(
                                "spark.sql.catalog.spark_catalog",
                                SparkGenericCatalog.class.getName())
                        .master("local[2]")
                        .getOrCreate();

        spark.sql("CREATE DATABASE IF NOT EXISTS my_db1");
        spark.sql("USE my_db1");
        spark.sql(
                "CREATE TABLE IF NOT EXISTS t1 (a INT, Bb INT, c STRING) USING paimon TBLPROPERTIES"
                        + " ('file.format'='avro')");

        assertThat(
                        spark.sql("SHOW TABLES").collectAsList().stream()
                                .map(s -> s.get(1))
                                .map(Object::toString))
                .containsExactlyInAnyOrder("t1");
        spark.close();

        SparkSession spark1 =
                SparkSession.builder()
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        // with case-insensitive false
                        .config("spark.case-insensitive", "false")
                        // with hive metastore
                        .config("spark.sql.catalogImplementation", "hive")
                        .config(
                                "spark.sql.catalog.spark_catalog",
                                SparkGenericCatalog.class.getName())
                        .master("local[2]")
                        .getOrCreate();

        spark1.sql("USE my_db1");
        assertThrows(
                RuntimeException.class,
                () ->
                        spark1.sql(
                                "CREATE TABLE IF NOT EXISTS t2 (a INT, Bb INT, c STRING) USING paimon TBLPROPERTIES"
                                        + " ('file.format'='avro')"));
        spark1.close();
    }

    @Test
    public void testBuildWithHive(@TempDir java.nio.file.Path tempDir) {
        // firstly, we use hive metastore to creata table, and check the result.
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession spark =
                SparkSession.builder()
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        // with hive metastore
                        .config("spark.sql.catalogImplementation", "hive")
                        .config(
                                "spark.sql.catalog.spark_catalog",
                                SparkGenericCatalog.class.getName())
                        .master("local[2]")
                        .getOrCreate();

        spark.sql("CREATE DATABASE my_db");
        spark.sql("USE my_db");
        spark.sql(
                "CREATE TABLE IF NOT EXISTS t1 (a INT, b INT, c STRING) USING paimon TBLPROPERTIES"
                        + " ('file.format'='avro')");

        assertThat(spark.sql("SHOW NAMESPACES").collectAsList().stream().map(Object::toString))
                .containsExactlyInAnyOrder("[default]", "[my_db], [my_db1]");

        assertThat(
                        spark.sql("SHOW TABLES").collectAsList().stream()
                                .map(s -> s.get(1))
                                .map(Object::toString))
                .containsExactlyInAnyOrder("t1");
        spark.close();

        // secondly, we close catalog with hive metastore, and start a filesystem metastore to check
        // the result.
        SparkSession spark2 =
                SparkSession.builder()
                        .config("spark.sql.catalog.paimon.warehouse", warehousePath.toString())
                        .config("spark.sql.catalogImplementation", "in-memory")
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .master("local[2]")
                        .getOrCreate();
        spark2.sql("USE paimon");
        spark2.sql("USE my_db");
        assertThat(spark2.sql("SHOW NAMESPACES").collectAsList().stream().map(Object::toString))
                .containsExactlyInAnyOrder("[default]", "[my_db]");
        assertThat(
                        spark2.sql("SHOW TABLES").collectAsList().stream()
                                .map(s -> s.get(1))
                                .map(Object::toString))
                .containsExactlyInAnyOrder("t1");
    }
}
