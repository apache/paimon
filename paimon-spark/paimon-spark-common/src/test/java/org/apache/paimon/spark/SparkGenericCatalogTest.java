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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Base tests for spark read. */
public class SparkGenericCatalogTest {

    protected SparkSession spark = null;

    protected Path warehousePath = null;

    @BeforeEach
    public void startMetastoreAndSpark(@TempDir java.nio.file.Path tempDir) {
        warehousePath = new Path("file:" + tempDir.toString().replaceAll("C:", ""));
        spark =
                SparkSession.builder()
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        .config("cd", "aa")
                        .master("local[2]")
                        .getOrCreate();
        spark.conf().set("spark.sql.catalog.spark_catalog", SparkGenericCatalog.class.getName());
    }

    @AfterEach
    public void stopMetastoreAndSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Test
    public void testPaimonTable() {
        spark.sql(
                "CREATE TABLE PT (a INT, b INT, c STRING) USING paimon TBLPROPERTIES"
                        + " ('file.format'='avro')");
        testReadWrite("PT");

        spark.sql("CREATE DATABASE my_db");
        spark.sql(
                "CREATE TABLE DB_PT (a INT, b INT, c STRING) USING paimon TBLPROPERTIES"
                        + " ('file.format'='avro')");
        testReadWrite("DB_PT");

        assertThat(spark.sql("SHOW NAMESPACES").collectAsList().stream().map(Object::toString))
                .containsExactlyInAnyOrder("[default]", "[my_db]");

        spark.sql(
                "CREATE TABLE PT1 (a INT, bB INT, c STRING) USING paimon TBLPROPERTIES"
                        + " ('file.format'='avro')");
    }

    @Test
    public void testSparkSessionReload() {
        spark.sql("CREATE DATABASE my_db");
        spark.close();

        spark =
                SparkSession.builder()
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        .master("local[2]")
                        .getOrCreate();
        spark.conf().set("spark.sql.catalog.spark_catalog", SparkGenericCatalog.class.getName());
        assertThatCode(
                        () ->
                                spark.sql(
                                        "CREATE TABLE my_db.DB_PT (a INT, b INT, c STRING) USING paimon TBLPROPERTIES"
                                                + " ('file.format'='avro')"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testCsvTable() {
        spark.sql("CREATE TABLE CT (a INT, b INT, c STRING) USING csv");
        testReadWrite("CT");
    }

    @Test
    public void testStructTable() {
        spark.sql(
                "CREATE TABLE ST (a INT, b STRUCT<b1: STRING, b2: STRING, b3: STRING>) USING paimon");
    }

    private void testReadWrite(String table) {
        spark.sql("INSERT INTO " + table + " VALUES (1, 2, '3'), (4, 5, '6')").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM " + table).collectAsList();
        assertThat(rows.stream().map(Object::toString))
                .containsExactlyInAnyOrder("[1,2,3]", "[4,5,6]");
    }
}
