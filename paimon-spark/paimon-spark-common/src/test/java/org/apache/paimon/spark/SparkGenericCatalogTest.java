/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Base tests for spark read. */
public class SparkGenericCatalogTest {

    protected static SparkSession spark = null;

    protected static Path warehousePath = null;

    @BeforeAll
    public static void startMetastoreAndSpark(@TempDir java.nio.file.Path tempDir) {
        warehousePath = new Path("file:" + tempDir.toString());
        spark =
                SparkSession.builder()
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        .master("local[2]")
                        .getOrCreate();
        spark.conf().set("spark.sql.catalog.spark_catalog", SparkGenericCatalog.class.getName());
    }

    @AfterAll
    public static void stopMetastoreAndSpark() {
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
    }

    @Test
    public void testCsvTable() {
        spark.sql("CREATE TABLE CT (a INT, b INT, c STRING) USING csv");
        testReadWrite("CT");
    }

    private void testReadWrite(String table) {
        spark.sql("INSERT INTO " + table + " VALUES (1, 2, '3'), (4, 5, '6')").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM " + table).collectAsList();
        assertThat(rows.stream().map(Object::toString))
                .containsExactlyInAnyOrder("[1,2,3]", "[4,5,6]");
    }
}
