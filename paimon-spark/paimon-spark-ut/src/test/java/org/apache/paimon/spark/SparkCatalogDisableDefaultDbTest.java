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

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@code disable-create-table-in-default-db} option in Spark. */
public class SparkCatalogDisableDefaultDbTest {

    private SparkSession spark;

    @AfterEach
    public void stopSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Test
    public void testDisableCreateTableInDefaultDb(@TempDir java.nio.file.Path tempDir) {
        Path warehousePath = new Path("file:" + tempDir.toString());
        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config("spark.sql.catalog.paimon.warehouse", warehousePath.toString())
                        .config(
                                "spark.sql.catalog.paimon.disable-create-table-in-default-db",
                                "true")
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .getOrCreate();

        // Creating table in default database should fail
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE paimon.default.t1 (a INT, b STRING) USING paimon"))
                .hasMessageContaining(
                        "Creating table in default database is disabled, please specify a database name.");

        // Creating a non-default database should succeed
        assertThatCode(() -> spark.sql("CREATE DATABASE paimon.my_db")).doesNotThrowAnyException();

        // Creating table in a non-default database should succeed
        assertThatCode(
                        () ->
                                spark.sql(
                                        "CREATE TABLE paimon.my_db.t1 (a INT, b STRING) USING paimon"))
                .doesNotThrowAnyException();

        // Verify the table is accessible
        spark.sql("INSERT INTO paimon.my_db.t1 VALUES (1, 'hello')").collect();
        assertThat(
                        spark.sql("SELECT * FROM paimon.my_db.t1").collectAsList().stream()
                                .map(Object::toString))
                .containsExactly("[1,hello]");
    }

    @Test
    public void testDisableCreateTableWithCustomDefaultDb(@TempDir java.nio.file.Path tempDir) {
        Path warehousePath = new Path("file:" + tempDir.toString());
        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config("spark.sql.catalog.paimon.warehouse", warehousePath.toString())
                        .config(
                                "spark.sql.catalog.paimon.disable-create-table-in-default-db",
                                "true")
                        .config("spark.sql.catalog.paimon.defaultDatabase", "custom_default")
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .getOrCreate();

        // Creating table in custom default database should fail
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE paimon.custom_default.t1 (a INT, b STRING) USING paimon"))
                .hasMessageContaining(
                        "Creating table in default database is disabled, please specify a database name.");

        // Creating a different database and table should succeed
        assertThatCode(() -> spark.sql("CREATE DATABASE paimon.other_db"))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                spark.sql(
                                        "CREATE TABLE paimon.other_db.t1 (a INT, b STRING) USING paimon"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testDefaultDatabaseNotCreatedWhenDisabled(@TempDir java.nio.file.Path tempDir) {
        Path warehousePath = new Path("file:" + tempDir.toString());
        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config("spark.sql.catalog.paimon.warehouse", warehousePath.toString())
                        .config(
                                "spark.sql.catalog.paimon.disable-create-table-in-default-db",
                                "true")
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .getOrCreate();

        // Default database should not have been auto-created
        assertThat(
                        spark.sql("SHOW DATABASES IN paimon").collectAsList().stream()
                                .map(r -> r.getString(0)))
                .doesNotContain("default");
    }
}
