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

package org.apache.paimon.spark.extensions;

import org.apache.paimon.fs.Path;
import org.apache.paimon.spark.SparkCatalog;
import org.apache.paimon.spark.SparkGenericCatalog;
import org.apache.paimon.spark.catalyst.plans.logical.PaimonDropPartitions;
import org.apache.paimon.utils.FileIOUtils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.DropPartitions;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;

import scala.Option;

/** Test for DropPartition parser. */
public class DropPartitionParserTest {

    private static SparkSession spark = null;
    private static ParserInterface parser = null;
    private final String dbName = "test_db";
    private final String tableName = "test_paimon";
    private final String hiveTableName = "test_hive";
    private static Path warehousePath;

    @BeforeAll
    public static void startSparkSession(@TempDir java.nio.file.Path tempDir) {
        warehousePath = new Path("file:" + tempDir.toString());
        // Stops and clears active session to avoid loading previous non-stopped session.
        Option<SparkSession> optionalSession =
                SparkSession.getActiveSession().orElse(SparkSession::getDefaultSession);
        if (!optionalSession.isEmpty()) {
            optionalSession.get().stop();
        }
        SparkSession.clearActiveSession();
        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config("spark.sql.catalog.paimon.warehouse", warehousePath.toString())
                        .config(
                                "spark.sql.catalog.spark_catalog",
                                SparkGenericCatalog.class.getName())
                        .config(
                                "spark.sql.catalog.spark_catalog.warehouse.dir",
                                warehousePath.toString())
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .getOrCreate();
        parser = spark.sessionState().sqlParser();
    }

    @AfterAll
    public static void stopSparkSession() throws IOException {
        if (spark != null) {
            FileIOUtils.deleteDirectory(new File(warehousePath.toString()));
            spark.stop();
            spark = null;
            parser = null;
        }
    }

    @AfterEach
    public void clear() {
        if (spark != null) {
            spark.sql("DROP TABLE IF EXISTS " + dbName + "." + tableName);
            spark.sql("DROP TABLE IF EXISTS " + dbName + "." + hiveTableName);
            spark.sql("DROP DATABASE " + dbName + " CASCADE");
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void testOnPaimonCatalog(boolean defaultCatalog)
            throws ParseException, NoSuchTableException {
        String catalogName = !defaultCatalog ? "paimon" : "spark_catalog";
        spark.sql("USE " + catalogName);

        spark.sql("CREATE DATABASE IF NOT EXISTS " + dbName);
        spark.sql("USE " + dbName);
        spark.sql(
                "CREATE TABLE "
                        + tableName
                        + " (id INT, name STRING) USING paimon PARTITIONED BY (dt STRING) ");
        TableCatalog catalog =
                (TableCatalog) spark.sessionState().catalogManager().currentCatalog();
        Table table = catalog.loadTable(Identifier.of(new String[] {dbName}, tableName));
        Assertions.assertNotNull(table);
        LogicalPlan plan =
                parser.parsePlan("ALTER TABLE " + tableName + " DROP PARTITION(dt='2024-01-01')");
        Assertions.assertTrue(plan instanceof PaimonDropPartitions);
        if (defaultCatalog) {
            spark.sql(
                    "CREATE TABLE IF NOT EXISTS "
                            + hiveTableName
                            + " (id INT, name STRING) USING parquet PARTITIONED BY (dt STRING) ");
            plan =
                    parser.parsePlan(
                            "ALTER TABLE " + hiveTableName + " DROP PARTITION(dt='2024-01-01')");
            Assertions.assertFalse(plan instanceof PaimonDropPartitions);
            Assertions.assertTrue(plan instanceof DropPartitions);
        }
    }
}
