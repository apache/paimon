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

package org.apache.paimon.hive.procedure;

import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.stream.Stream;

/** IT cases for migrating iceberg table to paimon table. */
@RunWith(PaimonEmbeddedHiveRunner.class)
public abstract class MigrateIcebergTableProcedureITCase {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @TempDir private java.nio.file.Path iceTempDir;
    @TempDir private java.nio.file.Path paiTempDir;

    @HiveSQL(files = {})
    protected static HiveShell hiveShell;

    private static Stream<Arguments> testIcebergArguments() {
        return Stream.of(Arguments.of(true, false), Arguments.of(false, false));
    }

    @Test
    public void initTest() {}

    @ParameterizedTest
    @MethodSource("testIcebergArguments")
    public void testMigrateIcebergUnPartitionedTable(boolean isPartitioned, boolean isHive)
            throws Exception {
        TableEnvironment tEnv =
                TableEnvironmentImpl.create(
                        EnvironmentSettings.newInstance().inBatchMode().build());

        // create iceberg catalog, database, table, and insert some data to iceberg table
        tEnv.executeSql(icebergCatalogDdl(isHive));
        tEnv.executeSql("USE CATALOG my_iceberg");
        tEnv.executeSql("CREATE DATABASE iceberg_db;");
        if (isPartitioned) {
            tEnv.executeSql(
                    "CREATE TABLE iceberg_db.iceberg_table (id string, id2 int, id3 int) PARTITIONED BY (id3)"
                            + " WITH ('format-version'='2')");
        } else {
            tEnv.executeSql(
                    "CREATE TABLE iceberg_db.iceberg_table (id string, id2 int, id3 int) WITH ('format-version'='2')");
        }
        tEnv.executeSql("INSERT INTO iceberg_db.iceberg_table VALUES ('a',1,1),('b',2,2),('c',3,3)")
                .await();

        tEnv.executeSql(paimonCatalogDdl(isHive));
        tEnv.executeSql("USE CATALOG my_paimon");
        tEnv.executeSql(
                        String.format(
                                "CALL sys.migrate_table(connector => 'iceberg', "
                                        + "iceberg_options => 'iceberg-meta-path=%s,target-database=%s,target-table=%s')",
                                iceTempDir + "/iceberg_db/iceberg_table/metadata",
                                "paimon_db",
                                "paimon_table"))
                .await();

        Assertions.assertThatList(
                        Arrays.asList(Row.of("a", 1, 1), Row.of("b", 2, 2), Row.of("c", 3, 3)))
                .containsExactlyInAnyOrderElementsOf(
                        ImmutableList.copyOf(
                                tEnv.executeSql("SELECT * FROM paimon_db.paimon_table").collect()));
    }

    private String icebergCatalogDdl(boolean isHive) {
        return isHive
                ? String.format(
                        "CREATE CATALOG my_iceberg WITH "
                                + "( 'type' = 'iceberg', 'catalog-type' = 'hive', 'uri' = '', "
                                + "'warehouse' = '%s', 'cache-enabled' = 'false' )",
                        iceTempDir)
                : String.format(
                        "CREATE CATALOG my_iceberg WITH "
                                + "( 'type' = 'iceberg', 'catalog-type' = 'hadoop',"
                                + "'warehouse' = '%s', 'cache-enabled' = 'false' )",
                        iceTempDir);
    }

    private String paimonCatalogDdl(boolean isHive) {
        return isHive
                ? String.format(
                        "CREATE CATALOG my_paimon WITH "
                                + "( 'type' = 'paimon', 'metastore' = 'hive', 'uri' = '', "
                                + "'warehouse' = '%s', 'cache-enabled' = 'false' )",
                        iceTempDir)
                : String.format(
                        "CREATE CATALOG my_paimon WITH ('type' = 'paimon', 'warehouse' = '%s')",
                        paiTempDir);
    }
}
