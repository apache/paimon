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

import org.apache.paimon.flink.action.ActionITCaseBase;
import org.apache.paimon.flink.action.MigrateIcebergTableAction;
import org.apache.paimon.flink.procedure.MigrateIcebergTableProcedure;
import org.apache.paimon.hive.TestHiveMetastore;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatList;

/** Tests for {@link MigrateIcebergTableProcedure}. */
public class MigrateIcebergTableProcedureITCase extends ActionITCaseBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(MigrateIcebergTableProcedureITCase.class);

    private static final TestHiveMetastore TEST_HIVE_METASTORE = new TestHiveMetastore();

    private static final int PORT = 9087;

    @TempDir java.nio.file.Path iceTempDir;
    @TempDir java.nio.file.Path paiTempDir;

    public static Stream<Arguments> actionArguments() {
        List<Arguments> argumentsList = new ArrayList<>();
        for (boolean isPartitioned : Arrays.asList(false, true)) {
            for (boolean startFlinkJob : Arrays.asList(false, true)) {
                argumentsList.add(Arguments.of(isPartitioned, startFlinkJob));
            }
        }
        return argumentsList.stream();
    }

    @BeforeEach
    public void beforeEach() {
        TEST_HIVE_METASTORE.start(PORT);
    }

    @AfterEach
    public void afterEach() throws Exception {
        TEST_HIVE_METASTORE.stop();
    }

    @Test
    public void testMigrateIcebergTableProcedure() throws Exception {
        TableEnvironment tEnv =
                TableEnvironmentImpl.create(
                        EnvironmentSettings.newInstance().inBatchMode().build());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        boolean isPartitioned = random.nextBoolean();
        boolean icebergIsHive = random.nextBoolean();
        boolean paimonIsHive = random.nextBoolean();
        boolean isNamedArgument = random.nextBoolean();

        // Logging the random arguments for debugging
        LOG.info(
                "isPartitioned:{}, icebergIsHive:{}, paimonIsHive:{}, isNamedArgument:{}",
                isPartitioned,
                icebergIsHive,
                paimonIsHive,
                isNamedArgument);

        // create iceberg catalog, database, table, and insert some data to iceberg table
        tEnv.executeSql(icebergCatalogDdl(icebergIsHive));

        String icebergTable = "iceberg_" + UUID.randomUUID().toString().replace("-", "_");
        tEnv.executeSql("USE CATALOG my_iceberg");
        if (isPartitioned) {
            tEnv.executeSql(
                    String.format(
                            "CREATE TABLE `default`.`%s` (id string, id2 int, id3 int) PARTITIONED BY (id3)",
                            icebergTable));
        } else {
            tEnv.executeSql(
                    String.format(
                            "CREATE TABLE `default`.`%s` (id string, id2 int, id3 int) WITH ('format-version'='2')",
                            icebergTable));
        }
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `default`.`%s` VALUES ('a',1,1),('b',2,2),('c',3,3)",
                                icebergTable))
                .await();

        tEnv.executeSql(paimonCatalogDdl(paimonIsHive));
        tEnv.executeSql("USE CATALOG my_paimon");

        String icebergOptions =
                icebergIsHive
                        ? "metadata.iceberg.storage=hive-catalog, metadata.iceberg.uri=thrift://localhost:"
                                + PORT
                        : "metadata.iceberg.storage=hadoop-catalog,iceberg_warehouse=" + iceTempDir;
        if (isNamedArgument) {
            tEnv.executeSql(
                            String.format(
                                    "CALL sys.migrate_iceberg_table(source_table => 'default.%s', "
                                            + "iceberg_options => '%s')",
                                    icebergTable, icebergOptions))
                    .await();
        } else {
            tEnv.executeSql(
                            String.format(
                                    "CALL sys.migrate_iceberg_table('default.%s','%s')",
                                    icebergTable, icebergOptions))
                    .await();
        }

        assertThatList(Arrays.asList(Row.of("a", 1, 1), Row.of("b", 2, 2), Row.of("c", 3, 3)))
                .containsExactlyInAnyOrderElementsOf(
                        ImmutableList.copyOf(
                                tEnv.executeSql(
                                                String.format(
                                                        "SELECT * FROM `default`.`%s`",
                                                        icebergTable))
                                        .collect()));
    }

    @ParameterizedTest
    @MethodSource("actionArguments")
    public void testMigrateIcebergTableAction(boolean isPartitioned, boolean forceStartFlinkJob)
            throws Exception {
        TableEnvironment tEnv =
                TableEnvironmentImpl.create(
                        EnvironmentSettings.newInstance().inBatchMode().build());

        // create iceberg catalog, database, table, and insert some data to iceberg table
        tEnv.executeSql(icebergCatalogDdl(true));

        String icebergTable = "iceberg_" + UUID.randomUUID().toString().replace("-", "_");
        tEnv.executeSql("USE CATALOG my_iceberg");
        if (isPartitioned) {
            tEnv.executeSql(
                    String.format(
                            "CREATE TABLE `default`.`%s` (id string, id2 int, id3 int) PARTITIONED BY (id3)",
                            icebergTable));
        } else {
            tEnv.executeSql(
                    String.format(
                            "CREATE TABLE `default`.`%s` (id string, id2 int, id3 int) WITH ('format-version'='2')",
                            icebergTable));
        }
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `default`.`%s` VALUES ('a',1,1),('b',2,2),('c',3,3)",
                                icebergTable))
                .await();

        String icebergOptions =
                "metadata.iceberg.storage=hive-catalog, metadata.iceberg.uri=thrift://localhost:"
                        + PORT;

        Map<String, String> catalogConf = new HashMap<>();
        catalogConf.put("warehouse", paiTempDir.toString());
        catalogConf.put("metastore", "hive");
        catalogConf.put("uri", "thrift://localhost:" + PORT);
        catalogConf.put("cache-enabled", "false");

        MigrateIcebergTableAction migrateIcebergTableAction =
                new MigrateIcebergTableAction(
                        "default." + icebergTable, catalogConf, icebergOptions, "", 6);
        migrateIcebergTableAction.forceStartFlinkJob(forceStartFlinkJob);
        migrateIcebergTableAction.run();

        tEnv.executeSql(paimonCatalogDdl(true));
        tEnv.executeSql("USE CATALOG my_paimon");
        assertThatList(Arrays.asList(Row.of("a", 1, 1), Row.of("b", 2, 2), Row.of("c", 3, 3)))
                .containsExactlyInAnyOrderElementsOf(
                        ImmutableList.copyOf(
                                tEnv.executeSql(
                                                String.format(
                                                        "SELECT * FROM `my_paimon`.`default`.`%s`",
                                                        icebergTable))
                                        .collect()));
    }

    private String icebergCatalogDdl(boolean isHive) {
        return isHive
                ? String.format(
                        "CREATE CATALOG my_iceberg WITH "
                                + "( 'type' = 'iceberg', 'catalog-type' = 'hive', 'uri' = 'thrift://localhost:%s', "
                                + "'warehouse' = '%s', 'cache-enabled' = 'false')",
                        PORT, iceTempDir)
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
                                + "( 'type' = 'paimon', 'metastore' = 'hive', 'uri' = 'thrift://localhost:%s', "
                                + "'warehouse' = '%s', 'cache-enabled' = 'false' )",
                        PORT, iceTempDir)
                : String.format(
                        "CREATE CATALOG my_paimon WITH ('type' = 'paimon', 'warehouse' = '%s')",
                        paiTempDir);
    }
}
