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
import org.apache.paimon.flink.action.MigrateTableAction;
import org.apache.paimon.flink.procedure.MigrateFileProcedure;
import org.apache.paimon.hive.TestHiveMetastore;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.conf.HiveConf;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

/** Tests for {@link MigrateFileProcedure}. */
public class MigrateTableProcedureITCase extends ActionITCaseBase {

    private static final TestHiveMetastore TEST_HIVE_METASTORE = new TestHiveMetastore();

    private static final int PORT = 9084;

    @TempDir java.nio.file.Path iceTempDir;
    @TempDir java.nio.file.Path paiTempDir;

    @BeforeEach
    public void beforeEach() {
        TEST_HIVE_METASTORE.start(PORT);
    }

    @AfterEach
    public void afterEach() throws Exception {
        TEST_HIVE_METASTORE.stop();
    }

    private static Stream<Arguments> testArguments() {
        return Stream.of(
                Arguments.of("orc", true),
                Arguments.of("avro", true),
                Arguments.of("parquet", true),
                Arguments.of("orc", false),
                Arguments.of("avro", false),
                Arguments.of("parquet", false));
    }

    private static Stream<Arguments> testIcebergArguments() {
        // isPartitioned, icebergIsHive, paimonIsHive, isNamedArgument
        return Stream.of(
                Arguments.of(true, false, false, true),
                Arguments.of(false, false, false, true),
                Arguments.of(true, true, false, true),
                Arguments.of(false, true, false, true),
                Arguments.of(true, true, false, false),
                Arguments.of(false, true, false, false),
                Arguments.of(true, false, true, true),
                Arguments.of(true, true, true, true));
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testMigrateProcedure(String format, boolean isNamedArgument) throws Exception {
        testUpgradeNonPartitionTable(format, isNamedArgument);
        resetMetastore();
        testUpgradePartitionTable(format, isNamedArgument);
    }

    private void resetMetastore() throws Exception {
        TEST_HIVE_METASTORE.stop();
        TEST_HIVE_METASTORE.reset();
        TEST_HIVE_METASTORE.start(PORT);
    }

    public void testUpgradePartitionTable(String format, boolean isNamedArgument) throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql(
                "CREATE TABLE hivetable (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + format);
        tEnv.executeSql("INSERT INTO hivetable VALUES" + data(100)).await();
        tEnv.executeSql("SHOW CREATE TABLE hivetable");

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<Row> r1 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hivetable").collect());

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'metastore' = 'hive', 'uri' = 'thrift://localhost:"
                        + PORT
                        + "' , 'warehouse' = '"
                        + System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname)
                        + "')");
        tEnv.useCatalog("PAIMON");
        if (isNamedArgument) {
            tEnv.executeSql(
                            "CALL sys.migrate_table(connector => 'hive', source_table => 'default.hivetable', options => 'file.format="
                                    + format
                                    + "')")
                    .await();
        } else {
            tEnv.executeSql(
                            "CALL sys.migrate_table('hive', 'default.hivetable', 'file.format="
                                    + format
                                    + "')")
                    .await();
        }
        List<Row> r2 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hivetable").collect());

        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    public void testUpgradeNonPartitionTable(String format, boolean isNamedArgument)
            throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE TABLE hivetable (id string, id2 int, id3 int) STORED AS " + format);
        tEnv.executeSql("INSERT INTO hivetable VALUES" + data(100)).await();
        tEnv.executeSql("SHOW CREATE TABLE hivetable");

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<Row> r1 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hivetable").collect());

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'metastore' = 'hive', 'uri' = 'thrift://localhost:"
                        + PORT
                        + "' , 'warehouse' = '"
                        + System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname)
                        + "')");
        tEnv.useCatalog("PAIMON");
        if (isNamedArgument) {
            tEnv.executeSql(
                            "CALL sys.migrate_table(connector => 'hive', source_table => 'default.hivetable', options => 'file.format="
                                    + format
                                    + "')")
                    .await();
        } else {
            tEnv.executeSql(
                            "CALL sys.migrate_table('hive', 'default.hivetable', 'file.format="
                                    + format
                                    + "')")
                    .await();
        }
        List<Row> r2 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hivetable").collect());

        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet", "avro"})
    public void testMigrateAction(String format) throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql(
                "CREATE TABLE hivetable (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + format);
        tEnv.executeSql("INSERT INTO hivetable VALUES" + data(100)).await();
        tEnv.executeSql("SHOW CREATE TABLE hivetable");

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<Row> r1 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hivetable").collect());
        Map<String, String> catalogConf = new HashMap<>();
        catalogConf.put("metastore", "hive");
        catalogConf.put("uri", "thrift://localhost:" + PORT);
        catalogConf.put(
                "warehouse", System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));
        MigrateTableAction migrateTableAction =
                new MigrateTableAction("hive", "default.hivetable", catalogConf, "", 6);
        migrateTableAction.run();

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'metastore' = 'hive', 'uri' = 'thrift://localhost:"
                        + PORT
                        + "' , 'warehouse' = '"
                        + System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname)
                        + "')");
        tEnv.useCatalog("PAIMON");
        List<Row> r2 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hivetable").collect());

        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    @ParameterizedTest
    @MethodSource("testIcebergArguments")
    public void testMigrateIcebergTableProcedure(
            boolean isPartitioned,
            boolean icebergIsHive,
            boolean paimonIsHive,
            boolean isNamedArgument)
            throws Exception {
        TableEnvironment tEnv =
                TableEnvironmentImpl.create(
                        EnvironmentSettings.newInstance().inBatchMode().build());

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
                                    "CALL sys.migrate_table(connector => 'iceberg', source_table => 'default.%s', "
                                            + "iceberg_options => '%s')",
                                    icebergTable, icebergOptions))
                    .await();
        } else {
            tEnv.executeSql(
                            String.format(
                                    "CALL sys.migrate_table('iceberg', 'default.%s','',1,'%s')",
                                    icebergTable, icebergOptions))
                    .await();
        }

        Assertions.assertThatList(
                        Arrays.asList(Row.of("a", 1, 1), Row.of("b", 2, 2), Row.of("c", 3, 3)))
                .containsExactlyInAnyOrderElementsOf(
                        ImmutableList.copyOf(
                                tEnv.executeSql(
                                                String.format(
                                                        "SELECT * FROM `default`.`%s`",
                                                        icebergTable))
                                        .collect()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMigrateIcebergTableAction(boolean isPartitioned) throws Exception {
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

        MigrateTableAction migrateTableAction =
                new MigrateTableAction(
                        "iceberg", "default." + icebergTable, catalogConf, "", 6, icebergOptions);
        migrateTableAction.run();

        tEnv.executeSql(paimonCatalogDdl(true));
        tEnv.executeSql("USE CATALOG my_paimon");
        Assertions.assertThatList(
                        Arrays.asList(Row.of("a", 1, 1), Row.of("b", 2, 2), Row.of("c", 3, 3)))
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

    protected static String data(int i) {
        Random random = new Random();
        StringBuilder stringBuilder = new StringBuilder();
        for (int m = 0; m < i; m++) {
            stringBuilder.append("(");
            stringBuilder.append("\"");
            stringBuilder.append('a' + m);
            stringBuilder.append("\",");
            stringBuilder.append(random.nextInt(10));
            stringBuilder.append(",");
            stringBuilder.append(random.nextInt(10));
            stringBuilder.append(")");
            if (m != i - 1) {
                stringBuilder.append(",");
            }
        }
        return stringBuilder.toString();
    }
}
