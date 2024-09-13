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
import org.apache.paimon.flink.action.MigrateDatabaseAction;
import org.apache.paimon.flink.procedure.MigrateDatabaseProcedure;
import org.apache.paimon.hive.TestHiveMetastore;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.conf.HiveConf;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.paimon.hive.procedure.MigrateTableProcedureITCase.data;

/** Tests for {@link MigrateDatabaseProcedure}. */
public class MigrateDatabaseProcedureITCase extends ActionITCaseBase {

    private static final TestHiveMetastore TEST_HIVE_METASTORE = new TestHiveMetastore();

    private static final int PORT = 9086;

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

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testMigrateDatabaseProcedure(String format, boolean isNamedArgument)
            throws Exception {
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
        tEnv.executeSql("CREATE DATABASE my_database");
        tEnv.executeSql("USE my_database");

        // write data into my_database.hivetable1
        tEnv.executeSql(
                "CREATE TABLE hivetable1 (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + format);
        tEnv.executeSql("INSERT INTO hivetable1 VALUES" + data(100)).await();
        tEnv.executeSql("SHOW CREATE TABLE hivetable1");

        // write data into my_database.hivetable2
        tEnv.executeSql(
                "CREATE TABLE hivetable2 (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + format);
        tEnv.executeSql("INSERT INTO hivetable2 VALUES" + data(100)).await();

        tEnv.executeSql("SHOW CREATE TABLE hivetable2");

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<Row> r1 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable1").collect());
        List<Row> r3 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable2").collect());

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'metastore' = 'hive', 'uri' = 'thrift://localhost:"
                        + PORT
                        + "' , 'warehouse' = '"
                        + System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname)
                        + "')");
        tEnv.useCatalog("PAIMON");
        if (isNamedArgument) {
            tEnv.executeSql(
                            "CALL sys.migrate_database(connector => 'hive', source_database => 'my_database', options => 'file.format="
                                    + format
                                    + "')")
                    .await();
        } else {
            tEnv.executeSql(
                            "CALL sys.migrate_database('hive', 'my_database', 'file.format="
                                    + format
                                    + "')")
                    .await();
        }
        List<Row> r2 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable1").collect());
        List<Row> r4 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable2").collect());

        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
        Assertions.assertThatList(r3).containsExactlyInAnyOrderElementsOf(r4);
    }

    public void testUpgradeNonPartitionTable(String format, boolean isNamedArgument)
            throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE my_database");
        tEnv.executeSql("USE my_database");

        // write data into my_database.hivetable1
        tEnv.executeSql(
                "CREATE TABLE hivetable1 (id string, id2 int, id3 int) STORED AS " + format);
        tEnv.executeSql("INSERT INTO hivetable1 VALUES" + data(100)).await();
        tEnv.executeSql("SHOW CREATE TABLE hivetable1");

        // write data into my_database.hivetable2
        tEnv.executeSql(
                "CREATE TABLE hivetable2 (id string, id2 int, id3 int) STORED AS " + format);
        tEnv.executeSql("INSERT INTO hivetable2 VALUES" + data(100)).await();

        tEnv.executeSql("SHOW CREATE TABLE hivetable2");

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<Row> r1 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable1").collect());
        List<Row> r3 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable2").collect());

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'metastore' = 'hive', 'uri' = 'thrift://localhost:"
                        + PORT
                        + "' , 'warehouse' = '"
                        + System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname)
                        + "')");
        tEnv.useCatalog("PAIMON");
        if (isNamedArgument) {
            tEnv.executeSql(
                            "CALL sys.migrate_database(connector => 'hive', source_database => 'my_database', options => 'file.format="
                                    + format
                                    + "')")
                    .await();
        } else {
            tEnv.executeSql(
                            "CALL sys.migrate_database('hive', 'my_database', 'file.format="
                                    + format
                                    + "')")
                    .await();
        }
        List<Row> r2 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable1").collect());
        List<Row> r4 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable2").collect());

        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
        Assertions.assertThatList(r3).containsExactlyInAnyOrderElementsOf(r4);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet", "avro"})
    public void testMigrateDatabaseAction(String format) throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE my_database");
        tEnv.executeSql("USE my_database");

        // write data into my_database.hivetable1
        tEnv.executeSql(
                "CREATE TABLE hivetable1 (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + format);
        tEnv.executeSql("INSERT INTO hivetable1 VALUES" + data(100)).await();
        tEnv.executeSql("SHOW CREATE TABLE hivetable1");

        // write data into my_database.hivetable2
        tEnv.executeSql(
                "CREATE TABLE hivetable2 (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + format);
        tEnv.executeSql("INSERT INTO hivetable2 VALUES" + data(100)).await();

        tEnv.executeSql("SHOW CREATE TABLE hivetable2");

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<Row> r1 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable1").collect());
        List<Row> r3 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable2").collect());

        Map<String, String> catalogConf = new HashMap<>();
        catalogConf.put("metastore", "hive");
        catalogConf.put("uri", "thrift://localhost:" + PORT);
        MigrateDatabaseAction migrateDatabaseAction =
                new MigrateDatabaseAction(
                        "hive",
                        System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
                        "my_database",
                        catalogConf,
                        "",
                        6);
        migrateDatabaseAction.run();

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'metastore' = 'hive', 'uri' = 'thrift://localhost:"
                        + PORT
                        + "' , 'warehouse' = '"
                        + System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname)
                        + "')");
        tEnv.useCatalog("PAIMON");
        List<Row> r2 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable1").collect());
        List<Row> r4 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM my_database.hivetable2").collect());

        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
        Assertions.assertThatList(r3).containsExactlyInAnyOrderElementsOf(r4);
    }
}
