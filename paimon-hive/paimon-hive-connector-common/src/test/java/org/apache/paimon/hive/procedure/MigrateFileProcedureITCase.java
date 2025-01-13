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
import org.apache.paimon.flink.action.MigrateFileAction;
import org.apache.paimon.flink.procedure.MigrateFileProcedure;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

/** Tests for {@link MigrateFileProcedure}. */
public class MigrateFileProcedureITCase extends ActionITCaseBase {

    private static final TestHiveMetastore TEST_HIVE_METASTORE = new TestHiveMetastore();

    private static final int PORT = 9085;

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
    public void testMigrateFile(String format, boolean isNamedArgument) throws Exception {
        test(format, isNamedArgument);
        testMigrateFileAction(format, isNamedArgument);
    }

    public void test(String format, boolean isNamedArgument) throws Exception {
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
        tEnv.executeSql(
                "CREATE TABLE paimontable (id STRING, id2 INT, id3 INT) PARTITIONED BY (id2, id3) with ('bucket' = '-1');");
        if (isNamedArgument) {
            tEnv.executeSql(
                            "CALL sys.migrate_file(connector => 'hive', source_table => 'default.hivetable', target_table => 'default.paimontable')")
                    .await();
        } else {
            tEnv.executeSql(
                            "CALL sys.migrate_file('hive', 'default.hivetable', 'default.paimontable')")
                    .await();
        }
        List<Row> r2 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM paimontable").collect());

        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    public void testMigrateFileAction(String format, boolean isNamedArgument) throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql(
                "CREATE TABLE hivetable01 (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + format);
        tEnv.executeSql("INSERT INTO hivetable01 VALUES" + data(100)).await();

        tEnv.executeSql(
                "CREATE TABLE hivetable02 (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + format);
        tEnv.executeSql("INSERT INTO hivetable02 VALUES" + data(100)).await();
        tEnv.executeSql("SHOW CREATE TABLE hivetable02");

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'metastore' = 'hive', 'uri' = 'thrift://localhost:"
                        + PORT
                        + "' , 'warehouse' = '"
                        + System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname)
                        + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql(
                "CREATE TABLE paimontable01 (id STRING, id2 INT, id3 INT) PARTITIONED BY (id2, id3) with ('bucket' = '-1');");
        tEnv.executeSql(
                "CREATE TABLE paimontable02 (id STRING, id2 INT, id3 INT) PARTITIONED BY (id2, id3) with ('bucket' = '-1');");

        if (isNamedArgument) {
            tEnv.executeSql(
                            "CALL sys.migrate_file(connector => 'hive', source_table => 'default.hivetable01', target_table => 'default.paimontable01', delete_origin => false)")
                    .await();
        } else {
            tEnv.executeSql(
                            "CALL sys.migrate_file('hive', 'default.hivetable01', 'default.paimontable01', false)")
                    .await();
        }

        tEnv.useCatalog("PAIMON_GE");
        Map<String, String> catalogConf = new HashMap<>();
        catalogConf.put("metastore", "hive");
        catalogConf.put("uri", "thrift://localhost:" + PORT);
        catalogConf.put(
                "warehouse", System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));
        MigrateFileAction migrateFileAction =
                new MigrateFileAction(
                        "hive",
                        "default.hivetable02",
                        "default.paimontable02",
                        false,
                        catalogConf,
                        "",
                        6);
        migrateFileAction.run();

        tEnv.useCatalog("HIVE");
        List<Row> r1 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hivetable01").collect());
        List<Row> r2 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hivetable02").collect());
        Assertions.assertThat(r1.size() == 0);
        Assertions.assertThat(r2.size() == 0);
    }

    private String data(int i) {
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
