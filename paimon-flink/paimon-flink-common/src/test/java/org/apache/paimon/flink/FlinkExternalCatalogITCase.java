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

package org.apache.paimon.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link FlinkExternalCatalog}. */
public class FlinkExternalCatalogITCase {

    private TableEnvironment tEnv;
    private String catalog = "PAIMON_EXTERNAL";
    private String catalog2 = "PAIMON_EXTERNAL2";

    @TempDir public static java.nio.file.Path path;

    @TempDir public static java.nio.file.Path path2;

    @BeforeEach
    public void setup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        Map<String, String> options = new HashMap<>();
        options.put("type", "paimon-external");
        options.put("warehouse", path.toUri().getPath());
        tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, false);
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH (" + "%s" + ")",
                        catalog,
                        options.entrySet().stream()
                                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(","))));
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH (" + "%s" + ")",
                        catalog2,
                        options.entrySet().stream()
                                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(","))));
    }

    @Test
    public void testMix() {
        tEnv.useCatalog(catalog);
        tEnv.executeSql(
                "CREATE TABLE word_count1 (\n"
                        + "    word STRING PRIMARY KEY NOT ENFORCED,\n"
                        + "    cnt BIGINT\n"
                        + ") with (\n"
                        + "\t'connector'='paimon'\n"
                        + ")");
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE file1 (\n"
                                + "    word STRING PRIMARY KEY NOT ENFORCED,\n"
                                + "    cnt BIGINT\n"
                                + ") with (\n"
                                + "    'connector'='filesystem',\n"
                                + "    'path'='file://%s', \n"
                                + "    'format'='json' \n"
                                + ")",
                        path2.toUri().getPath()));
        tEnv.executeSql("insert into file1 select * from `word_count1`");
    }

    @Test
    public void testUseSystemTable() {
        tEnv.useCatalog(catalog);
        tEnv.executeSql(
                "CREATE TABLE word_count2 (\n"
                        + "    word STRING PRIMARY KEY NOT ENFORCED,\n"
                        + "    cnt BIGINT\n"
                        + ") with (\n"
                        + "\t'connector'='paimon'\n"
                        + ")");
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE file2 (\n"
                                + "    word STRING PRIMARY KEY NOT ENFORCED,\n"
                                + "    rowkind String,\n"
                                + "    cnt BIGINT\n"
                                + ") with (\n"
                                + "    'connector'='filesystem',\n"
                                + "    'path'='file://%s', \n"
                                + "    'format'='json' \n"
                                + ")",
                        path2.toUri().getPath()));
        tEnv.executeSql("insert into file2 select * from word_count2$audit_log");
    }

    @Test
    public void testUseExternalTable() {
        tEnv.useCatalog(catalog);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE file3 (\n"
                                + "    word STRING PRIMARY KEY NOT ENFORCED,\n"
                                + "    rowkind String,\n"
                                + "    cnt BIGINT\n"
                                + ") with (\n"
                                + "    'connector'='filesystem',\n"
                                + "    'path'='file://%s', \n"
                                + "    'format'='json' \n"
                                + ")",
                        path2.toUri().getPath()));
        Table fileTable = tEnv.from("file3");
        tEnv.useCatalog(catalog2);
        Table fileTable2 = tEnv.from("file3");
        assertThat(fileTable.getResolvedSchema()).isEqualTo(fileTable2.getResolvedSchema());
    }
}
