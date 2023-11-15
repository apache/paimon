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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Integration test for {@link FlinkExternalCatalog}. */
public class FlinkExternalCatalogITCase {

    protected TableEnvironment tEnv;

    @TempDir public static java.nio.file.Path path;

    @TempDir public static java.nio.file.Path path2;

    @BeforeEach
    public void setup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        String catalog = "PAIMON_EXTERNAL";
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
        tEnv.useCatalog(catalog);
    }

    @Test
    public void testUseExternalTable() {
        tEnv.executeSql(
                "CREATE TABLE word_count (\n"
                        + "    word STRING PRIMARY KEY NOT ENFORCED,\n"
                        + "    cnt BIGINT\n"
                        + ") with (\n"
                        + "\t'connector'='paimon'\n"
                        + ")");
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE file (\n"
                                + "    word STRING PRIMARY KEY NOT ENFORCED,\n"
                                + "    cnt BIGINT\n"
                                + ") with (\n"
                                + "    'connector'='filesystem',\n"
                                + "    'path'='file://%s', \n"
                                + "    'format'='json' \n"
                                + ")",
                        path2.toUri().getPath()));
        tEnv.executeSql("insert into file select * from `word_count`");
    }
}
