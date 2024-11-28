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

package org.apache.paimon.flink.sink;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.CompiledPlanUtils;
import org.apache.flink.util.TimeUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class WriterChainingStrategyTest {
    private static final String TABLE_NAME = "paimon_table";

    @TempDir java.nio.file.Path tempDir;

    private StreamTableEnvironment tEnv;

    @BeforeEach
    public void beforeEach() {
        Configuration config = new Configuration();
        config.setString(
                "execution.checkpointing.interval",
                TimeUtils.formatWithHighestUnit(Duration.ofMillis(500)));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        tEnv = StreamTableEnvironment.create(env);

        String catalog = "PAIMON";
        Map<String, String> options = new HashMap<>();
        options.put("type", "paimon");
        options.put("warehouse", tempDir.toString());
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH ( %s )",
                        catalog,
                        options.entrySet().stream()
                                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(","))));
        tEnv.useCatalog(catalog);
    }

    @Test
    public void testAppendTable() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, data STRING, dt STRING) "
                                        + "WITH ('bucket' = '1', 'bucket-key'='id', 'write-only' = 'true')",
                                TABLE_NAME))
                .await();

        verifyChaining(false, true);
    }

    @Test
    public void testAppendTableWithUnawareBucket() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, data STRING, dt STRING) "
                                        + "WITH ('bucket' = '-1', 'write-only' = 'true')",
                                TABLE_NAME))
                .await();

        verifyChaining(true, true);
    }

    @Test
    public void testPrimaryKeyTable() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, data STRING, dt STRING, PRIMARY KEY (id) NOT ENFORCED) "
                                        + "WITH ('bucket' = '1', 'bucket-key'='id', 'write-only' = 'true')",
                                TABLE_NAME))
                .await();

        verifyChaining(false, true);
    }

    @Test
    public void testPrimaryKeyTableWithDynamicBucket() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, data STRING, dt STRING, PRIMARY KEY (id) NOT ENFORCED) "
                                        + "WITH ('bucket' = '-1', 'write-only' = 'true')",
                                TABLE_NAME))
                .await();

        verifyChaining(false, true);
    }

    @Test
    public void testPrimaryKeyTableWithMultipleWriter() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, data STRING, dt STRING, PRIMARY KEY (id) NOT ENFORCED) "
                                        + "WITH ('bucket' = '1', 'bucket-key'='id', 'write-only' = 'true', 'sink.parallelism' = '2')",
                                TABLE_NAME))
                .await();

        verifyChaining(false, false);
    }

    @Test
    public void testPrimaryKeyTableWithCrossPartitionUpdate() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, data STRING, dt STRING, PRIMARY KEY (id) NOT ENFORCED) "
                                        + "PARTITIONED BY ( dt ) WITH ('bucket' = '-1', 'write-only' = 'true')",
                                TABLE_NAME))
                .await();

        List<JobVertex> vertices = verifyChaining(false, true);
        JobVertex vertex = findVertex(vertices, "INDEX_BOOTSTRAP");
        assertThat(vertex.toString()).contains("Source");
    }

    @Test
    public void testPrimaryKeyTableWithLocalMerge() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, data STRING, dt STRING, PRIMARY KEY (id) NOT ENFORCED) "
                                        + "WITH ('bucket' = '-1', 'write-only' = 'true', 'local-merge-buffer-size' = '1MB')",
                                TABLE_NAME))
                .await();

        List<JobVertex> vertices = verifyChaining(false, true);
        JobVertex vertex = findVertex(vertices, "local merge");
        assertThat(vertex.toString()).contains("Source");
    }

    private List<JobVertex> verifyChaining(
            boolean isWriterChainedWithUpstream, boolean isWriterChainedWithDownStream) {
        CompiledPlan plan =
                tEnv.compilePlanSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'AAA', ''), (2, 'BBB', '')",
                                TABLE_NAME));
        List<Transformation<?>> transformations = CompiledPlanUtils.toTransformations(tEnv, plan);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        transformations.forEach(env::addOperator);

        List<JobVertex> vertices = new ArrayList<>();
        env.getStreamGraph().getJobGraph().getVertices().forEach(vertices::add);
        JobVertex vertex = findVertex(vertices, "Writer");

        if (isWriterChainedWithUpstream) {
            assertThat(vertex.toString()).contains("Source");
        } else {
            assertThat(vertex.toString()).doesNotContain("Source");
        }

        if (isWriterChainedWithDownStream) {
            assertThat(vertex.toString()).contains("Committer");
        } else {
            assertThat(vertex.toString()).doesNotContain("Committer");
        }

        return vertices;
    }

    private JobVertex findVertex(List<JobVertex> vertices, String key) {
        for (JobVertex vertex : vertices) {
            if (vertex.toString().contains(key)) {
                return vertex;
            }
        }
        throw new IllegalStateException(
                String.format(
                        "Cannot find vertex with keyword %s among job vertices %s", key, vertices));
    }
}
