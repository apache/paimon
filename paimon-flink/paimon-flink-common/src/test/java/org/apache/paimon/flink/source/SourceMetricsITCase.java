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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.util.MiniClusterWithClientExtension;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Metrics related IT cases for Paimon Flink source. */
public class SourceMetricsITCase {

    private static final int DEFAULT_PARALLELISM = 4;
    private static final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();

    @RegisterExtension
    protected static final MiniClusterWithClientExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterWithClientExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setConfiguration(reporter.addToConfiguration(new Configuration()))
                            .build());

    @TempDir protected static Path tempPath;

    @AfterEach
    public final void cleanupRunningJobs() throws Exception {
        ClusterClient<?> clusterClient = MINI_CLUSTER_EXTENSION.createRestClusterClient();
        for (JobStatusMessage path : clusterClient.listJobs().get()) {
            if (!path.getJobState().isTerminalState()) {
                try {
                    clusterClient.cancel(path.getJobId()).get();
                } catch (Exception ignored) {
                    // ignore exceptions when cancelling dangling jobs
                }
            }
        }
    }

    @Test
    public void testNumRecordsIn() throws Exception {
        TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql("CREATE TABLE T ( k INT, v INT, PRIMARY KEY (k) NOT ENFORCED )");
        tEnv.executeSql("INSERT INTO T VALUES (1, 10), (2, 20), (3, 30)").await();
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE B ( k INT, v INT ) WITH ( 'connector' = 'blackhole' )");
        TableResult tableResult = tEnv.executeSql("INSERT INTO B SELECT * FROM T");
        JobClient client = tableResult.getJobClient().get();
        JobID jobId = client.getJobID();
        tableResult.await();

        for (OperatorMetricGroup group : reporter.findOperatorMetricGroups(jobId, "Source: T")) {
            assertThat(group.getIOMetricGroup().getNumRecordsInCounter().getCount()).isEqualTo(3);
        }
    }
}
