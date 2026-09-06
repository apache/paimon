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
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT case for the split shuffle of a streaming read over an unaware bucket table. Such a table has
 * a single bucket, so a bucket based shuffle would send every split to one reader; splits must be
 * rebalanced over all of them instead.
 */
public class UnawareBucketSplitShuffleITCase {

    private static final int READER_PARALLELISM = 4;

    // matches the reader operator of every subtask, the planner names it "table_name[id]"
    private static final String READER_METRIC_PATTERN = "append_scalable_table\\[";

    private static final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();

    @RegisterExtension
    protected static final MiniClusterWithClientExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterWithClientExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(READER_PARALLELISM)
                            .setConfiguration(reporter.addToConfiguration(new Configuration()))
                            .build());

    @TempDir Path tempPath;

    @AfterEach
    public final void cleanupRunningJobs() throws Exception {
        ClusterClient<?> clusterClient = MINI_CLUSTER_EXTENSION.createRestClusterClient();
        for (JobStatusMessage job : clusterClient.listJobs().get()) {
            if (!job.getJobState().isTerminalState()) {
                try {
                    clusterClient.cancel(job.getJobId()).get();
                } catch (Exception ignored) {
                    // ignore exceptions when cancelling dangling jobs
                }
            }
        }
    }

    @Test
    public void testReadUnawareBucketTableWithRebalanceShuffle() throws Exception {
        TableEnvironment batchEnv = tableEnv(false);
        batchEnv.executeSql(
                "CREATE TABLE append_scalable_table (id INT, data STRING) "
                        + "WITH ('bucket' = '-1', 'consumer-id' = 'test', 'consumer.expiration-time' = '365 d', "
                        + "'target-file-size' = '1 B', 'source.split.target-size' = '1 B', 'scan.parallelism' = '4')");
        for (int i = 0; i < READER_PARALLELISM; i++) {
            batchEnv.executeSql("INSERT INTO append_scalable_table VALUES (1, 'AAA'), (2, 'BBB')")
                    .await();
        }

        TableResult result = tableEnv(true).executeSql("SELECT id FROM append_scalable_table");
        JobID jobId = result.getJobClient().get().getJobID();

        try (BlockingIterator<Row, Row> iterator = BlockingIterator.of(result.collect())) {
            // collect every row, which record arrives first is not deterministic once the splits
            // are spread over concurrent readers
            assertThat(iterator.collect(2 * READER_PARALLELISM))
                    .containsExactlyInAnyOrder(
                            Row.of(1), Row.of(1), Row.of(1), Row.of(1), Row.of(2), Row.of(2),
                            Row.of(2), Row.of(2));

            // the property under test: all rows are in, so every split has been handed out, and
            // the rebalance must have left no reader empty. The bucket shuffle used before #3955
            // sends every split of a single bucket table to one channel and gives [0, 0, 0, 8]
            assertThat(recordsReadPerReaderSubtask(jobId))
                    .hasSize(READER_PARALLELISM)
                    .allSatisfy(records -> assertThat(records).isPositive());
        }
    }

    // the reporter registers one metric group per reader subtask, so one entry per subtask
    private List<Long> recordsReadPerReaderSubtask(JobID jobId) throws Exception {
        for (int tries = 1; tries <= 20; tries++) {
            List<OperatorMetricGroup> groups =
                    reporter.findOperatorMetricGroups(jobId, READER_METRIC_PATTERN);
            if (groups.size() == READER_PARALLELISM) {
                return groups.stream()
                        .map(group -> group.getIOMetricGroup().getNumRecordsInCounter().getCount())
                        .collect(Collectors.toList());
            }
            Thread.sleep(1000);
        }
        throw new AssertionError(
                "Reader metric groups of job " + jobId + " did not show up, pattern may be stale");
    }

    private TableEnvironment tableEnv(boolean streaming) {
        EnvironmentSettings.Builder settings = EnvironmentSettings.newInstance();
        TableEnvironment tEnv =
                TableEnvironment.create(
                        (streaming ? settings.inStreamingMode() : settings.inBatchMode()).build());
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.useCatalog("mycat");
        return tEnv;
    }
}
