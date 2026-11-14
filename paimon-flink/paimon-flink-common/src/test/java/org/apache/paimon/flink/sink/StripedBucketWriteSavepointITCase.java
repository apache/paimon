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

import org.apache.paimon.flink.util.AbstractTestBase;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointStoppingException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Establishes the real behavior of restoring a striped write-only fixed-bucket sink from a
 * savepoint after the writer parallelism (and therefore the stripe count) changes.
 */
public class StripedBucketWriteSavepointITCase extends AbstractTestBase {

    private static final int NUM_KEYS = 5000;
    private static final int NUM_RECORDS = 100000;

    private String path;

    @BeforeEach
    public void before() throws Exception {
        path = getTempDirPath();
    }

    @Test
    @Timeout(180)
    public void testRescaleFromSavepointKeepsAllRows() throws Exception {
        // job 1: striped (writer parallelism 4 > bucket 2), stopped with a savepoint mid-stream
        String savepointPath = stopWithSavepoint(runJob(4, null));

        // job 2: restore from the savepoint with a different writer parallelism (and stripe count),
        // then run the bounded source to completion
        JobClient jobClient = runJob(2, savepointPath);
        while (jobClient.getJobStatus().get() != JobStatus.FINISHED) {
            assertThat(jobClient.getJobStatus().get().isGloballyTerminalState())
                    .as("job terminated without finishing: %s", jobClient.getJobStatus().get())
                    .isFalse();
            Thread.sleep(1000);
        }

        // every key must be present with its latest value: a rescaled restore neither drops
        // committed rows nor corrupts the striped routing
        Map<Integer, Integer> expected = new HashMap<>();
        for (int i = 0; i < NUM_RECORDS; i++) {
            expected.put(i % NUM_KEYS, i);
        }
        Map<Integer, Integer> actual = new HashMap<>();
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE CATALOG my_catalog WITH (",
                        "  'type' = 'paimon',",
                        "  'warehouse' = '" + path + "'",
                        ")"));
        tEnv.executeSql("USE CATALOG my_catalog");
        try (CloseableIterator<Row> it = tEnv.executeSql("SELECT * FROM T").collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                assertThat(row.getArity()).isEqualTo(2);
                actual.put((Integer) row.getField(0), (Integer) row.getField(1));
            }
        }
        assertThat(actual).isEqualTo(expected);
    }

    private String stopWithSavepoint(JobClient jobClient) throws Exception {
        while (true) {
            Thread.sleep(2000);
            if (jobClient.getJobStatus().get() == JobStatus.FINISHED) {
                throw new AssertionError("job finished before a savepoint could be taken");
            }
            try {
                return jobClient
                        .stopWithSavepoint(false, path + "/savepoint", SavepointFormatType.DEFAULT)
                        .get();
            } catch (Exception e) {
                if (ExceptionUtils.findThrowable(e, InterruptedException.class).isPresent()) {
                    throw e;
                }
                Optional<StopWithSavepointStoppingException> t =
                        ExceptionUtils.findThrowable(e, StopWithSavepointStoppingException.class);
                if (t.isPresent()) {
                    return t.get().getSavepointPath();
                }
                // the savepoint may fail transiently (job still initializing, or already
                // stopping); retry until it succeeds or the job finishes
            }
        }
    }

    private JobClient runJob(int sinkParallelism, String savepointPath) throws Exception {
        Configuration conf = new Configuration();
        if (savepointPath != null) {
            SavepointRestoreSettings.toConfiguration(
                    SavepointRestoreSettings.forPath(savepointPath, false), conf);
        }
        TableEnvironment tEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(500)
                        .parallelism(1)
                        .allowRestart()
                        .setConf(conf)
                        .setConf(StateBackendOptions.STATE_BACKEND, "hashmap")
                        .setConf(
                                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                                "file://" + path + "/checkpoint")
                        .build();

        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE CATALOG my_catalog WITH (",
                        "  'type' = 'paimon',",
                        "  'warehouse' = '" + path + "'",
                        ")"));
        tEnv.executeSql("USE CATALOG my_catalog");

        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE TEMPORARY TABLE S (",
                        "  a INT",
                        ") WITH (",
                        "  'connector' = 'datagen',",
                        "  'rows-per-second' = '20000',",
                        "  'fields.a.kind' = 'sequence',",
                        "  'fields.a.start' = '0',",
                        "  'fields.a.end' = '" + (NUM_RECORDS - 1) + "'",
                        ")"));

        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE TABLE IF NOT EXISTS T (",
                        "  k INT,",
                        "  v INT,",
                        "  PRIMARY KEY (k) NOT ENFORCED",
                        ") WITH (",
                        "  'bucket' = '2',",
                        "  'file.format' = 'avro',",
                        "  'write-only' = 'true',",
                        "  'sink.striped-bucket-write.enabled' = 'true'",
                        ")"));

        String insertIntoSql =
                String.format(
                        "INSERT INTO T /*+ OPTIONS('sink.parallelism' = '%d') */ "
                                + "SELECT (a %% %d) AS k, a AS v FROM S",
                        sinkParallelism, NUM_KEYS);
        JobClient jobClient = tEnv.executeSql(insertIntoSql).getJobClient().get();
        while (jobClient.getJobStatus().get() == JobStatus.INITIALIZING) {
            Thread.sleep(500);
        }
        return jobClient;
    }
}
