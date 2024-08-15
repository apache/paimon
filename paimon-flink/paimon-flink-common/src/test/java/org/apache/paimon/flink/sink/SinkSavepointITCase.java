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
import org.apache.paimon.utils.FailingFileIO;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link FlinkSink} when writing file store and with savepoints. */
public class SinkSavepointITCase extends AbstractTestBase {

    private String path;
    private String failingName;

    @BeforeEach
    public void before() throws Exception {
        path = getTempDirPath();
        // for failure tests
        failingName = UUID.randomUUID().toString();
    }

    @Test
    @Timeout(180)
    public void testRecoverFromSavepoint() throws Exception {
        String failingPath = FailingFileIO.getFailingPath(failingName, path);
        String savepointPath = null;

        ThreadLocalRandom random = ThreadLocalRandom.current();
        boolean enableFailure = random.nextBoolean();
        if (enableFailure) {
            FailingFileIO.reset(failingName, 5, 500);
        } else {
            FailingFileIO.reset(failingName, 0, 1);
        }

        OUTER:
        while (true) {
            // start a new job or recover from savepoint
            JobClient jobClient = runRecoverFromSavepointJob(failingPath, savepointPath);
            while (true) {
                // wait for a random number of time before stopping with savepoint
                Thread.sleep(random.nextInt(5000));
                if (jobClient.getJobStatus().get() == JobStatus.FINISHED) {
                    // job finished, check for result
                    break OUTER;
                }
                try {
                    // try to stop with savepoint
                    savepointPath =
                            jobClient
                                    .stopWithSavepoint(
                                            false, path + "/savepoint", SavepointFormatType.DEFAULT)
                                    .get();
                    break;
                } catch (Exception e) {
                    Optional<StopWithSavepointStoppingException> t =
                            ExceptionUtils.findThrowable(
                                    e, StopWithSavepointStoppingException.class);
                    if (t.isPresent()) {
                        // savepoint has been created but notifyCheckpointComplete is not called
                        //
                        // user should follow the exception message and recover job from the
                        // specific savepoint
                        savepointPath = t.get().getSavepointPath();
                        break;
                    }
                    // savepoint creation may fail due to various reasons (for example the job is in
                    // failing state, or the job has finished), just wait for a while and try again
                }
            }
            // wait for job to stop
            while (!jobClient.getJobStatus().get().isGloballyTerminalState()) {
                Thread.sleep(1000);
            }
            // recover from savepoint in the next round
        }

        checkRecoverFromSavepointBatchResult();
        checkRecoverFromSavepointStreamingResult();
    }

    private JobClient runRecoverFromSavepointJob(String failingPath, String savepointPath)
            throws Exception {
        Configuration conf = new Configuration();
        if (savepointPath != null) {
            SavepointRestoreSettings savepointRestoreSettings =
                    SavepointRestoreSettings.forPath(savepointPath, false);
            SavepointRestoreSettings.toConfiguration(savepointRestoreSettings, conf);
        }

        TableEnvironment tEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(500)
                        // input data must be strictly ordered for us to check changelog results
                        .parallelism(1)
                        .allowRestart()
                        .setConf(conf)
                        .setConf(StateBackendOptions.STATE_BACKEND, "filesystem")
                        .setConf(
                                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                                "file://" + path + "/checkpoint")
                        .build();

        String createCatalogSql =
                String.join(
                        "\n",
                        "CREATE CATALOG my_catalog WITH (",
                        "  'type' = 'paimon',",
                        "  'warehouse' = '" + failingPath + "'",
                        ")");
        FailingFileIO.retryArtificialException(() -> tEnv.executeSql(createCatalogSql));
        tEnv.executeSql("USE CATALOG my_catalog");

        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE TEMPORARY TABLE S (",
                        "  a INT",
                        ") WITH (",
                        "  'connector' = 'datagen',",
                        "  'rows-per-second' = '10000',",
                        "  'fields.a.kind' = 'sequence',",
                        "  'fields.a.start' = '0',",
                        "  'fields.a.end' = '99999'",
                        ")"));

        String createSinkSql =
                String.join(
                        "\n",
                        "CREATE TABLE IF NOT EXISTS T (",
                        "  k INT,",
                        "  v INT,",
                        "  PRIMARY KEY (k) NOT ENFORCED",
                        ") WITH (",
                        "  'bucket' = '4',",
                        "  'file.format' = 'avro',",
                        "  'changelog-producer' = 'full-compaction',",
                        "  'full-compaction.delta-commits' = '3'",
                        ")");
        FailingFileIO.retryArtificialException(() -> tEnv.executeSql(createSinkSql));

        // test changing sink parallelism by using a random parallelism
        String insertIntoSql =
                String.format(
                        "INSERT INTO T /*+ OPTIONS('sink.parallelism' = '%d') */ SELECT (a %% 15000) AS k, a AS v FROM S",
                        ThreadLocalRandom.current().nextInt(3) + 2);
        JobClient jobClient =
                FailingFileIO.retryArtificialException(() -> tEnv.executeSql(insertIntoSql))
                        .getJobClient()
                        .get();

        while (jobClient.getJobStatus().get() == JobStatus.INITIALIZING) {
            Thread.sleep(1000);
        }
        return jobClient;
    }

    private void checkRecoverFromSavepointBatchResult() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE CATALOG my_catalog WITH (",
                        "  'type' = 'paimon',",
                        "  'warehouse' = '" + path + "'",
                        ")"));
        tEnv.executeSql("USE CATALOG my_catalog");

        Map<Integer, Integer> expected = new HashMap<>();
        for (int i = 0; i < 100000; i++) {
            expected.put(i % 15000, i);
        }

        Map<Integer, Integer> actual = new HashMap<>();
        try (CloseableIterator<Row> it = tEnv.executeSql("SELECT * FROM T").collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                assertThat(row.getArity()).isEqualTo(2);
                actual.put((Integer) row.getField(0), (Integer) row.getField(1));
            }
        }
        assertThat(actual).isEqualTo(expected);
    }

    private void checkRecoverFromSavepointStreamingResult() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().streamingMode().build();
        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE CATALOG my_catalog WITH (",
                        "  'type' = 'paimon',",
                        "  'warehouse' = '" + path + "'",
                        ")"));
        tEnv.executeSql("USE CATALOG my_catalog");

        Map<Integer, Integer> expected = new HashMap<>();
        for (int i = 0; i < 100000; i++) {
            expected.put(i % 15000, i);
        }
        Set<Integer> expectedValues = new HashSet<>(expected.values());

        int endCount = 0;
        Map<Integer, Integer> actual = new HashMap<>();
        try (CloseableIterator<Row> it =
                tEnv.executeSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id' = '2') */")
                        .collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                assertThat(row.getArity()).isEqualTo(2);

                int k = (Integer) row.getField(0);
                int v = (Integer) row.getField(1);
                switch (row.getKind()) {
                    case INSERT:
                    case UPDATE_AFTER:
                        assertThat(actual).doesNotContainKey(k);
                        actual.put(k, v);
                        break;
                    case DELETE:
                    case UPDATE_BEFORE:
                        assertThat(actual).containsKey(k);
                        actual.remove(k);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unknown row kind " + row.getKind());
                }

                if (expectedValues.contains(v)) {
                    endCount++;
                }
                if (endCount >= expectedValues.size()) {
                    break;
                }
            }
        }
        assertThat(actual).isEqualTo(expected);
    }
}
