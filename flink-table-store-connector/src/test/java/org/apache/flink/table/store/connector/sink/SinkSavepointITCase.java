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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointStoppingException;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.store.file.utils.FailingFileIO;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** IT cases for {@link FileStoreSink} when writing file store and with savepoints. */
public class SinkSavepointITCase extends AbstractTestBase {

    private String path;
    private String failingName;

    @Before
    public void before() throws Exception {
        path = TEMPORARY_FOLDER.newFolder().toPath().toString();
        // for failure tests
        failingName = UUID.randomUUID().toString();
        FailingFileIO.reset(failingName, 100, 500);
    }

    @Test(timeout = 180000)
    public void testRecoverFromSavepoint() throws Exception {
        String failingPath = FailingFileIO.getFailingPath(failingName, path);
        String savepointPath = null;
        JobID jobId;
        ClusterClient<?> client = MINI_CLUSTER_RESOURCE.getClusterClient();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        OUTER:
        while (true) {
            // start a new job or recover from savepoint
            jobId = runRecoverFromSavepointJob(failingPath, savepointPath);
            while (true) {
                // wait for a random number of time before stopping with savepoint
                Thread.sleep(random.nextInt(5000));
                if (client.getJobStatus(jobId).get() == JobStatus.FINISHED) {
                    // job finished, check for result
                    break OUTER;
                }
                try {
                    // try to stop with savepoint
                    savepointPath =
                            client.stopWithSavepoint(
                                            jobId,
                                            false,
                                            path + "/savepoint",
                                            SavepointFormatType.DEFAULT)
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
            while (!client.getJobStatus(jobId).get().isGloballyTerminalState()) {
                Thread.sleep(1000);
            }
            // recover from savepoint in the next round
        }

        checkRecoverFromSavepointResult(failingPath);
    }

    private JobID runRecoverFromSavepointJob(String failingPath, String savepointPath)
            throws Exception {
        Configuration conf = new Configuration();
        if (savepointPath != null) {
            SavepointRestoreSettings savepointRestoreSettings =
                    SavepointRestoreSettings.forPath(savepointPath, false);
            SavepointRestoreSettings.toConfiguration(savepointRestoreSettings, conf);
        }

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(500));
        tEnv.getConfig().getConfiguration().set(StateBackendOptions.STATE_BACKEND, "filesystem");
        tEnv.getConfig()
                .getConfiguration()
                .set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + path + "/checkpoint");
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        // we're creating multiple table environments in the same process
        // if we do not set this option, stream node id will be different even with the same SQL
        // if stream node id is different then we can't recover from savepoint
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_LEGACY_TRANSFORMATION_UIDS, true);

        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE TABLE S (",
                        "  a INT",
                        ") WITH (",
                        "  'connector' = 'datagen',",
                        "  'rows-per-second' = '10000',",
                        "  'fields.a.kind' = 'sequence',",
                        "  'fields.a.start' = '0',",
                        "  'fields.a.end' = '99999'",
                        ")"));

        String createCatalogSql =
                String.join(
                        "\n",
                        "CREATE CATALOG my_catalog WITH (",
                        "  'type' = 'table-store',",
                        "  'warehouse' = '" + failingPath + "'",
                        ")");
        FailingFileIO.retryArtificialException(() -> tEnv.executeSql(createCatalogSql));

        tEnv.executeSql("USE CATALOG my_catalog");

        String createSinkSql =
                String.join(
                        "\n",
                        "CREATE TABLE IF NOT EXISTS T (",
                        "  a INT",
                        ") WITH (",
                        "  'bucket' = '2',",
                        "  'file.format' = 'avro'",
                        ")");
        FailingFileIO.retryArtificialException(() -> tEnv.executeSql(createSinkSql));

        String insertIntoSql = "INSERT INTO T SELECT * FROM default_catalog.default_database.S";
        JobID jobId =
                FailingFileIO.retryArtificialException(() -> tEnv.executeSql(insertIntoSql))
                        .getJobClient()
                        .get()
                        .getJobID();

        ClusterClient<?> client = MINI_CLUSTER_RESOURCE.getClusterClient();
        while (client.getJobStatus(jobId).get() == JobStatus.INITIALIZING) {
            Thread.sleep(1000);
        }
        return jobId;
    }

    private void checkRecoverFromSavepointResult(String failingPath) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        // no failure should occur when checking for answer
        FailingFileIO.reset(failingName, 0, 1);

        String createCatalogSql =
                String.join(
                        "\n",
                        "CREATE CATALOG my_catalog WITH (",
                        "  'type' = 'table-store',",
                        "  'warehouse' = '" + failingPath + "'",
                        ")");
        tEnv.executeSql(createCatalogSql);

        tEnv.executeSql("USE CATALOG my_catalog");

        List<Integer> actual = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql("SELECT * FROM T").collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                Assert.assertEquals(1, row.getArity());
                actual.add((Integer) row.getField(0));
            }
        }
        Collections.sort(actual);
        Assert.assertEquals(
                IntStream.range(0, 100000).boxed().collect(Collectors.toList()), actual);
    }
}
