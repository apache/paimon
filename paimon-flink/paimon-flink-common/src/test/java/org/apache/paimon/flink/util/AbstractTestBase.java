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

package org.apache.paimon.flink.util;

import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Similar to Flink's AbstractTestBase but using Junit5. */
public class AbstractTestBase {

    private static final int DEFAULT_PARALLELISM = 16;

    @RegisterExtension
    protected static final MiniClusterWithClientExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterWithClientExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .build());

    @TempDir protected static Path temporaryFolder;

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

    // ----------------------------------------------------------------------------------------------------------------
    //  Temporary File Utilities
    // ----------------------------------------------------------------------------------------------------------------

    protected String getTempDirPath() {
        return getTempDirPath("");
    }

    protected String getTempDirPath(String dirName) {
        return createAndRegisterTempFile(dirName).toString();
    }

    protected String getTempFilePath(String fileName) {
        return createAndRegisterTempFile(fileName).toString();
    }

    protected String createTempFile(String fileName, String contents) throws IOException {
        File f = createAndRegisterTempFile(fileName);
        if (!f.getParentFile().exists()) {
            f.getParentFile().mkdirs();
        }
        f.createNewFile();
        FileIOUtils.writeFileUtf8(f, contents);
        return f.toString();
    }

    /** Create a subfolder to avoid returning the same folder when passing same file name. */
    protected File createAndRegisterTempFile(String fileName) {
        return new File(
                temporaryFolder.toFile(), String.format("%s/%s", UUID.randomUUID(), fileName));
    }

    // ----------------------------------------------------------------------------------------------------------------
    //  Table Environment Utilities
    // ----------------------------------------------------------------------------------------------------------------

    protected TableEnvironmentBuilder tableEnvironmentBuilder() {
        return new TableEnvironmentBuilder();
    }

    /** Builder for {@link TableEnvironmentBuilder} in tests. */
    protected static class TableEnvironmentBuilder {

        private boolean streamingMode = true;
        private Integer parallelism = null;
        private Integer checkpointIntervalMs = null;
        private boolean allowRestart = false;
        private Configuration conf = new Configuration();

        public TableEnvironmentBuilder batchMode() {
            this.streamingMode = false;
            return this;
        }

        public TableEnvironmentBuilder streamingMode() {
            this.streamingMode = true;
            return this;
        }

        public TableEnvironmentBuilder parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public TableEnvironmentBuilder checkpointIntervalMs(int checkpointIntervalMs) {
            this.checkpointIntervalMs = checkpointIntervalMs;
            return this;
        }

        public TableEnvironmentBuilder allowRestart() {
            this.allowRestart = true;
            return this;
        }

        public TableEnvironmentBuilder allowRestart(boolean allowRestart) {
            this.allowRestart = allowRestart;
            return this;
        }

        public <T> TableEnvironmentBuilder setConf(ConfigOption<T> option, T value) {
            conf.set(option, value);
            return this;
        }

        public TableEnvironmentBuilder setConf(Configuration conf) {
            this.conf.addAll(conf);
            return this;
        }

        public TableEnvironment build() {
            TableEnvironment tEnv;
            if (streamingMode) {
                tEnv =
                        TableEnvironment.create(
                                EnvironmentSettings.newInstance().inStreamingMode().build());
                tEnv.getConfig()
                        .set(
                                ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
                                ExecutionConfigOptions.UpsertMaterialize.NONE);
                if (checkpointIntervalMs != null) {
                    tEnv.getConfig()
                            .getConfiguration()
                            .set(
                                    ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                                    Duration.ofMillis(checkpointIntervalMs));
                }
            } else {
                tEnv =
                        TableEnvironment.create(
                                EnvironmentSettings.newInstance().inBatchMode().build());
            }

            if (parallelism != null) {
                tEnv.getConfig()
                        .getConfiguration()
                        .set(
                                ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
                                parallelism);
            }

            if (allowRestart) {
                tEnv.getConfig()
                        .getConfiguration()
                        .set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
                tEnv.getConfig()
                        .getConfiguration()
                        .set(
                                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
                                Integer.MAX_VALUE);
                tEnv.getConfig()
                        .getConfiguration()
                        .set(
                                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                                Duration.ofSeconds(1));
            } else {
                tEnv.getConfig()
                        .getConfiguration()
                        .set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
            }

            tEnv.getConfig().getConfiguration().addAll(conf);

            return tEnv;
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    //  Stream Execution Environment Utilities
    // ----------------------------------------------------------------------------------------------------------------

    protected StreamExecutionEnvironmentBuilder streamExecutionEnvironmentBuilder() {
        return new StreamExecutionEnvironmentBuilder();
    }

    /** Builder for {@link StreamExecutionEnvironment} in tests. */
    protected static class StreamExecutionEnvironmentBuilder {

        private boolean streamingMode = true;
        private Integer parallelism = null;
        private Integer checkpointIntervalMs = null;
        private boolean allowRestart = false;
        private Configuration conf = new Configuration();

        public StreamExecutionEnvironmentBuilder batchMode() {
            this.streamingMode = false;
            return this;
        }

        public StreamExecutionEnvironmentBuilder streamingMode() {
            this.streamingMode = true;
            return this;
        }

        public StreamExecutionEnvironmentBuilder parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public StreamExecutionEnvironmentBuilder checkpointIntervalMs(int checkpointIntervalMs) {
            this.checkpointIntervalMs = checkpointIntervalMs;
            return this;
        }

        public StreamExecutionEnvironmentBuilder allowRestart() {
            this.allowRestart = true;
            return this;
        }

        public StreamExecutionEnvironmentBuilder allowRestart(boolean allowRestart) {
            this.allowRestart = allowRestart;
            return this;
        }

        public <T> StreamExecutionEnvironmentBuilder setConf(ConfigOption<T> option, T value) {
            conf.set(option, value);
            return this;
        }

        public StreamExecutionEnvironment build() {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            if (streamingMode) {
                env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
                if (checkpointIntervalMs != null) {
                    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
                    env.getCheckpointConfig().setCheckpointInterval(checkpointIntervalMs);
                }
            } else {
                env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            }

            if (parallelism != null) {
                env.setParallelism(parallelism);
            }

            Configuration conf = new Configuration();
            if (allowRestart) {
                conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
                conf.set(
                        RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
                        Integer.MAX_VALUE);
                conf.set(
                        RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                        Duration.ofSeconds(1));
            } else {
                conf.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
            }
            conf.addAll(this.conf);
            env.configure(conf);

            return env;
        }
    }

    protected void validateResult(
            FileStoreTable table,
            RowType rowType,
            SnapshotReader.Plan plan,
            List<String> expected,
            long timeout)
            throws Exception {
        List<String> actual = new ArrayList<>();
        long start = System.currentTimeMillis();
        while (actual.size() != expected.size()) {
            actual.addAll(getResult(table.newReadBuilder().newRead(), plan.splits(), rowType));

            if (System.currentTimeMillis() - start > timeout) {
                break;
            }
        }
        if (actual.size() != expected.size()) {
            throw new TimeoutException(
                    String.format(
                            "Cannot collect %s records in %s milliseconds.",
                            expected.size(), timeout));
        }
        actual.sort(String::compareTo);
        assertThat(actual).isEqualTo(expected);
    }

    protected void validateResult(
            FileStoreTable table,
            RowType rowType,
            TableScan scan,
            List<String> expected,
            long timeout)
            throws Exception {
        List<String> actual = new ArrayList<>();
        long start = System.currentTimeMillis();
        while (actual.size() != expected.size()) {
            TableScan.Plan plan = scan.plan();
            actual.addAll(getResult(table.newReadBuilder().newRead(), plan.splits(), rowType));

            if (System.currentTimeMillis() - start > timeout) {
                break;
            }
        }
        if (actual.size() != expected.size()) {
            throw new TimeoutException(
                    String.format(
                            "Cannot collect %s records in %s milliseconds.",
                            expected.size(), timeout));
        }
        actual.sort(String::compareTo);
        assertThat(actual).isEqualTo(expected);
    }

    protected List<String> getResult(TableRead read, List<Split> splits, RowType rowType)
            throws Exception {
        try (RecordReader<InternalRow> recordReader = read.createReader(splits)) {
            List<String> result = new ArrayList<>();
            recordReader.forEachRemaining(
                    row -> result.add(DataFormatTestUtil.internalRowToString(row, rowType)));
            return result;
        }
    }
}
