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

import org.apache.paimon.utils.FileIOUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.CollectModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

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
        private int numRestarts = 0;
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
            return allowRestart(Integer.MAX_VALUE);
        }

        public TableEnvironmentBuilder allowRestart(int numRestarts) {
            this.numRestarts = numRestarts;
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
                                    CheckpointingOptions.CHECKPOINTING_INTERVAL,
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

            if (numRestarts > 0) {
                tEnv.getConfig()
                        .getConfiguration()
                        .set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
                tEnv.getConfig()
                        .getConfiguration()
                        .set(
                                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
                                numRestarts);
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

    public static Transformation<?> translate(TableEnvironment env, String statement) {
        TableEnvironmentImpl envImpl = (TableEnvironmentImpl) env;
        List<Operation> operations = envImpl.getParser().parse(statement);

        if (operations.size() != 1) {
            throw new RuntimeException("No operation after parsing for " + statement);
        }

        Operation operation = operations.get(0);
        if (operation instanceof QueryOperation) {
            QueryOperation queryOperation = (QueryOperation) operation;
            CollectModifyOperation sinkOperation = new CollectModifyOperation(queryOperation);
            List<Transformation<?>> transformations;
            try {
                Method translate =
                        TableEnvironmentImpl.class.getDeclaredMethod("translate", List.class);
                translate.setAccessible(true);
                //noinspection unchecked
                transformations =
                        (List<Transformation<?>>)
                                translate.invoke(envImpl, Collections.singletonList(sinkOperation));
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            if (transformations.size() != 1) {
                throw new RuntimeException("No transformation after translating for " + statement);
            }

            return transformations.get(0);
        } else {
            throw new RuntimeException();
        }
    }
}
