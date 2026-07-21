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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.source.SplitListState;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for end-input watermark on unaware-bucket append tables. */
public class CoordinatorEndInputCommitITCase {

    private static final int DEFAULT_PARALLELISM = 2;
    private static final long WAIT_TIMEOUT_MILLIS = 60_000L;
    private static final InMemoryReporter reporter = InMemoryReporter.create();

    @RegisterExtension
    protected static final org.apache.paimon.flink.util.MiniClusterWithClientExtension
            MINI_CLUSTER_EXTENSION =
                    new org.apache.paimon.flink.util.MiniClusterWithClientExtension(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setConfiguration(
                                            reporter.addToConfiguration(new Configuration()))
                                    .build());

    @TempDir Path tempPath;

    @AfterEach
    public final void cleanupRunningJobs() throws Exception {
        ClusterClient<?> clusterClient = MINI_CLUSTER_EXTENSION.createRestClusterClient();
        for (JobStatusMessage job : clusterClient.listJobs().get()) {
            if (!job.getJobState().isTerminalState()) {
                try {
                    clusterClient.cancel(job.getJobId()).get(30, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testCoordinatorCommitEndInputInStreamingMode() throws Exception {
        assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                true,
                true,
                12345L);
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testOperatorCommitEndInputInStreamingMode() throws Exception {
        assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                false,
                true,
                12345L);
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testEmptyFinalCommitDoesNotUpdateEndInputWatermarkInStreamingMode()
            throws Exception {
        assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                true,
                false,
                12345L);
        assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                false,
                false,
                12345L);
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testForceCreateSnapshotUpdatesEndInputWatermarkInStreamingMode() throws Exception {
        assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                true,
                false,
                12345L,
                true);
        assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                false,
                false,
                12345L,
                true);
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testCoordinatorCommitEndInputInBatchMode() throws Exception {
        assertEndInputWatermark(
                EnvironmentSettings.newInstance().inBatchMode().build(),
                false,
                true,
                false,
                12345L);
    }

    private void assertEndInputWatermark(
            EnvironmentSettings settings,
            boolean streaming,
            boolean coordinatorCommit,
            boolean emitAfterFirstCheckpoint,
            long expectedWatermark)
            throws Exception {
        assertEndInputWatermark(
                settings,
                streaming,
                coordinatorCommit,
                emitAfterFirstCheckpoint,
                expectedWatermark,
                false);
    }

    private void assertEndInputWatermark(
            EnvironmentSettings settings,
            boolean streaming,
            boolean coordinatorCommit,
            boolean emitAfterFirstCheckpoint,
            long expectedWatermark,
            boolean forceCreateSnapshot)
            throws Exception {
        final long endInputWatermark = 12345L;
        String tableName = coordinatorCommit ? "T_END_INPUT_COORDINATOR" : "T_END_INPUT_OPERATOR";
        StreamExecutionEnvironment streamEnv = null;
        TableEnvironment tEnv;
        if (streaming) {
            streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            streamEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            streamEnv.setParallelism(DEFAULT_PARALLELISM);
            streamEnv.enableCheckpointing(200L);
            tEnv = StreamTableEnvironment.create(streamEnv);
        } else {
            tEnv = TableEnvironment.create(settings);
        }
        tEnv.getConfig()
                .getConfiguration()
                .setString("table.exec.resource.default-parallelism", "2");

        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableName
                        + " (id INT, data STRING) WITH ("
                        + "'bucket' = '-1', "
                        + "'write-only' = 'true', "
                        + (coordinatorCommit ? "'sink.coordinator-commit.enabled' = 'true', " : "")
                        + (forceCreateSnapshot
                                ? "'"
                                        + CoreOptions.COMMIT_FORCE_CREATE_SNAPSHOT.key()
                                        + "' = 'true', "
                                : "")
                        + "'end-input.watermark' = '"
                        + endInputWatermark
                        + "')");
        int expectedRowCount;
        if (streaming) {
            // Control the final-commit contents instead of relying on the timing between datagen
            // completion and periodic checkpoints. Emitting after a completed checkpoint leaves a
            // non-empty END_INPUT committable; emitting before it and ending only after it has
            // completed leaves an empty one.
            DataStream<Row> source =
                    streamEnv
                            .fromSource(
                                    emitAfterFirstCheckpoint
                                            ? new EmitAfterFirstCheckpointSource()
                                            : new EmitBeforeFirstCheckpointThenFinishSource(),
                                    WatermarkStrategy.noWatermarks(),
                                    "Controlled End Input Source")
                            .map(row -> row)
                            .returns(Types.ROW(Types.INT, Types.STRING))
                            .setParallelism(1);
            StreamTableEnvironment streamTableEnv = (StreamTableEnvironment) tEnv;
            streamTableEnv.createTemporaryView("src", streamTableEnv.fromDataStream(source));
            // The table planner restores the configured sink parallelism, so both source
            // readers emit one final record after checkpoint 1.
            expectedRowCount = DEFAULT_PARALLELISM;
        } else {
            tEnv.executeSql(
                    "CREATE TEMPORARY TABLE src (id INT, data STRING) WITH ("
                            + "'connector' = 'datagen', "
                            + "'number-of-rows' = '20', "
                            + "'rows-per-second' = '10', "
                            + "'fields.id.kind' = 'sequence', "
                            + "'fields.id.start' = '1', "
                            + "'fields.id.end' = '20', "
                            + "'fields.data.length' = '8')");
            expectedRowCount = 20;
        }

        tEnv.executeSql("INSERT INTO " + tableName + " SELECT * FROM src").await();

        FileStoreTable table =
                (FileStoreTable)
                        ((FlinkCatalog) tEnv.getCatalog("mycat").get())
                                .catalog()
                                .getTable(Identifier.create("default", tableName));
        waitUntilRowCount(table, expectedRowCount);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.watermark()).isEqualTo(expectedWatermark);
    }

    /** Emits the only record after checkpoint 1 has completed. */
    private static class EmitAfterFirstCheckpointSource extends AbstractNonCoordinatedSource<Row> {

        private static final long serialVersionUID = 1L;

        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<Row, SimpleSourceSplit> createReader(SourceReaderContext context) {
            return new Reader();
        }

        private static class Reader extends AbstractNonCoordinatedSourceReader<Row> {

            private final SplitListState<Boolean> emittedState =
                    new SplitListState<>(
                            "emitted", value -> String.valueOf(value), Boolean::parseBoolean);
            private boolean emitted;
            private boolean firstCheckpointCompleted;

            @Override
            public InputStatus pollNext(ReaderOutput<Row> output) {
                if (!firstCheckpointCompleted) {
                    return InputStatus.MORE_AVAILABLE;
                }
                if (!emitted) {
                    output.collect(Row.of(1, "end-input"));
                    emitted = true;
                    return InputStatus.MORE_AVAILABLE;
                }
                return InputStatus.END_OF_INPUT;
            }

            @Override
            public void addSplits(List<SimpleSourceSplit> splits) {
                emittedState.restoreState(splits);
                for (Boolean state : emittedState.get()) {
                    emitted = state;
                }
            }

            @Override
            public List<SimpleSourceSplit> snapshotState(long checkpointId) {
                emittedState.clear();
                emittedState.add(emitted);
                return emittedState.snapshotState();
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) {
                firstCheckpointCompleted = true;
            }
        }
    }

    /** Emits the only record before checkpoint 1, then ends only after it has completed. */
    private static class EmitBeforeFirstCheckpointThenFinishSource
            extends AbstractNonCoordinatedSource<Row> {

        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<Row, SimpleSourceSplit> createReader(SourceReaderContext context) {
            return new Reader();
        }

        private static class Reader extends AbstractNonCoordinatedSourceReader<Row> {

            private final SplitListState<Boolean> emittedState =
                    new SplitListState<>(
                            "emitted", value -> String.valueOf(value), Boolean::parseBoolean);
            private boolean emitted;
            private boolean firstCheckpointCompleted;

            @Override
            public InputStatus pollNext(ReaderOutput<Row> output) {
                if (!emitted) {
                    output.collect(Row.of(1, "before-checkpoint"));
                    emitted = true;
                    return InputStatus.MORE_AVAILABLE;
                }
                if (!firstCheckpointCompleted) {
                    return InputStatus.MORE_AVAILABLE;
                }
                return InputStatus.END_OF_INPUT;
            }

            @Override
            public void addSplits(List<SimpleSourceSplit> splits) {
                emittedState.restoreState(splits);
                for (Boolean state : emittedState.get()) {
                    emitted = state;
                }
            }

            @Override
            public List<SimpleSourceSplit> snapshotState(long checkpointId) {
                emittedState.clear();
                emittedState.add(emitted);
                return emittedState.snapshotState();
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) {
                firstCheckpointCompleted = true;
            }
        }
    }

    private long readRowCount(FileStoreTable table) throws Exception {
        RecordReader<InternalRow> reader =
                table.newRead().createReader(table.newSnapshotReader().read());
        long rowCount = 0L;
        try (CloseableIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            while (iterator.hasNext()) {
                iterator.next();
                rowCount++;
            }
        }
        return rowCount;
    }

    private void waitUntilRowCount(FileStoreTable table, long expectedRowCount) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        long rowCount;
        do {
            rowCount = readRowCount(table);
            if (rowCount == expectedRowCount) {
                return;
            }
            Thread.sleep(100);
        } while (System.currentTimeMillis() < deadline);

        assertThat(rowCount).isEqualTo(expectedRowCount);
    }
}
