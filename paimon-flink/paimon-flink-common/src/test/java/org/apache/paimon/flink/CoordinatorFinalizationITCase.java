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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.RowAppendTableSink;
import org.apache.paimon.flink.sink.SavepointTagUtils;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sink.listener.CommitListener;
import org.apache.paimon.flink.sink.listener.CommitListenerFactory;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.DataInputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real MiniCluster recovery across the data / MAX / listener boundaries. */
public class CoordinatorFinalizationITCase extends AbstractTestBase {

    private static final long INITIAL_WATERMARK =
            Instant.parse("2025-03-01T12:30:00Z").toEpochMilli();
    private static final long FINAL_WATERMARK =
            Instant.parse("2025-03-01T14:30:00Z").toEpochMilli();

    private enum FailurePoint {
        NONE,
        BEFORE_MAX,
        MAX_LISTENER,
        TAG_WRITE
    }

    // Shared only by tasks in this MiniCluster. The source emits once, then checkpoints its data
    // before the test permits EndInput. Restarts deliberately have no additional source input.
    private static final AtomicBoolean emitted = new AtomicBoolean();
    private static final AtomicBoolean terminalRowEmitted = new AtomicBoolean();
    private static final AtomicBoolean finish = new AtomicBoolean();
    private static final AtomicBoolean failed = new AtomicBoolean();
    private static final AtomicInteger listenerCalls = new AtomicInteger();
    private static CompletableFuture<Void> available;
    private static volatile FailurePoint failurePoint;
    private static volatile long publishedMaxSnapshot;
    private static volatile long expectedRecordCount;
    private static final List<String> observedCommits = new CopyOnWriteArrayList<>();

    private static void reset(FailurePoint point) {
        emitted.set(false);
        terminalRowEmitted.set(false);
        finish.set(false);
        failed.set(false);
        listenerCalls.set(0);
        available = new CompletableFuture<>();
        failurePoint = point;
        publishedMaxSnapshot = -1;
        expectedRecordCount = 2;
        observedCommits.clear();
    }

    @ParameterizedTest
    @EnumSource(
            value = FailurePoint.class,
            names = {"BEFORE_MAX", "MAX_LISTENER"})
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testRecoveryAcrossFinalization(FailurePoint point) throws Exception {
        reset(point);
        FileStoreTable table = createTable(false, false, true);
        JobClient client = startJob(table);
        try {
            waitForInitialCommit(client, table);
            finish.set(true);
            available.complete(null);
            client.getJobExecutionResult().get(60, TimeUnit.SECONDS);
            assertThat(failed.get()).isTrue();
            assertRows(table, 1, 2);
            Snapshot latest = table.snapshotManager().latestSnapshot();
            assertThat(latest.commitIdentifier()).isEqualTo(Long.MAX_VALUE);
            assertThat(latest.watermark()).isEqualTo(FINAL_WATERMARK);
            if (point == FailurePoint.MAX_LISTENER) {
                assertThat(listenerCalls.get()).isGreaterThanOrEqualTo(2);
                // The second listener call must reuse the already published MAX snapshot.
                assertThat(latest.id()).isEqualTo(publishedMaxSnapshot);
            }
        } finally {
            cancelIfRunning(client);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = FailurePoint.class,
            names = {"BEFORE_MAX", "MAX_LISTENER", "TAG_WRITE"})
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    public void testDrainingSavepointFailureRestoresCommitAndTag(FailurePoint point)
            throws Exception {
        reset(point);
        expectedRecordCount = 1;
        terminalRowEmitted.set(true);
        FileStoreTable table =
                createTable(false, false, true)
                        .copy(Collections.singletonMap("sink.savepoint.auto-tag", "true"));
        org.apache.paimon.fs.Path blockedTagDirectory =
                new org.apache.paimon.fs.Path(table.location(), "tag");
        if (point == FailurePoint.TAG_WRITE) {
            // Fail real tag file creation, after the data snapshot has already been published.
            table.fileIO().newOutputStream(blockedTagDirectory, false).close();
        }
        String directory = getTempDirPath("draining-savepoint");
        JobClient client = startJob(table, null, true);
        try {
            waitForInitialCommit(client, table);
            assertThatThrownBy(
                            () ->
                                    client.stopWithSavepoint(
                                                    true, directory, SavepointFormatType.DEFAULT)
                                            .get(60, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class);
            assertThatThrownBy(() -> client.getJobExecutionResult().get(60, TimeUnit.SECONDS))
                    .hasStackTraceContaining("StopWithSavepointStoppingException");
            assertRows(table, 1);
            if (point != FailurePoint.TAG_WRITE) {
                assertThat(failed.get()).isTrue();
            }
        } finally {
            cancelIfRunning(client);
        }
        String savepointPath = null;
        for (org.apache.paimon.fs.FileStatus status :
                table.fileIO().listStatus(new org.apache.paimon.fs.Path(directory))) {
            if (table.fileIO()
                    .exists(new org.apache.paimon.fs.Path(status.getPath(), "_metadata"))) {
                assertThat(savepointPath).isNull();
                savepointPath = status.getPath().toString();
            }
        }
        assertThat(savepointPath).isNotNull();
        long checkpointId;
        org.apache.flink.core.fs.Path metadata =
                new org.apache.flink.core.fs.Path(savepointPath, "_metadata");
        try (DataInputStream input = new DataInputStream(metadata.getFileSystem().open(metadata))) {
            checkpointId =
                    Checkpoints.loadCheckpointMetadata(
                                    input, getClass().getClassLoader(), savepointPath)
                            .getCheckpointId();
        }
        if (point == FailurePoint.TAG_WRITE) {
            assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                    .isEqualTo(checkpointId);
            assertThat(table.fileIO().delete(blockedTagDirectory, false)).isTrue();
            assertThat(table.tagManager().tagExists(SavepointTagUtils.tagNameOf(checkpointId)))
                    .isFalse();
        }
        finish.set(true);
        available.complete(null);
        JobClient restored = startJob(table, savepointPath, true);
        try {
            restored.getJobExecutionResult().get(60, TimeUnit.SECONDS);
            assertRows(table, 1);
            Map<Snapshot, List<String>> tags =
                    table.tagManager().tags(name -> name.startsWith(SavepointTagUtils.PREFIX));
            assertThat(tags).hasSize(1);
            Map.Entry<Snapshot, List<String>> tagged = tags.entrySet().iterator().next();
            assertThat(tagged.getKey().commitIdentifier()).isEqualTo(checkpointId);
            assertThat(tagged.getValue())
                    .containsExactly(SavepointTagUtils.tagNameOf(checkpointId));
            Snapshot latest = table.snapshotManager().latestSnapshot();
            assertThat(latest.commitIdentifier()).isEqualTo(Long.MAX_VALUE);
            if (point == FailurePoint.MAX_LISTENER) {
                assertThat(latest.id()).isEqualTo(publishedMaxSnapshot);
                assertThat(listenerCalls.get()).isGreaterThanOrEqualTo(2);
            }
        } finally {
            cancelIfRunning(restored);
        }
    }

    @ParameterizedTest(name = "endInputToDone={0}, validFinalWatermark={1}")
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testEmptyMaxDrivesPartitionMarkDone(boolean endInputToDone, boolean validWatermark)
            throws Exception {
        reset(FailurePoint.NONE);
        FileStoreTable table = createTable(true, endInputToDone, validWatermark);
        JobClient client = startJob(table);
        org.apache.paimon.fs.Path success =
                new org.apache.paimon.fs.Path(table.location(), "dt=2025-03-01 12/_SUCCESS");
        try {
            waitForInitialCommit(client, table);
            assertThat(table.fileIO().exists(success)).isFalse();
            finish.set(true);
            available.complete(null);
            client.getJobExecutionResult().get(60, TimeUnit.SECONDS);
            assertRows(table, 1);
            assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                    .isEqualTo(Long.MAX_VALUE);
            assertThat(table.fileIO().exists(success))
                    .as("Empty MAX follows the EndInput completion option: %s", observedCommits)
                    .isEqualTo(endInputToDone);
        } finally {
            cancelIfRunning(client);
        }
    }

    private FileStoreTable createTable(
            boolean markDone, boolean endInputToDone, boolean validWatermark) throws Exception {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.executeSql(
                "CREATE CATALOG finalcat WITH ('type'='paimon', 'warehouse'='"
                        + getTempDirPath()
                        + "')");
        env.executeSql("USE CATALOG finalcat");
        env.executeSql(
                "CREATE TABLE T (id INT, dt STRING) PARTITIONED BY (dt) WITH ("
                        + "'bucket'='-1', 'write-only'='true', 'sink.coordinator-commit.enabled'='true', "
                        + "'commit.force-create-snapshot'='true', 'end-input.watermark'='"
                        + (validWatermark ? FINAL_WATERMARK : Long.MIN_VALUE)
                        + "', "
                        + "'commit.custom-listeners'='end-input-failure-test'"
                        + (markDone
                                ? ", 'partition.mark-done-action'='success-file', "
                                        + "'partition.mark-done-action.mode'='watermark', 'partition.time-interval'='1 h', "
                                        + "'partition.idle-time-to-done'='15 min', 'partition.timestamp-formatter'='yyyy-MM-dd HH', "
                                        + "'partition.end-input-to-done'='"
                                        + endInputToDone
                                        + "'"
                                : "")
                        + ")");
        return (FileStoreTable)
                ((FlinkCatalog) env.getCatalog("finalcat").get())
                        .catalog()
                        .getTable(Identifier.create("default", "T"));
    }

    private JobClient startJob(FileStoreTable table) throws Exception {
        return startJob(table, null, false);
    }

    private JobClient startJob(FileStoreTable table, String savepointPath, boolean noRestart)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.setString("restart-strategy.type", noRestart ? "none" : "fixed-delay");
        if (savepointPath != null) {
            SavepointRestoreSettings.toConfiguration(
                    SavepointRestoreSettings.forPath(savepointPath), config);
        }
        config.setString("restart-strategy.fixed-delay.attempts", "1");
        config.setString("restart-strategy.fixed-delay.delay", "0 ms");
        config.setString("jobmanager.execution.failover-strategy", "full");
        env.configure(config, getClass().getClassLoader());
        env.setParallelism(1);
        env.enableCheckpointing(200);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        DataStreamSource<InternalRow> source =
                env.fromSource(
                        new ControlledSource(),
                        WatermarkStrategy.noWatermarks(),
                        "finalization-source",
                        InternalTypeInfo.fromRowType(table.rowType()));
        new FailureInjectingSink(table).sinkFrom(source);
        return env.executeAsync("coordinator-finalization-it");
    }

    private void waitForInitialCommit(JobClient client, FileStoreTable table) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Snapshot latest = table.snapshotManager().latestSnapshot();
            if (latest != null && latest.totalRecordCount() == 1) {
                assertThat(latest.commitIdentifier()).isLessThan(Long.MAX_VALUE);
                return;
            }
            if (client.getJobStatus().get().isTerminalState()) {
                client.getJobExecutionResult().get();
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Initial data did not commit");
    }

    private void assertRows(FileStoreTable table, Integer... expectedIds) throws Exception {
        List<Integer> ids = new ArrayList<>();
        RecordReader<InternalRow> reader =
                table.newRead().createReader(table.newSnapshotReader().read());
        try (RecordReaderIterator<InternalRow> rows = new RecordReaderIterator<>(reader)) {
            while (rows.hasNext()) {
                ids.add(rows.next().getInt(0));
            }
        }
        assertThat(ids).containsExactlyInAnyOrder(expectedIds);
    }

    private void cancelIfRunning(JobClient client) throws Exception {
        if (!client.getJobStatus().get().isTerminalState()) {
            client.cancel().get(30, TimeUnit.SECONDS);
        }
    }

    private static class FailureInjectingSink extends RowAppendTableSink {
        private static final long serialVersionUID = 1L;

        private FailureInjectingSink(FileStoreTable table) {
            super(table, null, 1);
        }

        @Override
        protected Committer.Factory<Committable, ManifestCommittable> createCommitterFactory() {
            FileStoreTable target = table;
            return context ->
                    new StoreCommitter(
                            target,
                            target.newCommit(context.commitUser()).ignoreEmptyCommit(false),
                            context) {
                        @Override
                        public int filterAndCommit(
                                List<ManifestCommittable> entries,
                                boolean check,
                                boolean listeners) {
                            if (failurePoint == FailurePoint.BEFORE_MAX
                                    && entries.stream()
                                            .anyMatch(e -> e.identifier() == Long.MAX_VALUE)
                                    && failed.compareAndSet(false, true)) {
                                assertThat(
                                                target.snapshotManager()
                                                        .latestSnapshot()
                                                        .commitIdentifier())
                                        .isLessThan(Long.MAX_VALUE);
                                assertThat(
                                                target.snapshotManager()
                                                        .latestSnapshot()
                                                        .totalRecordCount())
                                        .isEqualTo(expectedRecordCount);
                                throw new RuntimeException(
                                        "Injected failure before MAX publication");
                            }
                            return super.filterAndCommit(entries, check, listeners);
                        }
                    };
        }
    }

    /** Loaded through the same SPI as user-configured commit listeners. */
    public static class FailureListenerFactory implements CommitListenerFactory {
        @Override
        public String identifier() {
            return "end-input-failure-test";
        }

        @Override
        public Optional<CommitListener> create(Committer.Context context, FileStoreTable table) {
            return Optional.of(
                    new CommitListener() {
                        @Override
                        public void notifyCommittable(List<ManifestCommittable> entries) {
                            for (ManifestCommittable entry : entries) {
                                observedCommits.add(entry.toString());
                            }
                            if (entries.stream().anyMatch(e -> e.identifier() == Long.MAX_VALUE)) {
                                listenerCalls.incrementAndGet();
                                if (failurePoint == FailurePoint.MAX_LISTENER
                                        && failed.compareAndSet(false, true)) {
                                    Snapshot snapshot = table.snapshotManager().latestSnapshot();
                                    assertThat(snapshot.commitIdentifier())
                                            .isEqualTo(Long.MAX_VALUE);
                                    publishedMaxSnapshot = snapshot.id();
                                    assertThat(snapshot.totalRecordCount())
                                            .isEqualTo(expectedRecordCount);
                                    throw new RuntimeException(
                                            "Injected listener failure after MAX publication");
                                }
                            }
                        }

                        @Override
                        public void snapshotState() {}

                        @Override
                        public void close() {}
                    });
        }
    }

    private static class ControlledSource extends AbstractNonCoordinatedSource<InternalRow> {
        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<InternalRow, SimpleSourceSplit> createReader(
                SourceReaderContext context) {
            return new AbstractNonCoordinatedSourceReader<InternalRow>() {
                @Override
                public InputStatus pollNext(ReaderOutput<InternalRow> output) {
                    if (emitted.compareAndSet(false, true)) {
                        output.collect(GenericRow.of(1, BinaryString.fromString("2025-03-01 12")));
                        output.emitWatermark(new Watermark(INITIAL_WATERMARK));
                        return InputStatus.MORE_AVAILABLE;
                    }
                    if (finish.get()
                            && failurePoint != FailurePoint.NONE
                            && terminalRowEmitted.compareAndSet(false, true)) {
                        output.collect(GenericRow.of(2, BinaryString.fromString("2025-03-01 12")));
                        return InputStatus.MORE_AVAILABLE;
                    }
                    return finish.get() ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
                }

                @Override
                public CompletableFuture<Void> isAvailable() {
                    return !emitted.get() || finish.get()
                            ? CompletableFuture.completedFuture(null)
                            : available;
                }
            };
        }
    }
}
