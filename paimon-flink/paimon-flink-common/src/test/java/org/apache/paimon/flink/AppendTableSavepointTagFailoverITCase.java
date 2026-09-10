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
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.sink.SavepointTagUtils;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Failover and restore behavior of savepoint auto-tag for unaware-bucket append tables, asserting
 * that the coordinator-commit path stays at parity with the classic operator-commit path.
 */
public class AppendTableSavepointTagFailoverITCase extends AbstractTestBase {

    private static final long WAIT_TIMEOUT_MILLIS = 120_000L;

    @BeforeEach
    public void resetInjectors() {
        // These injectors coordinate through static fields shared across every test in this JVM;
        // reset them up front so no test inherits another's leftover arming/one-shot state.
        FailOnSavepointOperator.reset();
        BlockCheckpointAfterSavepointOperator.disarmBlocking();
        FailOnFirstPostRecoveryCommitCallback.reset();
    }

    /** The tag already exists on restore; recover() must re-tag idempotently. */
    @ParameterizedTest(name = "coordinatorCommit = {0}")
    @ValueSource(booleans = {true, false})
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    public void testSavepointTagIdempotentOnRestore(boolean coordinatorCommit) throws Exception {
        String tableName = coordinatorCommit ? "T_COORD_IDEMPOTENT" : "T_CLASSIC_IDEMPOTENT";
        FileStoreTable table = createTable(tableName, coordinatorCommit);

        // Phase 1: run the job, take a savepoint, and let its tag materialize. Keep the tag in
        // place so phase 2 restores into a state where the tag already exists.
        String savepointPath;
        long taggedIdentifier;
        JobClient firstClient = runSink(table, null);
        try {
            waitUntilSnapshotWithData(table);
            savepointPath =
                    firstClient
                            .triggerSavepoint(
                                    getTempDirPath("savepoint_" + tableName),
                                    SavepointFormatType.DEFAULT)
                            .get(60, TimeUnit.SECONDS);
            Map<Snapshot, List<String>> tags = waitUntilSavepointTagCreated(table);
            assertThat(tags).hasSize(1);
            taggedIdentifier = tags.keySet().iterator().next().commitIdentifier();
        } finally {
            firstClient.cancel().get(30, TimeUnit.SECONDS);
        }

        // Phase 2: restore from the savepoint while the tag is already present. recover() re-runs
        // the tag creation; SavepointTagger.createTag with ignoreIfExists must make it a no-op, so
        // the tag stays unique and unchanged.
        JobClient secondClient = runSink(table, savepointPath);
        try {
            // A snapshot committed after restore proves recover() has already run (its re-tag
            // happens before the resumed job commits again), so no fixed sleep is needed.
            waitUntilRecoveredAndCommitting(table);
            Map<Snapshot, List<String>> tags = savepointTags(table);
            assertThat(tags).hasSize(1);
            Map.Entry<Snapshot, List<String>> snapshotWithTags = tags.entrySet().iterator().next();
            assertThat(snapshotWithTags.getValue())
                    .containsExactly(
                            SavepointTagUtils.tagNameOf(
                                    snapshotWithTags.getKey().commitIdentifier()));
            assertThat(snapshotWithTags.getKey().commitIdentifier()).isEqualTo(taggedIdentifier);
        } finally {
            secondClient.cancel().get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * The tag was never created (no checkpoint completed after the savepoint); restore must create
     * it.
     */
    @ParameterizedTest(name = "coordinatorCommit = {0}")
    @ValueSource(booleans = {true, false})
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    public void testSavepointTagRecreatedOnRestore(boolean coordinatorCommit) throws Exception {
        String tableName = coordinatorCommit ? "T_COORD_RECREATE" : "T_CLASSIC_RECREATE";
        FileStoreTable table = createTable(tableName, coordinatorCommit);

        // Phase 1: take a savepoint, and block every normal checkpoint that would follow it so none
        // can materialize the tag before we cancel. The savepoint's snapshot is captured but its
        // tag
        // is never created yet.
        BlockCheckpointAfterSavepointOperator.armBlocking();
        String savepointPath;
        JobClient firstClient = runSink(table, null, true);
        try {
            waitUntilSnapshotWithData(table);
            savepointPath =
                    firstClient
                            .triggerSavepoint(
                                    getTempDirPath("savepoint_" + tableName),
                                    SavepointFormatType.DEFAULT)
                            .get(60, TimeUnit.SECONDS);
        } finally {
            firstClient.cancel().get(30, TimeUnit.SECONDS);
        }
        assertThat(savepointTags(table)).isEmpty();

        // Phase 2: restore from the savepoint with blocking disarmed (same topology so the writer
        // state maps back). The writer replays the pending savepoint bit; the commit that follows
        // materializes the savepoint's snapshot and its tag is created for the first time here (via
        // recover() on coordinator, or committer ListState on the classic path).
        BlockCheckpointAfterSavepointOperator.disarmBlocking();
        JobClient secondClient = runSink(table, savepointPath, true);
        try {
            Map<Snapshot, List<String>> tags = waitUntilSavepointTagCreated(table);
            assertThat(tags).hasSize(1);
            Map.Entry<Snapshot, List<String>> snapshotWithTags = tags.entrySet().iterator().next();
            assertThat(snapshotWithTags.getValue())
                    .containsExactly(
                            SavepointTagUtils.tagNameOf(
                                    snapshotWithTags.getKey().commitIdentifier()));
        } finally {
            secondClient.cancel().get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Region failover: a single writer subtask throws while a savepoint is in flight, so only that
     * region restarts and the coordinator keeps running. The interrupted savepoint never commits
     * its snapshot, so it produces no tag; a savepoint taken after recovery must be tagged. Region
     * failover is a coordinator-commit-only concern (the classic committer shares a failover region
     * with the writers), so this is not parameterized.
     */
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    public void testRegionFailoverPreservesSavepointTag() throws Exception {
        String tableName = "T_COORD_REGION_FAILOVER";
        FileStoreTable table = createTable(tableName, true);

        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.enableCheckpointing(200);
        DataStreamSource<RowData> source =
                env.fromSource(
                        new ContinuousSource(), WatermarkStrategy.noWatermarks(), "region-source");
        // forward + same parallelism so source, fail-injector, and writer are one failover region
        // per subtask; a single subtask's failure restarts only its region, not the whole job.
        DataStream<RowData> injected =
                source.forward()
                        .transform(
                                "fail-on-savepoint",
                                InternalTypeInfo.of(RowType.of(new IntType(), new VarCharType())),
                                new FailOnSavepointOperator(1))
                        .setParallelism(2);
        new FlinkSinkBuilder(table).forRowData(injected).build();
        JobClient client = env.executeAsync("region-failover-savepoint-tag");
        try {
            waitUntilSnapshotWithData(table);

            // Savepoint 1: subtask 1 throws while snapshotting it, which aborts the in-flight
            // savepoint and triggers a region failover. The trigger future must fail, and the
            // failure must be the savepoint being interrupted by that failover — the checkpoint
            // coordinator suspends — not some unrelated error. The injected exception is not on
            // this
            // chain: it fails the task (driving the failover) rather than propagating to the
            // trigger
            // future, which only sees the coordinator suspending, so CheckpointException is the
            // tightest type we can assert here.
            assertThatThrownBy(
                            () ->
                                    client.triggerSavepoint(
                                                    getTempDirPath("savepoint1_" + tableName),
                                                    SavepointFormatType.DEFAULT)
                                            .get(60, TimeUnit.SECONDS))
                    .satisfies(
                            e ->
                                    assertThat(
                                                    ExceptionUtils.findThrowable(
                                                            e, CheckpointException.class))
                                            .isPresent());
            assertThat(savepointTags(table)).isEmpty();

            // Wait until the job has recovered and resumed committing after the region failover.
            waitUntilRecoveredAndCommitting(table);

            // Savepoint 2: after recovery it must be tagged correctly, proving the region failover
            // did not break the coordinator's auto-tag state.
            client.triggerSavepoint(
                            getTempDirPath("savepoint2_" + tableName), SavepointFormatType.DEFAULT)
                    .get(60, TimeUnit.SECONDS);
            Map<Snapshot, List<String>> tags = waitUntilSavepointTagCreated(table);
            assertThat(tags).hasSize(1);
            Map.Entry<Snapshot, List<String>> snapshotWithTags = tags.entrySet().iterator().next();
            assertThat(snapshotWithTags.getValue())
                    .containsExactly(
                            SavepointTagUtils.tagNameOf(
                                    snapshotWithTags.getKey().commitIdentifier()));
            assertThat(table.snapshotManager().snapshotExists(snapshotWithTags.getKey().id()))
                    .isTrue();
        } finally {
            client.cancel().get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * An async savepoint is aborted (one writer failed while others succeeded), then a global
     * failover restores from the first normal checkpoint after the abort. The surviving writer
     * still carries the aborted savepoint's bit in that checkpoint's state, so the coordinator must
     * NOT recreate the savepoint's tag on restore. This is the coordinator-vs-operator consistency
     * gap: the operator path prunes the aborted id from its checkpointed set, while the coordinator
     * rebuilds its pending-tag set from the writer-replayed bits.
     *
     * <p>Deterministic reproduction of "restore exactly from the one checkpoint that carries the
     * stale bit":
     *
     * <ol>
     *   <li>Parallelism 2. A savepoint S is taken; subtask 1 throws in its snapshot, aborting S and
     *       forcing a region failover while the coordinator (and subtask 0) keep running. Subtask 0
     *       keeps S's savepoint bit in its in-memory pending buffer (the writer has no
     *       notifyCheckpointAborted hook to clear it).
     *   <li>The job resumes and the first normal checkpoint C after recovery persists subtask 0's
     *       buffer — including S's stale bit — into C's operator state.
     *   <li>A commit callback throws exactly once, on the commit of that first post-recovery
     *       checkpoint, to force a GLOBAL failover whose latest completed checkpoint is C.
     *   <li>Global restore replays C's committables (with S's stale bit) to a fresh coordinator,
     *       which re-collects S and, after recover() commits S's snapshot, would tag it. The tag is
     *       an orphan: S was aborted and its Flink savepoint no longer exists.
     * </ol>
     *
     * <p>The assertion is that no savepoint tag exists after the dust settles. This is
     * coordinator-commit-only (region failover requires the coordinator path), so it is not
     * parameterized.
     */
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    public void testAbortedSavepointNotRetaggedAfterGlobalFailover() throws Exception {
        String tableName = "T_COORD_ABORT_RETAG";
        FileStoreTable table =
                createTable(tableName, true, FailOnFirstPostRecoveryCommitCallback.class.getName());

        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.enableCheckpointing(200);
        DataStreamSource<RowData> source =
                env.fromSource(
                        new ContinuousSource(), WatermarkStrategy.noWatermarks(), "retag-source");
        // forward + same parallelism so each source/fail-injector/writer triple is its own failover
        // region; a single subtask's failure restarts only its region, keeping the coordinator up.
        DataStream<RowData> injected =
                source.forward()
                        .transform(
                                "fail-on-savepoint",
                                InternalTypeInfo.of(RowType.of(new IntType(), new VarCharType())),
                                new FailOnSavepointOperator(1))
                        .setParallelism(2);
        new FlinkSinkBuilder(table).forRowData(injected).build();
        JobClient client = env.executeAsync("aborted-savepoint-retag");
        try {
            waitUntilSnapshotWithData(table);

            // Savepoint S: subtask 1 throws while snapshotting it, which aborts S and triggers a
            // region failover; subtask 0 survives with S's savepoint bit still buffered. The
            // trigger
            // future must fail with the savepoint being interrupted by that failover (the
            // checkpoint
            // coordinator suspends), not some unrelated error. The injected exception is not on
            // this
            // chain: it fails the task rather than propagating to the trigger future, so
            // CheckpointException is the tightest type we can assert here.
            assertThatThrownBy(
                            () ->
                                    client.triggerSavepoint(
                                                    getTempDirPath("savepoint_" + tableName),
                                                    SavepointFormatType.DEFAULT)
                                            .get(60, TimeUnit.SECONDS))
                    .satisfies(
                            e ->
                                    assertThat(
                                                    ExceptionUtils.findThrowable(
                                                            e, CheckpointException.class))
                                            .isPresent());
            assertThat(savepointTags(table)).isEmpty();

            // The commit callback throws once on the first post-recovery commit, forcing a global
            // failover whose latest completed checkpoint C carries subtask 0's stale savepoint bit.
            waitUntilCallbackFired();
            // After the global failover the coordinator restores from C, replays the stale bit, and
            // (before the fix) would recreate the orphan tag during recover(). Wait for two fresh
            // post-restore commits so any restore-time tag work has fully run before we assert.
            waitUntilCommittedFurther(table, 2);

            // No savepoint tag must exist: S was aborted, so its tag would be an orphan pointing at
            // a savepoint that no longer exists.
            assertThat(savepointTags(table)).isEmpty();
        } finally {
            client.cancel().get(30, TimeUnit.SECONDS);
        }
    }

    private FileStoreTable createTable(String tableName, boolean coordinatorCommit)
            throws Exception {
        return createTable(tableName, coordinatorCommit, null);
    }

    private FileStoreTable createTable(
            String tableName, boolean coordinatorCommit, String commitCallbackClass)
            throws Exception {
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + getTempDirPath()
                        + "' )");
        tEnv.executeSql("USE CATALOG mycat");
        // A stable operator-uid suffix so the writer's state maps back on restore-from-savepoint.
        String coordinatorOption =
                coordinatorCommit
                        ? ", 'sink.coordinator-commit.enabled' = 'true', 'write-only' = 'true'"
                        : "";
        String commitCallbackOption =
                commitCallbackClass == null
                        ? ""
                        : ", 'commit.callbacks' = '" + commitCallbackClass + "'";
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableName
                        + " (id INT, data STRING) WITH ("
                        + "'bucket' = '-1', "
                        + "'sink.savepoint.auto-tag' = 'true', "
                        + "'commit.force-create-snapshot' = 'true', "
                        + "'sink.operator-uid.suffix' = 'failover-tag'"
                        + coordinatorOption
                        + commitCallbackOption
                        + ")");
        return (FileStoreTable)
                ((FlinkCatalog) tEnv.getCatalog("mycat").get())
                        .catalog()
                        .getTable(Identifier.create("default", tableName));
    }

    /** Runs the sink job at parallelism 1, optionally resuming from {@code savepointPath}. */
    private JobClient runSink(FileStoreTable table, String savepointPath) throws Exception {
        return runSink(table, savepointPath, false);
    }

    /**
     * Runs the sink job at parallelism 1, optionally resuming from {@code savepointPath}. When
     * {@code blockCheckpointAfterSavepoint} is set, a {@link BlockCheckpointAfterSavepointOperator}
     * is chained in so that any normal checkpoint taken after a savepoint stalls forever instead of
     * completing — used to keep a savepoint's tag from being materialized by a later checkpoint.
     */
    private JobClient runSink(
            FileStoreTable table, String savepointPath, boolean blockCheckpointAfterSavepoint)
            throws Exception {
        Configuration conf = new Configuration();
        if (savepointPath != null) {
            SavepointRestoreSettings.toConfiguration(
                    SavepointRestoreSettings.forPath(savepointPath, false), conf);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(200);
        DataStreamSource<RowData> source =
                env.fromSource(
                        new ContinuousSource(), WatermarkStrategy.noWatermarks(), "restore-source");
        DataStream<RowData> stream = source;
        if (blockCheckpointAfterSavepoint) {
            stream =
                    source.forward()
                            .transform(
                                    "block-checkpoint-after-savepoint",
                                    InternalTypeInfo.of(
                                            RowType.of(new IntType(), new VarCharType())),
                                    new BlockCheckpointAfterSavepointOperator());
        }
        new FlinkSinkBuilder(table).forRowData(stream).build();
        return env.executeAsync("savepoint-failover-tag");
    }

    private void waitUntilSnapshotWithData(FileStoreTable table) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            Snapshot latest = table.snapshotManager().latestSnapshot();
            if (latest != null && latest.totalRecordCount() > 0) {
                return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("no data-carrying snapshot committed within timeout");
    }

    /** Waits until a snapshot committed after the current latest one, proving the job resumed. */
    private void waitUntilRecoveredAndCommitting(FileStoreTable table) throws Exception {
        Long baseline = table.snapshotManager().latestSnapshotId();
        long base = baseline == null ? 0L : baseline;
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            Long latest = table.snapshotManager().latestSnapshotId();
            if (latest != null && latest > base) {
                return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("job did not resume committing after region failover");
    }

    /**
     * Waits until at least {@code count} more snapshots commit past the current latest.
     * Restore-time tag work runs before the resumed job commits again, so requiring several fresh
     * commits gives that work room to fully settle before we assert on tags.
     */
    private void waitUntilCommittedFurther(FileStoreTable table, int count) throws Exception {
        Long baseline = table.snapshotManager().latestSnapshotId();
        long target = (baseline == null ? 0L : baseline) + count;
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            Long latest = table.snapshotManager().latestSnapshotId();
            if (latest != null && latest >= target) {
                return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("job did not commit enough snapshots after restore");
    }

    private void waitUntilCallbackFired() throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (!FailOnFirstPostRecoveryCommitCallback.hasFired()
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        if (!FailOnFirstPostRecoveryCommitCallback.hasFired()) {
            throw new IllegalStateException(
                    "commit callback never fired; the global failover was not triggered");
        }
    }

    private Map<Snapshot, List<String>> waitUntilSavepointTagCreated(FileStoreTable table)
            throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        Map<Snapshot, List<String>> tags = savepointTags(table);
        while (tags.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
            tags = savepointTags(table);
        }
        assertThat(tags).describedAs("no savepoint tag was created").isNotEmpty();
        return tags;
    }

    private Map<Snapshot, List<String>> savepointTags(FileStoreTable table) {
        return table.tagManager().tags(name -> name.startsWith(SavepointTagUtils.PREFIX));
    }

    /** Emits one row per poll so every checkpoint window carries data. */
    private static class ContinuousSource extends AbstractNonCoordinatedSource<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<RowData, SimpleSourceSplit> createReader(SourceReaderContext ctx) {
            return new AbstractNonCoordinatedSourceReader<RowData>() {
                private int next;

                @Override
                public InputStatus pollNext(ReaderOutput<RowData> output)
                        throws InterruptedException {
                    output.collect(GenericRowData.of(next, StringData.fromString("v" + next)));
                    next++;
                    Thread.sleep(20);
                    return InputStatus.MORE_AVAILABLE;
                }
            };
        }
    }

    /**
     * Passthrough operator that throws exactly once, on the target subtask, while a savepoint is
     * being taken. This reproduces "some writers finished snapshotState, one did not", which forces
     * a region failover with the savepoint in flight.
     */
    private static class FailOnSavepointOperator extends AbstractStreamOperator<RowData>
            implements OneInputStreamOperator<RowData, RowData> {
        private static final long serialVersionUID = 1L;
        private static final AtomicBoolean FAILED = new AtomicBoolean(false);
        // Checkpoint id of the savepoint that was aborted, published by the throwing subtask so the
        // commit callback can tell "past the aborted savepoint" from ordinary earlier commits.
        private static volatile long savepointCheckpointId = -1L;
        private final int targetSubtask;

        FailOnSavepointOperator(int targetSubtask) {
            this.targetSubtask = targetSubtask;
        }

        static void reset() {
            FAILED.set(false);
            savepointCheckpointId = -1L;
        }

        static long savepointCheckpointId() {
            return savepointCheckpointId;
        }

        @Override
        public void processElement(StreamRecord<RowData> element) {
            output.collect(element);
        }

        @Override
        public OperatorSnapshotFutures snapshotState(
                long checkpointId,
                long timestamp,
                CheckpointOptions checkpointOptions,
                CheckpointStreamFactory storageLocation)
                throws Exception {
            int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            if (checkpointOptions.getCheckpointType().isSavepoint()
                    && subtask == targetSubtask
                    && FAILED.compareAndSet(false, true)) {
                savepointCheckpointId = checkpointId;
                throw new RuntimeException(
                        "intentional region-failover trigger on subtask " + subtask);
            }
            return super.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
        }
    }

    /**
     * Passthrough operator that lets a savepoint through, but blocks every normal checkpoint taken
     * afterwards by parking in {@code snapshotState} forever. The blocked checkpoint neither
     * completes nor declines, so it cannot materialize a savepoint tag and — unlike throwing — does
     * not trip a failover; it only ends when the job is cancelled (which interrupts this thread).
     * Armed per test through a static flag so the topology-sharing sibling case is unaffected.
     */
    private static class BlockCheckpointAfterSavepointOperator
            extends AbstractStreamOperator<RowData>
            implements OneInputStreamOperator<RowData, RowData> {
        private static final long serialVersionUID = 1L;
        private static volatile boolean armed = false;
        private static volatile boolean savepointSeen = false;

        static void armBlocking() {
            armed = true;
            savepointSeen = false;
        }

        static void disarmBlocking() {
            armed = false;
            savepointSeen = false;
        }

        @Override
        public void processElement(StreamRecord<RowData> element) {
            output.collect(element);
        }

        @Override
        public OperatorSnapshotFutures snapshotState(
                long checkpointId,
                long timestamp,
                CheckpointOptions checkpointOptions,
                CheckpointStreamFactory storageLocation)
                throws Exception {
            if (armed) {
                if (checkpointOptions.getCheckpointType().isSavepoint()) {
                    savepointSeen = true;
                } else if (savepointSeen) {
                    // Park until the job is cancelled; the interrupt ends the wait.
                    synchronized (this) {
                        while (true) {
                            wait();
                        }
                    }
                }
            }
            return super.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
        }
    }

    /**
     * Commit callback loaded by {@code commit.callbacks} that throws exactly once, on the first
     * commit whose identifier is past the aborted savepoint's checkpoint id. That commit is the
     * first normal checkpoint after the region failover, so throwing there forces a global failover
     * whose latest completed checkpoint carries the surviving writer's stale savepoint bit. Loaded
     * reflectively by class name, so it must be public with a no-arg constructor and coordinate
     * through static fields (writer TM and JM coordinator share one JVM under MiniCluster).
     */
    public static class FailOnFirstPostRecoveryCommitCallback implements CommitCallback {
        private static final AtomicBoolean FIRED = new AtomicBoolean(false);

        public FailOnFirstPostRecoveryCommitCallback() {}

        static void reset() {
            FIRED.set(false);
        }

        static boolean hasFired() {
            return FIRED.get();
        }

        @Override
        public void call(Context context) {
            long savepointId = FailOnSavepointOperator.savepointCheckpointId();
            if (savepointId > 0
                    && context.identifier > savepointId
                    && FIRED.compareAndSet(false, true)) {
                throw new RuntimeException(
                        "intentional global-failover trigger on commit " + context.identifier);
            }
        }

        @Override
        public void retry(ManifestCommittable committable) {}

        @Override
        public void close() {}
    }
}
