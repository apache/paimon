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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.VersionedSerializerWrapper;
import org.apache.paimon.flink.utils.TestingMetricUtils;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestCommittableSerializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.ThrowingConsumer;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link CommitterOperator}. */
public class CommitterOperatorTest extends CommitterOperatorTestBase {

    protected String initialCommitUser;

    @BeforeEach
    public void before() {
        super.before();
        initialCommitUser = UUID.randomUUID().toString();
    }

    // ------------------------------------------------------------------------
    //  Recoverable operator tests
    // ------------------------------------------------------------------------

    @Test
    public void testFailIntentionallyAfterRestore() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();
        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();
        write.write(GenericRow.of(1, 10L));
        write.write(GenericRow.of(2, 20L));

        long timestamp = 1;
        for (CommitMessage committable : write.prepareCommit(false, 8)) {
            testHarness.processElement(
                    new Committable(8, Committable.Kind.FILE, committable), timestamp++);
        }
        // checkpoint is completed but not notified, so no snapshot is committed
        OperatorSubtaskState snapshot = testHarness.snapshot(0, timestamp++);
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();
        testHarness.close();

        testHarness = createRecoverableTestHarness(table);
        try {
            // commit snapshot from state, fail intentionally
            testHarness.initializeState(snapshot);
            testHarness.open();
            fail("Expecting intentional exception");
        } catch (Exception e) {
            assertThat(e)
                    .hasMessageContaining(
                            "This exception is intentionally thrown "
                                    + "after committing the restored checkpoints. "
                                    + "By restarting the job we hope that "
                                    + "writers can start writing based on these new commits.");
        }
        assertResults(table, "1, 10", "2, 20");
        testHarness.close();

        // snapshot is successfully committed, no failure is needed
        testHarness = createRecoverableTestHarness(table);
        testHarness.initializeState(snapshot);
        testHarness.open();
        assertResults(table, "1, 10", "2, 20");
        testHarness.close();
    }

    @Test
    public void testCheckpointAbort() throws Exception {
        FileStoreTable table = createFileStoreTable();
        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();

        // files from multiple checkpoint
        // but no snapshot
        long cpId = 0;
        for (int i = 0; i < 10; i++) {
            cpId++;
            StreamTableWrite write =
                    table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();
            write.write(GenericRow.of(1, 10L));
            write.write(GenericRow.of(2, 20L));
            for (CommitMessage committable : write.prepareCommit(false, cpId)) {
                testHarness.processElement(
                        new Committable(cpId, Committable.Kind.FILE, committable), 1);
            }
        }

        // checkpoint is completed but not notified, so no snapshot is committed
        testHarness.snapshot(cpId, 1);
        testHarness.notifyOfCompletedCheckpoint(cpId);

        SnapshotManager snapshotManager = new SnapshotManager(LocalFileIO.create(), tablePath);

        // should create 10 snapshots
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(cpId);
        testHarness.close();
    }

    // ------------------------------------------------------------------------
    //  Lossy operator tests
    // ------------------------------------------------------------------------

    @Test
    public void testSnapshotLostWhenFailed() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createLossyTestHarness(table);
        testHarness.open();

        long timestamp = 1;

        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser);
        // this checkpoint is notified, should be committed
        StreamTableWrite write = streamWriteBuilder.newWrite();
        write.write(GenericRow.of(1, 10L));
        write.write(GenericRow.of(2, 20L));
        for (CommitMessage committable : write.prepareCommit(false, 1)) {
            testHarness.processElement(
                    new Committable(1, Committable.Kind.FILE, committable), timestamp++);
        }
        testHarness.snapshot(1, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(1);

        // this checkpoint is not notified, should not be committed
        write.write(GenericRow.of(3, 30L));
        write.write(GenericRow.of(4, 40L));
        for (CommitMessage committable : write.prepareCommit(false, 2)) {
            testHarness.processElement(
                    new Committable(2, Committable.Kind.FILE, committable), timestamp++);
        }
        OperatorSubtaskState snapshot = testHarness.snapshot(2, timestamp++);

        // reopen test harness
        write.close();
        testHarness.close();

        testHarness = createLossyTestHarness(table);
        testHarness.initializeState(snapshot);
        testHarness.open();

        // this checkpoint is notified, should be committed
        write = streamWriteBuilder.newWrite();
        write.write(GenericRow.of(5, 50L));
        write.write(GenericRow.of(6, 60L));
        for (CommitMessage committable : write.prepareCommit(false, 3)) {
            testHarness.processElement(
                    new Committable(3, Committable.Kind.FILE, committable), timestamp++);
        }
        testHarness.snapshot(3, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(3);

        write.close();
        testHarness.close();

        assertResults(table, "1, 10", "2, 20", "5, 50", "6, 60");
    }

    @Test
    public void testRestoreCommitUser() throws Exception {
        FileStoreTable table = createFileStoreTable();
        String commitUser = UUID.randomUUID().toString();

        // 1. Generate operatorSubtaskState
        List<OperatorSubtaskState> operatorSubtaskStates = new ArrayList<>();

        long timestamp = 1;
        long checkpoint = 1;
        for (int i = 0; i < 5; i++) {
            OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                    createLossyTestHarness(table, commitUser);

            testHarness.open();
            OperatorSubtaskState snapshot =
                    writeAndSnapshot(table, commitUser, timestamp, ++checkpoint, testHarness);
            operatorSubtaskStates.add(snapshot);
            testHarness.close();
        }

        // 2. Clearing redundant union list state
        OperatorSubtaskState operatorSubtaskState =
                AbstractStreamOperatorTestHarness.repackageState(
                        operatorSubtaskStates.toArray(new OperatorSubtaskState[0]));

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createLossyTestHarness(table);
        testHarness.initializeState(operatorSubtaskState);
        OperatorSubtaskState snapshot =
                writeAndSnapshot(table, initialCommitUser, timestamp, ++checkpoint, testHarness);
        testHarness.close();

        // 3. Check whether success
        List<String> actual = new ArrayList<>();

        OneInputStreamOperator<Committable, Committable> operator =
                createCommitterOperator(
                        table,
                        initialCommitUser,
                        new NoopCommittableStateManager(),
                        context -> {
                            ListState<String> state =
                                    context.getOperatorStateStore()
                                            .getUnionListState(
                                                    new ListStateDescriptor<>(
                                                            "commit_user_state", String.class));
                            state.get().forEach(actual::add);
                        });

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness1 =
                createTestHarness(operator);
        testHarness1.initializeState(snapshot);
        testHarness1.close();

        Assertions.assertThat(actual.size()).isEqualTo(1);

        Assertions.assertThat(actual).hasSameElementsAs(Lists.newArrayList(commitUser));
    }

    @Test
    public void testCommitInputEnd() throws Exception {
        FileStoreTable table = createFileStoreTable();
        String commitUser = UUID.randomUUID().toString();
        OneInputStreamOperator<Committable, Committable> operator =
                createCommitterOperator(table, commitUser, new NoopCommittableStateManager());
        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createTestHarness(operator);
        testHarness.open();
        Assertions.assertThatCode(
                        () -> {
                            long time = System.currentTimeMillis();
                            long cp = 0L;
                            testHarness.processElement(
                                    new Committable(
                                            Long.MAX_VALUE,
                                            Committable.Kind.FILE,
                                            new CommitMessageImpl(
                                                    BinaryRow.EMPTY_ROW,
                                                    0,
                                                    new DataIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()),
                                                    new CompactIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()))),
                                    cp);
                            testHarness.snapshot(cp++, time++);
                            testHarness.processElement(
                                    new Committable(
                                            Long.MAX_VALUE,
                                            Committable.Kind.FILE,
                                            new CommitMessageImpl(
                                                    BinaryRow.EMPTY_ROW,
                                                    0,
                                                    new DataIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()),
                                                    new CompactIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()))),
                                    cp);
                            testHarness.processElement(
                                    new Committable(
                                            Long.MAX_VALUE,
                                            Committable.Kind.FILE,
                                            new CommitMessageImpl(
                                                    BinaryRow.EMPTY_ROW,
                                                    0,
                                                    new DataIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()),
                                                    new CompactIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()))),
                                    cp);
                            testHarness.snapshot(cp++, time++);
                            testHarness.snapshot(cp, time);
                        })
                .doesNotThrowAnyException();

        if (operator instanceof CommitterOperator) {
            Assertions.assertThat(
                            ((ManifestCommittable)
                                            ((CommitterOperator) operator)
                                                    .committablesPerCheckpoint.get(Long.MAX_VALUE))
                                    .fileCommittables()
                                    .size())
                    .isEqualTo(3);
        }

        Assertions.assertThatCode(
                        () -> {
                            long time = System.currentTimeMillis();
                            long cp = 0L;
                            testHarness.processElement(
                                    new Committable(
                                            0L,
                                            Committable.Kind.FILE,
                                            new CommitMessageImpl(
                                                    BinaryRow.EMPTY_ROW,
                                                    0,
                                                    new DataIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()),
                                                    new CompactIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()))),
                                    cp);
                            testHarness.snapshot(cp++, time++);
                            testHarness.processElement(
                                    new Committable(
                                            0L,
                                            Committable.Kind.FILE,
                                            new CommitMessageImpl(
                                                    BinaryRow.EMPTY_ROW,
                                                    0,
                                                    new DataIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()),
                                                    new CompactIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()))),
                                    cp);
                            testHarness.processElement(
                                    new Committable(
                                            Long.MAX_VALUE,
                                            Committable.Kind.FILE,
                                            new CommitMessageImpl(
                                                    BinaryRow.EMPTY_ROW,
                                                    0,
                                                    new DataIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()),
                                                    new CompactIncrement(
                                                            Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            Collections.emptyList()))),
                                    cp);
                            testHarness.snapshot(cp++, time++);
                            testHarness.snapshot(cp, time);
                        })
                .hasRootCauseInstanceOf(RuntimeException.class)
                .cause()
                .hasMessageContaining("Repeatedly commit the same checkpoint files.");
    }

    private static OperatorSubtaskState writeAndSnapshot(
            FileStoreTable table,
            String commitUser,
            long timestamp,
            long checkpoint,
            OneInputStreamOperatorTestHarness<Committable, Committable> testHarness)
            throws Exception {
        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();

        StreamTableWrite write = streamWriteBuilder.withCommitUser(commitUser).newWrite();
        write.write(GenericRow.of(1, 10L));
        for (CommitMessage committable : write.prepareCommit(false, 1)) {
            testHarness.processElement(
                    new Committable(checkpoint, Committable.Kind.FILE, committable), ++timestamp);
        }
        OperatorSubtaskState snapshot = testHarness.snapshot(checkpoint, ++timestamp);
        return snapshot;
    }

    @Test
    public void testWatermarkCommit() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();
        long timestamp = 0;
        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        long cpId = 1;
        write.write(GenericRow.of(1, 10L));
        testHarness.processElement(
                new Committable(
                        cpId, Committable.Kind.FILE, write.prepareCommit(true, cpId).get(0)),
                timestamp++);
        testHarness.processWatermark(new Watermark(1024));
        testHarness.snapshot(cpId, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(cpId);
        assertThat(table.snapshotManager().latestSnapshot().watermark()).isEqualTo(1024L);

        cpId = 2;
        write.write(GenericRow.of(1, 20L));
        testHarness.processElement(
                new Committable(
                        cpId, Committable.Kind.FILE, write.prepareCommit(true, cpId).get(0)),
                timestamp++);
        testHarness.processWatermark(new Watermark(Long.MAX_VALUE));
        testHarness.snapshot(cpId, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(cpId);

        testHarness.close();
        write.close();
        assertThat(table.snapshotManager().latestSnapshot().watermark()).isEqualTo(1024L);
    }

    @Test
    public void testEmptyCommit() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();

        testHarness.snapshot(1, 1);
        testHarness.notifyOfCompletedCheckpoint(1);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot).isNull();
    }

    @Test
    public void testForceCreateSnapshotCommit() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options ->
                                options.set(
                                        CoreOptions.COMMIT_FORCE_CREATE_SNAPSHOT.key(), "true"));

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();

        testHarness.snapshot(1, 1);
        testHarness.notifyOfCompletedCheckpoint(1);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot).isNotNull();
    }

    @Test
    public void testEmptyCommitWithProcessTimeTag() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(
                                    CoreOptions.TAG_AUTOMATIC_CREATION,
                                    CoreOptions.TagCreationMode.PROCESS_TIME);
                            options.set(
                                    CoreOptions.TAG_CREATION_PERIOD,
                                    CoreOptions.TagCreationPeriod.DAILY);
                        });

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();

        testHarness.snapshot(1, 1);
        testHarness.notifyOfCompletedCheckpoint(1);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.id()).isEqualTo(1);
        assertThat(table.tagManager().tagCount()).isEqualTo(1);

        testHarness.snapshot(2, 2);
        testHarness.notifyOfCompletedCheckpoint(2);
        snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.id()).isEqualTo(1);
        assertThat(table.tagManager().tagCount()).isEqualTo(1);
    }

    // ------------------------------------------------------------------------
    //  Metrics tests
    // ------------------------------------------------------------------------

    @Test
    public void testCalcDataBytesSend() throws Exception {
        FileStoreTable table = createFileStoreTable();

        StreamTableWrite write = table.newWrite(initialCommitUser);
        write.write(GenericRow.of(1, 10L));
        write.write(GenericRow.of(1, 20L));
        List<CommitMessage> committable = write.prepareCommit(false, 0);
        write.close();

        ManifestCommittable manifestCommittable = new ManifestCommittable(0);
        for (CommitMessage commitMessage : committable) {
            manifestCommittable.addFileCommittable(commitMessage);
        }

        StreamTableCommit commit = table.newCommit(initialCommitUser);
        OperatorMetricGroup metricGroup = UnregisteredMetricsGroup.createOperatorMetricGroup();
        StoreCommitter committer = new StoreCommitter(commit, metricGroup);
        committer.commit(Collections.singletonList(manifestCommittable));
        CommitterMetrics metrics = committer.getCommitterMetrics();
        assertThat(metrics.getNumBytesOutCounter().getCount()).isEqualTo(285);
        assertThat(metrics.getNumRecordsOutCounter().getCount()).isEqualTo(2);
        committer.close();
    }

    @Test
    public void testCommitMetrics() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperator<Committable, Committable> operator =
                createCommitterOperator(
                        table,
                        null,
                        new RestoreAndFailCommittableStateManager<>(
                                () ->
                                        new VersionedSerializerWrapper<>(
                                                new ManifestCommittableSerializer())));
        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createTestHarness(operator);
        testHarness.open();
        long timestamp = 0;
        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        long cpId = 1;
        write.write(GenericRow.of(1, 100L));
        testHarness.processElement(
                new Committable(
                        cpId, Committable.Kind.FILE, write.prepareCommit(false, cpId).get(0)),
                timestamp++);
        testHarness.snapshot(cpId, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(cpId);

        MetricGroup commitMetricGroup =
                operator.getMetricGroup()
                        .addGroup("paimon")
                        .addGroup("table", table.name())
                        .addGroup("commit");
        assertThat(TestingMetricUtils.getGauge(commitMetricGroup, "lastTableFilesAdded").getValue())
                .isEqualTo(1L);
        assertThat(
                        TestingMetricUtils.getGauge(commitMetricGroup, "lastTableFilesDeleted")
                                .getValue())
                .isEqualTo(0L);
        assertThat(
                        TestingMetricUtils.getGauge(commitMetricGroup, "lastTableFilesAppended")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        TestingMetricUtils.getGauge(
                                        commitMetricGroup, "lastTableFilesCommitCompacted")
                                .getValue())
                .isEqualTo(0L);

        cpId = 2;
        write.write(GenericRow.of(1, 101L));
        // just flush the writer
        write.compact(BinaryRow.EMPTY_ROW, 0, false);
        write.write(GenericRow.of(2, 200L));
        // real compaction
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        testHarness.processElement(
                new Committable(
                        cpId, Committable.Kind.FILE, write.prepareCommit(true, cpId).get(0)),
                timestamp++);
        testHarness.snapshot(cpId, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(cpId);

        assertThat(TestingMetricUtils.getGauge(commitMetricGroup, "lastTableFilesAdded").getValue())
                .isEqualTo(3L);
        assertThat(
                        TestingMetricUtils.getGauge(commitMetricGroup, "lastTableFilesDeleted")
                                .getValue())
                .isEqualTo(3L);
        assertThat(
                        TestingMetricUtils.getGauge(commitMetricGroup, "lastTableFilesAppended")
                                .getValue())
                .isEqualTo(2L);
        assertThat(
                        TestingMetricUtils.getGauge(
                                        commitMetricGroup, "lastTableFilesCommitCompacted")
                                .getValue())
                .isEqualTo(4L);

        testHarness.close();
        write.close();
    }

    @Test
    public void testParallelism() throws Exception {
        FileStoreTable table = createFileStoreTable();
        String commitUser = UUID.randomUUID().toString();
        OneInputStreamOperator<Committable, Committable> operator =
                createCommitterOperator(table, commitUser, new NoopCommittableStateManager());
        try (OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createTestHarness(operator, 10, 10, 3)) {
            Assertions.assertThatCode(testHarness::open)
                    .hasMessage("Committer Operator parallelism in paimon MUST be one.");
        }
    }

    // ------------------------------------------------------------------------
    //  Test utils
    // ------------------------------------------------------------------------

    protected OneInputStreamOperatorTestHarness<Committable, Committable>
            createRecoverableTestHarness(FileStoreTable table) throws Exception {
        OneInputStreamOperator<Committable, Committable> operator =
                createCommitterOperator(
                        table,
                        null,
                        new RestoreAndFailCommittableStateManager<>(
                                () ->
                                        new VersionedSerializerWrapper<>(
                                                new ManifestCommittableSerializer())));
        return createTestHarness(operator);
    }

    private OneInputStreamOperatorTestHarness<Committable, Committable> createLossyTestHarness(
            FileStoreTable table) throws Exception {
        return createLossyTestHarness(table, null);
    }

    private OneInputStreamOperatorTestHarness<Committable, Committable> createLossyTestHarness(
            FileStoreTable table, String commitUser) throws Exception {
        OneInputStreamOperator<Committable, Committable> operator =
                createCommitterOperator(table, commitUser, new NoopCommittableStateManager());
        return createTestHarness(operator);
    }

    private OneInputStreamOperatorTestHarness<Committable, Committable> createTestHarness(
            OneInputStreamOperator<Committable, Committable> operator) throws Exception {
        return createTestHarness(operator, 1, 1, 0);
    }

    private OneInputStreamOperatorTestHarness<Committable, Committable> createTestHarness(
            OneInputStreamOperator<Committable, Committable> operator,
            int maxParallelism,
            int parallelism,
            int subTaskIndex)
            throws Exception {
        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<Committable, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(
                        operator,
                        maxParallelism,
                        parallelism,
                        subTaskIndex,
                        serializer,
                        new OperatorID());
        harness.setup(serializer);
        return harness;
    }

    protected OneInputStreamOperator<Committable, Committable> createCommitterOperator(
            FileStoreTable table,
            String commitUser,
            CommittableStateManager<ManifestCommittable> committableStateManager) {
        return new CommitterOperator<>(
                true,
                true,
                true,
                commitUser == null ? initialCommitUser : commitUser,
                (user, metricGroup) ->
                        new StoreCommitter(
                                table.newStreamWriteBuilder().withCommitUser(user).newCommit(),
                                metricGroup),
                committableStateManager);
    }

    protected OneInputStreamOperator<Committable, Committable> createCommitterOperator(
            FileStoreTable table,
            String commitUser,
            CommittableStateManager<ManifestCommittable> committableStateManager,
            ThrowingConsumer<StateInitializationContext, Exception> initializeFunction) {
        return new CommitterOperator<Committable, ManifestCommittable>(
                true,
                true,
                true,
                commitUser == null ? initialCommitUser : commitUser,
                (user, metricGroup) ->
                        new StoreCommitter(
                                table.newStreamWriteBuilder().withCommitUser(user).newCommit(),
                                metricGroup),
                committableStateManager) {
            @Override
            public void initializeState(StateInitializationContext context) throws Exception {
                initializeFunction.accept(context);
            }
        };
    }
}
