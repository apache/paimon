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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.coordinator.CheckpointCommittables;
import org.apache.paimon.flink.sink.coordinator.CheckpointCommittablesSerializer;
import org.apache.paimon.flink.sink.coordinator.CommittableEvent;
import org.apache.paimon.flink.sink.coordinator.CommittingWriteOperatorCoordinator;
import org.apache.paimon.flink.sink.coordinator.RestoredCommittableEvent;
import org.apache.paimon.flink.utils.InternalRowTypeSerializer;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.FlinkRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CoordinatorCommittingRowDataStoreWriteOperator}. */
public class CoordinatorCommittingRowDataStoreWriteOperatorTest extends CommitterOperatorTestBase {

    private static final TypeSerializer<CheckpointCommittables> COMMITTABLES_SERIALIZER =
            new SimpleVersionedSerializerTypeSerializerProxy<>(
                    () ->
                            new CheckpointCommittablesSerializer(
                                    new CommittableSerializer(new CommitMessageSerializer())));

    @Test
    @Timeout(30)
    public void testWriterSendsCommittablesToCoordinatorAndStillEmitsDownstream() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(CoreOptions.BUCKET, -1);
                            options.remove("bucket-key");
                        });
        String commitUser = UUID.randomUUID().toString();
        List<OperatorEvent> events = new ArrayList<>();

        CommittingWriteOperatorCoordinator coordinator =
                new CommittingWriteOperatorCoordinator(
                        new TestingContext(),
                        context ->
                                new StoreCommitter(
                                        table, table.newCommit(context.commitUser()), context),
                        true,
                        commitUser,
                        false,
                        null);
        coordinator.start();
        coordinator.waitProcessAllActions();

        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(table, commitUser, events::add);
        TypeSerializer<Committable> committableSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        harness.setup(committableSerializer);
        harness.open();

        harness.processElement(GenericRow.of(1, 1L), 1);
        harness.prepareSnapshotPreBarrier(1);
        harness.snapshot(1, 10);

        List<Committable> downstreamCommittables = extractCommittables(harness);
        assertThat(downstreamCommittables).hasSize(1);
        assertThat(events).hasSize(1);

        CommittableEvent event = (CommittableEvent) events.get(0);
        assertThat(event.getCheckpointId()).isEqualTo(1L);
        CheckpointCommittables decoded = event.deserialize(COMMITTABLES_SERIALIZER);
        assertThat(decoded.checkpointId()).isEqualTo(1L);
        assertThat(decoded.committables()).hasSize(1);

        coordinator.handleEventFromOperator(0, 0, event);
        coordinator.notifyCheckpointComplete(1);
        coordinator.waitProcessAllActions();
        harness.notifyOfCompletedCheckpoint(1);

        assertResults(table, "1, 1");

        harness.close();
        coordinator.close();
    }

    @Test
    public void
            testPendingCommittablesAccumulateAcrossUncompletedCheckpointsAndAreClearedOnComplete()
                    throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String commitUser = UUID.randomUUID().toString();
        List<OperatorEvent> events = new ArrayList<>();

        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(table, commitUser, events::add);
        TypeSerializer<Committable> committableSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        harness.setup(committableSerializer);
        harness.open();
        CoordinatorCommittingRowDataStoreWriteOperator operator =
                (CoordinatorCommittingRowDataStoreWriteOperator) harness.getOperator();

        // cp1: write data, snapshot, but checkpoint never completes (e.g. aborted)
        harness.processElement(GenericRow.of(1, 10L), 1);
        harness.prepareSnapshotPreBarrier(1);
        harness.snapshot(1, 10);
        assertCommittableEventCheckpoint(events.get(events.size() - 1), 1L);
        assertThat(operator.getPendingCommittables()).containsKey(1L);

        // cp2: another writing checkpoint, also aborted; cp1 buffer must still be retained
        harness.processElement(GenericRow.of(2, 20L), 2);
        harness.prepareSnapshotPreBarrier(2);
        harness.snapshot(2, 20);
        assertCommittableEventCheckpoint(events.get(events.size() - 1), 2L);
        assertThat(operator.getPendingCommittables()).containsKeys(1L, 2L);

        // cp3 completes -> headMap(3, true) clears every buffered checkpoint
        harness.processElement(GenericRow.of(3, 30L), 3);
        harness.prepareSnapshotPreBarrier(3);
        harness.snapshot(3, 30);
        assertCommittableEventCheckpoint(events.get(events.size() - 1), 3L);
        harness.notifyOfCompletedCheckpoint(3);
        assertThat(operator.getPendingCommittables()).isEmpty();

        harness.close();
    }

    @Test
    public void testRestoreReplaysBufferedCommittablesToCoordinator() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String commitUser = UUID.randomUUID().toString();
        TypeSerializer<Committable> committableSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());

        // session 1: write + snapshot, do NOT notify completion, then crash
        List<OperatorEvent> firstEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> firstHarness =
                createHarness(table, commitUser, firstEvents::add);
        firstHarness.setup(committableSerializer);
        firstHarness.open();
        firstHarness.processElement(GenericRow.of(1, 10L), 1);
        firstHarness.prepareSnapshotPreBarrier(1);
        OperatorSubtaskState snapshot = firstHarness.snapshot(1, 10);
        firstHarness.close();

        // session 2: restore. Expect exactly one RestoredCommittableEvent carrying the buffered
        // committables in a single payload.
        List<OperatorEvent> restoredEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> secondHarness =
                createHarness(table, commitUser, restoredEvents::add);
        secondHarness.setup(committableSerializer);
        restoreWithCheckpointId(secondHarness, snapshot, 1L);
        secondHarness.open();

        assertThat(restoredEvents).hasSize(1);
        RestoredCommittableEvent restoredEvent = (RestoredCommittableEvent) restoredEvents.get(0);
        assertThat(restoredEvent.getRestoredCheckpointId()).isEqualTo(1L);
        List<CheckpointCommittables> entries = restoredEvent.deserialize(COMMITTABLES_SERIALIZER);
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).checkpointId()).isEqualTo(1L);
        assertThat(entries.get(0).committables()).hasSize(1);

        // a fresh snapshot must persist a single empty-Long.MIN_VALUE marker for cp2 so the
        // coordinator can still align that (subtask, checkpoint) — the previous buffer was
        // cleared on restore and the new barrier has not seen any watermark yet.
        CoordinatorCommittingRowDataStoreWriteOperator operator =
                (CoordinatorCommittingRowDataStoreWriteOperator) secondHarness.getOperator();
        secondHarness.prepareSnapshotPreBarrier(2);
        secondHarness.snapshot(2, 20);
        assertThat(operator.getPendingCommittables()).containsOnlyKeys(2L);
        CheckpointCommittables cp2 = operator.getPendingCommittables().get(2L);
        assertThat(cp2.committables()).isEmpty();
        assertThat(cp2.watermark()).isEqualTo(Long.MIN_VALUE);

        secondHarness.close();
    }

    @Test
    public void testRestoreReplaysFrozenWatermarkPerCheckpoint() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String commitUser = UUID.randomUUID().toString();
        TypeSerializer<Committable> committableSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());

        // session 1: two barriers, each freezing a distinct watermark; neither checkpoint completes
        List<OperatorEvent> firstEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> firstHarness =
                createHarness(table, commitUser, firstEvents::add);
        firstHarness.setup(committableSerializer);
        firstHarness.open();

        firstHarness.processElement(GenericRow.of(1, 10L), 1);
        firstHarness.processWatermark(new Watermark(100L));
        firstHarness.prepareSnapshotPreBarrier(1);
        firstHarness.snapshot(1, 10);

        firstHarness.processElement(GenericRow.of(2, 20L), 2);
        firstHarness.processWatermark(new Watermark(500L));
        firstHarness.prepareSnapshotPreBarrier(2);
        OperatorSubtaskState snapshot = firstHarness.snapshot(2, 20);
        firstHarness.close();

        // session 2: restore. Expect a single RestoredCommittableEvent carrying both persisted
        // (checkpoint, watermark) entries in one payload.
        List<OperatorEvent> restoredEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> secondHarness =
                createHarness(table, commitUser, restoredEvents::add);
        secondHarness.setup(committableSerializer);
        restoreWithCheckpointId(secondHarness, snapshot, 2L);
        secondHarness.open();

        assertThat(restoredEvents).hasSize(1);
        RestoredCommittableEvent restoredEvent = (RestoredCommittableEvent) restoredEvents.get(0);
        assertThat(restoredEvent.getRestoredCheckpointId()).isEqualTo(2L);
        List<CheckpointCommittables> entries = restoredEvent.deserialize(COMMITTABLES_SERIALIZER);
        assertThat(entries).hasSize(2);

        assertThat(entries.get(0).checkpointId()).isEqualTo(1L);
        assertThat(entries.get(0).watermark()).isEqualTo(100L);
        assertThat(entries.get(0).committables()).hasSize(1);

        assertThat(entries.get(1).checkpointId()).isEqualTo(2L);
        assertThat(entries.get(1).watermark()).isEqualTo(500L);
        assertThat(entries.get(1).committables()).hasSize(1);

        secondHarness.close();
    }

    @Test
    public void testWatermarkArrivingAfterBarrierDoesNotChangeEmittedEvent() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String commitUser = UUID.randomUUID().toString();
        List<OperatorEvent> events = new ArrayList<>();

        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(table, commitUser, events::add);
        TypeSerializer<Committable> committableSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        harness.setup(committableSerializer);
        harness.open();

        harness.processElement(GenericRow.of(1, 10L), 1);
        harness.processWatermark(new Watermark(100L));
        harness.prepareSnapshotPreBarrier(1);
        harness.snapshot(1, 10);
        assertThat(events).hasSize(1);

        // Late-arriving watermark must not touch the already-emitted event for cp1.
        harness.processWatermark(new Watermark(500L));
        CheckpointCommittables cp1 =
                ((CommittableEvent) events.get(0)).deserialize(COMMITTABLES_SERIALIZER);
        assertThat(cp1.checkpointId()).isEqualTo(1L);
        assertThat(cp1.watermark()).isEqualTo(100L);

        // Next barrier freezes the newer watermark for cp2 without retroactively changing cp1.
        harness.processElement(GenericRow.of(2, 20L), 2);
        harness.prepareSnapshotPreBarrier(2);
        harness.snapshot(2, 20);
        assertThat(events).hasSize(2);
        assertThat(
                        ((CommittableEvent) events.get(0))
                                .deserialize(COMMITTABLES_SERIALIZER)
                                .watermark())
                .isEqualTo(100L);
        CheckpointCommittables cp2 =
                ((CommittableEvent) events.get(1)).deserialize(COMMITTABLES_SERIALIZER);
        assertThat(cp2.checkpointId()).isEqualTo(2L);
        assertThat(cp2.watermark()).isEqualTo(500L);

        harness.close();
    }

    @Test
    public void testEmptyRestoreStillSendsRestoreEvent() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String commitUser = UUID.randomUUID().toString();
        TypeSerializer<Committable> committableSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());

        // session 1: snapshot with no data, then crash
        List<OperatorEvent> firstEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> firstHarness =
                createHarness(table, commitUser, firstEvents::add);
        firstHarness.setup(committableSerializer);
        firstHarness.open();
        firstHarness.prepareSnapshotPreBarrier(1);
        OperatorSubtaskState snapshot = firstHarness.snapshot(1, 10);
        firstHarness.close();

        // session 2: restore. The buffer holds a single empty+Long.MIN_VALUE marker persisted at
        // the previous barrier so the coordinator can align even this "no-data, no-watermark"
        // checkpoint.
        List<OperatorEvent> restoredEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> secondHarness =
                createHarness(table, commitUser, restoredEvents::add);
        secondHarness.setup(committableSerializer);
        restoreWithCheckpointId(secondHarness, snapshot, 1L);
        secondHarness.open();

        assertThat(restoredEvents).hasSize(1);
        RestoredCommittableEvent restoredEvent = (RestoredCommittableEvent) restoredEvents.get(0);
        assertThat(restoredEvent.getRestoredCheckpointId()).isEqualTo(1L);
        List<CheckpointCommittables> entries = restoredEvent.deserialize(COMMITTABLES_SERIALIZER);
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).checkpointId()).isEqualTo(1L);
        assertThat(entries.get(0).committables()).isEmpty();
        assertThat(entries.get(0).watermark()).isEqualTo(Long.MIN_VALUE);

        secondHarness.close();
    }

    @Test
    public void testEndInputSendsMaxCheckpointAndForwardsCurrentWatermark() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(CoreOptions.BUCKET, -1);
                            options.remove("bucket-key");
                            options.set(FlinkConnectorOptions.END_INPUT_WATERMARK, 12345L);
                        });
        String commitUser = UUID.randomUUID().toString();
        List<OperatorEvent> events = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(table, commitUser, events::add);
        harness.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
        harness.open();

        harness.processElement(GenericRow.of(1, 10L), 1);
        harness.endInput();

        assertThat(events).hasSize(1);
        CommittableEvent event = (CommittableEvent) events.get(0);
        assertThat(event.getCheckpointId()).isEqualTo(Long.MAX_VALUE);
        CheckpointCommittables entry = event.deserialize(COMMITTABLES_SERIALIZER);
        assertThat(entry.checkpointId()).isEqualTo(Long.MAX_VALUE);
        assertThat(entry.watermark()).isEqualTo(Long.MIN_VALUE);
        assertThat(entry.committables()).hasSize(1);
        CoordinatorCommittingRowDataStoreWriteOperator operator =
                (CoordinatorCommittingRowDataStoreWriteOperator) harness.getOperator();
        assertThat(operator.getPendingCommittables()).containsKey(Long.MAX_VALUE);

        harness.close();
    }

    @Test
    public void testRestoredEndInputSurvivesAnotherSnapshot() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String commitUser = UUID.randomUUID().toString();
        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());

        List<OperatorEvent> firstEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> first =
                createHarness(table, commitUser, firstEvents::add);
        first.setup(serializer);
        first.open();
        first.processElement(GenericRow.of(1, 10L), 1);
        first.endInput();
        OperatorSubtaskState firstSnapshot = first.snapshot(1L, 10L);
        first.close();

        List<OperatorEvent> secondEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> second =
                createHarness(table, commitUser, secondEvents::add);
        second.setup(serializer);
        restoreWithCheckpointId(second, firstSnapshot, 1L);
        second.open();
        CoordinatorCommittingRowDataStoreWriteOperator secondOperator =
                (CoordinatorCommittingRowDataStoreWriteOperator) second.getOperator();
        assertThat(secondOperator.getPendingCommittables()).containsKey(Long.MAX_VALUE);
        RestoredCommittableEvent restored = (RestoredCommittableEvent) secondEvents.get(0);
        assertThat(restored.deserialize(COMMITTABLES_SERIALIZER))
                .extracting(CheckpointCommittables::checkpointId)
                .contains(Long.MAX_VALUE);

        OperatorSubtaskState secondSnapshot = second.snapshot(2L, 20L);
        second.close();

        List<OperatorEvent> thirdEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> third =
                createHarness(table, commitUser, thirdEvents::add);
        third.setup(serializer);
        restoreWithCheckpointId(third, secondSnapshot, 2L);
        third.open();
        RestoredCommittableEvent restoredAgain = (RestoredCommittableEvent) thirdEvents.get(0);
        assertThat(restoredAgain.deserialize(COMMITTABLES_SERIALIZER))
                .extracting(CheckpointCommittables::checkpointId)
                .contains(Long.MAX_VALUE);

        third.close();
    }

    private void assertCommittableEventCheckpoint(OperatorEvent event, long expectedCheckpointId) {
        CommittableEvent committableEvent = (CommittableEvent) event;
        assertThat(committableEvent.getCheckpointId()).isEqualTo(expectedCheckpointId);
    }

    private void restoreWithCheckpointId(
            OneInputStreamOperatorTestHarness<InternalRow, Committable> harness,
            OperatorSubtaskState snapshot,
            long restoredCheckpointId)
            throws Exception {
        // OneInputStreamOperatorTestHarness#initializeState(OperatorSubtaskState) hard-codes the
        // reported checkpoint id to 0. Wire the snapshot in manually and go through the
        // initializeEmptyState() path so the operator observes the real restored checkpoint id.
        TaskStateSnapshot taskState = new TaskStateSnapshot();
        taskState.putSubtaskStateByOperatorID(harness.getOperator().getOperatorID(), snapshot);
        TestTaskStateManager stateManager =
                (TestTaskStateManager) harness.getEnvironment().getTaskStateManager();
        stateManager.restoreLatestCheckpointState(
                Collections.singletonMap(restoredCheckpointId, taskState));
        harness.initializeEmptyState();
    }

    private FileStoreTable createUnawareBucketTable() throws Exception {
        return createFileStoreTable(
                options -> {
                    options.set(CoreOptions.BUCKET, -1);
                    options.remove("bucket-key");
                });
    }

    @SuppressWarnings("unchecked")
    private List<Committable> extractCommittables(
            OneInputStreamOperatorTestHarness<InternalRow, Committable> harness) {
        List<Committable> committables = new ArrayList<>();
        while (!harness.getOutput().isEmpty()) {
            committables.add(((StreamRecord<Committable>) harness.getOutput().poll()).getValue());
        }
        return committables;
    }

    private OneInputStreamOperatorTestHarness<InternalRow, Committable> createHarness(
            FileStoreTable table, String commitUser, OperatorEventGateway gateway)
            throws Exception {
        RowDataStoreWriteOperator.Factory operatorFactory =
                new RowDataStoreWriteOperator.Factory(
                        table,
                        (fileStoreTable,
                                initialCommitUser,
                                state,
                                ioManager,
                                memoryPool,
                                metricGroup) ->
                                new StoreSinkWriteImpl(
                                        fileStoreTable,
                                        initialCommitUser,
                                        state,
                                        ioManager,
                                        false,
                                        false,
                                        true,
                                        memoryPool,
                                        metricGroup),
                        commitUser) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <T extends StreamOperator<Committable>> T createStreamOperator(
                            StreamOperatorParameters<Committable> parameters) {
                        return (T)
                                new CoordinatorCommittingRowDataStoreWriteOperator(
                                        parameters,
                                        table,
                                        storeSinkWriteProvider,
                                        commitUser,
                                        gateway);
                    }

                    @Override
                    @SuppressWarnings("rawtypes")
                    public Class<? extends StreamOperator> getStreamOperatorClass(
                            ClassLoader classLoader) {
                        return CoordinatorCommittingRowDataStoreWriteOperator.class;
                    }
                };
        InternalTypeInfo<InternalRow> typeInfo =
                new InternalTypeInfo<>(new InternalRowTypeSerializer(table.rowType()));
        return new OneInputStreamOperatorTestHarness<>(
                operatorFactory, typeInfo.createSerializer(new ExecutionConfig()));
    }

    private static class TestingContext implements OperatorCoordinator.Context {

        @Override
        public OperatorID getOperatorId() {
            return new OperatorID();
        }

        public JobID getJobID() {
            return new JobID();
        }

        @Override
        public org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup metricGroup() {
            return null;
        }

        @Override
        public void failJob(Throwable cause) {
            throw new FlinkRuntimeException(cause);
        }

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public ClassLoader getUserCodeClassloader() {
            return Thread.currentThread().getContextClassLoader();
        }

        @Override
        public CoordinatorStore getCoordinatorStore() {
            return null;
        }

        @Override
        public boolean isConcurrentExecutionAttemptsSupported() {
            return false;
        }

        @Nullable
        @Override
        public CheckpointCoordinator getCheckpointCoordinator() {
            return null;
        }
    }
}
