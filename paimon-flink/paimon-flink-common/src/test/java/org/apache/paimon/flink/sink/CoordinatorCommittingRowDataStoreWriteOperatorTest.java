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
import org.apache.paimon.flink.sink.coordinator.CheckpointCommittables;
import org.apache.paimon.flink.sink.coordinator.CheckpointCommittablesSerializer;
import org.apache.paimon.flink.sink.coordinator.CommittableEvent;
import org.apache.paimon.flink.sink.coordinator.CommittingWriteOperatorCoordinator;
import org.apache.paimon.flink.sink.coordinator.RestoredCommittableEvent;
import org.apache.paimon.flink.sink.coordinator.TerminalWriterReleaseEvent;
import org.apache.paimon.flink.utils.InternalRowTypeSerializer;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
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
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.FlinkRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Tests for {@link CoordinatorCommittingRowDataStoreWriteOperator}. */
public class CoordinatorCommittingRowDataStoreWriteOperatorTest extends CommitterTestBase {

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
                        null,
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
    @Timeout(30)
    public void testEndInputSealsRealCheckpointAndReportsFromSnapshot() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String commitUser = UUID.randomUUID().toString();
        List<OperatorEvent> events = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(table, commitUser, events::add);
        harness.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
        harness.open();

        harness.processElement(GenericRow.of(1, 1L), 1L);
        harness.endInput();
        harness.endInput();
        assertThat(events).isEmpty();
        harness.prepareSnapshotPreBarrier(2L);
        assertThat(events).isEmpty();
        harness.snapshot(2L, 20L);
        harness.prepareSnapshotPreBarrier(3L);
        harness.snapshot(3L, 30L);
        assertThat(events).hasSize(2);
        CheckpointCommittables tail =
                ((CommittableEvent) events.get(0)).deserialize(COMMITTABLES_SERIALIZER);
        CheckpointCommittables marker =
                ((CommittableEvent) events.get(1)).deserialize(COMMITTABLES_SERIALIZER);
        assertThat(tail.checkpointId()).isEqualTo(2L);
        assertThat(tail.committables()).hasSize(1);
        assertThat(tail.committables().get(0).checkpointId()).isEqualTo(2L);
        assertThat(tail.terminal()).isTrue();
        assertThat(marker.checkpointId()).isEqualTo(3L);
        assertThat(marker.committables()).isEmpty();
        assertThat(marker.terminal()).isTrue();
        assertThat(extractCommittables(harness)).isEmpty();
        harness.close();
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
    @Timeout(30)
    public void testWatermarkStatusFrozenAtBarrierAcrossCheckpoints() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String commitUser = UUID.randomUUID().toString();
        List<OperatorEvent> events = new ArrayList<>();

        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(table, commitUser, events::add);
        TypeSerializer<Committable> committableSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        harness.setup(committableSerializer);
        harness.open();

        // cp1: writer is IDLE at barrier time.
        harness.processWatermark(new Watermark(100L));
        harness.processWatermarkStatus(WatermarkStatus.IDLE);
        harness.prepareSnapshotPreBarrier(1);
        harness.snapshot(1, 10);

        // cp2: back to ACTIVE with a new watermark. Idle status must not linger on cp2 just
        // because cp1 was idle.
        harness.processWatermarkStatus(WatermarkStatus.ACTIVE);
        harness.processWatermark(new Watermark(500L));
        harness.prepareSnapshotPreBarrier(2);
        harness.snapshot(2, 20);

        assertThat(events).hasSize(2);
        CheckpointCommittables cp1 =
                ((CommittableEvent) events.get(0)).deserialize(COMMITTABLES_SERIALIZER);
        assertThat(cp1.checkpointId()).isEqualTo(1L);
        assertThat(cp1.watermark()).isEqualTo(100L);
        assertThat(cp1.idle()).isTrue();

        CheckpointCommittables cp2 =
                ((CommittableEvent) events.get(1)).deserialize(COMMITTABLES_SERIALIZER);
        assertThat(cp2.checkpointId()).isEqualTo(2L);
        assertThat(cp2.watermark()).isEqualTo(500L);
        assertThat(cp2.idle()).isFalse();

        harness.close();
    }

    @Test
    @Timeout(30)
    public void testIdleFlagResetsToActiveOnRestore() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String commitUser = UUID.randomUUID().toString();
        TypeSerializer<Committable> committableSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());

        // session 1: end the session while IDLE so the restore path is exercised on a snapshot
        // taken from an idle writer.
        List<OperatorEvent> firstEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> firstHarness =
                createHarness(table, commitUser, firstEvents::add);
        firstHarness.setup(committableSerializer);
        firstHarness.open();

        firstHarness.processWatermark(new Watermark(100L));
        firstHarness.processWatermarkStatus(WatermarkStatus.IDLE);
        firstHarness.prepareSnapshotPreBarrier(1);
        OperatorSubtaskState snapshot = firstHarness.snapshot(1, 10);
        firstHarness.close();

        // session 2: restore. Flink runtime does not replay the last WatermarkStatus, and the
        // aligner treats channels as ACTIVE + Long.MIN_VALUE on rebuild, so the writer must also
        // default to ACTIVE. The next barrier — before any WatermarkStatus event — must emit
        // idle=false, matching Flink's own valve-rebuild contract.
        List<OperatorEvent> restoredEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> secondHarness =
                createHarness(table, commitUser, restoredEvents::add);
        secondHarness.setup(committableSerializer);
        restoreWithCheckpointId(secondHarness, snapshot, 1L);
        secondHarness.open();

        secondHarness.prepareSnapshotPreBarrier(2);
        secondHarness.snapshot(2, 20);

        // First event is the restore replay; the second is the freshly frozen cp2.
        assertThat(restoredEvents).hasSize(2);
        CommittableEvent cp2Event = (CommittableEvent) restoredEvents.get(1);
        CheckpointCommittables cp2 = cp2Event.deserialize(COMMITTABLES_SERIALIZER);
        assertThat(cp2.checkpointId()).isEqualTo(2L);
        assertThat(cp2.idle()).isFalse();

        secondHarness.close();
    }

    @Test
    @Timeout(30)
    public void testSavepointBitRidesOnCommittableEventAndPendingState() throws Exception {
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

        // cp1: a normal checkpoint carries savepoint=false in both the event and the pending state.
        harness.processElement(GenericRow.of(1, 10L), 1);
        harness.prepareSnapshotPreBarrier(1);
        harness.snapshot(1, 10);
        assertThat(
                        ((CommittableEvent) events.get(0))
                                .deserialize(COMMITTABLES_SERIALIZER)
                                .shouldCreateSavepointTag())
                .isFalse();
        assertThat(operator.getPendingCommittables().get(1L).shouldCreateSavepointTag()).isFalse();

        // cp2: a savepoint sets savepoint=true on both the emitted event and the persisted entry.
        harness.processElement(GenericRow.of(2, 20L), 2);
        harness.prepareSnapshotPreBarrier(2);
        harness.snapshotWithLocalState(
                2, 20, SavepointType.savepoint(SavepointFormatType.CANONICAL));
        assertThat(
                        ((CommittableEvent) events.get(1))
                                .deserialize(COMMITTABLES_SERIALIZER)
                                .shouldCreateSavepointTag())
                .isTrue();
        assertThat(operator.getPendingCommittables().get(2L).shouldCreateSavepointTag()).isTrue();

        harness.close();
    }

    @Test
    @Timeout(30)
    public void testOneAbortWithTail() throws Exception {
        probeAbort(false, 1);
    }

    @Test
    @Timeout(30)
    public void testSavepointBitStaysFalseWhenAutoTagDisabled() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String commitUser = UUID.randomUUID().toString();
        List<OperatorEvent> events = new ArrayList<>();

        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(table, commitUser, events::add, /* autoTagForSavepoint */ false);
        TypeSerializer<Committable> committableSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        harness.setup(committableSerializer);
        harness.open();
        CoordinatorCommittingRowDataStoreWriteOperator operator =
                (CoordinatorCommittingRowDataStoreWriteOperator) harness.getOperator();

        // Even on a savepoint, a writer without auto-tag enabled must not flag a tag intent: the
        // checkpoint is a savepoint, but no savepoint tag should be created for it.
        harness.processElement(GenericRow.of(1, 10L), 1);
        harness.prepareSnapshotPreBarrier(1);
        harness.snapshotWithLocalState(
                1, 10, SavepointType.savepoint(SavepointFormatType.CANONICAL));
        assertThat(
                        ((CommittableEvent) events.get(0))
                                .deserialize(COMMITTABLES_SERIALIZER)
                                .shouldCreateSavepointTag())
                .isFalse();
        assertThat(operator.getPendingCommittables().get(1L).shouldCreateSavepointTag()).isFalse();

        harness.close();
    }

    @Test
    @Timeout(30)
    public void testTwoAbortsWithTail() throws Exception {
        probeAbort(false, 2);
    }

    @Test
    @Timeout(30)
    public void testSavepointBitReplayedOnRestore() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String commitUser = UUID.randomUUID().toString();
        TypeSerializer<Committable> committableSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());

        // session 1: take a savepoint that is never notified complete, then crash.
        List<OperatorEvent> firstEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> firstHarness =
                createHarness(table, commitUser, firstEvents::add);
        firstHarness.setup(committableSerializer);
        firstHarness.open();
        firstHarness.processElement(GenericRow.of(1, 10L), 1);
        firstHarness.prepareSnapshotPreBarrier(1);
        OperatorSubtaskState snapshot =
                firstHarness
                        .snapshotWithLocalState(
                                1, 10, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                        .getJobManagerOwnedState();
        firstHarness.close();

        // session 2: restore replays the persisted savepoint bit in the RestoredCommittableEvent.
        List<OperatorEvent> restoredEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> secondHarness =
                createHarness(table, commitUser, restoredEvents::add);
        secondHarness.setup(committableSerializer);
        restoreWithCheckpointId(secondHarness, snapshot, 1L);
        secondHarness.open();

        assertThat(restoredEvents).hasSize(1);
        RestoredCommittableEvent restoredEvent = (RestoredCommittableEvent) restoredEvents.get(0);
        List<CheckpointCommittables> entries = restoredEvent.deserialize(COMMITTABLES_SERIALIZER);
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).checkpointId()).isEqualTo(1L);
        assertThat(entries.get(0).shouldCreateSavepointTag()).isTrue();

        secondHarness.close();
    }

    @Test
    @Timeout(30)
    public void testOneAbortEmpty() throws Exception {
        probeAbort(true, 1);
    }

    @Test
    @Timeout(30)
    public void testTwoAbortsEmpty() throws Exception {
        probeAbort(true, 2);
    }

    private void probeAbort(boolean empty, int aborts) throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String user = UUID.randomUUID().toString();
        List<OperatorEvent> events = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(table, user, events::add);
        harness.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
        harness.open();
        CoordinatorCommittingRowDataStoreWriteOperator writer =
                (CoordinatorCommittingRowDataStoreWriteOperator) harness.getOperator();
        if (!empty) {
            harness.processElement(GenericRow.of(1, 1L), 1);
        }
        harness.endInput();
        assertThat(events).isEmpty();
        OperatorSubtaskState snapshot = null;
        long success = 101 + aborts;
        for (long id = 101; id <= success; id++) {
            harness.prepareSnapshotPreBarrier(id);
            snapshot = harness.snapshot(id, id);
            assertThat(writer.getPendingCommittables()).containsKey(101L);
            assertThat(writer.getPendingCommittables().get(101L).committables())
                    .hasSize(empty ? 0 : 1);
            if (id < success) {
                writer.notifyCheckpointAborted(id);
                assertThat(writer.getPendingCommittables()).containsKey(101L);
            }
        }
        List<OperatorEvent> restoredEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> restored =
                createHarness(table, user, restoredEvents::add);
        restored.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
        restoreWithCheckpointId(restored, snapshot, success);
        restored.open();
        List<CheckpointCommittables> entries =
                ((RestoredCommittableEvent) restoredEvents.get(0))
                        .deserialize(COMMITTABLES_SERIALIZER);
        assertThat(entries).hasSize(aborts + 1);
        assertThat(entries.get(0).checkpointId()).isEqualTo(101L);
        assertThat(entries.get(0).terminal()).isTrue();
        assertThat(entries.get(0).committables()).hasSize(empty ? 0 : 1);
        CommittingWriteOperatorCoordinator coordinator =
                new CommittingWriteOperatorCoordinator(
                        new TestingContext(),
                        context ->
                                new StoreCommitter(
                                        table, table.newCommit(context.commitUser()), context),
                        true,
                        user,
                        null,
                        null);
        coordinator.start();
        coordinator.waitProcessAllActions();
        for (OperatorEvent event : events) {
            coordinator.handleEventFromOperator(0, 0, event);
        }
        coordinator.notifyCheckpointComplete(success);
        coordinator.waitProcessAllActions();
        // A release for the successful boundary also covers inherited aborted tails.
        writer.handleOperatorEvent(new TerminalWriterReleaseEvent(0, 0, success));
        harness.notifyOfCompletedCheckpoint(success);
        assertThat(writer.getPendingCommittables()).isEmpty();
        if (!empty) {
            assertResults(table, "1, 1");
            assertThat(table.snapshotManager().latestSnapshot().commitIdentifier()).isEqualTo(101L);
        } else {
            assertThat(table.snapshotManager().latestSnapshot()).isNull();
        }
        harness.endInput();
        harness.prepareSnapshotPreBarrier(success + 1);
        harness.snapshot(success + 1, 1000L);
        CheckpointCommittables afterRetirement =
                ((CommittableEvent) events.get(events.size() - 1))
                        .deserialize(COMMITTABLES_SERIALIZER);
        assertThat(afterRetirement.terminal()).isTrue();
        assertThat(afterRetirement.committables()).isEmpty();
        restored.endInput();
        restored.endInput();
        restored.prepareSnapshotPreBarrier(success + 1);
        restored.snapshot(success + 1, 1000L);
        CheckpointCommittables later =
                ((CommittableEvent) restoredEvents.get(1)).deserialize(COMMITTABLES_SERIALIZER);
        assertThat(later.terminal()).isTrue();
        assertThat(later.committables()).isEmpty();
        restored.close();
        harness.close();
        coordinator.close();
    }

    @Test
    @Timeout(30)
    public void testTerminalMarkerRequiresSuccessfulSealingAndNeverPreparesAgain()
            throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        List<OperatorEvent> events = new ArrayList<>();
        try (OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(table, UUID.randomUUID().toString(), events::add)) {
            harness.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
            harness.open();
            CoordinatorCommittingRowDataStoreWriteOperator writer =
                    (CoordinatorCommittingRowDataStoreWriteOperator) harness.getOperator();
            StoreSinkWrite write = spy(writer.getWrite());
            writer.write = write;
            harness.processElement(GenericRow.of(1, 1L), 1);
            harness.endInput();
            doThrow(new IOException("seal failed")).when(write).prepareCommit(true, 2L);
            assertThatThrownBy(() -> harness.prepareSnapshotPreBarrier(2L))
                    .hasMessageContaining("seal failed");
            assertThat(events).isEmpty();
            assertThat(writer.getPendingCommittables()).isEmpty();
            harness.prepareSnapshotPreBarrier(3L);
            harness.snapshot(3L, 30L);
            CheckpointCommittables tail =
                    ((CommittableEvent) events.get(0)).deserialize(COMMITTABLES_SERIALIZER);
            assertThat(tail.terminal()).isTrue();
            assertThat(tail.committables()).hasSize(1);
            verify(write).prepareCommit(true, 3L);
            clearInvocations(write);
            harness.endInput();
            harness.prepareSnapshotPreBarrier(4L);
            harness.snapshot(4L, 40L);
            verify(write, never()).prepareCommit(anyBoolean(), anyLong());
            assertThatThrownBy(() -> harness.processElement(GenericRow.of(2, 2L), 2))
                    .hasMessageContaining("after EndInput");
        }
    }

    @Test
    @Timeout(30)
    public void testReleaseBeforeWaitAndDuplicateRelease() throws Exception {
        try (OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                sealedWriterHarness()) {
            CoordinatorCommittingRowDataStoreWriteOperator writer =
                    (CoordinatorCommittingRowDataStoreWriteOperator) harness.getOperator();
            writer.handleOperatorEvent(new TerminalWriterReleaseEvent(0, 0, 5));
            writer.handleOperatorEvent(new TerminalWriterReleaseEvent(0, 0, 5));
            harness.getTaskMailbox()
                    .put(
                            new Mail(
                                    () -> {
                                        throw new AssertionError(
                                                "Matching release must avoid yielding");
                                    },
                                    -1,
                                    "unexpected yield"));
            writer.notifyCheckpointComplete(5);
        }
    }

    @Test
    @Timeout(30)
    public void testReleaseDuringTaskMailboxWaitRejectsStaleSignalsAndReentry() throws Exception {
        try (OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                sealedWriterHarness()) {
            CoordinatorCommittingRowDataStoreWriteOperator writer =
                    (CoordinatorCommittingRowDataStoreWriteOperator) harness.getOperator();
            writer.handleOperatorEvent(new TerminalWriterReleaseEvent(0, 0, 4));
            writer.handleOperatorEvent(new TerminalWriterReleaseEvent(1, 0, 5));
            writer.handleOperatorEvent(new TerminalWriterReleaseEvent(0, 1, 5));
            AtomicInteger processed = new AtomicInteger();
            harness.getTaskMailbox()
                    .put(
                            new Mail(
                                    () -> {
                                        writer.notifyCheckpointComplete(6);
                                        // A recursive wait would consume the following release
                                        // before
                                        // returning here.
                                        assertThat(processed.get()).isZero();
                                        processed.incrementAndGet();
                                    },
                                    -1,
                                    "nested checkpoint completion"));
            harness.getTaskMailbox()
                    .put(
                            new Mail(
                                    () -> {
                                        assertThat(processed.get()).isEqualTo(1);
                                        writer.handleOperatorEvent(
                                                new TerminalWriterReleaseEvent(0, 0, 5));
                                        processed.incrementAndGet();
                                    },
                                    -1,
                                    "commit release"));
            writer.notifyCheckpointComplete(5);
            assertThat(processed.get()).isEqualTo(2);
        }
    }

    @Test
    @Timeout(30)
    public void testMailboxFailureExitsReleaseWaitWithoutRelease() throws Exception {
        try (OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                sealedWriterHarness()) {
            CoordinatorCommittingRowDataStoreWriteOperator writer =
                    (CoordinatorCommittingRowDataStoreWriteOperator) harness.getOperator();
            harness.getTaskMailbox()
                    .put(
                            new Mail(
                                    () -> {
                                        throw new FlinkRuntimeException("task cancellation");
                                    },
                                    -1,
                                    "task failure"));
            assertThatThrownBy(() -> writer.notifyCheckpointComplete(5))
                    .hasStackTraceContaining("task cancellation");
            // Failure must not fabricate an release or leave the re-entry guard latched.
            AtomicInteger released = new AtomicInteger();
            harness.getTaskMailbox()
                    .put(
                            new Mail(
                                    () -> {
                                        released.incrementAndGet();
                                        writer.handleOperatorEvent(
                                                new TerminalWriterReleaseEvent(0, 0, 5));
                                    },
                                    -1,
                                    "later release"));
            writer.notifyCheckpointComplete(5);
            assertThat(released.get()).isEqualTo(1);
        }
    }

    @Test
    @Timeout(30)
    public void testReleaseReplayCoversLaterRestoredEmptyMarker() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        String user = UUID.randomUUID().toString();
        OperatorSubtaskState state;
        try (OneInputStreamOperatorTestHarness<InternalRow, Committable> original =
                createHarness(table, user, event -> {})) {
            original.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
            original.open();
            original.endInput();
            original.prepareSnapshotPreBarrier(1);
            original.snapshot(1, 10);
            CoordinatorCommittingRowDataStoreWriteOperator writer =
                    (CoordinatorCommittingRowDataStoreWriteOperator) original.getOperator();
            writer.handleOperatorEvent(new TerminalWriterReleaseEvent(0, 0, 1));
            original.notifyOfCompletedCheckpoint(1);
            original.prepareSnapshotPreBarrier(5);
            state = original.snapshot(5, 50);
        }
        try (OneInputStreamOperatorTestHarness<InternalRow, Committable> restored =
                createHarness(table, user, event -> {})) {
            restored.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
            restoreWithCheckpointId(restored, state, 5);
            restored.open();
            CoordinatorCommittingRowDataStoreWriteOperator writer =
                    (CoordinatorCommittingRowDataStoreWriteOperator) restored.getOperator();
            writer.handleOperatorEvent(new TerminalWriterReleaseEvent(0, 0, 1));
            AtomicInteger replayed = new AtomicInteger();
            restored.getTaskMailbox()
                    .put(
                            new Mail(
                                    () -> {
                                        replayed.incrementAndGet();
                                        writer.handleOperatorEvent(
                                                new TerminalWriterReleaseEvent(0, 0, 5));
                                    },
                                    -1,
                                    "restored boundary release"));
            writer.notifyCheckpointComplete(6);
            assertThat(replayed.get()).isEqualTo(1);
        }
    }

    private OneInputStreamOperatorTestHarness<InternalRow, Committable> sealedWriterHarness()
            throws Exception {
        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(
                        createUnawareBucketTable(), UUID.randomUUID().toString(), event -> {});
        harness.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
        harness.open();
        CoordinatorCommittingRowDataStoreWriteOperator writer =
                (CoordinatorCommittingRowDataStoreWriteOperator) harness.getOperator();
        // A release received before sealing must not become a release for a future tail.
        writer.handleOperatorEvent(new TerminalWriterReleaseEvent(0, 0, 10));
        harness.endInput();
        harness.prepareSnapshotPreBarrier(5);
        harness.snapshot(5, 50);
        return harness;
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
        return createHarness(table, commitUser, gateway, /* autoTagForSavepoint */ true);
    }

    private OneInputStreamOperatorTestHarness<InternalRow, Committable> createHarness(
            FileStoreTable table,
            String commitUser,
            OperatorEventGateway gateway,
            boolean autoTagForSavepoint)
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
                                        gateway,
                                        autoTagForSavepoint);
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
