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
import org.apache.paimon.flink.sink.coordinator.CommittableEvent;
import org.apache.paimon.flink.sink.coordinator.CommittingWriteOperatorCoordinator;
import org.apache.paimon.flink.sink.coordinator.WriterCommittables;
import org.apache.paimon.flink.utils.InternalRowTypeSerializer;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.FlinkRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CoordinatorCommittingRowDataStoreWriteOperator}. */
public class CoordinatorCommittingRowDataStoreWriteOperatorTest extends CommitterOperatorTestBase {

    private static final ListSerializer<Committable> COMMITTABLE_LIST_SERIALIZER =
            CommittableEvent.createCommittableListSerializer();

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
                        false);
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
        assertThat(event.isRestoring()).isFalse();
        assertThat(
                        WriterCommittables.from(event, COMMITTABLE_LIST_SERIALIZER)
                                .getCommittablesPerCheckpoint()
                                .get(1L))
                .hasSize(1);

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
        assertEventCheckpointAndRestoring(events.get(events.size() - 1), 1L, false);
        assertThat(operator.getPendingCommittables()).containsKey(1L);

        // cp2: another writing checkpoint, also aborted; cp1 buffer must still be retained
        harness.processElement(GenericRow.of(2, 20L), 2);
        harness.prepareSnapshotPreBarrier(2);
        harness.snapshot(2, 20);
        assertEventCheckpointAndRestoring(events.get(events.size() - 1), 2L, false);
        assertThat(operator.getPendingCommittables()).containsKeys(1L, 2L);

        // cp3 completes -> headMap(3, true) clears every buffered checkpoint
        harness.processElement(GenericRow.of(3, 30L), 3);
        harness.prepareSnapshotPreBarrier(3);
        harness.snapshot(3, 30);
        assertEventCheckpointAndRestoring(events.get(events.size() - 1), 3L, false);
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

        // session 2: restore from snapshot, expect a single isRestoring=true event carrying the
        // buffered committable, even though the writer has not produced anything new yet
        List<OperatorEvent> restoredEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> secondHarness =
                createHarness(table, commitUser, restoredEvents::add);
        secondHarness.setup(committableSerializer);
        secondHarness.initializeState(snapshot);
        secondHarness.open();

        assertThat(restoredEvents).hasSize(1);
        CommittableEvent restoredEvent = (CommittableEvent) restoredEvents.get(0);
        assertThat(restoredEvent.isRestoring()).isTrue();
        assertThat(decodeCommittables(restoredEvent)).hasSize(1);

        // a fresh snapshot must persist nothing (the buffer was cleared on restore)
        CoordinatorCommittingRowDataStoreWriteOperator operator =
                (CoordinatorCommittingRowDataStoreWriteOperator) secondHarness.getOperator();
        secondHarness.prepareSnapshotPreBarrier(2);
        secondHarness.snapshot(2, 20);
        assertThat(operator.getPendingCommittables()).isEmpty();

        secondHarness.close();
    }

    @Test
    public void testEmptyRestoreStillSendsIsRestoringSignal() throws Exception {
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

        // session 2: restore. Buffer is empty, but the operator must still notify the coordinator
        // so the coordinator can drive its own restore-alignment.
        List<OperatorEvent> restoredEvents = new ArrayList<>();
        OneInputStreamOperatorTestHarness<InternalRow, Committable> secondHarness =
                createHarness(table, commitUser, restoredEvents::add);
        secondHarness.setup(committableSerializer);
        secondHarness.initializeState(snapshot);
        secondHarness.open();

        assertThat(restoredEvents).hasSize(1);
        CommittableEvent restoredEvent = (CommittableEvent) restoredEvents.get(0);
        assertThat(restoredEvent.isRestoring()).isTrue();
        assertThat(decodeCommittables(restoredEvent)).isEmpty();

        secondHarness.close();
    }

    private void assertEventCheckpointAndRestoring(
            OperatorEvent event, long expectedCheckpointId, boolean expectedRestoring) {
        CommittableEvent committableEvent = (CommittableEvent) event;
        assertThat(committableEvent.getCheckpointId()).isEqualTo(expectedCheckpointId);
        assertThat(committableEvent.isRestoring()).isEqualTo(expectedRestoring);
    }

    private List<Committable> decodeCommittables(CommittableEvent event) throws Exception {
        return COMMITTABLE_LIST_SERIALIZER.deserialize(
                new DataInputDeserializer(event.getSerialized()));
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
