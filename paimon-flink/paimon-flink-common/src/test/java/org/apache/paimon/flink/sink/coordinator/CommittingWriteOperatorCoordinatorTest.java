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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.CommitterOperatorTestBase;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sink.state.CoordinatorState;
import org.apache.paimon.flink.sink.state.CoordinatorStateSerializer;
import org.apache.paimon.flink.sink.state.MemoryBackendStateStore;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;

import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link CommittingWriteOperatorCoordinator}. */
public class CommittingWriteOperatorCoordinatorTest extends CommitterOperatorTestBase {

    private static final ListSerializer<Committable> SERIALIZER =
            CommittableEvent.createCommittableListSerializer();

    private String commitUser;
    private volatile Throwable failureCause;

    @BeforeEach
    public void before() {
        super.before();
        commitUser = UUID.randomUUID().toString();
        failureCause = null;
    }

    @AfterEach
    public void checkNoFailure() {
        assertThat(failureCause).isNull();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testCommitSingleSubtask() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        TestingContext context = new TestingContext(new OperatorID(), 1);
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, context, false);
        coordinator.start();
        coordinator.waitProcessAllActions();
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);

        coordinator.handleEventFromOperator(0, 0, event(false, committable(table, 1, 1)));
        coordinator.notifyCheckpointComplete(1);
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1");
        coordinator.close();
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.CLOSED);
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testCommitFanInFromMultipleSubtasks() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        TestingContext context = new TestingContext(new OperatorID(), 2);
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, context, false);
        coordinator.start();
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(0, 0, event(false, committable(table, 1, 1)));
        coordinator.handleEventFromOperator(1, 0, event(false, committable(table, 1, 2)));
        coordinator.notifyCheckpointComplete(1);
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1", "2, 2");
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testRestoringAlignsBeforeRunning() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        TestingContext context = new TestingContext(new OperatorID(), 2);

        // first incarnation commits checkpoint 1 and captures the coordinator state
        CommittingWriteOperatorCoordinator first = createCoordinator(table, context, false);
        first.start();
        first.waitProcessAllActions();
        first.handleEventFromOperator(0, 0, event(false, committable(table, 1, 1)));
        first.handleEventFromOperator(1, 0, event(false, committable(table, 1, 2)));
        CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();
        first.checkpointCoordinator(1, checkpoint);
        first.notifyCheckpointComplete(1);
        first.waitProcessAllActions();
        byte[] state = checkpoint.get();
        first.close();
        assertResults(table, "1, 1", "2, 2");

        // second incarnation restores and stays RESTORING until both subtasks re-emit
        CommittingWriteOperatorCoordinator second = createCoordinator(table, context, false);
        second.resetToCheckpoint(1, state);
        assertThat(second.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RESTORING);
        second.start();
        second.waitProcessAllActions();
        assertThat(second.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RESTORING);
        assertThat(second.getCommitUser()).isEqualTo(commitUser);

        second.handleEventFromOperator(0, 1, event(true, committable(table, 1, 3)));
        second.waitProcessAllActions();
        assertThat(second.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RESTORING);

        second.handleEventFromOperator(1, 1, event(true, committable(table, 1, 4)));
        second.waitProcessAllActions();
        assertThat(second.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);

        // abandon path: restoring committables are dropped, not recommitted
        assertResults(table, "1, 1", "2, 2");
        second.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testSnapshotLostWhenFailed() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        TestingContext context = new TestingContext(new OperatorID(), 1);

        // first incarnation: cp1 fully committed, cp2 snapshotted but never notified
        CommittingWriteOperatorCoordinator first = createCoordinator(table, context, false);
        first.start();
        first.waitProcessAllActions();
        first.handleEventFromOperator(0, 0, event(false, committable(table, 1, 1)));
        first.notifyCheckpointComplete(1L);
        first.waitProcessAllActions();
        assertResults(table, "1, 1");

        first.handleEventFromOperator(0, 0, event(false, committable(table, 2, 2)));
        CompletableFuture<byte[]> cp2State = new CompletableFuture<>();
        first.checkpointCoordinator(2L, cp2State);
        first.waitProcessAllActions();
        byte[] state = cp2State.get();
        first.close();
        // cp2 was never notified — only cp1 is in the table
        assertResults(table, "1, 1");

        // second incarnation: restore from cp2 state, replay the cp2 restoring event. abandon
        // mode drops it; the snapshot from cp1 stays untouched.
        CommittingWriteOperatorCoordinator second = createCoordinator(table, context, false);
        second.resetToCheckpoint(2L, state);
        second.start();
        second.waitProcessAllActions();
        second.handleEventFromOperator(0, 0, event(true, committable(table, 2, 2)));
        second.waitProcessAllActions();
        assertThat(second.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        assertResults(table, "1, 1");

        // a fresh checkpoint after recovery commits normally
        second.handleEventFromOperator(0, 0, event(false, committable(table, 3, 3)));
        second.notifyCheckpointComplete(3L);
        second.waitProcessAllActions();
        assertResults(table, "1, 1", "3, 3");
        second.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testRejectCheckpointWhileRestoring() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        TestingContext context = new TestingContext(new OperatorID(), 1);
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, context, false);
        coordinator.resetToCheckpoint(2, emptyState());
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RESTORING);
        coordinator.start();
        coordinator.waitProcessAllActions();

        CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();
        coordinator.checkpointCoordinator(3, checkpoint);
        coordinator.waitProcessAllActions();
        assertThat(checkpoint.isCompletedExceptionally()).isTrue();

        coordinator.handleEventFromOperator(0, 0, event(true, committable(table, 2, 1)));
        coordinator.waitProcessAllActions();
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testFailIntentionallyAfterRestoring() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        TestingContext context = new TestingContext(new OperatorID(), 1);

        // capture coordinator state without committing checkpoint 1
        CommittingWriteOperatorCoordinator first = createCoordinator(table, context, true);
        first.start();
        first.handleEventFromOperator(0, 0, event(false, committable(table, 1, 1)));
        CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();
        first.checkpointCoordinator(1, checkpoint);
        first.waitProcessAllActions();
        byte[] state = checkpoint.get();
        first.close();
        // checkpoint 1 was never committed
        assertThat(table.latestSnapshot()).isNotPresent();

        // restore with failoverAfterRecovery: the restored committables are recommitted and an
        // intentional failure is raised to reinitialize all writers
        CommittingWriteOperatorCoordinator second = createCoordinator(table, context, true);
        second.resetToCheckpoint(1, state);
        second.start();
        second.waitProcessAllActions();
        second.handleEventFromOperator(0, 0, event(true, committable(table, 1, 1)));
        second.waitProcessAllActions();

        assertThat(failureCause).isInstanceOf(RuntimeException.class);
        assertThat(failureCause).hasMessageContaining("intentionally thrown");
        assertResults(table, "1, 1");
        failureCause = null;
        second.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testCheckpointAbort() throws Exception {
        // accumulate committables across many checkpoints without notification, then notify only
        // the last one. the coordinator must drain all pending checkpoints in a single commit.
        FileStoreTable table = createUnawareBucketTable();
        TestingContext context = new TestingContext(new OperatorID(), 1);
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, context, false);
        coordinator.start();
        coordinator.waitProcessAllActions();

        long lastCp = 10L;
        List<String> expected = new ArrayList<>();
        for (long cp = 1; cp <= lastCp; cp++) {
            int value = (int) cp;
            coordinator.handleEventFromOperator(0, 0, event(false, committable(table, cp, value)));
            expected.add(value + ", " + value);
        }
        coordinator.notifyCheckpointComplete(lastCp);
        coordinator.waitProcessAllActions();

        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.commitIdentifier()).isEqualTo(lastCp);
        Collections.sort(expected);
        assertResults(table, expected.toArray(new String[0]));
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testEmptyCommit() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        TestingContext context = new TestingContext(new OperatorID(), 1);
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, context, false);
        coordinator.start();
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(
                0, 0, CommittableEvent.create(1L, false, Collections.emptyList(), SERIALIZER));
        coordinator.notifyCheckpointComplete(1L);
        coordinator.waitProcessAllActions();

        assertThat(table.snapshotManager().latestSnapshot()).isNull();
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testForceCreateSnapshotCommit() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(CoreOptions.BUCKET, -1);
                            options.remove("bucket-key");
                            options.set(CoreOptions.COMMIT_FORCE_CREATE_SNAPSHOT, true);
                        });
        TestingContext context = new TestingContext(new OperatorID(), 1);
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, context, false);
        coordinator.start();
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(
                0, 0, CommittableEvent.create(1L, false, Collections.emptyList(), SERIALIZER));
        coordinator.notifyCheckpointComplete(1L);
        coordinator.waitProcessAllActions();

        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.commitIdentifier()).isEqualTo(1L);
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testPollManifestCommittablesForCheckpoint() throws Exception {
        String partitionKey = "a";
        int partitionValue = 100;
        long normalValue = 0L;
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(CoreOptions.BUCKET, -1);
                            options.remove("bucket-key");
                        },
                        Collections.singletonList(partitionKey));
        long checkpointId1 = 1024L;
        long checkpointId2 = 1025L;
        long checkpointId3 = 1027L;
        long watermark = System.currentTimeMillis();
        WriterCommittables[] writerCommittables = new WriterCommittables[3];
        // cp1, cp3 in subtask-0
        writerCommittables[0] =
                new WriterCommittables(
                        checkpointId1,
                        Collections.singletonList(
                                committable(table, checkpointId1, partitionValue, normalValue++)));
        writerCommittables[0].mergeWith(
                new WriterCommittables(checkpointId2, Collections.emptyList()));
        writerCommittables[0].mergeWith(
                new WriterCommittables(
                        checkpointId3,
                        Collections.singletonList(
                                committable(table, checkpointId3, partitionValue, normalValue++))));
        // cp1, cp2 in subtask-1
        writerCommittables[1] =
                new WriterCommittables(
                        checkpointId1,
                        Collections.singletonList(
                                committable(table, checkpointId1, partitionValue, normalValue++)));
        writerCommittables[1].mergeWith(
                new WriterCommittables(
                        checkpointId2,
                        Collections.singletonList(
                                committable(table, checkpointId2, partitionValue, normalValue++))));
        writerCommittables[1].mergeWith(
                new WriterCommittables(checkpointId3, Collections.emptyList()));
        // cp2, cp3 in subtask-2
        writerCommittables[2] = new WriterCommittables(checkpointId1, Collections.emptyList());
        writerCommittables[2].mergeWith(
                new WriterCommittables(
                        checkpointId2,
                        Collections.singletonList(
                                committable(table, checkpointId2, partitionValue, normalValue++))));
        writerCommittables[2].mergeWith(
                new WriterCommittables(
                        checkpointId3,
                        Collections.singletonList(
                                committable(table, checkpointId3, partitionValue, normalValue++))));

        StreamTableCommit commit = table.newCommit(commitUser);
        StoreCommitter committer =
                new StoreCommitter(
                        table, commit, Committer.createContext("", null, true, false, null, 1, 0));

        NavigableMap<Long, ManifestCommittable> result =
                CommittingWriteOperatorCoordinator.pollManifestCommittablesForCheckpoint(
                        checkpointId2, writerCommittables, committer, watermark);

        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, partitionValue);
        writer.complete();
        // verify result
        assertThat(result.size()).isEqualTo(2);
        // verify increasing order
        List<ManifestCommittable> manifestCommittables = new ArrayList<>(result.values());
        assertThat(manifestCommittables.get(0).identifier()).isEqualTo(checkpointId1);
        assertThat(manifestCommittables.get(1).identifier()).isEqualTo(checkpointId2);

        assertThat(result.get(checkpointId1)).isNotNull();
        assertThat(result.get(checkpointId1).identifier()).isEqualTo(checkpointId1);
        assertThat(result.get(checkpointId1).watermark()).isEqualTo(watermark);
        assertThat(result.get(checkpointId1).fileCommittables().size()).isEqualTo(2);
        assertThat(result.get(checkpointId1).fileCommittables().get(0).partition())
                .isEqualTo(partition);
        assertThat(result.get(checkpointId1).fileCommittables().get(1).partition())
                .isEqualTo(partition);

        assertThat(result.get(checkpointId2)).isNotNull();
        assertThat(result.get(checkpointId2).identifier()).isEqualTo(checkpointId2);
        assertThat(result.get(checkpointId2).watermark()).isEqualTo(watermark);
        assertThat(result.get(checkpointId2).fileCommittables().size()).isEqualTo(2);
        assertThat(result.get(checkpointId2).fileCommittables().get(0).partition())
                .isEqualTo(partition);
        assertThat(result.get(checkpointId2).fileCommittables().get(1).partition())
                .isEqualTo(partition);

        assertThat(result.get(checkpointId3)).isNull();

        // verify remaining subtask committables
        assertThat(writerCommittables[0].getCommittablesPerCheckpoint().size()).isEqualTo(1);
        assertThat(writerCommittables[0].getCommittablesPerCheckpoint().get(checkpointId3))
                .isNotNull();
        assertThat(writerCommittables[0].getCommittablesPerCheckpoint().get(checkpointId3).size())
                .isEqualTo(1);
        assertThat(writerCommittables[1].getCommittablesPerCheckpoint().isEmpty()).isTrue();
        assertThat(writerCommittables[2].getCommittablesPerCheckpoint().size()).isEqualTo(1);
        assertThat(writerCommittables[2].getCommittablesPerCheckpoint().get(checkpointId3))
                .isNotNull();
        assertThat(writerCommittables[2].getCommittablesPerCheckpoint().get(checkpointId3).size())
                .isEqualTo(1);

        committer.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testPartialFailoverWithoutRestoring() throws Exception {
        // non-restore -> trigger checkpoint -> partial failover -> checkpoint abort -> re-trigger
        // checkpoint -> checkpoint complete
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(CoreOptions.BUCKET, -1);
                            options.remove("bucket-key");
                            options.set(CoreOptions.MANIFEST_MERGE_MIN_COUNT.key(), "0");
                        },
                        Collections.singletonList("a"));
        TestingContext context = new TestingContext(new OperatorID(), 2);
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, context, false);
        // 1. start with non-restoring
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.CREATED);
        coordinator.start();
        coordinator.waitProcessAllActions();
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        // 2. trigger checkpoint 1
        long checkpointId = 1L;
        // checkpoint coordinator before task
        {
            CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
            coordinator.checkpointCoordinator(checkpointId, checkpointFuture);
            coordinator.waitProcessAllActions();
            assertThat(coordinator.getCurrentState())
                    .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
            assertThat(checkpointFuture.isDone()).isTrue();
            assertThat(checkpointFuture.isCompletedExceptionally()).isFalse();
        }
        // write data and checkpoint task-0
        coordinator.handleEventFromOperator(
                0,
                0,
                eventOf(
                        checkpointId,
                        false,
                        committables(
                                table, checkpointId, GenericRow.of(1, 2L), GenericRow.of(1, 3L))));
        // write data and checkpoint task-1
        coordinator.handleEventFromOperator(
                1,
                0,
                eventOf(
                        checkpointId,
                        false,
                        committables(
                                table, checkpointId, GenericRow.of(1, 4L), GenericRow.of(1, 5L))));
        // 3. partial failover, fail task-0
        coordinator.executionAttemptFailed(0, 0, new Exception("Fail subtask 0 as expected"));
        coordinator.executionAttemptReady(0, 1, new MockSubtaskGateway());
        coordinator.subtaskReset(0, -1);
        // 4. checkpoint 1 abort
        coordinator.notifyCheckpointAborted(checkpointId);
        coordinator.waitProcessAllActions();
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);

        // 5. re-trigger checkpoint
        checkpointId++;
        {
            CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
            coordinator.checkpointCoordinator(checkpointId, checkpointFuture);
            coordinator.waitProcessAllActions();
            assertThat(checkpointFuture.isDone()).isTrue();
            assertThat(checkpointFuture.isCompletedExceptionally()).isFalse();
            assertThat(coordinator.getCurrentState())
                    .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        }
        // rewrite some data and checkpoint task-0
        coordinator.handleEventFromOperator(
                0,
                1,
                eventOf(
                        checkpointId,
                        false,
                        committables(
                                table,
                                checkpointId,
                                GenericRow.of(1, 2L),
                                GenericRow.of(1, 3L),
                                GenericRow.of(1, 6L))));
        // write empty data and checkpoint task-1
        coordinator.handleEventFromOperator(
                1,
                0,
                CommittableEvent.create(checkpointId, false, Collections.emptyList(), SERIALIZER));
        // notify cp complete
        coordinator.notifyCheckpointComplete(checkpointId);
        coordinator.waitProcessAllActions();
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        assertResults(table, "1, 2", "1, 3", "1, 4", "1, 5", "1, 6");
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testPartialFailoverWithRestoring() throws Exception {
        // non-restore -> trigger checkpoint -> checkpoint complete -> partial failover ->
        // restore subtask -> trigger checkpoint -> checkpoint complete
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(CoreOptions.BUCKET, -1);
                            options.remove("bucket-key");
                            // set manifest merge min count to 0 to find repeated immediately
                            options.set(CoreOptions.MANIFEST_MERGE_MIN_COUNT.key(), "0");
                        },
                        Collections.singletonList("a"));
        TestingContext context = new TestingContext(new OperatorID(), 2);
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, context, false);
        // 1. start with non-restoring
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.CREATED);
        coordinator.start();
        coordinator.waitProcessAllActions();
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        // 2. trigger checkpoint 1
        long checkpointId = 1L;
        // checkpoint coordinator before task
        {
            CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
            coordinator.checkpointCoordinator(checkpointId, checkpointFuture);
            coordinator.waitProcessAllActions();
            assertThat(checkpointFuture.isDone()).isTrue();
            assertThat(checkpointFuture.isCompletedExceptionally()).isFalse();
            assertThat(coordinator.getCurrentState())
                    .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        }
        // write data and checkpoint task-0
        List<Committable> subtask0Committables =
                committables(table, checkpointId, GenericRow.of(1, 2L), GenericRow.of(1, 3L));
        coordinator.handleEventFromOperator(
                0, 0, eventOf(checkpointId, false, subtask0Committables));
        CommittableEvent restoreEvent =
                CommittableEvent.create(checkpointId, true, subtask0Committables, SERIALIZER);
        // write data and checkpoint task-1
        coordinator.handleEventFromOperator(
                1,
                0,
                eventOf(
                        checkpointId,
                        false,
                        committables(
                                table, checkpointId, GenericRow.of(1, 4L), GenericRow.of(1, 5L))));
        // 3. checkpoint complete
        coordinator.notifyCheckpointComplete(checkpointId);
        coordinator.waitProcessAllActions();
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        // 4. partial failover, fail task-0
        coordinator.executionAttemptFailed(0, 0, new Exception("Fail subtask 0 as expected"));
        coordinator.executionAttemptReady(0, 1, new MockSubtaskGateway());
        coordinator.subtaskReset(0, checkpointId);
        coordinator.waitProcessAllActions();
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);

        // 5. restore subtask-0
        coordinator.handleEventFromOperator(0, 1, restoreEvent);

        // 6. re-trigger checkpoint
        checkpointId++;
        {
            CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
            coordinator.checkpointCoordinator(checkpointId, checkpointFuture);
            coordinator.waitProcessAllActions();
            assertThat(checkpointFuture.isDone()).isTrue();
            assertThat(checkpointFuture.isCompletedExceptionally()).isFalse();
        }
        // rewrite some data and checkpoint task-0
        coordinator.handleEventFromOperator(
                0,
                1,
                eventOf(
                        checkpointId,
                        false,
                        committables(
                                table,
                                checkpointId,
                                GenericRow.of(1, 6L),
                                GenericRow.of(1, 7L),
                                GenericRow.of(1, 8L))));
        // write empty data and checkpoint task-1
        coordinator.handleEventFromOperator(
                1,
                0,
                CommittableEvent.create(checkpointId, false, Collections.emptyList(), SERIALIZER));
        // 7. notify cp complete
        coordinator.notifyCheckpointComplete(checkpointId);
        coordinator.waitProcessAllActions();
        assertThat(coordinator.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        assertResults(table, "1, 2", "1, 3", "1, 4", "1, 5", "1, 6", "1, 7", "1, 8");
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testRestoreEmptyMarkDoneState() throws Exception {
        // mark-done introduces a partition listener that is initialized lazily from coordinator
        // state. restoring an empty mark-done state must not crash the coordinator.
        Map<String, String> markDoneOption = new HashMap<>();
        markDoneOption.put(FlinkConnectorOptions.PARTITION_IDLE_TIME_TO_DONE.key(), "1h");

        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(CoreOptions.BUCKET, -1);
                            options.remove("bucket-key");
                        },
                        Collections.singletonList("a"));
        TestingContext context = new TestingContext(new OperatorID(), 1);

        // 1. capture state from a coordinator without mark-done enabled
        CommittingWriteOperatorCoordinator first = createCoordinator(table, context, false);
        first.start();
        first.waitProcessAllActions();
        first.handleEventFromOperator(0, 0, event(false, committable(table, 1, 1)));
        CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();
        first.checkpointCoordinator(1L, checkpoint);
        first.notifyCheckpointComplete(1L);
        first.waitProcessAllActions();
        byte[] state = checkpoint.get();
        first.close();

        // 2. restore with mark-done enabled — should initialize cleanly
        FileStoreTable markDoneTable = table.copy(markDoneOption);
        CommittingWriteOperatorCoordinator second =
                createCoordinator(markDoneTable, context, false);
        second.resetToCheckpoint(1L, state);
        second.start();
        second.waitProcessAllActions();
        second.handleEventFromOperator(0, 1, event(true, committable(markDoneTable, 1, 1)));
        second.waitProcessAllActions();
        assertThat(second.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        second.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testWriteRestoreOnlyCoordinatorIgnoresCommit() throws Exception {
        // the base WriteOperatorCoordinator answers checkpoint with empty bytes and ignores
        // committable events, preserving the pure write-restore behavior.
        FileStoreTable table = createUnawareBucketTable();
        WriteOperatorCoordinator coordinator = new WriteOperatorCoordinator(table);
        coordinator.start();
        CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();
        coordinator.checkpointCoordinator(1, checkpoint);
        assertThat(checkpoint.get()).isEmpty();
        coordinator.close();
    }

    // ------------------------------------------------------------------------

    private FileStoreTable createUnawareBucketTable() throws Exception {
        return createFileStoreTable(
                options -> {
                    options.set(CoreOptions.BUCKET, -1);
                    options.remove("bucket-key");
                });
    }

    private CommittingWriteOperatorCoordinator createCoordinator(
            FileStoreTable table, TestingContext context, boolean failoverAfterRecovery) {
        return new CommittingWriteOperatorCoordinator(
                context,
                commitContext ->
                        new StoreCommitter(
                                table,
                                table.newStreamWriteBuilder()
                                        .withCommitUser(commitContext.commitUser())
                                        .newCommit(),
                                commitContext),
                true,
                commitUser,
                failoverAfterRecovery);
    }

    private Committable committable(FileStoreTable table, long checkpointId, int value)
            throws Exception {
        try (StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(commitUser).newWrite()) {
            write.write(GenericRow.of(value, (long) value));
            List<CommitMessage> messages = write.prepareCommit(false, checkpointId);
            assertThat(messages).hasSize(1);
            return new Committable(checkpointId, messages.get(0));
        }
    }

    private Committable committable(
            FileStoreTable table, long checkpointId, int partitionValue, long normalValue)
            throws Exception {
        try (StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(commitUser).newWrite()) {
            write.write(GenericRow.of(partitionValue, normalValue));
            List<CommitMessage> messages = write.prepareCommit(false, checkpointId);
            assertThat(messages).hasSize(1);
            return new Committable(checkpointId, messages.get(0));
        }
    }

    private List<Committable> committables(
            FileStoreTable table, long checkpointId, GenericRow... rows) throws Exception {
        List<Committable> result = new ArrayList<>();
        try (StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(commitUser).newWrite()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            for (CommitMessage message : write.prepareCommit(false, checkpointId)) {
                result.add(new Committable(checkpointId, message));
            }
        }
        return result;
    }

    private CommittableEvent event(boolean isRestoring, Committable committable) throws Exception {
        List<Committable> list = new ArrayList<>();
        list.add(committable);
        return CommittableEvent.create(committable.checkpointId(), isRestoring, list, SERIALIZER);
    }

    private CommittableEvent eventOf(
            long checkpointId, boolean isRestoring, List<Committable> committables)
            throws Exception {
        return CommittableEvent.create(checkpointId, isRestoring, committables, SERIALIZER);
    }

    private byte[] emptyState() throws Exception {
        return SimpleVersionedSerialization.writeVersionAndSerialize(
                new CoordinatorStateSerializer(),
                new CoordinatorState(
                        commitUser, new MemoryBackendStateStore().getSerializedStates()));
    }

    private class TestingContext implements OperatorCoordinator.Context {

        private final OperatorID operatorID;
        private final int parallelism;

        private TestingContext(OperatorID operatorID, int parallelism) {
            this.operatorID = operatorID;
            this.parallelism = parallelism;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinatorMetricGroup metricGroup() {
            return null;
        }

        @Override
        public void failJob(Throwable cause) {
            failureCause = cause;
        }

        @Override
        public int currentParallelism() {
            return parallelism;
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

    private static class MockSubtaskGateway implements OperatorCoordinator.SubtaskGateway {

        @Override
        public CompletableFuture<Acknowledge> sendEvent(OperatorEvent evt) {
            throw new UnsupportedOperationException("Unsupported to send event " + evt);
        }

        @Override
        public ExecutionAttemptID getExecution() {
            throw new UnsupportedOperationException("Unsupported to get execution");
        }

        @Override
        public int getSubtask() {
            throw new UnsupportedOperationException("Unsupported to get subtask");
        }
    }
}
