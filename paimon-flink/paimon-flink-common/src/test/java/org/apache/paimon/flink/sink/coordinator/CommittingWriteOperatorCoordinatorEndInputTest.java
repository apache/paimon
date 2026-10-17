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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableSerializer;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.CommitterTestBase;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sink.state.CoordinatorState;
import org.apache.paimon.flink.sink.state.CoordinatorStateSerializer;
import org.apache.paimon.flink.sink.state.MemoryBackendStateStore;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.StreamTableWrite;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** EndInput scenarios for {@link CommittingWriteOperatorCoordinator}. */
@Timeout(30)
public class CommittingWriteOperatorCoordinatorEndInputTest extends CommitterTestBase {

    private static final TypeSerializer<CheckpointCommittables> SERIALIZER =
            new SimpleVersionedSerializerTypeSerializerProxy<>(
                    () ->
                            new CheckpointCommittablesSerializer(
                                    new CommittableSerializer(new CommitMessageSerializer())));

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

    @Test
    public void testRealTerminalPromotionAndLaterAlignment() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        coordinator.start();
        coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 1, false, 10));
        coordinator.handleEventFromOperator(
                1, 0, eventOf(1, Collections.singletonList(committable(table, 1, 2)), 20));
        coordinator.waitProcessAllActions();
        assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(-1);
        coordinator.notifyCheckpointComplete(1);
        coordinator.waitProcessAllActions();
        assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(1);
        assertThat(coordinator.terminalCoveredBy(1)).isEqualTo(-1);
        assertThat(table.snapshotManager().latestSnapshot().watermark()).isEqualTo(10);
        coordinator.handleEventFromOperator(
                1, 0, eventOf(2, Collections.singletonList(committable(table, 2, 3)), 200));
        coordinator.notifyCheckpointComplete(2);
        coordinator.waitProcessAllActions();
        assertResults(table, "1, 1", "2, 2", "3, 3");
        assertThat(table.snapshotManager().latestSnapshot().commitIdentifier()).isEqualTo(2);
        assertThat(table.snapshotManager().latestSnapshot().watermark()).isEqualTo(200);
        assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(1);
        coordinator.close();
    }

    @Test
    public void testEmptyTerminalPromotesAfterCommit() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 1);
        coordinator.start();
        coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 1, true, 100));
        coordinator.waitProcessAllActions();
        assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(-1);
        coordinator.notifyCheckpointComplete(1);
        coordinator.waitProcessAllActions();
        assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(1);
        assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                .isEqualTo(Long.MAX_VALUE);
        coordinator.close();
    }

    @Test
    public void testTerminalCandidateSurvivesTwoAborts() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 1);
        coordinator.start();
        coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 1, false, 100));
        coordinator.notifyCheckpointAborted(1);
        coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 2, true, 100));
        coordinator.notifyCheckpointAborted(2);
        coordinator.waitProcessAllActions();
        assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(-1);
        coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 3, true, 100));
        coordinator.notifyCheckpointComplete(3);
        coordinator.waitProcessAllActions();
        assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(3);
        assertResults(table, "1, 1");
        assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                .isEqualTo(Long.MAX_VALUE);
        coordinator.close();
    }

    @Test
    public void testFailedTerminalCommitDoesNotPromoteOrRetire() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator =
                createCoordinator(
                        table,
                        1,
                        c ->
                                new StoreCommitter(table, table.newCommit(c.commitUser()), c) {
                                    @Override
                                    public void commit(List<ManifestCommittable> entries) {
                                        throw new RuntimeException("terminal failure");
                                    }
                                });
        coordinator.start();
        coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 1, false, 100));
        coordinator.notifyCheckpointComplete(1);
        CompletableFuture<byte[]> next = new CompletableFuture<>();
        coordinator.checkpointCoordinator(2, next);
        coordinator.waitProcessAllActions();
        assertThat(next).isCompletedExceptionally();
        assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(-1);
        assertThat(coordinator.pendingCommittables(0)).containsKey(1L);
        assertThat(failureCause).hasMessageContaining("terminal failure");
        failureCause = null;
        coordinator.close();
    }

    @Test
    public void testRestoredRealMarkerPromotesAfterRecoveryCommit() throws Exception {
        testRecovery(false);
    }

    @Test
    public void testFailedRecoveryDoesNotPromoteOrAdvanceCheckpoint() throws Exception {
        testRecovery(true);
    }

    @Test
    public void testLaterCheckpointRestoresTerminalWithoutWriterEvent() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator original = createCoordinator(table, 2);
        original.start();
        CommittableEvent terminal = terminalEvent(table, 1, false, 10);
        original.handleEventFromOperator(0, 0, terminal);
        original.handleEventFromOperator(1, 0, emptyEvent(1));
        original.notifyCheckpointComplete(1);
        CompletableFuture<byte[]> snapshot = new CompletableFuture<>();
        original.checkpointCoordinator(2, snapshot);
        byte[] saved = snapshot.get(10, TimeUnit.SECONDS);
        original.close();

        CommittingWriteOperatorCoordinator restored = createCoordinator(table, 2);
        restored.resetToCheckpoint(2, saved);
        restored.start();
        restored.waitProcessAllActions();
        assertThat(restored.terminalCoveredBy(0)).isEqualTo(1);
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RESTORING);
        restored.handleEventFromOperator(1, 1, restoreEventEntries(2));
        restored.waitProcessAllActions();
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        // A late replay of already committed files cannot duplicate data or revert coverage.
        restored.handleEventFromOperator(
                0, 1, restoreEventEntries(2, terminal.deserialize(SERIALIZER)));
        restored.handleEventFromOperator(1, 1, emptyEvent(3));
        restored.notifyCheckpointComplete(3);
        restored.waitProcessAllActions();
        assertThat(restored.terminalCoveredBy(0)).isEqualTo(1);
        assertResults(table, "1, 1");
        restored.close();
    }

    @Test
    public void testFirstTerminalBoundaryStillRequiresWriterReplay() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator restored = createCoordinator(table, 1);
        restored.resetToCheckpoint(1, terminalState(-1));
        restored.start();
        restored.waitProcessAllActions();
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RESTORING);
        assertThat(restored.terminalCoveredBy(0)).isEqualTo(-1);
        restored.handleEventFromOperator(
                0,
                1,
                restoreEventEntries(1, terminalEvent(table, 1, false, 10).deserialize(SERIALIZER)));
        restored.waitProcessAllActions();
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        assertThat(restored.terminalCoveredBy(0)).isEqualTo(1);
        assertResults(table, "1, 1");
        restored.close();
    }

    @Test
    public void testAllTerminalRecoveryCompletesWithoutReporters() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator restored = createCoordinator(table, 2);
        restored.resetToCheckpoint(2, terminalState(1, 2));
        restored.start();
        CompletableFuture<byte[]> next = new CompletableFuture<>();
        restored.checkpointCoordinator(3, next);
        CoordinatorState saved =
                SimpleVersionedSerialization.readVersionAndDeSerialize(
                        new CoordinatorStateSerializer(), next.get(10, TimeUnit.SECONDS));
        assertThat(saved.getTerminalCoveredBy()).containsExactly(1, 2);
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        restored.close();
    }

    @Test
    public void testAllTerminalRecoveryFailureFencesCheckpoint() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator restored =
                createCoordinator(
                        table,
                        1,
                        c ->
                                new StoreCommitter(table, table.newCommit(c.commitUser()), c) {
                                    @Override
                                    public int filterAndCommit(
                                            List<ManifestCommittable> entries,
                                            boolean check,
                                            boolean listeners) {
                                        throw new RuntimeException(
                                                "zero reporter recovery failure");
                                    }
                                });
        restored.resetToCheckpoint(2, terminalState(1));
        restored.start();
        CompletableFuture<byte[]> next = new CompletableFuture<>();
        restored.checkpointCoordinator(3, next);
        restored.waitProcessAllActions();
        assertThat(next).isCompletedExceptionally();
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RESTORING);
        assertThat(failureCause).hasMessageContaining("zero reporter recovery failure");
        failureCause = null;
        restored.close();
    }

    @Test
    public void testOrdinaryRecoveryRequiresEveryWriter() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator restored = createCoordinator(table, 2);
        restored.resetToCheckpoint(2, emptyState());
        restored.start();
        restored.handleEventFromOperator(0, 1, restoreEventEntries(2));
        restored.waitProcessAllActions();
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RESTORING);
        restored.handleEventFromOperator(1, 1, restoreEventEntries(2));
        restored.waitProcessAllActions();
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        assertThat(restored.terminalCoveredBy(0)).isEqualTo(-1);
        assertThat(restored.terminalCoveredBy(1)).isEqualTo(-1);
        restored.close();
    }

    @Test
    public void testTerminalStateRescaleFailsRecovery() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator restored = createCoordinator(table, 1);
        restored.resetToCheckpoint(2, terminalState(1, -1));
        restored.start();
        CompletableFuture<byte[]> next = new CompletableFuture<>();
        restored.checkpointCoordinator(3, next);
        restored.waitProcessAllActions();
        assertThat(next).isCompletedExceptionally();
        assertThat(failureCause).hasMessageContaining("Cannot rescale terminal coordinator state");
        failureCause = null;
        restored.close();
    }

    @Test
    public void testRegionResetBeforeTerminalCoverageFails() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator restored = createCoordinator(table, 1);
        restored.resetToCheckpoint(2, terminalState(1));
        restored.start();
        restored.subtaskReset(0, 0);
        CompletableFuture<byte[]> next = new CompletableFuture<>();
        restored.checkpointCoordinator(3, next);
        restored.waitProcessAllActions();
        assertThat(next).isCompletedExceptionally();
        assertThat(failureCause).hasMessageContaining("Region reset before terminal coverage");
        failureCause = null;
        restored.close();
    }

    @Test
    public void testResetDuringRecoveryRequiresFreshContribution() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator restored = createCoordinator(table, 2);
        restored.resetToCheckpoint(2, terminalState(-1, -1));
        restored.start();
        restored.handleEventFromOperator(0, 1, restoreEventEntries(2));
        restored.subtaskReset(0, 2);
        restored.handleEventFromOperator(1, 1, restoreEventEntries(2));
        restored.waitProcessAllActions();
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RESTORING);
        restored.handleEventFromOperator(0, 2, restoreEventEntries(2));
        restored.waitProcessAllActions();
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        restored.close();
    }

    @Test
    public void testNoTerminalStateDoesNotRejectParallelismChange() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator restored = createCoordinator(table, 1);
        restored.resetToCheckpoint(2, terminalState(-1, -1));
        restored.start();
        restored.handleEventFromOperator(0, 1, restoreEventEntries(2));
        restored.waitProcessAllActions();
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        assertThat(restored.terminalCoveredBy(0)).isEqualTo(-1);
        restored.close();
    }

    @Test
    public void testRecoveryWithPendingCheckpointBeforeTerminalCoverage() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator restored = createCoordinator(table, 2);
        restored.resetToCheckpoint(5, terminalState(3, -1));
        restored.start();
        restored.handleEventFromOperator(
                1,
                1,
                restoreEventEntries(
                        5,
                        new CheckpointCommittables(
                                2, Collections.singletonList(committable(table, 2, 2)), 10)));
        restored.waitProcessAllActions();
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        assertThat(restored.terminalCoveredBy(0)).isEqualTo(3);
        assertResults(table, "2, 2");
        restored.close();
    }

    @Test
    public void testEarlyReleasePrecedesCommitAndLastWriterWaitsForFinalization() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        List<TerminalWriterReleaseEvent> first = new CopyOnWriteArrayList<>();
        List<TerminalWriterReleaseEvent> last = new CopyOnWriteArrayList<>();
        coordinator.start();
        coordinator.executionAttemptReady(0, 0, releaseGateway(0, 0, first));
        coordinator.executionAttemptReady(1, 0, releaseGateway(1, 0, last));
        coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 1, true, 10));
        coordinator.handleEventFromOperator(1, 0, emptyEvent(1));
        coordinator.waitProcessAllActions();
        assertThat(first).hasSize(1);
        assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(-1);
        assertThat(table.snapshotManager().latestSnapshot()).isNull();
        assertThat(last).isEmpty();
        coordinator.notifyCheckpointComplete(1);
        coordinator.waitProcessAllActions();
        assertThat(first).hasSize(1);
        assertThat(first.get(0).getCheckpointId()).isEqualTo(1);
        assertThat(first.get(0).getSubtask()).isZero();
        assertThat(first.get(0).getAttemptNumber()).isZero();
        // An empty marker captured before the task finishes advances its wait boundary.
        coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 2, true, 10));
        coordinator.waitProcessAllActions();
        assertThat(first).hasSize(2);
        assertThat(first.get(1).getCheckpointId()).isEqualTo(2);
        coordinator.handleEventFromOperator(1, 0, terminalEvent(table, 2, true, 20));
        coordinator.notifyCheckpointComplete(2);
        coordinator.waitProcessAllActions();
        assertThat(last).hasSize(1);
        assertThat(first).hasSize(2);
        assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                .isEqualTo(Long.MAX_VALUE);
        coordinator.executionAttemptReady(1, 1, releaseGateway(1, 1, last));
        coordinator.waitProcessAllActions();
        assertThat(last).hasSize(2);
        coordinator.close();
    }

    @Test
    public void testEarlyReleaseFollowsAbortsWithoutReleasingLastCandidate() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        List<TerminalWriterReleaseEvent> first = new CopyOnWriteArrayList<>();
        List<TerminalWriterReleaseEvent> last = new CopyOnWriteArrayList<>();
        try {
            coordinator.start();
            coordinator.executionAttemptReady(0, 0, releaseGateway(0, 0, first));
            coordinator.executionAttemptReady(1, 0, releaseGateway(1, 0, last));
            for (long checkpoint = 1; checkpoint <= 3; checkpoint++) {
                coordinator.handleEventFromOperator(
                        0, 0, terminalEvent(table, checkpoint, true, 10));
                coordinator.handleEventFromOperator(
                        1, 0, terminalEvent(table, checkpoint, true, 20));
                if (checkpoint < 3) {
                    coordinator.notifyCheckpointAborted(checkpoint);
                }
                coordinator.waitProcessAllActions();
                assertThat(first).hasSize((int) checkpoint);
                assertThat(first.get((int) checkpoint - 1).getCheckpointId()).isEqualTo(checkpoint);
                assertThat(last).isEmpty();
                assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(-1);
            }
            coordinator.notifyCheckpointComplete(3);
            coordinator.waitProcessAllActions();
            assertThat(first).hasSize(3);
            assertThat(last).hasSize(1);
            assertThat(last.get(0).getCheckpointId()).isEqualTo(3);
            assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                    .isEqualTo(Long.MAX_VALUE);
        } finally {
            coordinator.close();
        }
    }

    @Test
    public void testEarlyReleaseDoesNotBypassNextCheckpointCommitBarrier() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<TerminalWriterReleaseEvent> events = new CopyOnWriteArrayList<>();
        CommittingWriteOperatorCoordinator coordinator =
                createCoordinator(
                        table,
                        2,
                        c ->
                                new StoreCommitter(table, table.newCommit(c.commitUser()), c) {
                                    @Override
                                    public void commit(List<ManifestCommittable> entries)
                                            throws java.io.IOException, InterruptedException {
                                        entered.countDown();
                                        assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
                                        super.commit(entries);
                                    }
                                });
        try {
            coordinator.start();
            coordinator.executionAttemptReady(0, 0, releaseGateway(0, 0, events));
            coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 1, false, 10));
            coordinator.handleEventFromOperator(1, 0, emptyEvent(1));
            coordinator.notifyCheckpointComplete(1);
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(events).hasSize(1);
            assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(-1);
            assertThat(table.snapshotManager().latestSnapshot()).isNull();
            CompletableFuture<byte[]> next = new CompletableFuture<>();
            coordinator.checkpointCoordinator(2, next);
            assertThat(next).isNotDone();
            release.countDown();
            assertThat(next.get(10, TimeUnit.SECONDS)).isNotEmpty();
            assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(1);
            assertResults(table, "1, 1");
            assertThat(events).hasSize(1);
        } finally {
            release.countDown();
            coordinator.close();
        }
    }

    @Test
    public void testFailedCommitAfterEarlyReleaseFencesNextCheckpoint() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        List<TerminalWriterReleaseEvent> events = new CopyOnWriteArrayList<>();
        CommittingWriteOperatorCoordinator coordinator =
                createCoordinator(
                        table,
                        2,
                        c ->
                                new StoreCommitter(table, table.newCommit(c.commitUser()), c) {
                                    @Override
                                    public void commit(List<ManifestCommittable> entries) {
                                        throw new RuntimeException("early release commit failure");
                                    }
                                });
        coordinator.start();
        coordinator.executionAttemptReady(0, 0, releaseGateway(0, 0, events));
        coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 1, true, 10));
        coordinator.handleEventFromOperator(1, 0, emptyEvent(1));
        coordinator.notifyCheckpointComplete(1);
        CompletableFuture<byte[]> next = new CompletableFuture<>();
        coordinator.checkpointCoordinator(2, next);
        coordinator.waitProcessAllActions();
        assertThat(events).hasSize(1);
        assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(-1);
        assertThat(next).isCompletedExceptionally();
        assertThat(failureCause).hasMessageContaining("early release commit failure");
        failureCause = null;
        coordinator.close();
    }

    @Test
    public void testInFlightFinalCommitUsesOnlyReplacementReleaseTarget() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<TerminalWriterReleaseEvent> old = new CopyOnWriteArrayList<>();
        List<TerminalWriterReleaseEvent> replacement = new CopyOnWriteArrayList<>();
        CommittingWriteOperatorCoordinator coordinator =
                createCoordinator(
                        table,
                        1,
                        c ->
                                new StoreCommitter(table, table.newCommit(c.commitUser()), c) {
                                    @Override
                                    public void commit(List<ManifestCommittable> entries)
                                            throws java.io.IOException, InterruptedException {
                                        entered.countDown();
                                        try {
                                            assertThat(release.await(10, TimeUnit.SECONDS))
                                                    .isTrue();
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            throw new RuntimeException(e);
                                        }
                                        super.commit(entries);
                                    }
                                });
        try {
            coordinator.start();
            coordinator.executionAttemptReady(0, 0, releaseGateway(0, 0, old));
            coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 1, true, 10));
            coordinator.notifyCheckpointComplete(1);
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(old).isEmpty();
            coordinator.executionAttemptFailed(0, 0, new RuntimeException("attempt failed"));
            coordinator.executionAttemptReady(0, 1, releaseGateway(0, 1, replacement));
            // A late failure callback for the old attempt cannot remove its replacement.
            coordinator.executionAttemptFailed(0, 0, new RuntimeException("late failure"));
            release.countDown();
            coordinator.waitProcessAllActions();
            assertThat(old).isEmpty();
            assertThat(replacement).isNotEmpty();
            assertThat(replacement)
                    .allSatisfy(
                            event -> {
                                assertThat(event.getAttemptNumber()).isEqualTo(1);
                                assertThat(event.getCheckpointId()).isEqualTo(1);
                            });
        } finally {
            release.countDown();
            coordinator.close();
        }
    }

    @Test
    public void testDurableReleaseReplayAndAllTerminalRecoveryRelease() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        List<TerminalWriterReleaseEvent> events = new CopyOnWriteArrayList<>();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        coordinator.resetToCheckpoint(5, terminalState(1, -1));
        coordinator.start();
        coordinator.executionAttemptReady(0, 2, releaseGateway(0, 2, events));
        coordinator.waitProcessAllActions();
        assertThat(events).isNotEmpty();
        assertThat(events)
                .allSatisfy(
                        event -> {
                            assertThat(event.getSubtask()).isZero();
                            assertThat(event.getCheckpointId()).isEqualTo(5);
                            assertThat(event.getAttemptNumber()).isEqualTo(2);
                        });
        coordinator.close();
        events.clear();
        coordinator = createCoordinator(table, 2);
        coordinator.resetToCheckpoint(5, terminalState(1, 2));
        coordinator.start();
        coordinator.executionAttemptReady(0, 3, releaseGateway(0, 3, events));
        coordinator.executionAttemptReady(1, 3, releaseGateway(1, 3, events));
        coordinator.waitProcessAllActions();
        assertThat(events)
                .extracting(TerminalWriterReleaseEvent::getSubtask)
                .containsOnly(0, 1)
                .contains(0, 1);
        assertThat(events)
                .allSatisfy(
                        event -> {
                            assertThat(event.getAttemptNumber()).isEqualTo(3);
                            assertThat(event.getCheckpointId()).isGreaterThanOrEqualTo(5);
                        });
        coordinator.close();
    }

    private OperatorCoordinator.SubtaskGateway releaseGateway(
            int subtask, int attempt, List<TerminalWriterReleaseEvent> events) {
        OperatorCoordinator.SubtaskGateway gateway = mock(OperatorCoordinator.SubtaskGateway.class);
        ExecutionAttemptID execution = mock(ExecutionAttemptID.class);
        when(execution.getAttemptNumber()).thenReturn(attempt);
        when(gateway.getExecution()).thenReturn(execution);
        when(gateway.getSubtask()).thenReturn(subtask);
        doAnswer(
                        invocation -> {
                            events.add((TerminalWriterReleaseEvent) invocation.getArgument(0));
                            return CompletableFuture.completedFuture(Acknowledge.get());
                        })
                .when(gateway)
                .sendEvent(any(OperatorEvent.class));
        return gateway;
    }

    @Test
    public void testFinalizationIsDataFreeAndReleasesAllAfterSuccess() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        List<ManifestCommittable> tokens = new CopyOnWriteArrayList<>();
        List<TerminalWriterReleaseEvent> events = new CopyOnWriteArrayList<>();
        CommittingWriteOperatorCoordinator coordinator =
                new CommittingWriteOperatorCoordinator(
                        new TestingContext(new OperatorID(), 2),
                        c ->
                                new StoreCommitter(
                                        table,
                                        table.newCommit(c.commitUser()).ignoreEmptyCommit(false),
                                        c) {
                                    @Override
                                    public int filterAndCommit(
                                            List<ManifestCommittable> entries,
                                            boolean check,
                                            boolean listeners) {
                                        tokens.addAll(entries);
                                        assertThat(events).isEmpty();
                                        return super.filterAndCommit(entries, check, listeners);
                                    }
                                },
                        true,
                        commitUser,
                        null,
                        999L);
        coordinator.start();
        coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 1, false, 10));
        coordinator.handleEventFromOperator(1, 0, eventOf(1, Collections.emptyList(), 20));
        coordinator.notifyCheckpointComplete(1);
        coordinator.waitProcessAllActions();
        assertThat(tokens).isEmpty();
        assertThat(table.snapshotManager().latestSnapshot().watermark()).isEqualTo(10);
        // Neither gateway is registered yet; finish permission is replayed when ready.
        coordinator.handleEventFromOperator(
                1,
                0,
                CommittableEvent.create(
                        2,
                        new CheckpointCommittables(
                                2,
                                Collections.singletonList(committable(table, 2, 2)),
                                30,
                                false,
                                false,
                                true),
                        SERIALIZER));
        coordinator.notifyCheckpointComplete(2);
        coordinator.waitProcessAllActions();
        assertThat(tokens).hasSize(1);
        assertThat(tokens.get(0).identifier()).isEqualTo(Long.MAX_VALUE);
        assertThat(tokens.get(0).fileCommittables()).isEmpty();
        assertThat(tokens.get(0).watermark()).isEqualTo(999);
        assertThat(table.snapshotManager().latestSnapshot().watermark()).isEqualTo(999);
        assertResults(table, "1, 1", "2, 2");
        coordinator.executionAttemptReady(0, 1, releaseGateway(0, 1, events));
        coordinator.executionAttemptReady(1, 1, releaseGateway(1, 1, events));
        coordinator.notifyCheckpointComplete(3);
        coordinator.waitProcessAllActions();
        assertThat(events).extracting(TerminalWriterReleaseEvent::getSubtask).contains(0, 1);
        assertThat(tokens).hasSize(1);
        coordinator.close();
    }

    @Test
    public void testFinalReleaseWaitsForGlobalFinalization() throws Exception {
        testFinalizationBoundary(false);
    }

    @Test
    public void testFinalizationFailureFencesFinalReleaseAndCheckpoint() throws Exception {
        testFinalizationBoundary(true);
    }

    private void testFinalizationBoundary(boolean fail) throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<TerminalWriterReleaseEvent> events = new CopyOnWriteArrayList<>();
        CommittingWriteOperatorCoordinator coordinator =
                createCoordinator(
                        table,
                        2,
                        c ->
                                new StoreCommitter(table, table.newCommit(c.commitUser()), c) {
                                    @Override
                                    public int filterAndCommit(
                                            List<ManifestCommittable> entries,
                                            boolean check,
                                            boolean listeners) {
                                        entered.countDown();
                                        try {
                                            assertThat(release.await(10, TimeUnit.SECONDS))
                                                    .isTrue();
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            throw new RuntimeException(e);
                                        }
                                        if (fail) {
                                            throw new RuntimeException(
                                                    "global finalization failure");
                                        }
                                        return super.filterAndCommit(entries, check, listeners);
                                    }
                                });
        try {
            coordinator.start();
            coordinator.executionAttemptReady(0, 0, releaseGateway(0, 0, events));
            coordinator.executionAttemptReady(1, 0, releaseGateway(1, 0, events));
            coordinator.handleEventFromOperator(0, 0, terminalEvent(table, 1, true, 10));
            coordinator.handleEventFromOperator(1, 0, terminalEvent(table, 1, true, 20));
            coordinator.notifyCheckpointComplete(1);
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(events)
                    .extracting(TerminalWriterReleaseEvent::getSubtask)
                    .containsExactly(0);
            CompletableFuture<byte[]> next = new CompletableFuture<>();
            coordinator.checkpointCoordinator(2, next);
            release.countDown();
            coordinator.waitProcessAllActions();
            if (fail) {
                assertThat(events)
                        .extracting(TerminalWriterReleaseEvent::getSubtask)
                        .containsExactly(0);
                assertThat(next).isCompletedExceptionally();
                assertThat(failureCause).hasMessageContaining("global finalization failure");
                failureCause = null;
            } else {
                assertThat(events).hasSize(2);
                assertThat(next.get(10, TimeUnit.SECONDS)).isNotEmpty();
            }
        } finally {
            release.countDown();
            coordinator.close();
        }
    }

    @Test
    public void testAllTerminalRecoveryPreservesWatermarkAndFiltersMax() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator first = createCoordinator(table, 1);
        first.start();
        first.handleEventFromOperator(0, 0, terminalEvent(table, 1, true, 200));
        first.notifyCheckpointComplete(1);
        CompletableFuture<byte[]> saved = new CompletableFuture<>();
        first.checkpointCoordinator(2, saved);
        byte[] state = saved.get(10, TimeUnit.SECONDS);
        long snapshot = table.snapshotManager().latestSnapshotId();
        first.close();
        CommittingWriteOperatorCoordinator restored = createCoordinator(table, 1);
        restored.resetToCheckpoint(2, state);
        restored.start();
        List<TerminalWriterReleaseEvent> events = new CopyOnWriteArrayList<>();
        restored.executionAttemptReady(0, 1, releaseGateway(0, 1, events));
        restored.waitProcessAllActions();
        assertThat(events).isNotEmpty();
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot);
        assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                .isEqualTo(Long.MAX_VALUE);
        assertThat(table.snapshotManager().latestSnapshot().watermark()).isEqualTo(200);
        restored.close();
    }

    @Test
    public void testLegacyAllTerminalRequiresExplicitWatermark() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        byte[] legacy =
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        new CoordinatorStateSerializer(),
                        new CoordinatorState(commitUser, Collections.emptyMap(), new long[] {1}));
        CommittingWriteOperatorCoordinator restored = createCoordinator(table, 1);
        restored.resetToCheckpoint(2, legacy);
        restored.start();
        restored.waitProcessAllActions();
        assertThat(failureCause).hasMessageContaining("configure end-input.watermark");
        failureCause = null;
        restored.close();
        restored =
                new CommittingWriteOperatorCoordinator(
                        new TestingContext(new OperatorID(), 1),
                        c ->
                                new StoreCommitter(
                                        table,
                                        table.newCommit(c.commitUser()).ignoreEmptyCommit(false),
                                        c),
                        true,
                        commitUser,
                        null,
                        300L);
        restored.resetToCheckpoint(2, legacy);
        restored.start();
        restored.waitProcessAllActions();
        assertThat(restored.getCurrentState())
                .isEqualTo(CommittingWriteOperatorCoordinator.State.RUNNING);
        assertThat(table.snapshotManager().latestSnapshot().watermark()).isEqualTo(300);
        restored.close();
    }

    private byte[] terminalState(long... coverage) throws Exception {
        MemoryBackendStateStore store = new MemoryBackendStateStore();
        store.getListState(
                        new ListStateDescriptor<>(
                                CommittingWriteOperatorCoordinator.PROCESSED_WATERMARK_STATE,
                                LongSerializer.INSTANCE))
                .update(Collections.singletonList(100L));
        return SimpleVersionedSerialization.writeVersionAndSerialize(
                new CoordinatorStateSerializer(),
                new CoordinatorState(commitUser, store.getSerializedStates(), coverage));
    }

    private void testRecovery(boolean fail) throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator =
                createCoordinator(
                        table,
                        1,
                        c ->
                                new StoreCommitter(table, table.newCommit(c.commitUser()), c) {
                                    @Override
                                    public int filterAndCommit(
                                            List<ManifestCommittable> entries,
                                            boolean check,
                                            boolean listeners) {
                                        if (fail) {
                                            throw new RuntimeException("recovery failure");
                                        }
                                        return super.filterAndCommit(entries, check, listeners);
                                    }
                                });
        coordinator.resetToCheckpoint(2, emptyState());
        coordinator.start();
        coordinator.handleEventFromOperator(
                0,
                1,
                restoreEventEntries(
                        2,
                        new CheckpointCommittables(
                                1,
                                Collections.singletonList(committable(table, 1, 1)),
                                100,
                                false,
                                false,
                                true)));
        CompletableFuture<byte[]> next = new CompletableFuture<>();
        coordinator.checkpointCoordinator(3, next);
        coordinator.waitProcessAllActions();
        assertThat(coordinator.terminalCoveredBy(0)).isEqualTo(fail ? -1 : 2);
        if (fail) {
            assertThat(next).isCompletedExceptionally();
            assertThat(coordinator.pendingCommittables(0)).containsKey(1L);
            assertThat(failureCause).hasMessageContaining("recovery failure");
            failureCause = null;
        } else {
            assertThat(next.get(10, TimeUnit.SECONDS)).isNotEmpty();
            assertResults(table, "1, 1");
        }
        coordinator.close();
    }

    private CommittableEvent terminalEvent(
            FileStoreTable table, long checkpoint, boolean empty, long watermark) throws Exception {
        return CommittableEvent.create(
                checkpoint,
                new CheckpointCommittables(
                        checkpoint,
                        empty
                                ? Collections.emptyList()
                                : Collections.singletonList(committable(table, checkpoint, 1)),
                        watermark,
                        false,
                        false,
                        true),
                SERIALIZER);
    }

    private FileStoreTable createUnawareBucketTable() throws Exception {
        return createFileStoreTable(
                options -> {
                    options.set(CoreOptions.BUCKET, -1);
                    options.remove("bucket-key");
                });
    }

    private CommittingWriteOperatorCoordinator createCoordinator(
            FileStoreTable table, int parallelism) {
        return createCoordinator(
                table,
                parallelism,
                commitContext ->
                        new StoreCommitter(
                                table,
                                table.newCommit(commitContext.commitUser())
                                        .ignoreEmptyCommit(false),
                                commitContext));
    }

    private CommittingWriteOperatorCoordinator createCoordinator(
            FileStoreTable table,
            int parallelism,
            Committer.Factory<Committable, ManifestCommittable> committerFactory) {
        return new CommittingWriteOperatorCoordinator(
                new TestingContext(new OperatorID(), parallelism),
                committerFactory,
                true,
                commitUser,
                null,
                null);
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

    private CommittableEvent event(Committable committable) throws Exception {
        return eventOf(
                committable.checkpointId(), Collections.singletonList(committable), Long.MIN_VALUE);
    }

    private CommittableEvent emptyEvent(long checkpointId) throws Exception {
        return eventOf(checkpointId, Collections.emptyList(), Long.MIN_VALUE);
    }

    private CommittableEvent eventOf(
            long checkpointId, List<Committable> committables, long watermark) throws Exception {
        return CommittableEvent.create(
                checkpointId,
                new CheckpointCommittables(checkpointId, committables, watermark),
                SERIALIZER);
    }

    private RestoredCommittableEvent restoreEventEntries(
            long restoredCheckpointId, CheckpointCommittables... entries) throws Exception {
        List<CheckpointCommittables> restoredEntries = new ArrayList<>();
        Collections.addAll(restoredEntries, entries);
        return RestoredCommittableEvent.create(restoredCheckpointId, restoredEntries, SERIALIZER);
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

        public JobID getJobID() {
            return new JobID();
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
}
