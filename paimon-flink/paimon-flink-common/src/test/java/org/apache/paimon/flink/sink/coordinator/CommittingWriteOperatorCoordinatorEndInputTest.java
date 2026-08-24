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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** EndInput scenarios for {@link CommittingWriteOperatorCoordinator}. */
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

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testEndInputCommitsAfterCoveredCheckpointCompletesWithEmptyEndInput()
            throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        coordinator.start();
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(0, 0, event(committable(table, Long.MAX_VALUE, 1)));
        coordinator.handleEventFromOperator(1, 0, emptyEvent(Long.MAX_VALUE));
        coordinator.waitProcessAllActions();
        assertThat(table.latestSnapshot()).isNotPresent();

        coordinator.handleEventFromOperator(0, 0, emptyEvent(11L));
        coordinator.handleEventFromOperator(1, 0, emptyEvent(11L));
        coordinator.notifyCheckpointComplete(11L);
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1");
        assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                .isEqualTo(Long.MAX_VALUE);
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testPartialEndInputCommitsOnlyOrdinaryCheckpoint() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        coordinator.start();
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(0, 0, event(committable(table, Long.MAX_VALUE, 1)));
        coordinator.handleEventFromOperator(0, 0, emptyEvent(1L));
        coordinator.handleEventFromOperator(1, 0, event(committable(table, 1L, 2)));
        coordinator.notifyCheckpointComplete(1L);
        coordinator.waitProcessAllActions();
        assertResults(table, "2, 2");

        coordinator.handleEventFromOperator(1, 0, event(committable(table, Long.MAX_VALUE, 3)));
        coordinator.handleEventFromOperator(0, 0, emptyEvent(2L));
        coordinator.handleEventFromOperator(1, 0, emptyEvent(2L));
        coordinator.notifyCheckpointComplete(2L);
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1", "2, 2", "3, 3");
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testCheckpointBeforeEndInputDoesNotCoverEndInput() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 1);
        coordinator.start();
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(0, 0, emptyEvent(11L));
        coordinator.handleEventFromOperator(0, 0, event(committable(table, Long.MAX_VALUE, 1)));
        coordinator.notifyCheckpointComplete(11L);
        coordinator.waitProcessAllActions();
        assertThat(table.latestSnapshot()).isNotPresent();

        coordinator.handleEventFromOperator(0, 0, emptyEvent(12L));
        coordinator.notifyCheckpointComplete(12L);
        coordinator.waitProcessAllActions();
        assertResults(table, "1, 1");
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testAbortedCheckpointDoesNotCommitEndInput() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        coordinator.start();
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(0, 0, event(committable(table, Long.MAX_VALUE, 1)));
        coordinator.handleEventFromOperator(1, 0, emptyEvent(Long.MAX_VALUE));
        coordinator.handleEventFromOperator(0, 0, emptyEvent(1L));
        coordinator.handleEventFromOperator(1, 0, emptyEvent(1L));
        coordinator.notifyCheckpointAborted(1L);
        coordinator.waitProcessAllActions();
        assertThat(table.latestSnapshot()).isNotPresent();

        coordinator.handleEventFromOperator(0, 0, emptyEvent(2L));
        coordinator.handleEventFromOperator(1, 0, emptyEvent(2L));
        coordinator.notifyCheckpointComplete(2L);
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1");
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testRunningWriterFailoverKeepsOtherPendingEndInput() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        coordinator.start();
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(0, 0, event(committable(table, Long.MAX_VALUE, 1)));
        coordinator.handleEventFromOperator(0, 0, emptyEvent(1L));
        coordinator.handleEventFromOperator(1, 0, event(committable(table, 1L, 2)));
        coordinator.notifyCheckpointComplete(1L);
        coordinator.waitProcessAllActions();
        assertResults(table, "2, 2");

        coordinator.subtaskReset(1, 1L);
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(1, 1, event(committable(table, Long.MAX_VALUE, 3)));
        coordinator.handleEventFromOperator(0, 0, emptyEvent(2L));
        coordinator.handleEventFromOperator(1, 1, emptyEvent(2L));
        coordinator.notifyCheckpointComplete(2L);
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1", "2, 2", "3, 3");
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testSubtaskResetDropsEndInputAfterRestoreCheckpoint() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 1);
        coordinator.start();
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(0, 0, emptyEvent(1L));
        coordinator.handleEventFromOperator(0, 0, event(committable(table, Long.MAX_VALUE, 1)));
        coordinator.notifyCheckpointComplete(1L);
        coordinator.waitProcessAllActions();
        assertThat(table.latestSnapshot()).isNotPresent();

        coordinator.subtaskReset(0, 1L);
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(0, 1, event(committable(table, Long.MAX_VALUE, 2)));
        coordinator.handleEventFromOperator(0, 1, emptyEvent(2L));
        coordinator.notifyCheckpointComplete(2L);
        coordinator.waitProcessAllActions();

        assertResults(table, "2, 2");
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testRegionRestoreKeepsPendingEndInput() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        coordinator.start();
        coordinator.waitProcessAllActions();

        Committable firstEndInput = committable(table, Long.MAX_VALUE, 1);
        coordinator.handleEventFromOperator(0, 0, event(firstEndInput));
        coordinator.handleEventFromOperator(0, 0, emptyEvent(2L));
        coordinator.handleEventFromOperator(1, 0, event(committable(table, 2L, 2)));
        coordinator.notifyCheckpointComplete(2L);
        coordinator.waitProcessAllActions();
        assertResults(table, "2, 2");

        coordinator.subtaskReset(0, 2L);
        coordinator.handleEventFromOperator(
                0,
                1,
                restoreEventEntries(
                        2L,
                        new CheckpointCommittables(
                                Long.MAX_VALUE,
                                Collections.singletonList(firstEndInput),
                                Long.MIN_VALUE)));
        coordinator.handleEventFromOperator(0, 1, emptyEvent(3L));
        coordinator.handleEventFromOperator(1, 0, event(committable(table, Long.MAX_VALUE, 3)));
        coordinator.handleEventFromOperator(1, 0, emptyEvent(3L));
        coordinator.notifyCheckpointComplete(3L);
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1", "2, 2", "3, 3");
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testGlobalRestoreOfPartialEndInputWaitsForAllWriters() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        coordinator.resetToCheckpoint(10L, emptyState());
        coordinator.start();
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(
                0,
                1,
                restoreEventEntries(
                        10L,
                        new CheckpointCommittables(
                                Long.MAX_VALUE,
                                Collections.singletonList(committable(table, Long.MAX_VALUE, 1)),
                                Long.MIN_VALUE)));
        coordinator.handleEventFromOperator(
                1,
                1,
                restoreEventEntries(
                        10L,
                        new CheckpointCommittables(10L, Collections.emptyList(), Long.MIN_VALUE)));
        coordinator.waitProcessAllActions();
        assertThat(table.latestSnapshot()).isNotPresent();

        coordinator.handleEventFromOperator(1, 1, event(committable(table, Long.MAX_VALUE, 2)));
        coordinator.handleEventFromOperator(0, 1, emptyEvent(11L));
        coordinator.handleEventFromOperator(1, 1, emptyEvent(11L));
        coordinator.notifyCheckpointComplete(11L);
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1", "2, 2");
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testGlobalRestoreOfAllEndInputCommitsThroughRecovery() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        coordinator.resetToCheckpoint(10L, emptyState());
        coordinator.start();
        coordinator.waitProcessAllActions();

        coordinator.handleEventFromOperator(
                0,
                1,
                restoreEventEntries(
                        10L,
                        new CheckpointCommittables(
                                Long.MAX_VALUE,
                                Collections.singletonList(committable(table, Long.MAX_VALUE, 1)),
                                Long.MIN_VALUE)));
        coordinator.handleEventFromOperator(
                1,
                1,
                restoreEventEntries(
                        10L,
                        new CheckpointCommittables(
                                Long.MAX_VALUE,
                                Collections.singletonList(committable(table, Long.MAX_VALUE, 2)),
                                Long.MIN_VALUE)));
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1", "2, 2");
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testGlobalRecoveryFiltersEndInputCommittedBeforeFailure() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        Committable firstEndInput = committable(table, Long.MAX_VALUE, 1);
        Committable secondEndInput = committable(table, Long.MAX_VALUE, 2);
        CommittingWriteOperatorCoordinator first =
                createCoordinator(
                        table,
                        2,
                        commitContext ->
                                new FailAfterCommitCommitter(
                                        new StoreCommitter(
                                                table,
                                                table.newStreamWriteBuilder()
                                                        .withCommitUser(commitContext.commitUser())
                                                        .newCommit(),
                                                commitContext)));
        first.start();
        first.waitProcessAllActions();

        first.handleEventFromOperator(0, 0, event(firstEndInput));
        first.handleEventFromOperator(1, 0, event(secondEndInput));
        first.handleEventFromOperator(0, 0, emptyEvent(1L));
        first.handleEventFromOperator(1, 0, emptyEvent(1L));
        first.notifyCheckpointComplete(1L);
        first.waitProcessAllActions();

        assertThat(failureCause).isNotNull();
        assertResults(table, "1, 1", "2, 2");
        long committedSnapshotId = table.snapshotManager().latestSnapshotId();
        first.close();
        failureCause = null;

        CommittingWriteOperatorCoordinator recovered = createCoordinator(table, 2);
        recovered.resetToCheckpoint(1L, emptyState());
        recovered.start();
        recovered.waitProcessAllActions();
        recovered.handleEventFromOperator(
                0,
                1,
                restoreEventEntries(
                        1L,
                        new CheckpointCommittables(
                                Long.MAX_VALUE,
                                Collections.singletonList(firstEndInput),
                                Long.MIN_VALUE)));
        recovered.handleEventFromOperator(
                1,
                1,
                restoreEventEntries(
                        1L,
                        new CheckpointCommittables(
                                Long.MAX_VALUE,
                                Collections.singletonList(secondEndInput),
                                Long.MIN_VALUE)));
        recovered.waitProcessAllActions();

        assertResults(table, "1, 1", "2, 2");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(committedSnapshotId);
        recovered.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testRegionRestoreDoesNotRecommitEndInputAlreadyCommitted() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 1);
        coordinator.start();
        coordinator.waitProcessAllActions();

        Committable endInput = committable(table, Long.MAX_VALUE, 1);
        coordinator.handleEventFromOperator(0, 0, event(endInput));
        coordinator.handleEventFromOperator(0, 0, emptyEvent(1L));
        coordinator.notifyCheckpointComplete(1L);
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1");
        long committedSnapshotId = table.snapshotManager().latestSnapshotId();

        coordinator.subtaskReset(0, 1L);
        coordinator.handleEventFromOperator(
                0,
                1,
                restoreEventEntries(
                        1L,
                        new CheckpointCommittables(
                                Long.MAX_VALUE,
                                Collections.singletonList(endInput),
                                Long.MIN_VALUE)));
        coordinator.handleEventFromOperator(0, 1, emptyEvent(2L));
        coordinator.notifyCheckpointComplete(2L);
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(committedSnapshotId);
        coordinator.close();
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    public void testDuplicateEndInputEventDoesNotDuplicateCommittables() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        CommittingWriteOperatorCoordinator coordinator = createCoordinator(table, 2);
        coordinator.start();
        coordinator.waitProcessAllActions();

        Committable endInput = committable(table, Long.MAX_VALUE, 1);
        coordinator.handleEventFromOperator(0, 0, event(endInput));
        coordinator.handleEventFromOperator(0, 0, event(endInput));
        coordinator.handleEventFromOperator(1, 0, emptyEvent(Long.MAX_VALUE));
        coordinator.handleEventFromOperator(0, 0, emptyEvent(1L));
        coordinator.handleEventFromOperator(1, 0, emptyEvent(1L));
        coordinator.notifyCheckpointComplete(1L);
        coordinator.waitProcessAllActions();

        assertResults(table, "1, 1");
        coordinator.close();
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
                                table.newStreamWriteBuilder()
                                        .withCommitUser(commitContext.commitUser())
                                        .newCommit(),
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

    /** {@link Committer} decorator that makes a successful commit appear to have failed. */
    private static class FailAfterCommitCommitter
            implements Committer<Committable, ManifestCommittable> {

        private final Committer<Committable, ManifestCommittable> delegate;

        private FailAfterCommitCommitter(Committer<Committable, ManifestCommittable> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean forceCreatingSnapshot() {
            return delegate.forceCreatingSnapshot();
        }

        @Override
        public ManifestCommittable combine(
                long checkpointId, long watermark, List<Committable> committables)
                throws IOException {
            return delegate.combine(checkpointId, watermark, committables);
        }

        @Override
        public ManifestCommittable combine(
                long checkpointId,
                long watermark,
                ManifestCommittable committable,
                List<Committable> committables) {
            return delegate.combine(checkpointId, watermark, committable, committables);
        }

        @Override
        public void commit(List<ManifestCommittable> committables)
                throws IOException, InterruptedException {
            delegate.commit(committables);
            throw new IOException("Commit succeeded before the simulated failure");
        }

        @Override
        public int filterAndCommit(
                List<ManifestCommittable> committables,
                boolean checkAppendFiles,
                boolean partitionMarkDoneRecoverFromState)
                throws IOException {
            delegate.filterAndCommit(
                    committables, checkAppendFiles, partitionMarkDoneRecoverFromState);
            throw new IOException("Commit succeeded before the simulated failure");
        }

        @Override
        public Map<Long, List<Committable>> groupByCheckpoint(
                Collection<Committable> committables) {
            return delegate.groupByCheckpoint(committables);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
