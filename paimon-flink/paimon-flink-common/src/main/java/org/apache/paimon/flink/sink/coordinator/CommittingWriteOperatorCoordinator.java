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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.state.CoordinatorState;
import org.apache.paimon.flink.sink.state.CoordinatorStateSerializer;
import org.apache.paimon.flink.sink.state.MemoryBackendStateStore;
import org.apache.paimon.manifest.ManifestCommittable;

import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * {@link OperatorCoordinator} that runs the Paimon committer on the JobManager for the
 * unaware-bucket append write path. Writers stream per-checkpoint committables to this coordinator
 * over the {@link OperatorEvent} channel; on {@link #notifyCheckpointComplete} the coordinator
 * aligns committables across subtasks and commits them from a dedicated single-thread executor, so
 * the JM main thread is never blocked by table I/O.
 *
 * <p>Wrap this class with a {@link RecreateOnResetOperatorCoordinator} (see {@link Provider}). The
 * wrapper discards this instance on global failover and creates a new one in its place, which keeps
 * the lifecycle inside a single instance simple. See {@link #resetToCheckpoint} and {@link State}
 * for the resulting state machine.
 */
public class CommittingWriteOperatorCoordinator implements OperatorCoordinator {

    private static final Logger LOG =
            LoggerFactory.getLogger(CommittingWriteOperatorCoordinator.class);

    private final OperatorCoordinator.Context context;
    private final Committer.Factory<Committable, ManifestCommittable> committerFactory;
    private final boolean streamingCheckpointEnabled;
    private final boolean failoverAfterRecovery;
    private final int parallelism;

    private final WriterCommittables[] subtaskCommittables;
    private final ListSerializer<Committable> committableSerializer;
    private final CoordinatorStateSerializer stateSerializer;
    private final ExecutorService commitExecutor;
    private final SimpleWatermarkValve watermarkValve;

    // Populated by resetToCheckpoint and consumed by start. Plain fields are sufficient: both
    // callbacks run on the same scheduler thread in order.
    private long restoredCheckpointId = OperatorCoordinator.NO_CHECKPOINT;
    private byte[] restoredCheckpointData;

    private State state;
    private Committer<Committable, ManifestCommittable> committer;
    private String commitUser;
    private MemoryBackendStateStore stateStore;

    public CommittingWriteOperatorCoordinator(
            OperatorCoordinator.Context context,
            Committer.Factory<Committable, ManifestCommittable> committerFactory,
            boolean streamingCheckpointEnabled,
            String initialCommitUser,
            boolean failoverAfterRecovery) {
        this.context = context;
        this.committerFactory = committerFactory;
        this.streamingCheckpointEnabled = streamingCheckpointEnabled;
        this.commitUser = initialCommitUser;
        this.failoverAfterRecovery = failoverAfterRecovery;
        this.parallelism = context.currentParallelism();
        this.subtaskCommittables = new WriterCommittables[parallelism];
        this.committableSerializer = CommittableEvent.createCommittableListSerializer();
        this.stateSerializer = new CoordinatorStateSerializer();
        this.commitExecutor =
                Executors.newSingleThreadExecutor(
                        new CoordinatorExecutorThreadFactory("WriteCommitCoordinator", context));
        this.watermarkValve = new SimpleWatermarkValve(parallelism);
        this.state = State.CREATED;
    }

    @Override
    public void start() throws Exception {
        // Invoked at most once. If resetToCheckpoint ran first it already moved state to RESTORING
        // and stashed the bytes; otherwise we are in CREATED and there is nothing to restore.
        checkState(
                state == State.CREATED || state == State.RESTORING,
                "Coordinator already started, illegal state %s",
                state);
        runInEventLoop(
                () -> {
                    if (state == State.RESTORING) {
                        restoreState(restoredCheckpointId, restoredCheckpointData);
                        // not needed after deserialization; release the reference
                        restoredCheckpointData = null;
                        initializeCommitter(true);
                        // stay in RESTORING until writers re-emit committables and align catches up
                    } else {
                        restoreState(OperatorCoordinator.NO_CHECKPOINT, null);
                        initializeCommitter(false);
                        transitionState(State.RUNNING);
                    }
                },
                "starting");
    }

    @Override
    public void close() throws Exception {
        transitionState(State.CLOSED);
        if (commitExecutor != null) {
            commitExecutor.shutdownNow();
        }
        if (committer != null) {
            committer.close();
            committer = null;
        }
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        runCheckpointInEventLoop(
                () -> {
                    if (state != State.RUNNING) {
                        // if checkpoint is executed before finishing restoring, just fail it
                        result.completeExceptionally(
                                new IllegalStateException(
                                        "Checkpoint of commit coordinator should be taken in RUNNING state, while current state is "
                                                + state));
                        return;
                    }
                    committer.snapshotState();
                    byte[] checkpointData =
                            SimpleVersionedSerialization.writeVersionAndSerialize(
                                    stateSerializer,
                                    new CoordinatorState(
                                            commitUser,
                                            watermarkValve.getCurrentWatermark(),
                                            stateStore.getSerializedStates()));
                    result.complete(checkpointData);
                },
                result,
                "taking checkpoint %d",
                checkpointId);
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        runInEventLoop(
                () -> {
                    if (event instanceof CommittableEvent) {
                        handleCommittableEvent(subtask, (CommittableEvent) event);
                    } else if (event instanceof WatermarkEvent) {
                        handleWatermarkEvent(subtask, (WatermarkEvent) event);
                    } else {
                        // TODO: end input handling
                        throw new UnsupportedOperationException("Unsupported event type: " + event);
                    }
                },
                "handling operator event %s from subtask %d (#%d)",
                event,
                subtask,
                attemptNumber);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        runInEventLoop(
                () -> {
                    if (state != State.RUNNING) {
                        throw new IllegalStateException(
                                "Completing checkpoint should be notified in RUNNING state, while current state is "
                                        + state);
                    }
                    // writers always report a committable per (subtask, checkpoint) during
                    // snapshot, even if empty; missing means the writer is broken
                    if (!alignCommittables(checkpointId)) {
                        throw new IllegalStateException("Not all committables reported by writer");
                    }
                    commitUpToCheckpoint(
                            checkpointId,
                            pollManifestCommittablesForCheckpoint(
                                    checkpointId,
                                    subtaskCommittables,
                                    committer,
                                    watermarkValve.getCurrentWatermark()),
                            committables -> {
                                try {
                                    committer.commit(committables);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
                },
                "completing checkpoint %d",
                checkpointId);
    }

    /**
     * Called by the framework at most once, before {@link #start()}. May be skipped entirely if the
     * job has no checkpoint or savepoint to restore from; in that case the coordinator goes
     * straight from {@code CREATED} to {@code RUNNING} in {@code start()}.
     *
     * <p>When invoked, {@code checkpointId} is the persisted checkpoint id and {@code
     * checkpointData} is its bytes; for batch mode, disabled checkpointing, or no completed
     * checkpoint, the framework calls it with {@link OperatorCoordinator#NO_CHECKPOINT} and {@code
     * null}, which is treated as a no-op.
     *
     * <p>The wrapping {@link RecreateOnResetOperatorCoordinator} replaces this instance with a
     * fresh one before any further reset, so this method is never called on an already-started
     * coordinator.
     */
    @Override
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
        checkState(
                state == State.CREATED,
                "resetToCheckpoint must run before start, but current state is %s",
                state);
        if (checkpointId == OperatorCoordinator.NO_CHECKPOINT) {
            // nothing to restore; start() will initialize an empty committer
            return;
        }
        restoredCheckpointId = checkpointId;
        restoredCheckpointData = checkpointData;
        transitionState(State.RESTORING);
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        runInEventLoop(
                () -> {
                    WriterCommittables writerCommittables = subtaskCommittables[subtask];
                    if (writerCommittables != null) {
                        // sanity check subtask state
                        Map<Long, List<Committable>> committables =
                                writerCommittables.getCommittablesBeforeCheckpoint(
                                        checkpointId, false);
                        if (!committables.isEmpty()) {
                            throw new IllegalStateException(
                                    String.format(
                                            "Writer [%d] contains invalid committables before checkpoint %d",
                                            subtask, checkpointId));
                        }
                        writerCommittables.reset();
                    }
                },
                "resetting subtask %d to checkpoint %d",
                subtask,
                checkpointId);
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, Throwable reason) {}

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {}

    private void handleCommittableEvent(int subtask, CommittableEvent event) throws Exception {
        if (state == State.RESTORING) {
            updateSubtaskCommittables(subtask, event);
            if (alignCommittables(event.getCheckpointId())) {
                recover(event.getCheckpointId());
                transitionState(State.RUNNING);
            }
        } else if (state == State.RUNNING) {
            if (event.isRestoring()) {
                // a region failover replayed restoring committables while the coordinator itself is
                // not restoring; it already holds the committed state, so ignore them
                LOG.info(
                        "Ignore restoring committables from subtask {} of checkpoint {}, coordinator is running.",
                        subtask,
                        event.getCheckpointId());
            } else {
                updateSubtaskCommittables(subtask, event);
            }
        } else {
            throw new IllegalStateException(
                    "Illegal state " + state + " while handling committable event " + event);
        }
    }

    private void updateSubtaskCommittables(int subtask, CommittableEvent event) throws IOException {
        WriterCommittables incoming = WriterCommittables.from(event, committableSerializer);
        if (subtaskCommittables[subtask] != null) {
            subtaskCommittables[subtask].mergeWith(incoming);
        } else {
            subtaskCommittables[subtask] = incoming;
        }
    }

    private void handleWatermarkEvent(int subtask, WatermarkEvent event) {
        watermarkValve.updateSubtaskWatermark(subtask, event.getWatermark());
    }

    private boolean alignCommittables(long checkpointId) {
        for (WriterCommittables committables : subtaskCommittables) {
            if (committables == null || committables.getMaxCheckpointId() < checkpointId) {
                return false;
            }
        }
        return true;
    }

    // replaces CommittableStateManager because committables are not stored in the committer
    private void recover(long checkpointId) throws Exception {
        if (failoverAfterRecovery) {
            // recommit the restored committables and trigger a failover to reinitialize all writers
            commitUpToCheckpoint(
                    checkpointId,
                    pollManifestCommittablesForCheckpoint(
                            checkpointId,
                            subtaskCommittables,
                            committer,
                            watermarkValve.getCurrentWatermark()),
                    committables -> {
                        int numCommitted = committer.filterAndCommit(committables, true, true);
                        if (numCommitted > 0) {
                            throw new RuntimeException(
                                    "This exception is intentionally thrown after committing the "
                                            + "restored checkpoints. By restarting the job we hope "
                                            + "that writers can start writing based on these new commits.");
                        }
                    });
        } else {
            // just abandon the restoring committables
            for (WriterCommittables subtaskCommit : subtaskCommittables) {
                subtaskCommit.clearCommittablesBeforeCheckpoint(checkpointId, true);
            }
        }
    }

    @VisibleForTesting
    static NavigableMap<Long, ManifestCommittable> pollManifestCommittablesForCheckpoint(
            long checkpointId,
            WriterCommittables[] subtaskCommittables,
            Committer<Committable, ManifestCommittable> committer,
            long watermark)
            throws IOException {
        NavigableMap<Long, ManifestCommittable> committablesPerCheckpoint = new TreeMap<>();
        for (WriterCommittables committables : subtaskCommittables) {
            NavigableMap<Long, List<Committable>> perCheckpoint =
                    committables.getCommittablesBeforeCheckpoint(checkpointId, true);
            for (Map.Entry<Long, List<Committable>> entry : perCheckpoint.entrySet()) {
                long currentCheckpointId = entry.getKey();
                List<Committable> currentCommittables = entry.getValue();
                if (currentCommittables.isEmpty()) {
                    continue;
                }
                ManifestCommittable manifestCommittable =
                        committablesPerCheckpoint.get(currentCheckpointId);
                if (manifestCommittable == null) {
                    committablesPerCheckpoint.put(
                            currentCheckpointId,
                            committer.combine(currentCheckpointId, watermark, currentCommittables));
                } else {
                    committer.combine(
                            currentCheckpointId,
                            watermark,
                            manifestCommittable,
                            currentCommittables);
                }
            }
            committables.clearCommittablesBeforeCheckpoint(checkpointId, true);
        }
        return committablesPerCheckpoint;
    }

    private void commitUpToCheckpoint(
            long checkpointId, Map<Long, ManifestCommittable> toCommit, CommitAction commitAction)
            throws Exception {
        List<ManifestCommittable> committables = new ArrayList<>(toCommit.values());
        if (committables.isEmpty() && committer.forceCreatingSnapshot()) {
            committables =
                    Collections.singletonList(
                            committer.combine(
                                    checkpointId,
                                    watermarkValve.getCurrentWatermark(),
                                    Collections.emptyList()));
        }
        commitAction.accept(committables);
    }

    private void restoreState(long checkpointId, byte[] checkpointData) throws IOException {
        if (checkpointData == null) {
            stateStore = new MemoryBackendStateStore();
        } else {
            CoordinatorState coordinatorState =
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            stateSerializer, checkpointData);
            commitUser = coordinatorState.getCommitUser();
            watermarkValve.reset(coordinatorState.getWatermark());
            stateStore = new MemoryBackendStateStore(coordinatorState.getCommitterStates());
        }
    }

    private void initializeCommitter(boolean isRestored) {
        // Coordinator runs at parallelism 1 (single instance per JobVertex), matching
        // CommitterOperator's contract; hardcode parallelism=1 / subtaskIndex=0
        Committer.Context committerContext =
                Committer.createContext(
                        commitUser,
                        context.metricGroup(),
                        streamingCheckpointEnabled,
                        isRestored,
                        stateStore,
                        1,
                        0);
        committer = committerFactory.create(committerContext);
    }

    private void transitionState(State targetState) {
        if (state != targetState) {
            LOG.info("Transition state from {} to {}", state, targetState);
            state = targetState;
        }
    }

    /**
     * Block until every action previously submitted to the single-thread commit executor has
     * finished. Tests use this as a fence after firing events into the coordinator.
     */
    @VisibleForTesting
    public void waitProcessAllActions() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        runInEventLoop(() -> future.complete(null), "waitProcessAllActions");
        future.get();
    }

    @VisibleForTesting
    void runInEventLoop(
            ThrowingRunnable<Throwable> action,
            String actionName,
            Object... actionNameFormatParameters) {
        commitExecutor.execute(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        LOG.error(
                                "Uncaught exception in CommittingWriteOperatorCoordinator while {}. Triggering job failover.",
                                String.format(actionName, actionNameFormatParameters),
                                t);
                        context.failJob(t);
                    }
                });
    }

    /**
     * Same as {@link #runInEventLoop} but also completes {@code result} exceptionally on failure,
     * so that Flink's checkpoint coordinator can abort the checkpoint immediately instead of
     * waiting for the checkpoint timeout.
     */
    private void runCheckpointInEventLoop(
            ThrowingRunnable<Throwable> action,
            CompletableFuture<?> result,
            String actionName,
            Object... actionNameFormatParameters) {
        commitExecutor.execute(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        LOG.error(
                                "Uncaught exception in CommittingWriteOperatorCoordinator while {}. Triggering job failover.",
                                String.format(actionName, actionNameFormatParameters),
                                t);
                        result.completeExceptionally(t);
                        context.failJob(t);
                    }
                });
    }

    @VisibleForTesting
    State getCurrentState() {
        return state;
    }

    @VisibleForTesting
    String getCommitUser() {
        return commitUser;
    }

    /**
     * Lifecycle state of the commit coordinator.
     *
     * <pre>
     *   CREATED ──(resetToCheckpoint, real id)──► RESTORING ──(writers re-emit, align done)──► RUNNING ──► CLOSED
     *      │                                                                                    ▲
     *      └────────────────(start, nothing to restore)─────────────────────────────────────────┘
     * </pre>
     */
    public enum State {
        /** Initial state; resetToCheckpoint may move it to RESTORING before start. */
        CREATED,

        /**
         * Restored state has been loaded, but commits are still rejected until every writer subtask
         * has re-emitted its pending committables and alignment catches up.
         */
        RESTORING,

        /** Accepting checkpoints and commits. */
        RUNNING,

        CLOSED
    }

    /** Commits a list of {@link ManifestCommittable}s, possibly throwing checked exceptions. */
    private interface CommitAction {
        void accept(List<ManifestCommittable> committables) throws Exception;
    }

    /**
     * Provider that wraps the inner {@link CommittingWriteOperatorCoordinator} in a {@link
     * RecreateOnResetOperatorCoordinator}: on global failover the inner is replaced with a fresh
     * instance, so the inner never needs to handle "reset after start".
     */
    public static class Provider extends RecreateOnResetOperatorCoordinator.Provider {

        private static final long serialVersionUID = 1L;

        private final Committer.Factory<Committable, ManifestCommittable> committerFactory;
        private final boolean streamingCheckpointEnabled;
        private final String initialCommitUser;
        private final boolean failoverAfterRecovery;

        public Provider(
                OperatorID operatorId,
                Committer.Factory<Committable, ManifestCommittable> committerFactory,
                boolean streamingCheckpointEnabled,
                String initialCommitUser,
                boolean failoverAfterRecovery) {
            super(operatorId);
            this.committerFactory = committerFactory;
            this.streamingCheckpointEnabled = streamingCheckpointEnabled;
            this.initialCommitUser = initialCommitUser;
            this.failoverAfterRecovery = failoverAfterRecovery;
        }

        @Override
        public OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
            return new CommittingWriteOperatorCoordinator(
                    context,
                    committerFactory,
                    streamingCheckpointEnabled,
                    initialCommitUser,
                    failoverAfterRecovery);
        }
    }
}
