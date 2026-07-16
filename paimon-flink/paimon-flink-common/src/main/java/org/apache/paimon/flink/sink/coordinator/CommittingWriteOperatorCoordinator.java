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
import org.apache.paimon.flink.sink.CommittableSerializer;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.state.CoordinatorState;
import org.apache.paimon.flink.sink.state.CoordinatorStateSerializer;
import org.apache.paimon.flink.sink.state.MemoryBackendStateStore;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
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
    private static final long END_INPUT_CHECKPOINT_ID = Long.MAX_VALUE;

    private final OperatorCoordinator.Context context;
    private final Committer.Factory<Committable, ManifestCommittable> committerFactory;
    private final boolean streamingCheckpointEnabled;
    private final boolean failoverAfterRecovery;

    private WriterCommittables[] subtaskCommittables;
    private final TypeSerializer<CheckpointCommittables> committablesSerializer;
    private final CoordinatorStateSerializer stateSerializer;
    private CoordinatorExecutorThreadFactory coordinatorThreadFactory;
    private ExecutorService commitExecutor;

    // Populated by resetToCheckpoint and consumed by start. Plain fields are sufficient: both
    // callbacks run on the same scheduler thread in order.
    private long restoredCheckpointId = OperatorCoordinator.NO_CHECKPOINT;
    private byte[] restoredCheckpointData;

    private State state;
    private Committer<Committable, ManifestCommittable> committer;
    private String commitUser;
    private MemoryBackendStateStore stateStore;
    private boolean endInputCommitted;

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
        this.committablesSerializer =
                new SimpleVersionedSerializerTypeSerializerProxy<>(
                        () ->
                                new CheckpointCommittablesSerializer(
                                        new CommittableSerializer(new CommitMessageSerializer())));
        this.stateSerializer = new CoordinatorStateSerializer();
        this.state = State.CREATED;
        this.endInputCommitted = false;
    }

    @Override
    public void start() throws Exception {
        // Invoked at most once. If resetToCheckpoint ran first it already moved state to RESTORING
        // and stashed the bytes; otherwise we are in CREATED and there is nothing to restore.
        checkState(
                state == State.CREATED || state == State.RESTORING,
                "Coordinator already started, illegal state %s",
                state);
        // RecreateOnResetOperatorCoordinator constructs the inner coordinator with a lazily
        // initialized Context. In particular, AdaptiveBatchScheduler creates the inner instance
        // before currentParallelism() is available. start() is the first lifecycle callback where
        // the Context is guaranteed to be initialized.
        subtaskCommittables = new WriterCommittables[context.currentParallelism()];
        coordinatorThreadFactory =
                new CoordinatorExecutorThreadFactory("WriteCommitCoordinator", context);
        commitExecutor = Executors.newSingleThreadExecutor(coordinatorThreadFactory);
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
                                            commitUser, stateStore.getSerializedStates()));
                    result.complete(checkpointData);
                },
                result,
                "taking checkpoint %d",
                checkpointId);
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        boolean waitForBatchEndInputCommit =
                !streamingCheckpointEnabled
                        && event instanceof CommittableEvent
                        && ((CommittableEvent) event).getCheckpointId() == END_INPUT_CHECKPOINT_ID;
        ThrowingRunnable<Throwable> eventHandler =
                () -> {
                    if (event instanceof CommittableEvent) {
                        handleCommittableEvent(subtask, (CommittableEvent) event);
                    } else if (event instanceof RestoredCommittableEvent) {
                        handleRestoredCommittableEvent(subtask, (RestoredCommittableEvent) event);
                    } else {
                        throw new UnsupportedOperationException("Unsupported event type: " + event);
                    }
                };

        if (waitForBatchEndInputCommit) {
            runInEventLoopSync(
                    eventHandler,
                    "handling operator event %s from subtask %d (#%d)",
                    event,
                    subtask,
                    attemptNumber);
        } else {
            runInEventLoop(
                    eventHandler,
                    "handling operator event %s from subtask %d (#%d)",
                    event,
                    subtask,
                    attemptNumber);
        }
    }

    /**
     * Runs an action in the commit event loop and waits for completion. This is only used for the
     * final end-input event in batch mode without checkpointing. It prevents the asynchronous
     * coordinator event handling from returning before the final commit has completed, which could
     * otherwise let the batch job finish prematurely.
     */
    private void runInEventLoopSync(
            ThrowingRunnable<Throwable> action,
            String actionName,
            Object... actionNameFormatParameters) {
        checkState(
                coordinatorThreadFactory == null
                        || !coordinatorThreadFactory.isCurrentThreadCoordinatorThread(),
                "Cannot synchronously wait for an action submitted to the commit event loop.");
        CompletableFuture<Void> completion = new CompletableFuture<>();
        runInEventLoop(
                () -> {
                    try {
                        action.run();
                        completion.complete(null);
                    } catch (Throwable t) {
                        completion.completeExceptionally(t);
                        throw t;
                    }
                },
                actionName,
                actionNameFormatParameters);

        try {
            completion.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while committing batch end input", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to commit batch end input", e.getCause());
        }
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
                    if (endInputCommitted) {
                        LOG.debug(
                                "Ignore completion of checkpoint {} because end input has already been committed.",
                                checkpointId);
                        return;
                    }
                    // writers always report a committable per (subtask, checkpoint) during
                    // snapshot until they finish. An ended writer covers every later ordinary
                    // checkpoint because it cannot produce more data.
                    if (!alignCommittables(checkpointId)) {
                        throw new IllegalStateException("Not all committables reported by writer");
                    }
                    boolean finalCommit = allSubtasksEndInput();
                    long targetCheckpointId = finalCommit ? END_INPUT_CHECKPOINT_ID : checkpointId;
                    Map<Long, Long> watermarkPerCheckpoint =
                            alignWatermarkPerCheckpoint(targetCheckpointId, subtaskCommittables);
                    commitUpToCheckpoint(
                            targetCheckpointId,
                            pollManifestCommittablesForCheckpoint(
                                    targetCheckpointId,
                                    subtaskCommittables,
                                    watermarkPerCheckpoint,
                                    committer),
                            watermarkPerCheckpoint,
                            committables -> {
                                try {
                                    if (finalCommit) {
                                        committer.filterAndCommit(committables, false, true);
                                    } else {
                                        committer.commit(committables);
                                    }
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
                    if (finalCommit) {
                        endInputCommitted = true;
                    }
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
                        Map<Long, CheckpointCommittables> committables =
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
        if (state == State.RUNNING) {
            if (endInputCommitted && event.getCheckpointId() == END_INPUT_CHECKPOINT_ID) {
                LOG.debug(
                        "Ignore repeated end input event from subtask {} after final commit.",
                        subtask);
                return;
            }
            updateSubtaskCommittables(
                    subtask, WriterCommittables.from(event, committablesSerializer));
            commitEndInputIfCheckpointDisabled();
        } else {
            throw new IllegalStateException(
                    "Illegal state " + state + " while handling committable event " + event);
        }
    }

    private void handleRestoredCommittableEvent(int subtask, RestoredCommittableEvent event)
            throws Exception {
        if (state == State.RESTORING) {
            updateSubtaskCommittables(
                    subtask, WriterCommittables.fromRestore(event, committablesSerializer));
            if (alignCommittables(event.getRestoredCheckpointId())) {
                boolean finalCommit = allSubtasksEndInput();
                recover(finalCommit ? END_INPUT_CHECKPOINT_ID : event.getRestoredCheckpointId());
                if (finalCommit) {
                    endInputCommitted = true;
                }
                transitionState(State.RUNNING);
            }
        } else if (state == State.RUNNING) {
            // A region failover replays entries from an already committed checkpoint. Ordinary
            // entries can be ignored, but an end-input entry is newer than every ordinary
            // checkpoint and may still be waiting for the other subtasks to finish.
            WriterCommittables restored =
                    WriterCommittables.fromRestore(event, committablesSerializer);
            if (restored.isEndInput() && !endInputCommitted) {
                // region failover will reset
                updateSubtaskCommittables(
                        subtask, new WriterCommittables(restored.getEndInputCommittables()));
                commitEndInputIfCheckpointDisabled();
            } else {
                LOG.info(
                        "Ignore restore committables from subtask {} of checkpoint {}, coordinator is running.",
                        subtask,
                        event.getRestoredCheckpointId());
            }
        } else {
            throw new IllegalStateException(
                    "Illegal state "
                            + state
                            + " while handling restore committables event "
                            + event);
        }
    }

    private void updateSubtaskCommittables(int subtask, WriterCommittables incoming) {
        if (subtaskCommittables[subtask] != null) {
            subtaskCommittables[subtask].mergeWith(incoming);
        } else {
            subtaskCommittables[subtask] = incoming;
        }
    }

    private boolean alignCommittables(long checkpointId) {
        for (WriterCommittables committables : subtaskCommittables) {
            if (committables == null || !committables.coversCheckpoint(checkpointId)) {
                return false;
            }
        }
        return true;
    }

    private boolean allSubtasksEndInput() {
        for (WriterCommittables committables : subtaskCommittables) {
            if (committables == null || !committables.isEndInput()) {
                return false;
            }
        }
        return true;
    }

    private void commitEndInputIfCheckpointDisabled() throws Exception {
        if (streamingCheckpointEnabled || endInputCommitted || !allSubtasksEndInput()) {
            return;
        }

        Map<Long, Long> watermarkPerCheckpoint =
                alignWatermarkPerCheckpoint(END_INPUT_CHECKPOINT_ID, subtaskCommittables);
        commitUpToCheckpoint(
                END_INPUT_CHECKPOINT_ID,
                pollManifestCommittablesForCheckpoint(
                        END_INPUT_CHECKPOINT_ID,
                        subtaskCommittables,
                        watermarkPerCheckpoint,
                        committer),
                watermarkPerCheckpoint,
                committables -> committer.filterAndCommit(committables, false, true));
        endInputCommitted = true;
    }

    // replaces CommittableStateManager because committables are not stored in the committer
    private void recover(long checkpointId) throws Exception {
        if (failoverAfterRecovery) {
            // recommit the restored committables and trigger a failover to reinitialize all writers
            Map<Long, Long> watermarkPerCheckpoint =
                    alignWatermarkPerCheckpoint(checkpointId, subtaskCommittables);
            commitUpToCheckpoint(
                    checkpointId,
                    pollManifestCommittablesForCheckpoint(
                            checkpointId, subtaskCommittables, watermarkPerCheckpoint, committer),
                    watermarkPerCheckpoint,
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
            Map<Long, Long> watermarkPerCheckpoint,
            Committer<Committable, ManifestCommittable> committer)
            throws IOException {
        NavigableMap<Long, ManifestCommittable> committablesPerCheckpoint = new TreeMap<>();
        for (WriterCommittables committables : subtaskCommittables) {
            NavigableMap<Long, CheckpointCommittables> perCheckpoint =
                    committables.getCommittablesBeforeCheckpoint(checkpointId, true);
            for (Map.Entry<Long, CheckpointCommittables> entry : perCheckpoint.entrySet()) {
                long currentCheckpointId = entry.getKey();
                List<Committable> currentCommittables = entry.getValue().committables();
                if (currentCommittables.isEmpty()) {
                    continue;
                }
                long watermark = watermarkPerCheckpoint.get(currentCheckpointId);
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
        // A checkpoint could be aligned with all subtasks reporting empty committables; in that
        // case there is nothing to combine, but the per-checkpoint watermark stays available in
        // watermarkPerCheckpoint for commitUpToCheckpoint's forceCreatingSnapshot fallback.
        return committablesPerCheckpoint;
    }

    /**
     * Reduce the per-subtask watermark of each checkpoint (up to {@code checkpointId}, inclusive)
     * into a single watermark to attach to the committed snapshot. Every subtask must have an entry
     * for {@code checkpointId} by contract (writers emit one event per barrier, even empty), so
     * this method observes each subtask through {@link WriterCommittables#watermarkAt}, which
     * returns {@link Long#MIN_VALUE} for missing entries — matching {@code CommitterOperator}'s
     * initial-watermark semantics and giving idle handling a single hook to grow into later.
     */
    @VisibleForTesting
    static Map<Long, Long> alignWatermarkPerCheckpoint(
            long checkpointId, WriterCommittables[] subtaskCommittables) {
        Set<Long> checkpoints = new TreeSet<>();
        for (WriterCommittables committables : subtaskCommittables) {
            checkpoints.addAll(
                    committables.getCommittablesBeforeCheckpoint(checkpointId, true).keySet());
        }
        Map<Long, Long> watermarkPerCheckpoint = new HashMap<>();
        for (long cp : checkpoints) {
            long min = Long.MAX_VALUE;
            for (WriterCommittables committables : subtaskCommittables) {
                min = Math.min(min, committables.watermarkAt(cp));
            }
            watermarkPerCheckpoint.put(cp, min);
        }
        return watermarkPerCheckpoint;
    }

    private void commitUpToCheckpoint(
            long checkpointId,
            Map<Long, ManifestCommittable> toCommit,
            Map<Long, Long> watermarkPerCheckpoint,
            CommitAction commitAction)
            throws Exception {
        List<ManifestCommittable> committables = new ArrayList<>(toCommit.values());
        if (committables.isEmpty() && committer.forceCreatingSnapshot()) {
            // Empty commit: the aligned watermark still needs to travel with the forced snapshot,
            // otherwise the snapshot would silently regress the table's watermark to
            // Long.MIN_VALUE. Writers persist an entry per (subtask, checkpoint), so the map
            // always carries this checkpoint.
            Long watermark = watermarkPerCheckpoint.get(checkpointId);
            checkNotNull(
                    watermark, "watermarkPerCheckpoint is missing checkpoint %s", checkpointId);
            committables =
                    Collections.singletonList(
                            committer.combine(checkpointId, watermark, Collections.emptyList()));
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
