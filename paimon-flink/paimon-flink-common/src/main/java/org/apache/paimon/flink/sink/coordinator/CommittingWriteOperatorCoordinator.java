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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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

    private final OperatorCoordinator.Context context;
    private final Committer.Factory<Committable, ManifestCommittable> committerFactory;
    private final boolean streamingCheckpointEnabled;
    private final int parallelism;
    @Nullable private final SavepointTagger.Factory savepointTaggerFactory;

    private final WriterCommittables[] subtaskCommittables;
    // Successful commit coverage, copied into each later coordinator checkpoint.
    private final long[] terminalCoveredBy;
    // Permission to finish before commit, rebuilt from candidate reports after global recovery.
    // The last terminal candidate never receives this permission: it guards finalization.
    private final long[] earlyReleaseCheckpoint;
    // Scheduler callbacks invalidate targets immediately, even while a commit is in flight.
    private final Map<Integer, SubtaskGateway> releaseTargets = new ConcurrentHashMap<>();
    // Required writers still missing their restore contribution; executor-confined.
    private final BitSet requiredRestoreWriters;
    private boolean recoveryInitialized;
    private final TypeSerializer<CheckpointCommittables> committablesSerializer;
    private final CoordinatorStateSerializer stateSerializer;
    private final ExecutorService commitExecutor;
    // Rebuilt per coordinator instance; state is purely in-memory, matching Flink's
    // StatusWatermarkValve which is also reconstructed per task instance without checkpointing.
    private final WatermarkAligner watermarkAligner;

    // Populated by resetToCheckpoint and consumed by start. Plain fields are sufficient: both
    // callbacks run on the same scheduler thread in order.
    private long restoredCheckpointId = OperatorCoordinator.NO_CHECKPOINT;
    private byte[] restoredCheckpointData;

    // Accessed only on the commit executor. Latch before notifying Flink of failure.
    @Nullable private Throwable fatalFailure;

    private State state;
    private Committer<Committable, ManifestCommittable> committer;
    private String commitUser;
    private MemoryBackendStateStore stateStore;
    // Built in initializeAfterRestore once commitUser is known; null when auto-tag is disabled.
    @Nullable private SavepointTagger savepointTagger;
    private final Long endInputWatermark;
    private ListState<Long> processedWatermarkState;
    @Nullable private Long lastProcessedWatermark;
    private boolean globalFinalizationCompleted;

    @VisibleForTesting
    static final String PROCESSED_WATERMARK_STATE = "coordinator-last-processed-watermark";

    public CommittingWriteOperatorCoordinator(
            OperatorCoordinator.Context context,
            Committer.Factory<Committable, ManifestCommittable> committerFactory,
            boolean streamingCheckpointEnabled,
            String initialCommitUser,
            @Nullable SavepointTagger.Factory savepointTaggerFactory,
            @Nullable Long endInputWatermark) {
        this.endInputWatermark = endInputWatermark;
        this.context = context;
        this.committerFactory = committerFactory;
        this.streamingCheckpointEnabled = streamingCheckpointEnabled;
        this.commitUser = initialCommitUser;
        this.savepointTaggerFactory = savepointTaggerFactory;
        this.parallelism = context.currentParallelism();
        this.subtaskCommittables = new WriterCommittables[parallelism];
        this.terminalCoveredBy = new long[parallelism];
        Arrays.fill(terminalCoveredBy, -1L);
        this.earlyReleaseCheckpoint = new long[parallelism];
        Arrays.fill(earlyReleaseCheckpoint, -1L);
        this.requiredRestoreWriters = new BitSet(parallelism);
        requiredRestoreWriters.set(0, parallelism);
        this.committablesSerializer =
                new SimpleVersionedSerializerTypeSerializerProxy<>(
                        () ->
                                new CheckpointCommittablesSerializer(
                                        new CommittableSerializer(new CommitMessageSerializer())));
        this.stateSerializer = new CoordinatorStateSerializer();
        this.commitExecutor =
                Executors.newSingleThreadExecutor(
                        new CoordinatorExecutorThreadFactory("WriteCommitCoordinator", context));
        this.watermarkAligner = new WatermarkAligner(parallelism);
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
                        initializeAfterRestore(true);
                        recoveryInitialized = true;
                        tryCompleteRecovery();
                        for (int i = 0; i < parallelism; i++) {
                            sendWriterRelease(i, restoredCheckpointId);
                        }
                    } else {
                        restoreState(OperatorCoordinator.NO_CHECKPOINT, null);
                        initializeAfterRestore(false);
                        transitionState(State.RUNNING);
                    }
                },
                "starting");
    }

    @Override
    public void close() throws Exception {
        if (commitExecutor != null) {
            waitProcessAllActions();
        }
        transitionState(State.CLOSED);
        if (commitExecutor != null) {
            commitExecutor.shutdown();
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
                                            stateStore.getSerializedStates(),
                                            terminalCoveredBy));
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
                    } else if (event instanceof RestoredCommittableEvent) {
                        handleRestoredCommittableEvent(subtask, (RestoredCommittableEvent) event);
                    } else {
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
                    if (globalFinalizationCompleted) {
                        for (int i = 0; i < parallelism; i++) {
                            if (earlyReleaseCheckpoint[i] < 0) {
                                sendWriterRelease(i, checkpointId);
                            }
                        }
                        return;
                    }
                    // writers always report a committable per (subtask, checkpoint) during
                    // snapshot, even if empty; missing means the writer is broken
                    if (!alignCommittables(checkpointId)) {
                        throw new IllegalStateException("Not all committables reported by writer");
                    }
                    Map<Long, Long> watermarks =
                            alignWatermarkPerCheckpoint(
                                    checkpointId,
                                    subtaskCommittables,
                                    watermarkAligner,
                                    terminalCoveredBy);
                    commitUpToCheckpoint(
                            checkpointId,
                            collectManifestCommittablesForCheckpoint(
                                    checkpointId, subtaskCommittables, watermarks, committer),
                            watermarks,
                            committer::commit);
                    recordProcessedWatermark(watermarks.get(checkpointId));
                    retireAndPromote(checkpointId);
                },
                "completing checkpoint %d",
                checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // Runs tag I/O on the commit executor, never the JM main thread. An aborted savepoint may
        // already have been tagged by a later checkpoint's completion (cumulative commit), so drop
        // the pending intent and remove any tag that was created.
        runInEventLoop(
                () -> {
                    if (savepointTagger != null) {
                        savepointTagger.dropAborted(checkpointId);
                    }
                },
                "aborting checkpoint %d",
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
        releaseTargets.remove(subtask);
        runInEventLoop(
                () -> {
                    checkState(
                            terminalCoveredBy[subtask] < 0
                                    || checkpointId >= terminalCoveredBy[subtask],
                            "Region reset before terminal coverage requires global recovery");
                    earlyReleaseCheckpoint[subtask] = -1;
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
                    if (state == State.RESTORING && terminalCoveredBy[subtask] < 0) {
                        requiredRestoreWriters.set(subtask);
                    }
                },
                "resetting subtask %d to checkpoint %d",
                subtask,
                checkpointId);
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, Throwable reason) {
        releaseTargets.computeIfPresent(
                subtask,
                (key, gateway) ->
                        gateway.getExecution().getAttemptNumber() == attemptNumber
                                ? null
                                : gateway);
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        checkState(
                subtask == gateway.getSubtask()
                        && attemptNumber == gateway.getExecution().getAttemptNumber(),
                "Invalid writer release gateway");
        releaseTargets.put(subtask, gateway);
        runInEventLoop(
                () -> {
                    if (releaseTargets.get(subtask) == gateway) {
                        sendWriterRelease(subtask, restoredCheckpointId);
                    }
                },
                "replaying finish permission to writer %d",
                subtask);
    }

    private void handleCommittableEvent(int subtask, CommittableEvent event) throws Exception {
        if (state == State.RUNNING) {
            WriterCommittables incoming = WriterCommittables.from(event, committablesSerializer);
            if (terminalCoveredBy[subtask] >= 0) {
                checkState(
                        incoming.getCommittablesPerCheckpoint().values().stream()
                                .allMatch(
                                        entry ->
                                                entry.terminal() && entry.committables().isEmpty()),
                        "Terminal writer reported new output");
                // The task may capture another empty marker before it finishes. Its wait
                // boundary advances even though the original tail is already committed.
                sendWriterRelease(subtask, incoming.getMaxCheckpointId());
                return;
            }
            updateSubtaskCommittables(subtask, incoming);
            releaseEarlyWriter(subtask, incoming.getMaxCheckpointId());
        } else {
            throw new IllegalStateException(
                    "Illegal state " + state + " while handling committable event " + event);
        }
    }

    private void handleRestoredCommittableEvent(int subtask, RestoredCommittableEvent event)
            throws Exception {
        if (terminalCoveredBy[subtask] >= 0) {
            // Already committed payloads may be replayed by a writer that still starts on restore.
            // Validate their boundary, then ignore them without changing durable coverage.
            WriterCommittables restored =
                    WriterCommittables.fromRestore(event, committablesSerializer);
            checkState(
                    event.getRestoredCheckpointId() >= terminalCoveredBy[subtask],
                    "Restore before terminal coverage requires global recovery");
            for (CheckpointCommittables entry : restored.getCommittablesPerCheckpoint().values()) {
                checkState(
                        entry.committables().isEmpty()
                                || entry.checkpointId() <= terminalCoveredBy[subtask],
                        "Terminal writer restored output beyond committed coverage");
            }
            sendWriterRelease(subtask, event.getRestoredCheckpointId());
            return;
        }
        if (state == State.RESTORING) {
            checkState(
                    event.getRestoredCheckpointId() == restoredCheckpointId,
                    "Unexpected restored checkpoint %s, expected %s",
                    event.getRestoredCheckpointId(),
                    restoredCheckpointId);
            updateSubtaskCommittables(
                    subtask, WriterCommittables.fromRestore(event, committablesSerializer));
            requiredRestoreWriters.clear(subtask);
            releaseEarlyWriter(subtask, event.getRestoredCheckpointId());
            tryCompleteRecovery();
        } else if (state == State.RUNNING) {
            // Ordinary region replay was already reconciled before this reset. A covered
            // terminal remains covered in this coordinator instance. An uncovered restored
            // marker needs global reconciliation rather than silently dropping its final files.
            WriterCommittables restored =
                    WriterCommittables.fromRestore(event, committablesSerializer);
            if (terminalCoveredBy[subtask] < 0
                    && restored.hasTerminalCandidate(event.getRestoredCheckpointId())) {
                throw new IllegalStateException(
                        "Uncovered terminal restore requires global recovery");
            }
        } else {
            throw new IllegalStateException(
                    "Illegal state "
                            + state
                            + " while handling restore committables event "
                            + event);
        }
    }

    private void tryCompleteRecovery() throws Exception {
        if (state == State.RESTORING && recoveryInitialized && requiredRestoreWriters.isEmpty()) {
            recover(restoredCheckpointId);
            transitionState(State.RUNNING);
        }
    }

    private void updateSubtaskCommittables(int subtask, WriterCommittables incoming) {
        if (savepointTagger != null) {
            // Collect savepoint intents as events arrive (steady state and restore both funnel
            // here), rebuilding the pending-tag set without checkpointing it.
            for (CheckpointCommittables checkpointCommittables :
                    incoming.getCommittablesPerCheckpoint().values()) {
                if (checkpointCommittables.shouldCreateSavepointTag()) {
                    savepointTagger.add(checkpointCommittables.checkpointId());
                }
            }
        }
        if (subtaskCommittables[subtask] != null) {
            subtaskCommittables[subtask].mergeWith(incoming);
        } else {
            subtaskCommittables[subtask] = incoming;
        }
    }

    private boolean alignCommittables(long checkpointId) {
        for (int i = 0; i < parallelism; i++) {
            if (terminalCoveredBy[i] >= 0 && terminalCoveredBy[i] < checkpointId) {
                continue;
            }
            WriterCommittables committables = subtaskCommittables[i];
            if (committables == null || committables.getMaxCheckpointId() < checkpointId) {
                return false;
            }
        }
        return true;
    }

    private void recover(long checkpointId) throws Exception {
        // Legacy all-terminal state cannot reconstruct a missing finite watermark with no
        // reporters.
        checkState(
                !allTerminal() || lastProcessedWatermark != null || endInputWatermark != null,
                "Restored all-terminal state lacks watermark; configure end-input.watermark");
        if (allTerminal()) {
            // No ordinary writer payload remains. Reconcile without manufacturing a real
            // checkpoint snapshot after a previously published MAX, even with forced snapshots.
            committer.filterAndCommit(Collections.emptyList(), true, true);
            retireAndPromote(checkpointId);
            return;
        }
        Map<Long, Long> watermarks =
                alignWatermarkPerCheckpoint(
                        checkpointId, subtaskCommittables, watermarkAligner, terminalCoveredBy);
        commitUpToCheckpoint(
                checkpointId,
                collectManifestCommittablesForCheckpoint(
                        checkpointId, subtaskCommittables, watermarks, committer),
                watermarks,
                entries -> committer.filterAndCommit(entries, true, true));
        recordProcessedWatermark(watermarks.get(checkpointId));
        retireAndPromote(checkpointId);
    }

    private void recordProcessedWatermark(long watermark) throws Exception {
        lastProcessedWatermark =
                lastProcessedWatermark == null
                        ? watermark
                        : Math.max(lastProcessedWatermark, watermark);
        processedWatermarkState.update(Collections.singletonList(lastProcessedWatermark));
    }

    private boolean allTerminal() {
        return Arrays.stream(terminalCoveredBy).allMatch(coverage -> coverage >= 0);
    }

    private void retireAndPromote(long checkpointId) throws Exception {
        if (savepointTagger != null) {
            savepointTagger.tagUpTo(checkpointId);
        }
        BitSet promoted = new BitSet(parallelism);
        for (int i = 0; i < parallelism; i++) {
            WriterCommittables entries = subtaskCommittables[i];
            if (entries != null) {
                if (terminalCoveredBy[i] < 0 && entries.hasTerminalCandidate(checkpointId)) {
                    terminalCoveredBy[i] = checkpointId;
                    promoted.set(i);
                }
                entries.clearCommittablesBeforeCheckpoint(checkpointId, true);
            }
        }
        if (allTerminal()) {
            if (!globalFinalizationCompleted) {
                long watermark =
                        endInputWatermark != null
                                ? endInputWatermark
                                : checkNotNull(lastProcessedWatermark, "Missing final watermark");
                ManifestCommittable finalization =
                        committer.combine(Long.MAX_VALUE, watermark, Collections.emptyList());
                committer.filterAndCommit(Collections.singletonList(finalization), false, true);
                globalFinalizationCompleted = true;
            }
            // Release the remaining writers only after global finalization succeeds.
            for (int i = 0; i < parallelism; i++) {
                if (earlyReleaseCheckpoint[i] < 0) {
                    sendWriterRelease(i, checkpointId);
                }
            }
            return;
        }
        for (int i = promoted.nextSetBit(0); i >= 0; i = promoted.nextSetBit(i + 1)) {
            if (earlyReleaseCheckpoint[i] < 0) {
                sendWriterRelease(i, checkpointId);
            }
        }
    }

    private void releaseEarlyWriter(int subtask, long checkpointId) {
        if (!subtaskCommittables[subtask].hasTerminalCandidate(checkpointId)) {
            return;
        }
        if (earlyReleaseCheckpoint[subtask] >= 0) {
            // An aborted checkpoint can move this writer's wait boundary forward. Keep its
            // early permission, while the last candidate still guards global finalization.
            if (checkpointId > earlyReleaseCheckpoint[subtask]) {
                earlyReleaseCheckpoint[subtask] = checkpointId;
                sendWriterRelease(subtask, checkpointId);
            }
            return;
        }
        for (int i = 0; i < parallelism; i++) {
            if (terminalCoveredBy[i] < 0
                    && (subtaskCommittables[i] == null
                            || !subtaskCommittables[i].hasTerminalCandidate(checkpointId))) {
                // With one concurrent checkpoint, K's writer state covers an unfinished
                // commit and the commit executor fences the next coordinator checkpoint.
                // Another writer remains responsible for the final completion barrier.
                earlyReleaseCheckpoint[subtask] = checkpointId;
                sendWriterRelease(subtask, checkpointId);
                return;
            }
        }
    }

    private void sendWriterRelease(int subtask, long replayCheckpoint) {
        if (fatalFailure != null) {
            return;
        }
        long releaseCheckpoint = earlyReleaseCheckpoint[subtask];
        if (releaseCheckpoint < 0) {
            if (terminalCoveredBy[subtask] < 0 || (allTerminal() && !globalFinalizationCompleted)) {
                return;
            }
            releaseCheckpoint = terminalCoveredBy[subtask];
        }
        SubtaskGateway gateway = releaseTargets.get(subtask);
        if (gateway != null) {
            // A later restored marker can require a later checkpoint than its original tail.
            long coverage = Math.max(releaseCheckpoint, replayCheckpoint);
            gateway.sendEvent(
                    new TerminalWriterReleaseEvent(
                            subtask, gateway.getExecution().getAttemptNumber(), coverage));
        }
    }

    @VisibleForTesting
    Map<Long, CheckpointCommittables> pendingCommittables(int subtask) {
        return Collections.unmodifiableMap(
                subtaskCommittables[subtask].getCommittablesPerCheckpoint());
    }

    @VisibleForTesting
    long terminalCoveredBy(int subtask) {
        return terminalCoveredBy[subtask];
    }

    @VisibleForTesting
    static NavigableMap<Long, ManifestCommittable> collectManifestCommittablesForCheckpoint(
            long checkpointId,
            WriterCommittables[] subtaskCommittables,
            Map<Long, Long> watermarkPerCheckpoint,
            Committer<Committable, ManifestCommittable> committer)
            throws IOException {
        NavigableMap<Long, ManifestCommittable> committablesPerCheckpoint = new TreeMap<>();
        for (WriterCommittables committables : subtaskCommittables) {
            if (committables == null) {
                continue;
            }
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
        }
        // A checkpoint could be aligned with all subtasks reporting empty committables; in that
        // case there is nothing to combine, but the per-checkpoint watermark stays available in
        // watermarkPerCheckpoint for commitUpToCheckpoint's forceCreatingSnapshot fallback.
        return committablesPerCheckpoint;
    }

    /**
     * Aggregate each pending checkpoint's per-subtask (watermark, idle) pairs into a single
     * watermark by delegating to {@link WatermarkAligner}. Returns a map from checkpoint id to the
     * aligned watermark, covering every checkpoint up to {@code checkpointId} inclusive.
     */
    @VisibleForTesting
    static Map<Long, Long> alignWatermarkPerCheckpoint(
            long checkpointId, WriterCommittables[] subtaskCommittables, WatermarkAligner aligner) {
        return alignWatermarkPerCheckpoint(checkpointId, subtaskCommittables, aligner, null);
    }

    private static Map<Long, Long> alignWatermarkPerCheckpoint(
            long checkpointId,
            WriterCommittables[] subtaskCommittables,
            WatermarkAligner aligner,
            long[] terminalCoveredBy) {
        // TreeSet keeps checkpoint ids in ascending order, matching the aligner's contract that
        // successive align() calls advance monotonically.
        Set<Long> checkpoints = new TreeSet<>();
        for (WriterCommittables committables : subtaskCommittables) {
            if (committables == null) {
                continue;
            }
            checkpoints.addAll(
                    committables.getCommittablesBeforeCheckpoint(checkpointId, true).keySet());
        }
        // An empty checkpoint can still require a forced snapshot with its aligned watermark.
        checkpoints.add(checkpointId);
        Map<Long, Long> watermarkPerCheckpoint = new HashMap<>();
        for (long cp : checkpoints) {
            watermarkPerCheckpoint.put(
                    cp,
                    aligner.align(subtaskWatermarksAt(cp, subtaskCommittables, terminalCoveredBy)));
        }
        return watermarkPerCheckpoint;
    }

    private static SubtaskWatermark[] subtaskWatermarksAt(
            long checkpointId, WriterCommittables[] subtaskCommittables, long[] terminalCoveredBy) {
        SubtaskWatermark[] subtaskWatermarks = new SubtaskWatermark[subtaskCommittables.length];
        for (int i = 0; i < subtaskCommittables.length; i++) {
            if (terminalCoveredBy != null
                    && terminalCoveredBy[i] >= 0
                    && terminalCoveredBy[i] <= checkpointId) {
                subtaskWatermarks[i] = new SubtaskWatermark(Long.MIN_VALUE, true);
                continue;
            }
            // A durably terminal writer need not replay historical watermark entries. For
            // checkpoints before its coverage, retain the ordinary unknown/active fallback.
            WriterCommittables entries = subtaskCommittables[i];
            subtaskWatermarks[i] =
                    new SubtaskWatermark(
                            entries == null ? Long.MIN_VALUE : entries.watermarkAt(checkpointId),
                            entries != null && entries.isIdleAt(checkpointId));
        }
        return subtaskWatermarks;
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

    private void restoreState(long checkpointId, byte[] checkpointData) throws Exception {
        if (checkpointData == null) {
            stateStore = new MemoryBackendStateStore();
        } else {
            CoordinatorState coordinatorState =
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            stateSerializer, checkpointData);
            long[] restoredTerminal = coordinatorState.getTerminalCoveredBy();
            boolean hasTerminal = Arrays.stream(restoredTerminal).anyMatch(value -> value >= 0);
            checkState(
                    !hasTerminal || coordinatorState.getWriterParallelism() == parallelism,
                    "Cannot rescale terminal coordinator state from %s to %s writers",
                    coordinatorState.getWriterParallelism(),
                    parallelism);
            if (hasTerminal) {
                for (int i = 0; i < parallelism; i++) {
                    checkState(
                            restoredTerminal[i] <= checkpointId,
                            "Terminal coverage exceeds restored checkpoint");
                    terminalCoveredBy[i] = restoredTerminal[i];
                    if (restoredTerminal[i] >= 0) {
                        requiredRestoreWriters.clear(i);
                    }
                }
            }
            commitUser = coordinatorState.getCommitUser();
            stateStore = new MemoryBackendStateStore(coordinatorState.getCommitterStates());
        }
        processedWatermarkState =
                stateStore.getListState(
                        new ListStateDescriptor<>(
                                PROCESSED_WATERMARK_STATE, LongSerializer.INSTANCE));
        for (Long watermark : processedWatermarkState.get()) {
            checkState(lastProcessedWatermark == null, "Invalid processed watermark state");
            lastProcessedWatermark = watermark;
        }
    }

    private void initializeAfterRestore(boolean isRestored) {
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
        // Bind the tagger to the (possibly restored) commit user, so findSnapshotsForIdentifiers
        // matches the snapshots this coordinator commits.
        if (savepointTaggerFactory != null) {
            savepointTagger = savepointTaggerFactory.create(commitUser);
        }
    }

    private void transitionState(State targetState) {
        if (state != targetState) {
            LOG.info("Transition state from {} to {}", state, targetState);
            state = targetState;
        }
    }

    /**
     * Block until every action previously submitted to the single-thread commit executor has
     * finished.
     */
    public void waitProcessAllActions() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        // A drain is cleanup, not normal progression; it must also complete after failure.
        commitExecutor.execute(() -> future.complete(null));
        future.get();
    }

    @VisibleForTesting
    void runInEventLoop(
            ThrowingRunnable<Throwable> action,
            String actionName,
            Object... actionNameFormatParameters) {
        commitExecutor.execute(
                () -> {
                    if (fatalFailure != null) {
                        return;
                    }
                    try {
                        action.run();
                    } catch (Throwable t) {
                        fail(t, actionName, actionNameFormatParameters);
                    }
                });
    }

    private void runCheckpointInEventLoop(
            ThrowingRunnable<Throwable> action,
            CompletableFuture<?> result,
            String actionName,
            Object... actionNameFormatParameters) {
        commitExecutor.execute(
                () -> {
                    if (fatalFailure != null) {
                        result.completeExceptionally(fatalFailure);
                        return;
                    }
                    try {
                        action.run();
                    } catch (Throwable t) {
                        result.completeExceptionally(t);
                        fail(t, actionName, actionNameFormatParameters);
                    }
                });
    }

    private void fail(Throwable failure, String actionName, Object... parameters) {
        if (fatalFailure == null) {
            fatalFailure = failure;
            LOG.error(
                    "Fatal coordinator failure while {}. Triggering job failover.",
                    String.format(actionName, parameters),
                    failure);
            context.failJob(failure);
        }
    }

    @VisibleForTesting
    State getCurrentState() {
        return state;
    }

    @VisibleForTesting
    String getCommitUser() {
        return commitUser;
    }

    /** Lifecycle state of the commit coordinator. */
    public enum State {
        /** Initial state; resetToCheckpoint may move it to RESTORING before start. */
        CREATED,

        /**
         * Commits are rejected until restored state is initialized, every required nonterminal
         * writer has replayed its pending committables, and recovery commit succeeds.
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
        @Nullable private final SavepointTagger.Factory savepointTaggerFactory;
        private final Long endInputWatermark;

        public Provider(
                OperatorID operatorId,
                Committer.Factory<Committable, ManifestCommittable> committerFactory,
                boolean streamingCheckpointEnabled,
                String initialCommitUser,
                @Nullable SavepointTagger.Factory savepointTaggerFactory,
                @Nullable Long endInputWatermark) {
            super(operatorId);
            this.endInputWatermark = endInputWatermark;
            this.committerFactory = committerFactory;
            this.streamingCheckpointEnabled = streamingCheckpointEnabled;
            this.initialCommitUser = initialCommitUser;
            this.savepointTaggerFactory = savepointTaggerFactory;
        }

        @Override
        public OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
            return new CommittingWriteOperatorCoordinator(
                    context,
                    committerFactory,
                    streamingCheckpointEnabled,
                    initialCommitUser,
                    savepointTaggerFactory,
                    endInputWatermark);
        }
    }
}
