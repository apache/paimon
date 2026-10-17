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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.coordinator.CheckpointCommittables;
import org.apache.paimon.flink.sink.coordinator.CheckpointCommittablesSerializer;
import org.apache.paimon.flink.sink.coordinator.CommittableEvent;
import org.apache.paimon.flink.sink.coordinator.CommittingWriteOperatorCoordinator;
import org.apache.paimon.flink.sink.coordinator.RestoredCommittableEvent;
import org.apache.paimon.flink.sink.coordinator.TerminalWriterReleaseEvent;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Write operator that hands committables to a JM-side {@link CommittingWriteOperatorCoordinator}
 * which performs the commit.
 *
 * <p>This operator is stateful: it keeps an independent operator state that buffers the
 * per-checkpoint committables which have not yet been covered by a completed Flink checkpoint, so
 * they survive a global failover and can be replayed on restore.
 */
public class CoordinatorCommittingRowDataStoreWriteOperator
        extends StatelessRowDataStoreWriteOperator implements OperatorEventHandler {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(CoordinatorCommittingRowDataStoreWriteOperator.class);

    @VisibleForTesting
    static final String PENDING_COMMITTABLE_STATE_NAME = "pending_committable_state";

    private final OperatorEventGateway operatorEventGateway;

    /** Whether savepoint auto-tagging is enabled; when off the writer never flags a tag intent. */
    private final boolean autoTagForSavepoint;

    /** Persisted buffer of pending checkpoints not yet covered by a completed Flink checkpoint. */
    private transient ListState<CheckpointCommittables> pendingCommittableState;

    /** In-memory view of {@link #pendingCommittableState}, keyed by checkpoint id. */
    private transient NavigableMap<Long, CheckpointCommittables> pendingCommittables;

    /** Latest watermark observed on the input; forwarded on subsequent events. */
    private transient long currentWatermark;

    /**
     * Latest {@code WatermarkStatus} observed on the input, mirroring what Flink's upstream {@code
     * StatusWatermarkValve} exposes. Frozen at barrier time alongside {@link #currentWatermark} so
     * the coordinator can reproduce valve-faithful idle handling from the per-checkpoint entries.
     * Not checkpointed: on restore we default to ACTIVE and let upstream re-emit {@link
     * WatermarkStatus#IDLE} if it still applies, matching Flink valve's rebuilt initial state.
     */
    private transient boolean currentIdle;

    private transient CheckpointCommittablesSerializer stateSerializer;
    private transient TypeSerializer<CheckpointCommittables> eventSerializer;
    private boolean endOfInput;
    private boolean terminalCaptured;
    private final transient MailboxExecutor taskMailbox;
    private final int subtask;
    private final int attemptNumber;
    private long requiredReleaseCheckpoint = -1;
    private long releasedCheckpoint = -1;
    private boolean waitingForRelease;

    public CoordinatorCommittingRowDataStoreWriteOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser,
            OperatorEventGateway operatorEventGateway,
            boolean autoTagForSavepoint) {
        super(parameters, table, storeSinkWriteProvider, initialCommitUser);
        this.operatorEventGateway = Preconditions.checkNotNull(operatorEventGateway);
        this.autoTagForSavepoint = autoTagForSavepoint;
        this.taskMailbox =
                parameters.getContainingTask().getMailboxExecutorFactory().createExecutor(-1);
        this.subtask =
                parameters
                        .getContainingTask()
                        .getEnvironment()
                        .getTaskInfo()
                        .getIndexOfThisSubtask();
        this.attemptNumber =
                parameters.getContainingTask().getEnvironment().getTaskInfo().getAttemptNumber();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        stateSerializer =
                new CheckpointCommittablesSerializer(
                        new CommittableSerializer(new CommitMessageSerializer()));
        // Wire the same versioned serializer as a TypeSerializer for the event channel, so the
        // payload version is embedded in the wire format instead of being carried as a separate
        // event field.
        eventSerializer =
                new SimpleVersionedSerializerTypeSerializerProxy<>(
                        () ->
                                new CheckpointCommittablesSerializer(
                                        new CommittableSerializer(new CommitMessageSerializer())));
        pendingCommittableState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(
                                        new ListStateDescriptor<>(
                                                PENDING_COMMITTABLE_STATE_NAME,
                                                BytePrimitiveArraySerializer.INSTANCE)),
                        stateSerializer);
        pendingCommittables = new TreeMap<>();
        currentWatermark = Long.MIN_VALUE;
        currentIdle = false;

        if (context.isRestored()) {
            Preconditions.checkState(context.getRestoredCheckpointId().isPresent());
            long restoredCheckpointId = context.getRestoredCheckpointId().getAsLong();

            List<CheckpointCommittables> restored = new ArrayList<>();
            for (CheckpointCommittables entry : pendingCommittableState.get()) {
                restored.add(entry);
                Preconditions.checkState(
                        entry.checkpointId() != Long.MAX_VALUE,
                        "Legacy per-writer MAX state cannot be restored by coordinator commit.");
                if (entry.terminal()) {
                    // Replay the original contribution once, but retain the sealed-input fact
                    // independently of the ordinary pending-file buffer.
                    endOfInput = true;
                    terminalCaptured = true;
                    requiredReleaseCheckpoint =
                            requiredReleaseCheckpoint < 0
                                    ? entry.checkpointId()
                                    : Math.min(requiredReleaseCheckpoint, entry.checkpointId());
                }
            }
            pendingCommittableState.clear();

            LOG.info(
                    "Restore pending committables {} of checkpoint {}",
                    restored,
                    restoredCheckpointId);

            // Contract: send exactly one RestoredCommittableEvent per subtask, even when the
            // buffer is empty, so the coordinator can drive its own restore alignment.
            operatorEventGateway.sendEventToCoordinator(
                    RestoredCommittableEvent.create(
                            restoredCheckpointId, restored, eventSerializer));
        }
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception {
        // Ordering within a checkpoint: emitCommittables already ran (in prepareSnapshotPreBarrier,
        // before the barrier) and buffered this checkpoint's committables into pendingCommittables.
        if (autoTagForSavepoint && checkpointOptions.getCheckpointType().isSavepoint()) {
            pendingCommittables.computeIfPresent(
                    checkpointId,
                    (id, checkpointCommittables) ->
                            checkpointCommittables.withShouldCreateSavepointTag(true));
        }
        // Report here, not in emitCommittables, so the savepoint-tag intent is known before
        // sending.
        pendingCommittableState.clear();
        pendingCommittableState.addAll(new ArrayList<>(pendingCommittables.values()));
        reportToCoordinator(checkpointId);
        return super.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Early writers receive permission at terminal reporting; only the last candidate
        // waits for ordinary commit and global finalization before the task can finish.
        if (terminalCaptured && checkpointId >= requiredReleaseCheckpoint && !waitingForRelease) {
            waitingForRelease = true;
            try {
                while (releasedCheckpoint < requiredReleaseCheckpoint) {
                    taskMailbox.yield();
                }
            } finally {
                waitingForRelease = false;
            }
        }
        super.notifyCheckpointComplete(checkpointId);
        // Operator state already persisted these; retain the terminal capture identity for ACKs.
        pendingCommittables.headMap(checkpointId, true).clear();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        if (!autoTagForSavepoint) {
            return;
        }
        // Drop only the savepoint-tag intent on the aborted committables (keep the data). A later
        // checkpoint must not persist a stale intent, or the coordinator would recreate a tag for a
        // gone savepoint. Mirrors the operator path pruning the aborted id.
        pendingCommittables.computeIfPresent(
                checkpointId,
                (id, checkpointCommittables) ->
                        checkpointCommittables.withShouldCreateSavepointTag(false));
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        Preconditions.checkArgument(
                checkpointId != Long.MAX_VALUE, "A real checkpoint is required");
        if (terminalCaptured) {
            // Keep the terminal fact in every later snapshot, including after file retirement.
            pendingCommittables.put(
                    checkpointId,
                    new CheckpointCommittables(
                            checkpointId,
                            Collections.emptyList(),
                            currentWatermark,
                            currentIdle,
                            false,
                            true));
        } else {
            emitCommittables(endOfInput, checkpointId);
        }
    }

    @Override
    public void endInput() {
        endOfInput = true;
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        Preconditions.checkState(!endOfInput, "Cannot write records after EndInput");
        super.processElement(element);
    }

    @Override
    protected void emitCommittables(boolean waitCompaction, long checkpointId) throws IOException {
        // prepareCommit(true, K) waits for flush/compaction before the marker can be captured.
        List<Committable> committables = prepareCommit(waitCompaction, checkpointId);
        CheckpointCommittables entry =
                new CheckpointCommittables(
                        checkpointId,
                        committables,
                        currentWatermark,
                        currentIdle,
                        false,
                        endOfInput);
        pendingCommittables.put(checkpointId, entry);
        terminalCaptured = endOfInput;
        if (terminalCaptured) {
            requiredReleaseCheckpoint = checkpointId;
        }
        // Downstream operators have already ended when the terminal checkpoint is captured.
        // Its tail travels only through the coordinator event sent from snapshotState.
        if (!endOfInput) {
            committables.forEach(committable -> output.collect(new StreamRecord<>(committable)));
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent event) {
        Preconditions.checkArgument(
                event instanceof TerminalWriterReleaseEvent, "Unexpected coordinator event");
        TerminalWriterReleaseEvent release = (TerminalWriterReleaseEvent) event;
        if (terminalCaptured
                && release.getSubtask() == subtask
                && release.getAttemptNumber() == attemptNumber
                && release.getCheckpointId() >= requiredReleaseCheckpoint) {
            releasedCheckpoint = Math.max(releasedCheckpoint, release.getCheckpointId());
        }
    }

    /**
     * Sends the buffered committables for {@code checkpointId} to the coordinator, one per
     * checkpoint.
     */
    private void reportToCoordinator(long checkpointId) throws IOException {
        operatorEventGateway.sendEventToCoordinator(
                CommittableEvent.create(
                        checkpointId, pendingCommittables.get(checkpointId), eventSerializer));
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        // Skip Long.MAX_VALUE watermarks (batch or bounded stream end markers).
        if (mark.getTimestamp() != Long.MAX_VALUE) {
            currentWatermark = mark.getTimestamp();
        }
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        super.processWatermarkStatus(watermarkStatus);
        currentIdle = watermarkStatus.isIdle();
    }

    @VisibleForTesting
    NavigableMap<Long, CheckpointCommittables> getPendingCommittables() {
        return pendingCommittables;
    }
}
