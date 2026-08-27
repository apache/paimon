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
import org.apache.paimon.flink.sink.coordinator.CheckpointCommittables;
import org.apache.paimon.flink.sink.coordinator.CheckpointCommittablesSerializer;
import org.apache.paimon.flink.sink.coordinator.CommittableEvent;
import org.apache.paimon.flink.sink.coordinator.CommittingWriteOperatorCoordinator;
import org.apache.paimon.flink.sink.coordinator.RestoredCommittableEvent;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
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
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Write operator that hands committables to a JM-side {@link CommittingWriteOperatorCoordinator}
 * which performs the commit.
 *
 * <p>This operator is stateful: it keeps an independent operator state that buffers the
 * per-checkpoint committables which have not yet been acknowledged by the coordinator, so they
 * survive a global failover and can be replayed on restore.
 */
public class CoordinatorCommittingRowDataStoreWriteOperator
        extends StatelessRowDataStoreWriteOperator {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(CoordinatorCommittingRowDataStoreWriteOperator.class);
    private static final long END_INPUT_CHECKPOINT_ID = Long.MAX_VALUE;

    @VisibleForTesting
    static final String PENDING_COMMITTABLE_STATE_NAME = "pending_committable_state";

    private final OperatorEventGateway operatorEventGateway;

    /** Whether savepoint auto-tagging is enabled; when off the writer never flags a tag intent. */
    private final boolean autoTagForSavepoint;

    /** Persisted buffer of pending checkpoints not yet acknowledged by the coordinator. */
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
                if (entry.checkpointId() == END_INPUT_CHECKPOINT_ID) {
                    // The terminal committable can only be replayed from state after a task or
                    // region failover. Ordinary restored checkpoint entries keep their existing
                    // one-shot replay behavior.
                    pendingCommittables.put(entry.checkpointId(), entry);
                    endOfInput = true;
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
        reportToCoordinator(checkpointId);
        pendingCommittableState.clear();
        pendingCommittableState.addAll(new ArrayList<>(pendingCommittables.values()));
        return super.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        // operator state already persisted these; we no longer need to replay them on the next
        // restore
        pendingCommittables.headMap(checkpointId, true).clear();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        if (endOfInput) {
            // The marker proves this real checkpoint was snapshotted after the terminal
            // committable. It deliberately contains no newly prepared data.
            emitCheckpointMarker(checkpointId);
        } else {
            emitCommittables(false, checkpointId);
        }
    }

    @Override
    public void endInput() throws Exception {
        endOfInput = true;
        emitCommittables(true, END_INPUT_CHECKPOINT_ID);
        // endInput is not followed by snapshotState, so report the terminal entry directly.
        reportToCoordinator(END_INPUT_CHECKPOINT_ID);
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
    protected void emitCommittables(boolean waitCompaction, long checkpointId) throws IOException {
        List<Committable> committables = prepareCommit(waitCompaction, checkpointId);
        CheckpointCommittables entry =
                new CheckpointCommittables(
                        checkpointId, committables, currentWatermark, currentIdle);
        // Always buffer the per-checkpoint entry so an empty barrier — even one that has not seen
        // a real watermark yet — survives restore. The coordinator relies on every subtask
        // having an entry for the checkpoint being aligned so its watermark min stays sound.
        pendingCommittables.put(checkpointId, entry);
        // Still forward committables downstream even though the commit happens on the coordinator.
        // The downstream is a DiscardingSink, but emitting keeps numRecordsOut observable and
        // preserves the operator's IO metrics.
        committables.forEach(committable -> output.collect(new StreamRecord<>(committable)));
    }

    private void emitCheckpointMarker(long checkpointId) throws IOException {
        CheckpointCommittables marker =
                new CheckpointCommittables(
                        checkpointId, new ArrayList<>(), currentWatermark, currentIdle);
        pendingCommittables.put(checkpointId, marker);
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
