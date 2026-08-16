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
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
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

    @VisibleForTesting
    static final String PENDING_COMMITTABLE_STATE_NAME = "pending_committable_state";

    private final OperatorEventGateway operatorEventGateway;

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

    public CoordinatorCommittingRowDataStoreWriteOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser,
            OperatorEventGateway operatorEventGateway) {
        super(parameters, table, storeSinkWriteProvider, initialCommitUser);
        this.operatorEventGateway = Preconditions.checkNotNull(operatorEventGateway);
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
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        pendingCommittableState.clear();
        pendingCommittableState.addAll(new ArrayList<>(pendingCommittables.values()));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        // operator state already persisted these; we no longer need to replay them on the next
        // restore
        pendingCommittables.headMap(checkpointId, true).clear();
    }

    @Override
    protected void emitCommittables(boolean waitCompaction, long checkpointId) throws IOException {
        List<Committable> committables = prepareCommit(waitCompaction, checkpointId);
        CheckpointCommittables entry =
                new CheckpointCommittables(
                        checkpointId, committables, currentWatermark, currentIdle);
        // Emit an event per (subtask, checkpoint) regardless of whether committables is empty.
        operatorEventGateway.sendEventToCoordinator(
                CommittableEvent.create(checkpointId, entry, eventSerializer));
        // Always buffer the per-checkpoint entry so an empty barrier — even one that has not seen
        // a real watermark yet — survives restore. The coordinator relies on every subtask
        // having an entry for the checkpoint being aligned so its watermark min stays sound.
        pendingCommittables.put(checkpointId, entry);
        // Still forward committables downstream even though the commit happens on the coordinator.
        // The downstream is a DiscardingSink, but emitting keeps numRecordsOut observable and
        // preserves the operator's IO metrics.
        committables.forEach(committable -> output.collect(new StreamRecord<>(committable)));
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
