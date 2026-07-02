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
import org.apache.paimon.flink.sink.coordinator.CommittableEvent;
import org.apache.paimon.flink.sink.coordinator.CommittingWriteOperatorCoordinator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    /** Persisted buffer of committables not yet acknowledged by the coordinator. */
    private transient ListState<Committable> pendingCommittableState;

    /** In-memory view of {@link #pendingCommittableState}, grouped by checkpoint id. */
    private transient NavigableMap<Long, List<Committable>> pendingCommittables;

    private transient ListSerializer<Committable> serializer;

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
        serializer = CommittableEvent.createCommittableListSerializer();
        pendingCommittableState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(
                                        new ListStateDescriptor<>(
                                                PENDING_COMMITTABLE_STATE_NAME,
                                                BytePrimitiveArraySerializer.INSTANCE)),
                        new CommittableSerializer(new CommitMessageSerializer()));
        pendingCommittables = new TreeMap<>();

        if (context.isRestored()) {
            // Always replay on restore, even if the buffer is empty: the coordinator relies on
            // this signal to drive its own restore alignment.
            Preconditions.checkState(context.getRestoredCheckpointId().isPresent());
            long checkpointId = context.getRestoredCheckpointId().getAsLong();
            List<Committable> restored = new ArrayList<>();
            pendingCommittableState.get().forEach(restored::add);
            LOG.info("Restore {} of checkpoint {} from state", restored, checkpointId);
            pendingCommittableState.clear();
            sendCommittableEventToCoordinator(checkpointId, true, restored);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        pendingCommittableState.clear();
        for (List<Committable> committables : pendingCommittables.values()) {
            pendingCommittableState.addAll(committables);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        // operator state already persisted these; we no longer need to replay them on the next
        // restore
        Map<Long, List<Committable>> completed = pendingCommittables.headMap(checkpointId, true);
        completed.clear();
    }

    @Override
    protected void emitCommittables(boolean waitCompaction, long checkpointId) throws IOException {
        List<Committable> committables = prepareCommit(waitCompaction, checkpointId);
        // send the event regardless of whether the list is empty: the coordinator uses an event
        // per (subtask, checkpoint) for alignment
        sendCommittableEventToCoordinator(checkpointId, false, committables);
        if (!committables.isEmpty()) {
            pendingCommittables.put(checkpointId, committables);
        }
        // Still forward committables downstream even though the commit happens on the coordinator.
        // The downstream is a DiscardingSink, but emitting keeps numRecordsOut observable and
        // preserves the operator's IO metrics.
        committables.forEach(committable -> output.collect(new StreamRecord<>(committable)));
    }

    private void sendCommittableEventToCoordinator(
            long checkpointId, boolean isRestoring, List<Committable> committables)
            throws IOException {
        CommittableEvent event =
                CommittableEvent.create(checkpointId, isRestoring, committables, serializer);
        operatorEventGateway.sendEventToCoordinator(event);
    }

    @VisibleForTesting
    NavigableMap<Long, List<Committable>> getPendingCommittables() {
        return pendingCommittables;
    }
}
