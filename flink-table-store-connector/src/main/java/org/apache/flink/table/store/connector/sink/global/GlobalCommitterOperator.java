/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.connector.sink.global;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An operator that processes committables of a {@link Sink}. */
public class GlobalCommitterOperator<CommT, GlobalCommT> extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<CommittableMessage<CommT>, Void>, BoundedOneInput {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalCommitterOperator.class);

    /** Record all the committables until commit. */
    private final Deque<CommT> committables = new ArrayDeque<>();

    /**
     * Aggregate committables to global committables and commit the global committables to the
     * external system.
     */
    private final GlobalCommitter<CommT, GlobalCommT> globalCommitter;

    /** The operator's state descriptor. */
    private static final ListStateDescriptor<byte[]> STREAMING_COMMITTER_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "streaming_committer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

    /** Group the committable by the checkpoint id. */
    private final NavigableMap<Long, GlobalCommT> committablesPerCheckpoint;

    /** The committable's serializer. */
    private final SimpleVersionedSerializer<GlobalCommT> committableSerializer;

    /** The operator's state. */
    private ListState<GlobalCommT> streamingCommitterState;

    public GlobalCommitterOperator(
            GlobalCommitter<CommT, GlobalCommT> globalCommitter,
            SimpleVersionedSerializer<GlobalCommT> committableSerializer) {
        this.globalCommitter = checkNotNull(globalCommitter);
        this.committableSerializer = committableSerializer;
        this.committablesPerCheckpoint = new TreeMap<>();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        streamingCommitterState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(STREAMING_COMMITTER_RAW_STATES_DESC),
                        committableSerializer);
        List<GlobalCommT> restored = new ArrayList<>();
        streamingCommitterState.get().forEach(restored::add);
        streamingCommitterState.clear();
        globalCommitter.commit(globalCommitter.filterRecoveredCommittables(restored));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        List<CommT> committables = pollCommittables();
        if (committables.size() > 0) {
            committablesPerCheckpoint.put(
                    context.getCheckpointId(),
                    globalCommitter.combine(context.getCheckpointId(), committables));
        }
        streamingCommitterState.update(new ArrayList<>(committablesPerCheckpoint.values()));
    }

    @Override
    public void endInput() throws Exception {
        List<CommT> allCommittables = pollCommittables();
        if (!allCommittables.isEmpty()) {
            globalCommitter.commit(
                    Collections.singletonList(
                            globalCommitter.combine(Long.MAX_VALUE, allCommittables)));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        LOG.info("Committing the state for checkpoint {}", checkpointId);
        NavigableMap<Long, GlobalCommT> headMap =
                committablesPerCheckpoint.headMap(checkpointId, true);
        globalCommitter.commit(new ArrayList<>(headMap.values()));
        headMap.clear();
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<CommT>> element) {
        CommittableMessage<CommT> message = element.getValue();
        if (message instanceof CommittableWithLineage) {
            this.committables.addAll(
                    Collections.singletonList(
                            ((CommittableWithLineage<CommT>) message).getCommittable()));
        }
    }

    @Override
    public void close() throws Exception {
        globalCommitter.close();
        committablesPerCheckpoint.clear();
        committables.clear();
        super.close();
    }

    private List<CommT> pollCommittables() {
        List<CommT> committables = new ArrayList<>(this.committables);
        this.committables.clear();
        return committables;
    }
}
