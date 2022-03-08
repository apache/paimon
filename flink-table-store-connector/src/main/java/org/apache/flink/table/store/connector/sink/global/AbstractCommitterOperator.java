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
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.SerializableSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/** An operator that processes committables of a {@link Sink}. */
public abstract class AbstractCommitterOperator<IN, CommT>
        extends AbstractStreamOperator<CommittableMessage<IN>>
        implements OneInputStreamOperator<CommittableMessage<IN>, CommittableMessage<IN>>,
                BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCommitterOperator.class);

    /** Record all the inputs until commit. */
    private final Deque<IN> inputs = new ArrayDeque<>();

    /** The operator's state descriptor. */
    private static final ListStateDescriptor<byte[]> STREAMING_COMMITTER_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "streaming_committer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

    /** Group the committable by the checkpoint id. */
    private final NavigableMap<Long, List<CommT>> committablesPerCheckpoint;

    /** The committable's serializer. */
    private final SerializableSupplier<SimpleVersionedSerializer<CommT>> committableSerializer;

    /** The operator's state. */
    private ListState<CommT> streamingCommitterState;

    public AbstractCommitterOperator(
            SerializableSupplier<SimpleVersionedSerializer<CommT>> committableSerializer) {
        this.committableSerializer = committableSerializer;
        this.committablesPerCheckpoint = new TreeMap<>();
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        streamingCommitterState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(STREAMING_COMMITTER_RAW_STATES_DESC),
                        committableSerializer.get());
        List<CommT> restored = new ArrayList<>();
        streamingCommitterState.get().forEach(restored::add);
        streamingCommitterState.clear();
        commit(true, restored);
    }

    public abstract void commit(boolean isRecover, List<CommT> committables) throws Exception;

    public abstract List<CommT> toCommittables(long checkpoint, List<IN> inputs) throws Exception;

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        List<IN> poll = pollInputs();
        if (poll.size() > 0) {
            committablesPerCheckpoint.put(
                    context.getCheckpointId(), toCommittables(context.getCheckpointId(), poll));
        }
        streamingCommitterState.update(committables(committablesPerCheckpoint));
    }

    private List<CommT> committables(NavigableMap<Long, List<CommT>> map) {
        List<CommT> committables = new ArrayList<>();
        map.values().forEach(committables::addAll);
        return committables;
    }

    @Override
    public void endInput() throws Exception {
        List<IN> poll = pollInputs();
        if (!poll.isEmpty()) {
            commit(false, toCommittables(Long.MAX_VALUE, poll));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        LOG.info("Committing the state for checkpoint {}", checkpointId);
        NavigableMap<Long, List<CommT>> headMap =
                committablesPerCheckpoint.headMap(checkpointId, true);
        commit(false, committables(headMap));
        headMap.clear();
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<IN>> element) {
        output.collect(element);
        CommittableMessage<IN> message = element.getValue();
        if (message instanceof CommittableWithLineage) {
            this.inputs.add(((CommittableWithLineage<IN>) message).getCommittable());
        }
    }

    @Override
    public void close() throws Exception {
        committablesPerCheckpoint.clear();
        inputs.clear();
        super.close();
    }

    private List<IN> pollInputs() {
        List<IN> poll = new ArrayList<>(this.inputs);
        this.inputs.clear();
        return poll;
    }
}
