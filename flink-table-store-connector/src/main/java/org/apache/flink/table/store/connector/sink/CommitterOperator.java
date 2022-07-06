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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Committer operator to commit {@link Committable}. */
public class CommitterOperator extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<Committable, Committable>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** Record all the inputs until commit. */
    private final Deque<Committable> inputs = new ArrayDeque<>();

    /** Group the committable by the checkpoint id. */
    private final NavigableMap<Long, ManifestCommittable> committablesPerCheckpoint;

    /** The committable's serializer. */
    private final SerializableSupplier<SimpleVersionedSerializer<ManifestCommittable>>
            committableSerializer;

    /** ManifestCommittable state of this job. Used to filter out previous successful commits. */
    private ListState<ManifestCommittable> streamingCommitterState;

    private final SerializableFunction<String, Committer> committerFactory;

    /**
     * Aggregate committables to global committables and commit the global committables to the
     * external system.
     */
    private Committer committer;

    public CommitterOperator(
            SerializableFunction<String, Committer> committerFactory,
            SerializableSupplier<SimpleVersionedSerializer<ManifestCommittable>>
                    committableSerializer) {
        this.committableSerializer = committableSerializer;
        this.committablesPerCheckpoint = new TreeMap<>();
        this.committerFactory = checkNotNull(committerFactory);
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // commit user name state of this job
        // each job can only have one user name and this name must be consistent across restarts
        ListState<String> commitUserState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("commit_user_state", String.class));
        // we cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint
        while (true) {
            for (String user : commitUserState.get()) {
                committer = committerFactory.apply(user);
            }
            if (committer != null) {
                break;
            }
            commitUserState.add(UUID.randomUUID().toString());
        }

        streamingCommitterState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(
                                        new ListStateDescriptor<>(
                                                "streaming_committer_raw_states",
                                                BytePrimitiveArraySerializer.INSTANCE)),
                        committableSerializer.get());
        List<ManifestCommittable> restored = new ArrayList<>();
        streamingCommitterState.get().forEach(restored::add);
        streamingCommitterState.clear();
        commit(true, restored);
    }

    public void commit(boolean isRecover, List<ManifestCommittable> committables) throws Exception {
        if (isRecover) {
            committables = committer.filterRecoveredCommittables(committables);
        }
        committer.commit(committables);
    }

    public ManifestCommittable toCommittables(long checkpoint, List<Committable> inputs)
            throws Exception {
        return committer.combine(checkpoint, inputs);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        List<Committable> poll = pollInputs();
        if (poll.size() > 0) {
            committablesPerCheckpoint.put(
                    context.getCheckpointId(), toCommittables(context.getCheckpointId(), poll));
        }
        streamingCommitterState.update(committables(committablesPerCheckpoint));
    }

    private List<ManifestCommittable> committables(NavigableMap<Long, ManifestCommittable> map) {
        return new ArrayList<>(map.values());
    }

    @Override
    public void endInput() throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        commitUpToCheckpoint(checkpointId);
    }

    private void commitUpToCheckpoint(long checkpointId) throws Exception {
        NavigableMap<Long, ManifestCommittable> headMap =
                committablesPerCheckpoint.headMap(checkpointId, true);
        commit(false, committables(headMap));
        headMap.clear();
    }

    @Override
    public void processElement(StreamRecord<Committable> element) {
        output.collect(element);
        this.inputs.add(element.getValue());
    }

    @Override
    public void close() throws Exception {
        committablesPerCheckpoint.clear();
        inputs.clear();
        super.close();
    }

    private List<Committable> pollInputs() {
        List<Committable> poll = new ArrayList<>(this.inputs);
        this.inputs.clear();
        return poll;
    }
}
