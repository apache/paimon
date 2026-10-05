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

import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.flink.VersionedSerializerWrapper;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.utils.SerializableSupplier;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link CommittableStateManager} which stores uncommitted {@link ManifestCommittable}s in state.
 *
 * <p>When the job restarts, regular checkpoint committables are restored and committed. An
 * incomplete END_INPUT committable remains pending until the operator receives complete end input.
 */
public class RestoreCommittableStateManager<GlobalCommitT>
        implements CommittableStateManager<GlobalCommitT> {

    private static final long serialVersionUID = 1L;

    /** The committable's serializer. */
    private final SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer;

    private final boolean partitionMarkDoneRecoverFromState;

    private final EndInputCommittableHandler<GlobalCommitT> endInputHandler;

    /** GlobalCommitT state of this job. Used to filter out previous successful commits. */
    private ListState<GlobalCommitT> streamingCommitterState;

    /** Whether every committer operator completed end input before the restored checkpoint. */
    private ListState<Boolean> completeEndInputState;

    public RestoreCommittableStateManager(
            SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer,
            boolean partitionMarkDoneRecoverFromState,
            EndInputCommittableHandler<GlobalCommitT> endInputHandler) {
        this.committableSerializer = committableSerializer;
        this.partitionMarkDoneRecoverFromState = partitionMarkDoneRecoverFromState;
        this.endInputHandler = endInputHandler;
    }

    @Override
    public List<GlobalCommitT> initializeState(
            Committer.Context context, Committer<?, GlobalCommitT> committer) throws Exception {
        completeEndInputState =
                context.stateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        "streaming_committer_complete_end_input_state",
                                        BooleanSerializer.INSTANCE));
        boolean hasCompleteEndInputState = false;
        boolean restoredCompleteEndInput = true;
        for (Boolean value : completeEndInputState.get()) {
            hasCompleteEndInputState = true;
            restoredCompleteEndInput &= Boolean.TRUE.equals(value);
        }
        restoredCompleteEndInput &= hasCompleteEndInputState;
        completeEndInputState.clear();

        streamingCommitterState =
                new SimpleVersionedListState<>(
                        context.stateStore()
                                .getListState(
                                        new ListStateDescriptor<>(
                                                "streaming_committer_raw_states",
                                                BytePrimitiveArraySerializer.INSTANCE)),
                        new VersionedSerializerWrapper<>(committableSerializer.get()));
        List<GlobalCommitT> restored = new ArrayList<>();
        streamingCommitterState.get().forEach(restored::add);
        streamingCommitterState.clear();

        List<GlobalCommitT> pendingEndInput = new ArrayList<>();
        if (!restoredCompleteEndInput) {
            restored.removeIf(
                    committable -> {
                        if (endInputHandler.isEndInput(committable)) {
                            if (pendingEndInput.isEmpty()) {
                                pendingEndInput.add(committable);
                            } else {
                                pendingEndInput.set(
                                        0,
                                        endInputHandler.merge(pendingEndInput.get(0), committable));
                            }
                            return true;
                        }
                        return false;
                    });
        }
        recover(restored, committer);
        return pendingEndInput;
    }

    protected int recover(List<GlobalCommitT> committables, Committer<?, GlobalCommitT> committer)
            throws Exception {
        return committer.filterAndCommit(committables, true, partitionMarkDoneRecoverFromState);
    }

    @Override
    public void snapshotState(List<GlobalCommitT> committables, boolean completeEndInput)
            throws Exception {
        streamingCommitterState.update(committables);
        completeEndInputState.update(Collections.singletonList(completeEndInput));
    }
}
