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
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link CommittableStateManager} which stores uncommitted {@link ManifestCommittable}s in state.
 *
 * <p>When the job restarts, these {@link ManifestCommittable}s will be restored and committed, then
 * an intended failure will occur, hoping that after the job restarts, all writers can start writing
 * based on the restored snapshot.
 *
 * <p>Useful for committing snapshots containing records. For example snapshots produced by table
 * store writers.
 */
public class RestoreAndFailCommittableStateManager<GlobalCommitT>
        implements CommittableStateManager<GlobalCommitT> {

    private static final long serialVersionUID = 1L;

    /** The committable's serializer. */
    private final SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer;

    /** GlobalCommitT state of this job. Used to filter out previous successful commits. */
    private ListState<GlobalCommitT> streamingCommitterState;

    public RestoreAndFailCommittableStateManager(
            SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer) {
        this.committableSerializer = committableSerializer;
    }

    @Override
    public void initializeState(
            StateInitializationContext context, Committer<?, GlobalCommitT> committer)
            throws Exception {
        streamingCommitterState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(
                                        new ListStateDescriptor<>(
                                                "streaming_committer_raw_states",
                                                BytePrimitiveArraySerializer.INSTANCE)),
                        new VersionedSerializerWrapper<>(committableSerializer.get()));
        List<GlobalCommitT> restored = new ArrayList<>();
        streamingCommitterState.get().forEach(restored::add);
        streamingCommitterState.clear();
        recover(restored, committer);
    }

    private void recover(List<GlobalCommitT> committables, Committer<?, GlobalCommitT> committer)
            throws Exception {
        int numCommitted = committer.filterAndCommit(committables);
        if (numCommitted > 0) {
            throw new RuntimeException(
                    "This exception is intentionally thrown "
                            + "after committing the restored checkpoints. "
                            + "By restarting the job we hope that "
                            + "writers can start writing based on these new commits.");
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context, List<GlobalCommitT> committables)
            throws Exception {
        streamingCommitterState.update(committables);
    }
}
