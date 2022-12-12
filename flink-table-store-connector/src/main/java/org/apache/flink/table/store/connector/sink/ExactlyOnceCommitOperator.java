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
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link Committable}s for each snapshot will be committed exactly once.
 *
 * <p>Useful for committing snapshots containing records. For example snapshots produced by table
 * store writers.
 */
public class ExactlyOnceCommitOperator extends AtMostOnceCommitterOperator {

    /** ManifestCommittable state of this job. Used to filter out previous successful commits. */
    private ListState<ManifestCommittable> streamingCommitterState;

    /** The committable's serializer. */
    private final SerializableSupplier<SimpleVersionedSerializer<ManifestCommittable>>
            committableSerializer;

    public ExactlyOnceCommitOperator(
            boolean streamingCheckpointEnabled,
            String initialCommitUser,
            SerializableFunction<String, Committer> committerFactory,
            SerializableSupplier<SimpleVersionedSerializer<ManifestCommittable>>
                    committableSerializer) {
        super(streamingCheckpointEnabled, initialCommitUser, committerFactory);
        this.committableSerializer = committableSerializer;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

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
        recover(restored);
    }

    private void recover(List<ManifestCommittable> committables) throws Exception {
        committables = committer.filterRecoveredCommittables(committables);
        if (!committables.isEmpty()) {
            committer.commit(committables);
            throw new RuntimeException(
                    "This exception is intentionally thrown "
                            + "after committing the restored checkpoints. "
                            + "By restarting the job we hope that "
                            + "writers can start writing based on these new commits.");
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        streamingCommitterState.update(committables(committablesPerCheckpoint));
    }
}
