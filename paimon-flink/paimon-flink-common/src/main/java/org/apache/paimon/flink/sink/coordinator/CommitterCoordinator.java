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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.utils.SerializableSupplier;

import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Coordinator for global, as commit node in JM to support region failover for write operators. */
public class CommitterCoordinator<CommitT, GlobalCommitT> {
    public static final long END_INPUT_CHECKPOINT_ID = Long.MAX_VALUE;

    private transient boolean endInput;

    private final boolean streamingCheckpointEnabled;
    private final String initialCommitUser;

    private final Long endInputWatermark;
    protected final NavigableMap<Long, GlobalCommitT> committablesPerCheckpoint;
    protected final Committer.Factory<CommitT, GlobalCommitT> committerFactory;

    protected Committer<CommitT, GlobalCommitT> committer;

    private PwcStateManager stateManager;

    SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer;

    private transient long globalWatermark;

    public CommitterCoordinator(
            boolean streamingCheckpointEnabled,
            String initialCommitUser,
            Committer.Factory<CommitT, GlobalCommitT> committerFactory,
            SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer,
            Long endInputWatermark) {
        this.streamingCheckpointEnabled = streamingCheckpointEnabled;
        this.committablesPerCheckpoint = new TreeMap<>();
        this.committerFactory = checkNotNull(committerFactory);
        this.initialCommitUser = initialCommitUser;
        this.committableSerializer = committableSerializer;
        this.globalWatermark = Long.MIN_VALUE;
        this.endInputWatermark = endInputWatermark;
    }

    public void init(int parallelism) throws Exception {
        this.globalWatermark = Long.MIN_VALUE;
        this.endInput = false;
        if (committer == null) {
            committer =
                    committerFactory.create(
                            Committer.createContext(
                                    this.initialCommitUser,
                                    null,
                                    true,
                                    false,
                                    null,
                                    parallelism,
                                    0));
        }
    }

    public void initStateManager(OperatorID operatorId, String baseDir) throws IOException {
        this.stateManager =
                new PwcStateManager(
                        baseDir,
                        operatorId,
                        new PwcStateSerializer(this.committableSerializer.get()),
                        3);
    }

    public void restore(long restoredCheckpointId) throws IOException {
        List<Long> ckIds = stateManager.listCheckpointIds();

        List<GlobalCommitT> commitsToFilter = new ArrayList<>();

        for (Long ckId : ckIds) {
            if (ckId > restoredCheckpointId) {
                stateManager.deleteState(ckId);
                continue;
            }

            PwcState<GlobalCommitT> state = stateManager.readState(ckId);
            if (state != null) {
                commitsToFilter.add(state.globalCommit);
            }
        }

        if (!commitsToFilter.isEmpty()) {
            committer.filterAndCommit(commitsToFilter, true, false);
        }

        // 清理 HDFS
        for (Long ckId : ckIds) {
            if (ckId <= restoredCheckpointId) {
                stateManager.deleteState(ckId);
            }
        }
    }

    public void processWaterMark(long watermark) {
        if (watermark != Long.MAX_VALUE) {
            this.globalWatermark = Math.max(globalWatermark, watermark);
        }
    }

    private void pollInputs(Collection<CommitT> e) throws Exception {
        Map<Long, List<CommitT>> grouped = committer.groupByCheckpoint(e);

        for (Map.Entry<Long, List<CommitT>> entry : grouped.entrySet()) {
            Long cp = entry.getKey();
            List<CommitT> committables = entry.getValue();
            // To prevent the asynchronous completion of tasks with multiple concurrent bounded
            // stream inputs, which leads to some tasks passing a Committable with cp =
            // END_INPUT_CHECKPOINT_ID during the endInput method call of the current checkpoint,
            // while other tasks pass a Committable with END_INPUT_CHECKPOINT_ID during other
            // checkpoints hence causing an error here, we have a special handling for Committables
            // with END_INPUT_CHECKPOINT_ID: instead of throwing an error, we merge them.
            if (cp != null
                    && cp == END_INPUT_CHECKPOINT_ID
                    && committablesPerCheckpoint.containsKey(cp)) {
                // Merge the END_INPUT_CHECKPOINT_ID committables here.
                GlobalCommitT commitT =
                        committer.combine(
                                cp,
                                globalWatermark,
                                committablesPerCheckpoint.get(cp),
                                committables);
                committablesPerCheckpoint.put(cp, commitT);
            } else if (committablesPerCheckpoint.containsKey(cp)) {
                throw new RuntimeException(
                        String.format(
                                "Repeatedly commit the same checkpoint files. \n"
                                        + "The previous files is %s, \n"
                                        + "and the subsequent files is %s",
                                committablesPerCheckpoint.get(cp), committables));
            } else {
                committablesPerCheckpoint.put(cp, toCommittables(cp, committables));
            }
        }
    }

    public boolean save(List<CommitT> committables, long checkpointId) throws Exception {
        pollInputs(committables);
        if (checkpointId == END_INPUT_CHECKPOINT_ID) {
            endInput();
        }
        persistCheckpointState(checkpointId);
        return true;
    }

    private void persistCheckpointState(long checkpointId) throws IOException {
        PwcState state =
                new PwcState(
                        checkpointId,
                        checkpointId == END_INPUT_CHECKPOINT_ID
                                ? this.committablesPerCheckpoint.lastEntry().getValue()
                                : this.committablesPerCheckpoint.get(checkpointId));
        stateManager.writeState(state);
    }

    public boolean isEndInput() {
        return endInput && streamingCheckpointEnabled;
    }

    private void commitUpToCheckpoint(long checkpointId) throws Exception {
        NavigableMap<Long, GlobalCommitT> headMap =
                committablesPerCheckpoint.headMap(checkpointId, true);
        List<GlobalCommitT> committables = committables(headMap);
        if (committables.isEmpty() && committer.forceCreatingSnapshot()) {
            committables =
                    Collections.singletonList(
                            toCommittables(checkpointId, Collections.emptyList()));
        }

        if (checkpointId == END_INPUT_CHECKPOINT_ID) {
            // In new versions of Flink, if a batch job fails, it might restart from some operator
            // in the middle.
            // If the job is restarted from the commit operator, endInput will be called again, and
            // the same commit messages will be committed again.
            // So when `endInput` is called, we must check if the corresponding snapshot exists.
            // However, if the snapshot does not exist, then append files must be new files. So
            // there is no need to check for duplicated append files.
            committer.filterAndCommit(committables, false, true);
        } else {
            committer.commit(committables);
        }
        headMap.clear();
    }

    private GlobalCommitT toCommittables(long checkpoint, List<CommitT> inputs) throws Exception {
        return committer.combine(checkpoint, globalWatermark, inputs);
    }

    private List<GlobalCommitT> committables(NavigableMap<Long, GlobalCommitT> map) {
        return new ArrayList<>(map.values());
    }

    public void endInput() throws Exception {
        endInput = true;
        if (endInputWatermark != null) {
            globalWatermark = endInputWatermark;
        }

        if (streamingCheckpointEnabled) {
            return; // streaming means still have snapshot and commit!
        }

        commitUpToCheckpoint(END_INPUT_CHECKPOINT_ID);
    }

    public void close() throws Exception {
        committablesPerCheckpoint.clear();
        if (committer != null) {
            committer.close();
        }
    }

    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        commitUpToCheckpoint(endInput ? END_INPUT_CHECKPOINT_ID : checkpointId);
        stateManager.deleteState(checkpointId);
    }

    public void notifyCheckpointAborted(long checkpointId) {
        committablesPerCheckpoint.remove(checkpointId);
        stateManager.deleteState(checkpointId);
    }
}
