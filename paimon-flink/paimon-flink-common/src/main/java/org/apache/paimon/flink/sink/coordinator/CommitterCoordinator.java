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

import org.apache.paimon.flink.sink.Committer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** JM-side global committer used by PaimonWriterCoordinator. */
public class CommitterCoordinator<CommitT, GlobalCommitT> {

    public static final long END_INPUT_CHECKPOINT_ID = Long.MAX_VALUE;

    private final boolean streamingCheckpointEnabled;
    private final Committer.Factory<CommitT, GlobalCommitT> committerFactory;
    private final Long endInputWatermark;
    private final NavigableMap<Long, GlobalCommitT> committablesPerCheckpoint;

    private Committer<CommitT, GlobalCommitT> committer;
    private long globalWatermark;
    private boolean endInput;

    public CommitterCoordinator(
            boolean streamingCheckpointEnabled,
            Committer.Factory<CommitT, GlobalCommitT> committerFactory,
            Long endInputWatermark) {
        this.streamingCheckpointEnabled = streamingCheckpointEnabled;
        this.committerFactory = checkNotNull(committerFactory);
        this.endInputWatermark = endInputWatermark;
        this.committablesPerCheckpoint = new TreeMap<>();
        this.globalWatermark = Long.MIN_VALUE;
    }

    public void init(int parallelism, String commitUser) throws Exception {
        this.globalWatermark = Long.MIN_VALUE;
        this.endInput = false;
        if (committer == null) {
            committer =
                    committerFactory.create(
                            Committer.createContext(
                                    commitUser,
                                    null,
                                    streamingCheckpointEnabled,
                                    false,
                                    null,
                                    parallelism,
                                    0));
        }
    }

    public void save(List<CommitT> committables, long checkpointId, long watermark)
            throws Exception {
        processWatermark(watermark);
        pollInputs(committables);
        if (checkpointId == END_INPUT_CHECKPOINT_ID) {
            endInput();
        }
    }

    private void pollInputs(Collection<CommitT> inputs) throws Exception {
        Map<Long, List<CommitT>> grouped = committer.groupByCheckpoint(inputs);
        for (Map.Entry<Long, List<CommitT>> entry : grouped.entrySet()) {
            Long checkpoint = entry.getKey();
            List<CommitT> committables = entry.getValue();
            if (checkpoint != null
                    && checkpoint == END_INPUT_CHECKPOINT_ID
                    && committablesPerCheckpoint.containsKey(checkpoint)) {
                GlobalCommitT merged =
                        committer.combine(
                                checkpoint,
                                globalWatermark,
                                committablesPerCheckpoint.get(checkpoint),
                                committables);
                committablesPerCheckpoint.put(checkpoint, merged);
            } else if (committablesPerCheckpoint.containsKey(checkpoint)) {
                continue;
            } else {
                committablesPerCheckpoint.put(
                        checkpoint, committer.combine(checkpoint, globalWatermark, committables));
            }
        }
    }

    private void processWatermark(long watermark) {
        if (watermark != Long.MAX_VALUE) {
            globalWatermark = Math.max(globalWatermark, watermark);
        }
    }

    private void endInput() throws Exception {
        endInput = true;
        if (endInputWatermark != null) {
            globalWatermark = endInputWatermark;
        }
        if (!streamingCheckpointEnabled) {
            commitUpToCheckpoint(END_INPUT_CHECKPOINT_ID);
        }
    }

    public boolean isEndInput() {
        return endInput && streamingCheckpointEnabled;
    }

    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        commitUpToCheckpoint(endInput ? END_INPUT_CHECKPOINT_ID : checkpointId);
    }

    public int filterAndCommitUpToCheckpoint(long checkpointId) throws Exception {
        NavigableMap<Long, GlobalCommitT> headMap =
                committablesPerCheckpoint.headMap(checkpointId, true);
        List<GlobalCommitT> committables = new ArrayList<>(headMap.values());
        if (committables.isEmpty() && committer.forceCreatingSnapshot()) {
            committables =
                    Collections.singletonList(
                            committer.combine(
                                    checkpointId, globalWatermark, Collections.emptyList()));
        }
        int committed = committer.filterAndCommit(committables, true, false);
        headMap.clear();
        return committed;
    }

    private void commitUpToCheckpoint(long checkpointId) throws Exception {
        NavigableMap<Long, GlobalCommitT> headMap =
                committablesPerCheckpoint.headMap(checkpointId, true);
        List<GlobalCommitT> committables = new ArrayList<>(headMap.values());
        if (committables.isEmpty() && committer.forceCreatingSnapshot()) {
            committables =
                    Collections.singletonList(
                            committer.combine(
                                    checkpointId, globalWatermark, Collections.emptyList()));
        }
        if (checkpointId == END_INPUT_CHECKPOINT_ID) {
            committer.filterAndCommit(committables, false, true);
        } else {
            committer.commit(committables);
        }
        headMap.clear();
    }

    public void notifyCheckpointAborted(long checkpointId) {
        // Checkpoint abort is not committable abort. Keep pending committables for a later
        // completed checkpoint.
    }

    public void close() throws Exception {
        committablesPerCheckpoint.clear();
        if (committer != null) {
            committer.close();
        }
    }
}
