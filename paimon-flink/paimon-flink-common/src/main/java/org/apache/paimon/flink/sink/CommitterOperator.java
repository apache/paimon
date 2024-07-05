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

import org.apache.paimon.utils.Preconditions;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Operator to commit {@link Committable}s for each snapshot. */
public class CommitterOperator<CommitT, GlobalCommitT> extends AbstractStreamOperator<CommitT>
        implements OneInputStreamOperator<CommitT, CommitT>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** Record all the inputs until commit. */
    private final Deque<CommitT> inputs = new ArrayDeque<>();

    /**
     * If checkpoint is enabled we should do nothing in {@link CommitterOperator#endInput}.
     * Remaining data will be committed in {@link CommitterOperator#notifyCheckpointComplete}. If
     * checkpoint is not enabled we need to commit remaining data in {@link
     * CommitterOperator#endInput}.
     */
    private final boolean streamingCheckpointEnabled;

    /** Whether to check the parallelism while runtime. */
    private final boolean forceSingleParallelism;

    /**
     * This commitUser is valid only for new jobs. After the job starts, this commitUser will be
     * recorded into the states of write and commit operators. When the job restarts, commitUser
     * will be recovered from states and this value is ignored.
     */
    private final String initialCommitUser;

    /** Group the committable by the checkpoint id. */
    protected final NavigableMap<Long, GlobalCommitT> committablesPerCheckpoint;

    private final Committer.Factory<CommitT, GlobalCommitT> committerFactory;

    private final CommittableStateManager<GlobalCommitT> committableStateManager;

    /**
     * Aggregate committables to global committables and commit the global committables to the
     * external system.
     */
    protected Committer<CommitT, GlobalCommitT> committer;

    private transient long currentWatermark;

    private transient boolean endInput;

    private transient String commitUser;

    private final Long endInputWatermark;

    public CommitterOperator(
            boolean streamingCheckpointEnabled,
            boolean forceSingleParallelism,
            boolean chaining,
            String initialCommitUser,
            Committer.Factory<CommitT, GlobalCommitT> committerFactory,
            CommittableStateManager<GlobalCommitT> committableStateManager) {
        this(
                streamingCheckpointEnabled,
                forceSingleParallelism,
                chaining,
                initialCommitUser,
                committerFactory,
                committableStateManager,
                null);
    }

    public CommitterOperator(
            boolean streamingCheckpointEnabled,
            boolean forceSingleParallelism,
            boolean chaining,
            String initialCommitUser,
            Committer.Factory<CommitT, GlobalCommitT> committerFactory,
            CommittableStateManager<GlobalCommitT> committableStateManager,
            Long endInputWatermark) {
        this.streamingCheckpointEnabled = streamingCheckpointEnabled;
        this.forceSingleParallelism = forceSingleParallelism;
        this.initialCommitUser = initialCommitUser;
        this.committablesPerCheckpoint = new TreeMap<>();
        this.committerFactory = checkNotNull(committerFactory);
        this.committableStateManager = committableStateManager;
        this.endInputWatermark = endInputWatermark;
        setChainingStrategy(chaining ? ChainingStrategy.ALWAYS : ChainingStrategy.NEVER);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        Preconditions.checkArgument(
                !forceSingleParallelism || getRuntimeContext().getNumberOfParallelSubtasks() == 1,
                "Committer Operator parallelism in paimon MUST be one.");

        this.currentWatermark = Long.MIN_VALUE;
        this.endInput = false;
        // each job can only have one user name and this name must be consistent across restarts
        // we cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint
        commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);
        // parallelism of commit operator is always 1, so commitUser will never be null
        committer =
                committerFactory.create(
                        Committer.createContext(
                                commitUser,
                                getMetricGroup(),
                                streamingCheckpointEnabled,
                                context.isRestored(),
                                context.getOperatorStateStore()));

        committableStateManager.initializeState(context, committer);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        // Do not consume Long.MAX_VALUE watermark in case of batch or bounded stream
        if (mark.getTimestamp() != Long.MAX_VALUE) {
            this.currentWatermark = mark.getTimestamp();
        }
    }

    private GlobalCommitT toCommittables(long checkpoint, List<CommitT> inputs) throws Exception {
        return committer.combine(checkpoint, currentWatermark, inputs);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        pollInputs();
        committableStateManager.snapshotState(context, committables(committablesPerCheckpoint));
    }

    private List<GlobalCommitT> committables(NavigableMap<Long, GlobalCommitT> map) {
        return new ArrayList<>(map.values());
    }

    @Override
    public void endInput() throws Exception {
        endInput = true;
        if (endInputWatermark != null) {
            currentWatermark = endInputWatermark;
        }

        if (streamingCheckpointEnabled) {
            return;
        }

        pollInputs();
        commitUpToCheckpoint(Long.MAX_VALUE);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        commitUpToCheckpoint(endInput ? Long.MAX_VALUE : checkpointId);
    }

    private void commitUpToCheckpoint(long checkpointId) throws Exception {
        NavigableMap<Long, GlobalCommitT> headMap =
                committablesPerCheckpoint.headMap(checkpointId, true);
        List<GlobalCommitT> committables = committables(headMap);
        committer.commit(committables);
        headMap.clear();

        if (committables.isEmpty()) {
            if (committer.forceCreatingSnapshot()) {
                GlobalCommitT commit = toCommittables(checkpointId, Collections.emptyList());
                committer.commit(Collections.singletonList(commit));
            }
        }
    }

    @Override
    public void processElement(StreamRecord<CommitT> element) {
        output.collect(element);
        this.inputs.add(element.getValue());
    }

    @Override
    public void close() throws Exception {
        committablesPerCheckpoint.clear();
        inputs.clear();
        if (committer != null) {
            committer.close();
        }
        super.close();
    }

    public String getCommitUser() {
        return commitUser;
    }

    private void pollInputs() throws Exception {
        Map<Long, List<CommitT>> grouped = committer.groupByCheckpoint(inputs);

        for (Map.Entry<Long, List<CommitT>> entry : grouped.entrySet()) {
            Long cp = entry.getKey();
            List<CommitT> committables = entry.getValue();
            // To prevent the asynchronous completion of tasks with multiple concurrent bounded
            // stream inputs, which leads to some tasks passing a Committable with cp =
            // Long.MAX_VALUE during the endInput method call of the current checkpoint, while other
            // tasks pass a Committable with Long.MAX_VALUE during other checkpoints hence causing
            // an error here, we have a special handling for Committables with Long.MAX_VALUE:
            // instead of throwing an error, we merge them.
            if (cp != null && cp == Long.MAX_VALUE && committablesPerCheckpoint.containsKey(cp)) {
                // Merge the Long.MAX_VALUE committables here.
                GlobalCommitT commitT =
                        committer.combine(
                                cp,
                                currentWatermark,
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

        this.inputs.clear();
    }
}
