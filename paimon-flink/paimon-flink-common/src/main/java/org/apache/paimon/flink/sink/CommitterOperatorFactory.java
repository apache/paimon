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

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * CommitterOperator}.
 */
public class CommitterOperatorFactory<CommitT, GlobalCommitT>
        extends AbstractStreamOperatorFactory<CommitT>
        implements OneInputStreamOperatorFactory<CommitT, CommitT> {
    protected final boolean streamingCheckpointEnabled;

    /** Whether to check the parallelism while runtime. */
    protected final boolean forceSingleParallelism;
    /**
     * This commitUser is valid only for new jobs. After the job starts, this commitUser will be
     * recorded into the states of write and commit operators. When the job restarts, commitUser
     * will be recovered from states and this value is ignored.
     */
    protected final String initialCommitUser;

    /** Group the committable by the checkpoint id. */
    protected final NavigableMap<Long, GlobalCommitT> committablesPerCheckpoint;

    protected final Committer.Factory<CommitT, GlobalCommitT> committerFactory;

    protected final CommittableStateManager<GlobalCommitT> committableStateManager;

    /**
     * Aggregate committables to global committables and commit the global committables to the
     * external system.
     */
    protected Committer<CommitT, GlobalCommitT> committer;

    protected final Long endInputWatermark;

    public CommitterOperatorFactory(
            boolean streamingCheckpointEnabled,
            boolean forceSingleParallelism,
            String initialCommitUser,
            Committer.Factory<CommitT, GlobalCommitT> committerFactory,
            CommittableStateManager<GlobalCommitT> committableStateManager) {
        this(
                streamingCheckpointEnabled,
                forceSingleParallelism,
                initialCommitUser,
                committerFactory,
                committableStateManager,
                null);
    }

    public CommitterOperatorFactory(
            boolean streamingCheckpointEnabled,
            boolean forceSingleParallelism,
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
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<CommitT>> T createStreamOperator(
            StreamOperatorParameters<CommitT> parameters) {
        return (T)
                new CommitterOperator<>(
                        parameters,
                        streamingCheckpointEnabled,
                        forceSingleParallelism,
                        initialCommitUser,
                        committerFactory,
                        committableStateManager,
                        endInputWatermark);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return CommitterOperator.class;
    }
}
