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

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The {@code Committer} is responsible for creating and committing an aggregated committable, which
 * we call committable (see {@link #combine}).
 *
 * <p>The {@code Committer} runs with parallelism equal to 1.
 */
public interface Committer<CommitT, GlobalCommitT> extends AutoCloseable {

    boolean forceCreatingSnapshot();

    /** Compute an aggregated committable from a list of committables. */
    GlobalCommitT combine(long checkpointId, long watermark, List<CommitT> committables)
            throws IOException;

    GlobalCommitT combine(
            long checkpointId, long watermark, GlobalCommitT t, List<CommitT> committables);

    /** Commits the given {@link GlobalCommitT}. */
    void commit(List<GlobalCommitT> globalCommittables) throws IOException, InterruptedException;

    /**
     * Filter out all {@link GlobalCommitT} which have committed, and commit the remaining {@link
     * GlobalCommitT}.
     */
    int filterAndCommit(List<GlobalCommitT> globalCommittables, boolean checkAppendFiles)
            throws IOException;

    default int filterAndCommit(List<GlobalCommitT> globalCommittables) throws IOException {
        return filterAndCommit(globalCommittables, true);
    }

    Map<Long, List<CommitT>> groupByCheckpoint(Collection<CommitT> committables);

    /** Factory to create {@link Committer}. */
    interface Factory<CommitT, GlobalCommitT> extends Serializable {

        Committer<CommitT, GlobalCommitT> create(Context context);
    }

    /** Context to create {@link Committer}. */
    interface Context {

        String commitUser();

        @Nullable
        OperatorMetricGroup metricGroup();

        boolean streamingCheckpointEnabled();

        boolean isRestored();

        OperatorStateStore stateStore();
    }

    static Context createContext(
            String commitUser,
            @Nullable OperatorMetricGroup metricGroup,
            boolean streamingCheckpointEnabled,
            boolean isRestored,
            OperatorStateStore stateStore) {
        return new Committer.Context() {
            @Override
            public String commitUser() {
                return commitUser;
            }

            @Override
            public OperatorMetricGroup metricGroup() {
                return metricGroup;
            }

            @Override
            public boolean streamingCheckpointEnabled() {
                return streamingCheckpointEnabled;
            }

            @Override
            public boolean isRestored() {
                return isRestored;
            }

            @Override
            public OperatorStateStore stateStore() {
                return stateStore;
            }
        };
    }
}
