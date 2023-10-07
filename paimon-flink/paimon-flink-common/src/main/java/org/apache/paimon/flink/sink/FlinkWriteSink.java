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

import org.apache.paimon.flink.VersionedSerializerWrapper;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestCommittableSerializer;
import org.apache.paimon.metrics.commit.CommitMetrics;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;

import javax.annotation.Nullable;

import java.util.Map;

/** A {@link FlinkSink} to write records. */
public abstract class FlinkWriteSink<T> extends FlinkSink<T> {

    private static final long serialVersionUID = 1L;

    @Nullable private final Map<String, String> overwritePartition;

    public FlinkWriteSink(FileStoreTable table, @Nullable Map<String, String> overwritePartition) {
        super(table, overwritePartition != null);
        this.overwritePartition = overwritePartition;
    }

    @Override
    protected Committer.Factory<Committable, ManifestCommittable> createCommitterFactory(
            boolean streamingCheckpointEnabled) {
        // If checkpoint is enabled for streaming job, we have to
        // commit new files list even if they're empty.
        // Otherwise we can't tell if the commit is successful after
        // a restart.

        return (user, metricGroup) -> {
            TableCommitImpl tableCommit =
                    table.newCommit(user)
                            .withOverwrite(overwritePartition)
                            .ignoreEmptyCommit(!streamingCheckpointEnabled);
            FileStoreCommit storeCommit = tableCommit.getStoreCommit();
            CommitMetrics commitMetrics = null;
            if (storeCommit instanceof FileStoreCommitImpl) {
                commitMetrics = ((FileStoreCommitImpl) storeCommit).getCommitMetrics();
            }
            return new StoreCommitter(
                    tableCommit, new CommitterMetrics(metricGroup, commitMetrics));
        };
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        return new RestoreAndFailCommittableStateManager<>(
                () -> new VersionedSerializerWrapper<>(new ManifestCommittableSerializer()));
    }
}
