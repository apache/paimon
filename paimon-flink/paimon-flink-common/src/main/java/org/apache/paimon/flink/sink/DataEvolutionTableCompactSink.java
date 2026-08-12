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

import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compaction Sink for data-evolution table. */
public class DataEvolutionTableCompactSink extends FlinkSink<DataEvolutionCompactTask> {

    private final Snapshot snapshot;
    private final boolean materializeDeletionVectors;

    public DataEvolutionTableCompactSink(FileStoreTable table, Snapshot snapshot) {
        this(table, snapshot, false);
    }

    public DataEvolutionTableCompactSink(
            FileStoreTable table, Snapshot snapshot, boolean materializeDeletionVectors) {
        super(table, true);
        this.snapshot = snapshot;
        this.materializeDeletionVectors = materializeDeletionVectors;
    }

    public static DataStreamSink<?> sink(
            FileStoreTable table, DataStream<DataEvolutionCompactTask> input, Snapshot snapshot) {
        return sink(table, input, snapshot, false);
    }

    public static DataStreamSink<?> sink(
            FileStoreTable table,
            DataStream<DataEvolutionCompactTask> input,
            Snapshot snapshot,
            boolean materializeDeletionVectors) {
        boolean isStreaming = isStreaming(input);
        checkArgument(!isStreaming, "Data evolution compaction sink only supports batch mode yet.");
        return new DataEvolutionTableCompactSink(table, snapshot, materializeDeletionVectors)
                .sinkFrom(input);
    }

    @Override
    public DataStreamSink<?> sinkFrom(
            DataStream<DataEvolutionCompactTask> input, String initialCommitUser) {
        DataStream<Committable> written = doWrite(input, initialCommitUser, null);
        if (table.coreOptions().deletionVectorsEnabled()) {
            written =
                    written.transform(
                                    "Data Evolution Compact Deletion Vector Rewriter : "
                                            + table.name(),
                                    new CommittableTypeInfo(),
                                    new DataEvolutionCompactDeletionVectorOperator.Factory(
                                            table, snapshot))
                            .forceNonParallel();
        }
        return doCommit(written, initialCommitUser);
    }

    @Override
    protected OneInputStreamOperatorFactory<DataEvolutionCompactTask, Committable>
            createWriteOperatorFactory(StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new DataEvolutionCompactionWorkerOperator.Factory(table, commitUser);
    }

    @Override
    protected Committer.Factory<Committable, ManifestCommittable> createCommitterFactory() {
        return context -> {
            TableCommitImpl commit = table.newCommit(context.commitUser());
            if (materializeDeletionVectors) {
                commit.rowIdCheckConflictForMaterializeDvCompaction(snapshot.id());
            }
            return new StoreCommitter(table, commit, context);
        };
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        return new NoopCommittableStateManager();
    }
}
