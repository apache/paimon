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

/** Sink which commits physically materialized data-evolution deletion vectors. */
public class DataEvolutionDeletionVectorMaterializeSink
        extends FlinkSink<DataEvolutionCompactTask> {

    private final Snapshot snapshot;

    public DataEvolutionDeletionVectorMaterializeSink(FileStoreTable table, Snapshot snapshot) {
        super(table, true);
        this.snapshot = snapshot;
    }

    public static DataStreamSink<?> sink(
            FileStoreTable table, DataStream<DataEvolutionCompactTask> input, Snapshot snapshot) {
        checkArgument(
                !isStreaming(input),
                "Deletion vector materialize sink only supports batch mode yet.");
        return new DataEvolutionDeletionVectorMaterializeSink(table, snapshot).sinkFrom(input);
    }

    @Override
    public DataStreamSink<?> sinkFrom(
            DataStream<DataEvolutionCompactTask> input, String initialCommitUser) {
        DataStream<Committable> written = doWrite(input, initialCommitUser, null);
        written =
                written.transform(
                                "Data Evolution Deletion Vector Materialize Commit Preparation : "
                                        + table.name(),
                                new CommittableTypeInfo(),
                                new DataEvolutionCommitPreparationOperator.Factory(table, snapshot))
                        .forceNonParallel();
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
            commit.rowIdCheckConflictForMaterializeDvCompaction(snapshot.id());
            return new StoreCommitter(table, commit, context);
        };
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        return new NoopCommittableStateManager();
    }
}
