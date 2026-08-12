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

package org.apache.paimon.flink.compact;

import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.DataEvolutionDeletionVectorMaterializeSink;
import org.apache.paimon.flink.source.DataEvolutionDeletionVectorMaterializeSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;

import javax.annotation.Nullable;

/** Builds a Flink job which applies deletion vectors to the latest table state. */
public class DataEvolutionDeletionVectorMaterialize {

    private final transient StreamExecutionEnvironment env;
    private final String tableIdentifier;
    private final FileStoreTable table;

    @Nullable private PartitionPredicate partitionPredicate;

    public DataEvolutionDeletionVectorMaterialize(
            StreamExecutionEnvironment env, String tableIdentifier, FileStoreTable table) {
        this.env = env;
        this.tableIdentifier = tableIdentifier;
        this.table = table;
    }

    public void withPartitionPredicate(PartitionPredicate partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
    }

    public void build() {
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        if (snapshot == null) {
            env.fromSequence(0, 0)
                    .name("Nothing to Materialize Source")
                    .sinkTo(new DiscardingSink<>());
            return;
        }
        DataEvolutionDeletionVectorMaterializeSource source =
                new DataEvolutionDeletionVectorMaterializeSource(
                        table, partitionPredicate, snapshot);
        DataStreamSource<DataEvolutionCompactTask> sourceStream =
                DataEvolutionDeletionVectorMaterializeSource.buildSource(
                        env, source, tableIdentifier);
        sinkFromSource(sourceStream, snapshot);
    }

    private void sinkFromSource(
            DataStreamSource<DataEvolutionCompactTask> input, Snapshot snapshot) {
        Options conf = Options.fromMap(table.options());
        Integer workerParallelism =
                conf.get(FlinkConnectorOptions.UNAWARE_BUCKET_COMPACTION_PARALLELISM);
        PartitionTransformation<DataEvolutionCompactTask> transformation =
                new PartitionTransformation<>(
                        input.getTransformation(), new RebalancePartitioner<>());
        transformation.setParallelism(
                workerParallelism == null ? env.getParallelism() : workerParallelism);

        DataStream<DataEvolutionCompactTask> rebalanced = new DataStream<>(env, transformation);
        DataEvolutionDeletionVectorMaterializeSink.sink(table, rebalanced, snapshot);
    }
}
