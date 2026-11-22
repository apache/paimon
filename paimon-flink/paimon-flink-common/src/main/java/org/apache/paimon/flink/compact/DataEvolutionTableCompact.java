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
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.DataEvolutionTableCompactSink;
import org.apache.paimon.flink.source.DataEvolutionTableCompactSource;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;

/** Build for data-evolution table flink compaction job. */
public class DataEvolutionTableCompact {

    private final transient StreamExecutionEnvironment env;
    private final String tableIdentifier;
    private final FileStoreTable table;

    @Nullable private PartitionPredicate partitionPredicate;
    @Nullable private Duration partitionIdleTime;

    public DataEvolutionTableCompact(
            StreamExecutionEnvironment env, String tableIdentifier, FileStoreTable table) {
        this.env = env;
        this.tableIdentifier = tableIdentifier;
        this.table = table;
    }

    public void withPartitionPredicate(PartitionPredicate partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
    }

    public void withPartitionIdleTime(@Nullable Duration partitionIdleTime) {
        this.partitionIdleTime = partitionIdleTime;
    }

    public void build() {
        DataEvolutionCompactCoordinator.validateOptions(table.coreOptions());
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        if (snapshot == null) {
            env.fromSequence(0, 0).name("Nothing to Compact Source").sinkTo(new DiscardingSink<>());
            return;
        }
        DataEvolutionTableCompactSource source =
                new DataEvolutionTableCompactSource(table, partitionFilter(snapshot), snapshot);
        DataStreamSource<DataEvolutionCompactTask> sourceStream =
                DataEvolutionTableCompactSource.buildSource(env, source, tableIdentifier);

        sinkFromSource(sourceStream, snapshot);
    }

    @Nullable
    private PartitionPredicate partitionFilter(Snapshot snapshot) {
        if (partitionIdleTime == null) {
            return partitionPredicate;
        }

        long cutoff = Instant.now().minus(partitionIdleTime).toEpochMilli();
        SnapshotReader snapshotReader = table.newSnapshotReader().withSnapshot(snapshot);
        snapshotReader.withPartitionFilter(partitionPredicate);
        Set<BinaryRow> idlePartitions =
                snapshotReader.partitionEntries().stream()
                        .filter(entry -> entry.lastFileCreationTime() <= cutoff)
                        .map(PartitionEntry::partition)
                        .collect(Collectors.toSet());
        return idlePartitions.isEmpty()
                ? PartitionPredicate.ALWAYS_FALSE
                : PartitionPredicate.fromMultiple(table.store().partitionType(), idlePartitions);
    }

    private void sinkFromSource(
            DataStreamSource<DataEvolutionCompactTask> input, Snapshot snapshot) {
        Options conf = Options.fromMap(table.options());
        Integer compactionWorkerParallelism =
                conf.get(FlinkConnectorOptions.UNAWARE_BUCKET_COMPACTION_PARALLELISM);
        PartitionTransformation<DataEvolutionCompactTask> transformation =
                new PartitionTransformation<>(
                        input.getTransformation(), new RebalancePartitioner<>());
        if (compactionWorkerParallelism != null) {
            transformation.setParallelism(compactionWorkerParallelism);
        } else {
            transformation.setParallelism(env.getParallelism());
        }

        DataStream<DataEvolutionCompactTask> rebalanced = new DataStream<>(env, transformation);
        DataEvolutionTableCompactSink.sink(table, rebalanced, snapshot);
    }
}
