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

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.UnawareBucketCompactionSink;
import org.apache.paimon.flink.source.BucketUnawareCompactSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;

import javax.annotation.Nullable;

/**
 * Build for unaware-bucket table flink compaction job.
 *
 * <p>Note: This compaction job class is only used for unaware-bucket compaction, at start-up, it
 * scans all the files from the latest snapshot, filter large file, and add small files into memory,
 * generates compaction task for them. At continuous, it scans the delta files from the follow-up
 * snapshot. We need to enable checkpoint for this compaction job, checkpoint will trigger committer
 * stage to commit all the compacted files.
 */
public class UnawareBucketCompactionTopoBuilder {

    private final transient StreamExecutionEnvironment env;
    private final String tableIdentifier;
    private final FileStoreTable table;

    private boolean isContinuous = false;

    @Nullable private Predicate partitionPredicate;

    public UnawareBucketCompactionTopoBuilder(
            StreamExecutionEnvironment env, String tableIdentifier, FileStoreTable table) {
        this.env = env;
        this.tableIdentifier = tableIdentifier;
        this.table = table;
    }

    public void withContinuousMode(boolean isContinuous) {
        this.isContinuous = isContinuous;
    }

    public void withPartitionPredicate(Predicate predicate) {
        this.partitionPredicate = predicate;
    }

    public void build() {
        // build source from UnawareSourceFunction
        DataStreamSource<AppendOnlyCompactionTask> source = buildSource();

        // from source, construct the full flink job
        sinkFromSource(source);
    }

    public DataStream<Committable> fetchUncommitted(String commitUser) {
        DataStreamSource<AppendOnlyCompactionTask> source = buildSource();

        // rebalance input to default or assigned parallelism
        DataStream<AppendOnlyCompactionTask> rebalanced = rebalanceInput(source);

        return new UnawareBucketCompactionSink(table)
                .doWrite(rebalanced, commitUser, rebalanced.getParallelism());
    }

    private DataStreamSource<AppendOnlyCompactionTask> buildSource() {
        long scanInterval = table.coreOptions().continuousDiscoveryInterval().toMillis();
        BucketUnawareCompactSource source =
                new BucketUnawareCompactSource(
                        table, isContinuous, scanInterval, partitionPredicate);

        return BucketUnawareCompactSource.buildSource(env, source, isContinuous, tableIdentifier);
    }

    private void sinkFromSource(DataStreamSource<AppendOnlyCompactionTask> input) {
        DataStream<AppendOnlyCompactionTask> rebalanced = rebalanceInput(input);

        UnawareBucketCompactionSink.sink(table, rebalanced);
    }

    private DataStream<AppendOnlyCompactionTask> rebalanceInput(
            DataStreamSource<AppendOnlyCompactionTask> input) {
        Options conf = Options.fromMap(table.options());
        Integer compactionWorkerParallelism =
                conf.get(FlinkConnectorOptions.UNAWARE_BUCKET_COMPACTION_PARALLELISM);
        PartitionTransformation<AppendOnlyCompactionTask> transformation =
                new PartitionTransformation<>(
                        input.getTransformation(), new RebalancePartitioner<>());
        if (compactionWorkerParallelism != null) {
            transformation.setParallelism(compactionWorkerParallelism);
        } else {
            // cause source function for unaware-bucket table compaction has only one parallelism,
            // we need to set to default parallelism by hand.
            transformation.setParallelism(env.getParallelism());
        }
        return new DataStream<>(env, transformation);
    }
}
