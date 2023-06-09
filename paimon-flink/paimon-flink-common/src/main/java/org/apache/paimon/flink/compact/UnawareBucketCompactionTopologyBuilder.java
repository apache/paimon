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
import org.apache.paimon.flink.sink.AppendOnlyTableCompactionWorkerOperator;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.CommitterOperator;
import org.apache.paimon.flink.sink.CompactionTaskTypeInfo;
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.flink.sink.NoopCommittableStateManager;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.source.UnawareBucketSourceFunction;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.StreamSource;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Build for unaware-bucket table flink compaction job.
 *
 * <p>Note: This compaction job class is only used for unaware-bucket compaction, at start-up, it
 * scans all the files from the latest snapshot, filter large file, and add small files into memory,
 * generates compaction task for them. At continuous, it scans the delta files from the follow-up
 * snapshot. We need to enable checkpoint for this compaction job, checkpoint will trigger committer
 * stage to commit all the compacted files.
 */
public class UnawareBucketCompactionTopologyBuilder implements Serializable {

    private static final String COMPACTION_COORDINATOR_NAME = "Compaction Coordinator";
    private static final String COMPACTION_WORKER_NAME = "Compaction Worker";
    private static final String COMPACTION_COMMITTER_NAME = "Compaction Committer";

    private transient StreamExecutionEnvironment env;
    private final String tableIdentifier;
    private final AppendOnlyFileStoreTable table;
    @Nullable private List<Map<String, String>> specifiedPartitions = null;
    private boolean isContinuous = false;

    public UnawareBucketCompactionTopologyBuilder(
            StreamExecutionEnvironment env,
            String tableIdentifier,
            AppendOnlyFileStoreTable table) {
        this.env = env;
        this.tableIdentifier = tableIdentifier;
        this.table = table;
    }

    public void withContinuousMode(boolean isContinuous) {
        this.isContinuous = isContinuous;
    }

    public void withPartitions(List<Map<String, String>> partitions) {
        this.specifiedPartitions = partitions;
    }

    public void build() {

        // build source from UnawareSourceFunction
        DataStreamSource<AppendOnlyCompactionTask> source = buildSource();

        // from source, construct the full flink job
        sinkFromSource(source);
    }

    private DataStreamSource<AppendOnlyCompactionTask> buildSource() {
        UnawareBucketSourceFunction source = new UnawareBucketSourceFunction(table);
        long scanInterval = table.coreOptions().continuousDiscoveryInterval().toMillis();
        source.withStreaming(isContinuous);
        source.withScanInterval(scanInterval);
        source.withFilter(getPartitionFilter());

        final StreamSource<AppendOnlyCompactionTask, UnawareBucketSourceFunction> sourceOperator =
                new StreamSource<>(source);
        return new DataStreamSource<>(
                env,
                new CompactionTaskTypeInfo(),
                sourceOperator,
                false,
                COMPACTION_COORDINATOR_NAME + " -> " + tableIdentifier,
                isContinuous ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED);
    }

    private void sinkFromSource(DataStreamSource<AppendOnlyCompactionTask> input) {
        String commitUser = UUID.randomUUID().toString();
        Options conf = Options.fromMap(table.options());
        Integer compactionWorkerParallelism =
                conf.get(FlinkConnectorOptions.COMPACTION_PARALLELISM);

        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        boolean streamingCheckpointEnabled =
                isContinuous && checkpointConfig.isCheckpointingEnabled();
        if (streamingCheckpointEnabled) {
            FlinkSink.assertCheckpointConfiguration(env);
        }

        // compaction worker stage, to execute compaction tasks
        CommittableTypeInfo committableTypeInfo = new CommittableTypeInfo();
        SingleOutputStreamOperator<Committable> compacted =
                input.transform(
                        COMPACTION_WORKER_NAME + " -> " + table.name(),
                        committableTypeInfo,
                        new AppendOnlyTableCompactionWorkerOperator(table, commitUser));

        if (compactionWorkerParallelism != null) {
            compacted.setParallelism(compactionWorkerParallelism);
        }

        // commit stage, to commit compaction messages
        SingleOutputStreamOperator<?> committed =
                compacted
                        .transform(
                                COMPACTION_COMMITTER_NAME + " -> " + table.name(),
                                committableTypeInfo,
                                new CommitterOperator(
                                        streamingCheckpointEnabled,
                                        commitUser,
                                        (SerializableFunction<String, Committer>)
                                                s -> new StoreCommitter(table.newCommit(s)),
                                        new NoopCommittableStateManager()))
                        .setParallelism(1)
                        .setMaxParallelism(1);

        committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    private Predicate getPartitionFilter() {
        Predicate partitionPredicate = null;
        if (specifiedPartitions != null) {
            partitionPredicate =
                    PredicateBuilder.or(
                            specifiedPartitions.stream()
                                    .map(p -> PredicateBuilder.partition(p, table.rowType()))
                                    .toArray(Predicate[]::new));
        }
        return partitionPredicate;
    }
}
