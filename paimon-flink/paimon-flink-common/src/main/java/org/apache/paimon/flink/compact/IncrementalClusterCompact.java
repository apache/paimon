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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.cluster.IncrementalClusterManager;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.cluster.IncrementalClusterSplitSource;
import org.apache.paimon.flink.cluster.RewriteIncrementalClusterCommittableOperator;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.sink.RowAppendTableSink;
import org.apache.paimon.flink.sorter.TableSortInfo;
import org.apache.paimon.flink.sorter.TableSorter;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_PARALLELISM;

/** Compact for incremental clustering. */
public class IncrementalClusterCompact {

    protected final StreamExecutionEnvironment env;
    protected final FileStoreTable table;
    protected final IncrementalClusterManager clusterManager;
    protected final String commitUser;
    protected final InternalRowPartitionComputer partitionComputer;

    protected final @Nullable Integer parallelism;
    protected final int localSampleMagnification;
    protected final Map<BinaryRow, CompactUnit> compactUnits;
    protected final Map<BinaryRow, Pair<List<DataSplit>, CommitMessage>> compactSplits;

    public IncrementalClusterCompact(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            PartitionPredicate partitionPredicate,
            @Nullable Boolean fullCompaction) {
        this.env = env;
        this.table = table;
        this.clusterManager = new IncrementalClusterManager(table, partitionPredicate);
        Options options = new Options(table.options());
        this.partitionComputer = table.store().partitionComputer();
        this.commitUser = CoreOptions.createCommitUser(options);
        this.parallelism = options.get(SCAN_PARALLELISM);
        this.localSampleMagnification = table.coreOptions().getLocalSampleMagnification();
        if (localSampleMagnification < 20) {
            throw new IllegalArgumentException(
                    String.format(
                            "the config '%s=%d' should not be set too small, greater than or equal to 20 is needed.",
                            CoreOptions.SORT_COMPACTION_SAMPLE_MAGNIFICATION.key(),
                            localSampleMagnification));
        }
        // non-full strategy as default for incremental clustering
        this.compactUnits =
                clusterManager.createCompactUnits(fullCompaction != null && fullCompaction);
        this.compactSplits = clusterManager.toSplitsAndRewriteDvFiles(compactUnits);
    }

    public void build() throws Exception {
        if (compactUnits.isEmpty()) {
            env.fromSequence(0, 0).name("Nothing to Cluster Source").sinkTo(new DiscardingSink<>());
            return;
        }

        List<DataStream<Committable>> dataStreams = new ArrayList<>();
        for (Map.Entry<BinaryRow, Pair<List<DataSplit>, CommitMessage>> entry :
                compactSplits.entrySet()) {
            dataStreams.addAll(
                    buildCompactOperator(
                            entry.getKey(),
                            entry.getValue().getKey(),
                            entry.getValue().getRight(),
                            parallelism));
        }

        buildCommitOperator(dataStreams);
    }

    /**
     * Build for one partition.
     *
     * @param parallelism Give the caller the opportunity to set parallelism
     */
    protected List<DataStream<Committable>> buildCompactOperator(
            BinaryRow partition,
            List<DataSplit> splits,
            CommitMessage dvCommitMessage,
            @Nullable Integer parallelism) {
        LinkedHashMap<String, String> partitionSpec =
                partitionComputer.generatePartValues(partition);

        // 2.1 generate source for current partition
        Pair<DataStream<RowData>, DataStream<Committable>> sourcePair =
                IncrementalClusterSplitSource.buildSource(
                        env, table, partitionSpec, splits, dvCommitMessage, parallelism);

        // 2.2 cluster in partition
        Integer sinkParallelism = parallelism;
        if (sinkParallelism == null) {
            sinkParallelism = sourcePair.getLeft().getParallelism();
        }
        TableSortInfo sortInfo =
                new TableSortInfo.Builder()
                        .setSortColumns(clusterManager.clusterKeys())
                        .setSortStrategy(clusterManager.clusterCurve())
                        .setSinkParallelism(sinkParallelism)
                        .setLocalSampleSize(sinkParallelism * localSampleMagnification)
                        .setGlobalSampleSize(sinkParallelism * 1000)
                        .setRangeNumber(sinkParallelism * 10)
                        .build();
        DataStream<RowData> sorted =
                TableSorter.getSorter(env, sourcePair.getLeft(), table, sortInfo).sort();

        // 2.3 write and then reorganize the committable
        // set parallelism to null, and it'll forward parallelism when doWrite()
        RowAppendTableSink sink = new RowAppendTableSink(table, null, null);
        boolean blobAsDescriptor = table.coreOptions().blobAsDescriptor();
        DataStream<Committable> written =
                sink.doWrite(
                        FlinkSinkBuilder.mapToInternalRow(
                                sorted,
                                table.rowType(),
                                blobAsDescriptor,
                                table.catalogEnvironment().catalogContext()),
                        commitUser,
                        null);
        DataStream<Committable> clusterCommittable =
                written.forward()
                        .transform(
                                "Rewrite cluster committable",
                                new CommittableTypeInfo(),
                                new RewriteIncrementalClusterCommittableOperator(
                                        table,
                                        compactUnits.entrySet().stream()
                                                .collect(
                                                        Collectors.toMap(
                                                                Map.Entry::getKey,
                                                                unit ->
                                                                        unit.getValue()
                                                                                .outputLevel()))))
                        .setParallelism(written.getParallelism());

        List<DataStream<Committable>> dataStreams = new ArrayList<>();
        dataStreams.add(clusterCommittable);
        dataStreams.add(sourcePair.getRight());
        return dataStreams;
    }

    protected void buildCommitOperator(List<DataStream<Committable>> dataStreams) {
        RowAppendTableSink sink = new RowAppendTableSink(table, null, null);
        DataStream<Committable> dataStream = dataStreams.get(0);
        for (int i = 1; i < dataStreams.size(); i++) {
            dataStream = dataStream.union(dataStreams.get(i));
        }
        sink.doCommit(dataStream, commitUser);
    }
}
