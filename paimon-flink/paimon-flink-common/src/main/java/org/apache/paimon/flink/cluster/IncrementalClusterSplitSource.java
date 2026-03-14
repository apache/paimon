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

package org.apache.paimon.flink.cluster;

import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.source.operator.ReadOperator;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Pair;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** Source for Incremental Clustering. */
public class IncrementalClusterSplitSource extends AbstractNonCoordinatedSource<Split> {

    private static final long serialVersionUID = 2L;

    private final List<Split> splits;

    public IncrementalClusterSplitSource(List<Split> splits) {
        this.splits = splits;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<Split, SimpleSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new IncrementalClusterSplitSource.Reader();
    }

    private class Reader extends AbstractNonCoordinatedSourceReader<Split> {

        @Override
        public InputStatus pollNext(ReaderOutput<Split> output) {
            for (Split split : splits) {
                DataSplit dataSplit = (DataSplit) split;
                output.collect(dataSplit);
            }
            return InputStatus.END_OF_INPUT;
        }
    }

    public static Pair<DataStream<RowData>, DataStream<Committable>> buildSource(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            Map<String, String> partitionSpec,
            List<DataSplit> splits,
            @Nullable CommitMessage dvCommitMessage,
            @Nullable Integer parallelism) {
        DataStream<Split> source =
                env.fromSource(
                                new IncrementalClusterSplitSource((List) splits),
                                WatermarkStrategy.noWatermarks(),
                                String.format(
                                        "Incremental-cluster split generator: %s - %s",
                                        table.fullName(), partitionSpec),
                                new JavaTypeInfo<>(Split.class))
                        .forceNonParallel();

        PartitionTransformation<Split> partitioned =
                new PartitionTransformation<>(
                        source.getTransformation(), new RebalancePartitioner<>());
        if (parallelism != null) {
            partitioned.setParallelism(parallelism);
        }

        return Pair.of(
                new DataStream<>(source.getExecutionEnvironment(), partitioned)
                        .transform(
                                String.format(
                                        "Incremental-cluster reader: %s - %s",
                                        table.fullName(), partitionSpec),
                                InternalTypeInfo.of(
                                        LogicalTypeConversion.toLogicalType(table.rowType())),
                                new ReadOperator(table::newRead, null, null))
                        .setParallelism(partitioned.getParallelism()),
                source.forward()
                        .transform(
                                "Remove files to be clustered",
                                new CommittableTypeInfo(),
                                new RemoveClusterBeforeFilesOperator(dvCommitMessage))
                        .forceNonParallel());
    }
}
