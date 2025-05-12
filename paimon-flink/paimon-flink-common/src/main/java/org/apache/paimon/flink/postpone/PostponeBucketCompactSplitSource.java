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

package org.apache.paimon.flink.postpone;

import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.source.operator.ReadOperator;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Source for compacting postpone bucket tables. This source scans all files from {@code bucket =
 * -2} directory and distributes the files to the readers.
 */
public class PostponeBucketCompactSplitSource extends AbstractNonCoordinatedSource<Split> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(PostponeBucketCompactSplitSource.class);

    private final FileStoreTable table;
    private final Map<String, String> partitionSpec;

    public PostponeBucketCompactSplitSource(
            FileStoreTable table, Map<String, String> partitionSpec) {
        this.table = table;
        this.partitionSpec = partitionSpec;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<Split, SimpleSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new Reader();
    }

    private class Reader extends AbstractNonCoordinatedSourceReader<Split> {

        @Override
        public InputStatus pollNext(ReaderOutput<Split> output) throws Exception {
            List<Split> splits =
                    table.newSnapshotReader()
                            .withPartitionFilter(partitionSpec)
                            .withBucket(BucketMode.POSTPONE_BUCKET)
                            .read()
                            .splits();

            for (Split split : splits) {
                DataSplit dataSplit = (DataSplit) split;
                List<DataFileMeta> files = new ArrayList<>(dataSplit.dataFiles());
                // we must replay the written records in exact order
                files.sort(Comparator.comparing(DataFileMeta::creationTime));
                for (DataFileMeta meta : files) {
                    DataSplit s =
                            DataSplit.builder()
                                    .withPartition(dataSplit.partition())
                                    .withBucket(dataSplit.bucket())
                                    .withBucketPath(dataSplit.bucketPath())
                                    .withTotalBuckets(dataSplit.totalBuckets())
                                    .withDataFiles(Collections.singletonList(meta))
                                    .isStreaming(false)
                                    .build();
                    output.collect(s);
                }
            }

            return InputStatus.END_OF_INPUT;
        }
    }

    public static Pair<DataStream<RowData>, DataStream<Committable>> buildSource(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            Map<String, String> partitionSpec,
            @Nullable Integer parallelism) {
        DataStream<Split> source =
                env.fromSource(
                                new PostponeBucketCompactSplitSource(table, partitionSpec),
                                WatermarkStrategy.noWatermarks(),
                                String.format(
                                        "Compact split generator: %s - %s",
                                        table.fullName(), partitionSpec),
                                new JavaTypeInfo<>(Split.class))
                        .forceNonParallel();

        FlinkStreamPartitioner<Split> partitioner =
                new FlinkStreamPartitioner<>(new SplitChannelComputer());
        PartitionTransformation<Split> partitioned =
                new PartitionTransformation<>(source.getTransformation(), partitioner);
        if (parallelism != null) {
            partitioned.setParallelism(parallelism);
        }

        return Pair.of(
                new DataStream<>(source.getExecutionEnvironment(), partitioned)
                        .transform(
                                String.format(
                                        "Compact split reader: %s - %s",
                                        table.fullName(), partitionSpec),
                                InternalTypeInfo.of(
                                        LogicalTypeConversion.toLogicalType(table.rowType())),
                                new ReadOperator(table::newRead, null)),
                source.forward()
                        .transform(
                                "Remove new files",
                                new CommittableTypeInfo(),
                                new RemovePostponeBucketFilesOperator())
                        .forceNonParallel());
    }

    private static class SplitChannelComputer implements ChannelComputer<Split> {

        private transient int numChannels;
        private transient Pattern pattern;

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
            // see PostponeBucketTableWriteOperator
            this.pattern = Pattern.compile("-s-(\\d+?)-");
        }

        @Override
        public int channel(Split record) {
            DataSplit dataSplit = (DataSplit) record;
            String fileName = dataSplit.dataFiles().get(0).fileName();

            Matcher matcher = pattern.matcher(fileName);
            Preconditions.checkState(
                    matcher.find(),
                    "Data file name %s does not match the pattern. This is unexpected.",
                    fileName);

            // use long to avoid overflow
            long subtaskId = Long.parseLong(matcher.group(1));
            // send records written by the same subtask to the same subtask
            // to make sure we replay the written records in the exact order
            long channel = (Math.abs(dataSplit.partition().hashCode()) + subtaskId) % numChannels;
            return (int) channel;
        }
    }
}
