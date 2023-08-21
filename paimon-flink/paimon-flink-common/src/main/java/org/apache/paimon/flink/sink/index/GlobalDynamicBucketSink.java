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

package org.apache.paimon.flink.sink.index;

import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.DynamicBucketRowWriteOperator;
import org.apache.paimon.flink.sink.FlinkWriteSink;
import org.apache.paimon.flink.sink.RowWithBucketChannelComputer;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/** Sink for global dynamic bucket table. */
public class GlobalDynamicBucketSink extends FlinkWriteSink<Tuple2<RowData, Integer>> {

    private static final long serialVersionUID = 1L;

    public GlobalDynamicBucketSink(
            FileStoreTable table, @Nullable Map<String, String> overwritePartition) {
        super(table, overwritePartition);
    }

    @Override
    protected OneInputStreamOperator<Tuple2<RowData, Integer>, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new DynamicBucketRowWriteOperator(table, writeProvider, commitUser);
    }

    public DataStreamSink<?> build(DataStream<RowData> input, @Nullable Integer parallelism) {
        String initialCommitUser = UUID.randomUUID().toString();

        TableSchema schema = table.schema();
        RowType rowType = schema.logicalRowType();
        List<String> primaryKeys = schema.primaryKeys();
        List<String> partitionKeys = schema.partitionKeys();
        RowDataSerializer rowSerializer = new RowDataSerializer(toLogicalType(rowType));
        RowType keyPartType =
                schema.projectedLogicalRowType(
                        Stream.concat(primaryKeys.stream(), partitionKeys.stream())
                                .collect(Collectors.toList()));
        RowDataSerializer keyPartSerializer = new RowDataSerializer(toLogicalType(keyPartType));

        // Topology:
        // input -- bootstrap -- shuffle by key hash --> bucket-assigner -- shuffle by bucket -->
        // writer --> committer

        DataStream<Tuple2<KeyPartOrRow, RowData>> bootstraped =
                input.transform(
                                "INDEX_BOOTSTRAP",
                                new InternalTypeInfo<>(
                                        new KeyWithRowSerializer<>(
                                                keyPartSerializer, rowSerializer)),
                                new IndexBootstrapOperator<>(
                                        new IndexBootstrap(table), FlinkRowData::new))
                        .setParallelism(input.getParallelism());

        // 1. shuffle by key hash
        Integer assignerParallelism = table.coreOptions().dynamicBucketAssignerParallelism();
        if (assignerParallelism == null) {
            assignerParallelism = parallelism;
        }

        KeyPartRowChannelComputer channelComputer =
                new KeyPartRowChannelComputer(rowType, keyPartType, primaryKeys);
        DataStream<Tuple2<KeyPartOrRow, RowData>> partitionByKeyHash =
                partition(bootstraped, channelComputer, assignerParallelism);

        // 2. bucket-assigner
        TupleTypeInfo<Tuple2<RowData, Integer>> rowWithBucketType =
                new TupleTypeInfo<>(input.getType(), BasicTypeInfo.INT_TYPE_INFO);
        DataStream<Tuple2<RowData, Integer>> bucketAssigned =
                partitionByKeyHash
                        .transform(
                                "dynamic-bucket-assigner",
                                rowWithBucketType,
                                GlobalIndexAssignerOperator.forRowData(table))
                        .setParallelism(partitionByKeyHash.getParallelism());

        // 3. shuffle by bucket

        DataStream<Tuple2<RowData, Integer>> partitionByBucket =
                partition(bucketAssigned, new RowWithBucketChannelComputer(schema), parallelism);

        // 4. writer and committer
        return sinkFrom(partitionByBucket, initialCommitUser);
    }
}
