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

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.utils.SerializableFunction;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.FlinkConnectorOptions.generateCustomUid;
import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/** Sink for dynamic bucket table. */
public abstract class DynamicBucketSink<T> extends FlinkWriteSink<Tuple2<T, Integer>> {

    private static final long serialVersionUID = 1L;

    private static final String DYNAMIC_BUCKET_ASSIGNER_NAME = "dynamic-bucket-assigner";

    public DynamicBucketSink(
            FileStoreTable table, @Nullable Map<String, String> overwritePartition) {
        super(table, overwritePartition);
    }

    protected abstract ChannelComputer<T> assignerChannelComputer(Integer numAssigners);

    protected abstract ChannelComputer<Tuple2<T, Integer>> channelComputer2();

    protected abstract SerializableFunction<TableSchema, PartitionKeyExtractor<T>>
            extractorFunction();

    protected HashBucketAssignerOperator<T> createHashBucketAssignerOperator(
            String commitUser,
            Table table,
            Integer numAssigners,
            SerializableFunction<TableSchema, PartitionKeyExtractor<T>> extractorFunction,
            boolean overwrite) {
        return new HashBucketAssignerOperator<>(
                commitUser, table, numAssigners, extractorFunction, overwrite);
    }

    public DataStreamSink<?> build(DataStream<T> input, @Nullable Integer parallelism) {
        String initialCommitUser = createCommitUser(table.coreOptions().toConfiguration());

        // Topology:
        // input -- shuffle by key hash --> bucket-assigner -- shuffle by partition & bucket -->
        // writer --> committer

        // 1. shuffle by key hash
        Integer assignerParallelism = table.coreOptions().dynamicBucketAssignerParallelism();
        if (assignerParallelism == null) {
            assignerParallelism = parallelism;
        }

        Integer numAssigners = table.coreOptions().dynamicBucketInitialBuckets();
        DataStream<T> partitionByKeyHash =
                partition(input, assignerChannelComputer(numAssigners), assignerParallelism);

        // 2. bucket-assigner
        HashBucketAssignerOperator<T> assignerOperator =
                createHashBucketAssignerOperator(
                        initialCommitUser, table, numAssigners, extractorFunction(), false);
        TupleTypeInfo<Tuple2<T, Integer>> rowWithBucketType =
                new TupleTypeInfo<>(partitionByKeyHash.getType(), BasicTypeInfo.INT_TYPE_INFO);
        SingleOutputStreamOperator<Tuple2<T, Integer>> bucketAssigned =
                partitionByKeyHash
                        .transform(
                                DYNAMIC_BUCKET_ASSIGNER_NAME, rowWithBucketType, assignerOperator)
                        .setParallelism(partitionByKeyHash.getParallelism());

        String uidSuffix = table.options().get(SINK_OPERATOR_UID_SUFFIX.key());
        if (!StringUtils.isNullOrWhitespaceOnly(uidSuffix)) {
            bucketAssigned =
                    bucketAssigned.uid(
                            generateCustomUid(
                                    DYNAMIC_BUCKET_ASSIGNER_NAME, table.name(), uidSuffix));
        }

        // 3. shuffle by partition & bucket
        DataStream<Tuple2<T, Integer>> partitionByBucket =
                partition(bucketAssigned, channelComputer2(), parallelism);

        // 4. writer and committer
        return sinkFrom(partitionByBucket, initialCommitUser);
    }
}
