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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.ChannelComputer;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.FlinkWriteSink;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.cdc.CdcDynamicBucketWriteOperator;
import org.apache.paimon.flink.sink.cdc.CdcHashKeyChannelComputer;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.CdcRecordUtils;
import org.apache.paimon.flink.sink.cdc.CdcWithBucketChannelComputer;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/** Sink for global dynamic bucket table and {@link CdcRecord} inputs. */
public class GlobalDynamicCdcBucketSink extends FlinkWriteSink<Tuple2<CdcRecord, Integer>> {

    private static final long serialVersionUID = 1L;

    public GlobalDynamicCdcBucketSink(FileStoreTable table) {
        super(table, null);
    }

    @Override
    protected OneInputStreamOperator<Tuple2<CdcRecord, Integer>, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new CdcDynamicBucketWriteOperator(table, writeProvider, commitUser);
    }

    public DataStreamSink<?> build(DataStream<CdcRecord> input, @Nullable Integer parallelism) {
        String initialCommitUser = UUID.randomUUID().toString();

        TableSchema schema = table.schema();
        List<String> primaryKeys = schema.primaryKeys();
        List<String> partitionKeys = schema.partitionKeys();
        RowType keyPartType =
                schema.projectedLogicalRowType(
                        Stream.concat(primaryKeys.stream(), partitionKeys.stream())
                                .collect(Collectors.toList()));
        List<String> keyPartNames = keyPartType.getFieldNames();
        RowDataToObjectArrayConverter keyPartConverter =
                new RowDataToObjectArrayConverter(keyPartType);

        // Topology:
        // input -- bootstrap -- shuffle by key hash --> bucket-assigner -- shuffle by bucket -->
        // writer --> committer

        TupleTypeInfo<Tuple2<KeyPartOrRow, CdcRecord>> tuple2TupleType =
                new TupleTypeInfo<>(new EnumTypeInfo<>(KeyPartOrRow.class), input.getType());
        DataStream<Tuple2<KeyPartOrRow, CdcRecord>> bootstraped =
                input.transform(
                                "INDEX_BOOTSTRAP",
                                tuple2TupleType,
                                new IndexBootstrapOperator<>(
                                        new IndexBootstrap(table),
                                        row ->
                                                CdcRecordUtils.fromGenericRow(
                                                        GenericRow.ofKind(
                                                                row.getRowKind(),
                                                                keyPartConverter.convert(row)),
                                                        keyPartNames)))
                        .setParallelism(input.getParallelism());

        // 1. shuffle by key hash
        Integer assignerParallelism = table.coreOptions().dynamicBucketAssignerParallelism();
        if (assignerParallelism == null) {
            assignerParallelism = parallelism;
        }

        ChannelComputer<Tuple2<KeyPartOrRow, CdcRecord>> channelComputer =
                ChannelComputer.transform(
                        new CdcHashKeyChannelComputer(schema), tuple2 -> tuple2.f1);
        DataStream<Tuple2<KeyPartOrRow, CdcRecord>> partitionByKeyHash =
                partition(bootstraped, channelComputer, assignerParallelism);

        // 2. bucket-assigner
        TupleTypeInfo<Tuple2<CdcRecord, Integer>> rowWithBucketType =
                new TupleTypeInfo<>(input.getType(), BasicTypeInfo.INT_TYPE_INFO);
        DataStream<Tuple2<CdcRecord, Integer>> bucketAssigned =
                partitionByKeyHash
                        .transform(
                                "dynamic-bucket-assigner",
                                rowWithBucketType,
                                GlobalIndexAssignerOperator.forCdcRecord(table))
                        .setParallelism(partitionByKeyHash.getParallelism());

        // 3. shuffle by bucket

        DataStream<Tuple2<CdcRecord, Integer>> partitionByBucket =
                partition(bucketAssigned, new CdcWithBucketChannelComputer(schema), parallelism);

        // 4. writer and committer
        return sinkFrom(partitionByBucket, initialCommitUser);
    }
}
