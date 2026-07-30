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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.PostponeMergePlan;
import org.apache.paimon.table.source.PostponeMergeReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

/** Builds the Flink batch topology for postpone merge-on-read. */
final class PostponeMergeOnRead {

    static final int PARTITION = 0;
    static final int BUCKET = 1;
    static final int INPUT_KIND = 2;
    static final int REAL_SPLIT = 3;
    static final int KEY = 4;
    static final int WRITER_LOCAL_ORDER = 5;
    static final int ROW_KIND = 6;
    static final int VALUE = 7;

    static final byte REAL_SPLIT_KIND = 0;
    static final byte POSTPONE_RECORD_KIND = 1;

    static final RowType CARRIER_TYPE =
            RowType.of(
                    DataTypes.BYTES().notNull(),
                    DataTypes.INT().notNull(),
                    DataTypes.TINYINT().notNull(),
                    DataTypes.BYTES(),
                    DataTypes.BYTES(),
                    DataTypes.BIGINT().notNull(),
                    DataTypes.TINYINT().notNull(),
                    DataTypes.BYTES());

    private PostponeMergeOnRead() {}

    static boolean configured(Table table) {
        if (!(table instanceof FileStoreTable)) {
            return false;
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        return fileStoreTable.coreOptions().postponeMergeOnRead()
                && fileStoreTable.bucketMode() == BucketMode.POSTPONE_MODE
                && !fileStoreTable.primaryKeys().isEmpty();
    }

    static boolean usesCustomSource(Table table) {
        return configured(table)
                && ((FileStoreTable) table).coreOptions().startupMode()
                        != CoreOptions.StartupMode.COMPACTED_FULL;
    }

    static DataStream<RowData> build(
            StreamExecutionEnvironment env,
            String sourceName,
            FileStoreTable table,
            PostponeMergeReadBuilder readBuilder,
            PostponeMergePlan plan,
            int parallelism,
            TypeInformation<RowData> outputType,
            @Nullable NestedProjectedRowData outerProject,
            @Nullable Long limit,
            boolean blobAsDescriptor) {
        PostponeMergeInputSource inputSource = new PostponeMergeInputSource(plan);
        DataStream<Split> inputs =
                env.fromSource(
                                new PaimonDataStreamSource<>(inputSource, table),
                                WatermarkStrategy.noWatermarks(),
                                sourceName + " - Postpone merge inputs",
                                new JavaTypeInfo<>(Split.class))
                        .forceNonParallel();

        DataStream<Split> distributedInputs = FlinkStreamPartitioner.rebalance(inputs, parallelism);
        SingleOutputStreamOperator<InternalRow> carriers =
                distributedInputs
                        .transform(
                                "Postpone merge input",
                                InternalTypeInfo.fromRowType(CARRIER_TYPE),
                                new PostponeMergeInputOperator(
                                        readBuilder,
                                        plan.keyType(),
                                        plan.mergeReadType(),
                                        plan.bucketRouter()))
                        .setParallelism(parallelism);

        DataStream<InternalRow> partitionedCarriers =
                FlinkStreamPartitioner.partition(
                        carriers, new CarrierChannelComputer(), parallelism);
        return partitionedCarriers
                .transform(
                        "Postpone merge on read",
                        outputType,
                        new PostponeMergeOperator(
                                readBuilder,
                                plan.resultReadType(),
                                table.options(),
                                outerProject,
                                limit,
                                blobAsDescriptor))
                .setParallelism(parallelism);
    }

    private static class CarrierChannelComputer implements ChannelComputer<InternalRow> {

        private static final long serialVersionUID = 1L;

        private int numChannels;

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
        }

        @Override
        public int channel(InternalRow carrier) {
            BinaryRow partition =
                    SerializationUtils.deserializeBinaryRow(carrier.getBinary(PARTITION));
            return ChannelComputer.select(partition, carrier.getInt(BUCKET), numChannels);
        }
    }
}
