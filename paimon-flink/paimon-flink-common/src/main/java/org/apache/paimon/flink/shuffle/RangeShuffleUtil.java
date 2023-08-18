/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.shuffle;

import org.apache.paimon.utils.Pair;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.table.data.RowData;

import java.util.Comparator;
import java.util.List;

/** Topo. */
public class RangeShuffleUtil {

    /**
     * The RelNode with range-partition distribution will create the following transformations.
     *
     * <p>Explanation of the following figure: "[LSample, n]" means operator is LSample and
     * parallelism is n, "LSample" means LocalSampleOperator, "GSample" means GlobalSampleOperator,
     * "ARange" means AssignRangeId, "RRange" means RemoveRangeId.
     *
     * <pre>{@code
     * [IN,n]->[LSample,n]->[GSample,1]-BROADCAST
     *    \                                    \
     *     -----------------------------BATCH-[ARange,n]-PARTITION->[RRange,m]->
     * }</pre>
     *
     * <p>The streams except the sample and histogram process stream will been blocked, so the the
     * sample and histogram process stream does not care about requiredExchangeMode.
     */
    public static <T> DataStream<Pair<T, RowData>> rangeShuffleByKey(
            DataStream<Pair<T, RowData>> inputDataStream,
            Comparator<T> keyComparator,
            Class<T> keyClass,
            int sampleSize,
            int rangeNum) {
        Transformation<Pair<T, RowData>> input = inputDataStream.getTransformation();

        OneInputTransformation<Pair<T, RowData>, T> keyInput =
                new OneInputTransformation<>(
                        input,
                        "ABSTRACT KEY",
                        new StreamMap<>(Pair::getLeft),
                        TypeInformation.of(keyClass),
                        input.getParallelism());

        // 1. Fixed size sample in each partitions.
        OneInputTransformation<T, Tuple2<Double, T>> localSample =
                new OneInputTransformation<>(
                        keyInput,
                        "LOCAL SAMPLE",
                        new LocalSampleOperator<>(sampleSize),
                        new TupleTypeInfo<>(
                                BasicTypeInfo.DOUBLE_TYPE_INFO, TypeInformation.of(keyClass)),
                        keyInput.getParallelism());

        // 2. Collect all the samples and gather them into a sorted key range.
        OneInputTransformation<Tuple2<Double, T>, List<T>> sampleAndHistogram =
                new OneInputTransformation<>(
                        localSample,
                        "GLOBAL SAMPLE",
                        new GlobalSampleOperator<>(sampleSize, keyComparator, rangeNum),
                        new ListTypeInfo<>(TypeInformation.of(keyClass)),
                        1);

        // 3. Take range boundaries as broadcast input and take the tuple of partition id and
        // record as output.
        // The shuffle mode of input edge must be BATCH to avoid dead lock. See
        // DeadlockBreakupProcessor.
        TwoInputTransformation<List<T>, Pair<T, RowData>, Tuple2<Integer, Pair<T, RowData>>>
                preparePartition =
                        new TwoInputTransformation<>(
                                new PartitionTransformation<>(
                                        sampleAndHistogram,
                                        new BroadcastPartitioner<>(),
                                        StreamExchangeMode.BATCH),
                                new PartitionTransformation<>(
                                        input,
                                        new ForwardPartitioner<>(),
                                        StreamExchangeMode.BATCH),
                                "ASSIGN RANGE INDEX",
                                new AssignRangeIndexOperator<>(keyComparator),
                                new TupleTypeInfo<>(
                                        BasicTypeInfo.INT_TYPE_INFO, input.getOutputType()),
                                input.getParallelism());

        // 4. Remove the partition id. (shuffle according range partition)
        return new DataStream<>(
                inputDataStream.getExecutionEnvironment(),
                new OneInputTransformation<>(
                        new PartitionTransformation<>(
                                preparePartition,
                                new CustomPartitionerWrapper<>(
                                        new AssignRangeIndexOperator.RangePartitioner(rangeNum),
                                        new AssignRangeIndexOperator.Tuple2KeySelector<>()),
                                StreamExchangeMode.BATCH),
                        "REMOVE KEY",
                        new RemoveRangeIndexOperator<>(),
                        input.getOutputType(),
                        ExecutionConfig.PARALLELISM_DEFAULT));
    }
}
