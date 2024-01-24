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
import org.apache.paimon.utils.SerializableSupplier;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.XORShiftRandom;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * RangeShuffle Util to shuffle the input stream by the sampling range. See `rangeShuffleBykey`
 * method how to build the topo.
 */
public class RangeShuffle {

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
    public static <T> DataStream<Tuple2<T, RowData>> rangeShuffleByKey(
            DataStream<Tuple2<T, RowData>> inputDataStream,
            SerializableSupplier<Comparator<T>> keyComparator,
            TypeInformation<T> keyTypeInformation,
            int sampleSize,
            int rangeNum,
            int outParallelism) {
        Transformation<Tuple2<T, RowData>> input = inputDataStream.getTransformation();

        OneInputTransformation<Tuple2<T, RowData>, T> keyInput =
                new OneInputTransformation<>(
                        input,
                        "ABSTRACT KEY",
                        new StreamMap<>(a -> a.f0),
                        keyTypeInformation,
                        input.getParallelism());

        // 1. Fixed size sample in each partitions.
        OneInputTransformation<T, Tuple2<Double, T>> localSample =
                new OneInputTransformation<>(
                        keyInput,
                        "LOCAL SAMPLE",
                        new LocalSampleOperator<>(sampleSize),
                        new TupleTypeInfo<>(BasicTypeInfo.DOUBLE_TYPE_INFO, keyTypeInformation),
                        keyInput.getParallelism());

        // 2. Collect all the samples and gather them into a sorted key range.
        OneInputTransformation<Tuple2<Double, T>, List<T>> sampleAndHistogram =
                new OneInputTransformation<>(
                        localSample,
                        "GLOBAL SAMPLE",
                        new GlobalSampleOperator<>(sampleSize, keyComparator, rangeNum),
                        new ListTypeInfo<>(keyTypeInformation),
                        1);

        // 3. Take range boundaries as broadcast input and take the tuple of partition id and
        // record as output.
        // The shuffle mode of input edge must be BATCH to avoid dead lock. See
        // DeadlockBreakupProcessor.
        TwoInputTransformation<List<T>, Tuple2<T, RowData>, Tuple2<Integer, Tuple2<T, RowData>>>
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
                        outParallelism));
    }

    /**
     * LocalSampleOperator wraps the sample logic on the partition side (the first phase of
     * distributed sample algorithm). Outputs sampled weight with record.
     *
     * <p>See {@link Sampler}.
     */
    @Internal
    public static class LocalSampleOperator<T> extends TableStreamOperator<Tuple2<Double, T>>
            implements OneInputStreamOperator<T, Tuple2<Double, T>>, BoundedOneInput {

        private static final long serialVersionUID = 1L;

        private final int numSample;

        private transient Collector<Tuple2<Double, T>> collector;
        private transient Sampler<T> sampler;

        public LocalSampleOperator(int numSample) {
            this.numSample = numSample;
        }

        @Override
        public void open() throws Exception {
            super.open();
            this.collector = new StreamRecordCollector<>(output);
            sampler = new Sampler<>(numSample, System.nanoTime());
        }

        @Override
        public void processElement(StreamRecord<T> streamRecord) throws Exception {
            sampler.collect(streamRecord.getValue());
        }

        @Override
        public void endInput() throws Exception {
            Iterator<Tuple2<Double, T>> sampled = sampler.sample();
            while (sampled.hasNext()) {
                collector.collect(sampled.next());
            }
        }
    }

    /**
     * Global sample for range partition. Inputs weight with record. Outputs list of sampled record.
     *
     * <p>See {@link Sampler}.
     */
    private static class GlobalSampleOperator<T> extends TableStreamOperator<List<T>>
            implements OneInputStreamOperator<Tuple2<Double, T>, List<T>>, BoundedOneInput {

        private static final long serialVersionUID = 1L;

        private final int numSample;
        private final int rangesNum;
        private final SerializableSupplier<Comparator<T>> comparatorSupplier;

        private transient Comparator<T> keyComparator;
        private transient Collector<List<T>> collector;
        private transient Sampler<T> sampler;

        public GlobalSampleOperator(
                int numSample,
                SerializableSupplier<Comparator<T>> comparatorSupplier,
                int rangesNum) {
            this.numSample = numSample;
            this.comparatorSupplier = comparatorSupplier;
            this.rangesNum = rangesNum;
        }

        @Override
        public void open() throws Exception {
            super.open();
            this.keyComparator = comparatorSupplier.get();
            this.sampler = new Sampler<>(numSample, 0L);
            this.collector = new StreamRecordCollector<>(output);
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Double, T>> record) throws Exception {
            Tuple2<Double, T> tuple = record.getValue();
            sampler.collect(tuple.f0, tuple.f1);
        }

        @Override
        public void endInput() throws Exception {
            Iterator<Tuple2<Double, T>> sampled = sampler.sample();

            List<T> sampledData = new ArrayList<>();
            while (sampled.hasNext()) {
                sampledData.add(sampled.next().f1);
            }

            sampledData.sort(keyComparator);

            int boundarySize = rangesNum - 1;
            @SuppressWarnings("unchecked")
            T[] boundaries = (T[]) new Object[boundarySize];
            if (sampledData.size() > 0) {
                double avgRange = sampledData.size() / (double) rangesNum;
                for (int i = 1; i < rangesNum; i++) {
                    T record = sampledData.get((int) (i * avgRange));
                    boundaries[i - 1] = record;
                }
                collector.collect(Arrays.asList(boundaries));
            } else {
                collector.collect(Collections.emptyList());
            }
        }
    }

    /**
     * This two-input-operator require a input with RangeBoundaries as broadcast input, and generate
     * Tuple2 which includes range index and record from the other input itself as output.
     */
    private static class AssignRangeIndexOperator<T>
            extends TableStreamOperator<Tuple2<Integer, Tuple2<T, RowData>>>
            implements TwoInputStreamOperator<
                            List<T>, Tuple2<T, RowData>, Tuple2<Integer, Tuple2<T, RowData>>>,
                    InputSelectable {

        private static final long serialVersionUID = 1L;

        private final SerializableSupplier<Comparator<T>> keyComparatorSupplier;

        private transient List<Pair<T, RandomList>> keyIndex;
        private transient Collector<Tuple2<Integer, Tuple2<T, RowData>>> collector;
        private transient Comparator<T> keyComparator;

        public AssignRangeIndexOperator(SerializableSupplier<Comparator<T>> keyComparatorSupplier) {
            this.keyComparatorSupplier = keyComparatorSupplier;
        }

        @Override
        public void open() throws Exception {
            super.open();
            this.keyComparator = keyComparatorSupplier.get();
            this.collector = new StreamRecordCollector<>(output);
        }

        @Override
        public void processElement1(StreamRecord<List<T>> streamRecord) {
            keyIndex = new ArrayList<>();

            T last = null;
            int index = 0;
            for (T t : streamRecord.getValue()) {
                if (last != null && keyComparator.compare(last, t) == 0) {
                    keyIndex.get(keyIndex.size() - 1).getRight().add(index++);
                } else {
                    Pair<T, RandomList> pair = Pair.of(t, new RandomList());
                    pair.getRight().add(index++);
                    keyIndex.add(pair);

                    last = t;
                }
            }
        }

        @Override
        public void processElement2(StreamRecord<Tuple2<T, RowData>> streamRecord) {
            if (keyIndex == null || keyIndex.isEmpty()) {
                throw new RuntimeException(
                        "There should be one data from the first input. And boundaries should not be empty.");
            }
            Tuple2<T, RowData> row = streamRecord.getValue();
            collector.collect(new Tuple2<>(binarySearch(row.f0), row));
        }

        @Override
        public InputSelection nextSelection() {
            return keyIndex == null ? InputSelection.FIRST : InputSelection.ALL;
        }

        private int binarySearch(T key) {
            int lastIndex = this.keyIndex.size() - 1;
            int low = 0;
            int high = lastIndex;

            while (low <= high) {
                final int mid = (low + high) >>> 1;
                final Pair<T, RandomList> indexPair = keyIndex.get(mid);
                final int result = keyComparator.compare(key, indexPair.getLeft());

                if (result > 0) {
                    low = mid + 1;
                } else if (result < 0) {
                    high = mid - 1;
                } else {
                    return indexPair.getRight().get();
                }
            }

            // key not found, but the low index is the target
            // bucket, since the boundaries are the upper bound
            return low > lastIndex
                    ? keyIndex.get(lastIndex).getRight().get()
                    : keyIndex.get(low).getRight().get();
        }

        /** A {@link KeySelector} to select by f0 of tuple2. */
        public static class Tuple2KeySelector<T>
                implements KeySelector<Tuple2<Integer, Tuple2<T, RowData>>, Integer> {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer getKey(Tuple2<Integer, Tuple2<T, RowData>> tuple2) throws Exception {
                return tuple2.f0;
            }
        }

        /** A {@link Partitioner} to partition by id with range. */
        public static class RangePartitioner implements Partitioner<Integer> {

            private static final long serialVersionUID = 1L;

            private final int totalRangeNum;

            public RangePartitioner(int totalRangeNum) {
                this.totalRangeNum = totalRangeNum;
            }

            @Override
            public int partition(Integer key, int numPartitions) {
                Preconditions.checkArgument(
                        numPartitions < totalRangeNum,
                        "Num of subPartitions should < totalRangeNum: " + totalRangeNum);
                int partition = key / (totalRangeNum / numPartitions);
                return Math.min(numPartitions - 1, partition);
            }
        }
    }

    /** Remove the range index and return the actual record. */
    private static class RemoveRangeIndexOperator<T> extends TableStreamOperator<Tuple2<T, RowData>>
            implements OneInputStreamOperator<
                    Tuple2<Integer, Tuple2<T, RowData>>, Tuple2<T, RowData>> {

        private static final long serialVersionUID = 1L;

        private transient Collector<Tuple2<T, RowData>> collector;

        @Override
        public void open() throws Exception {
            super.open();
            this.collector = new StreamRecordCollector<>(output);
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, Tuple2<T, RowData>>> streamRecord)
                throws Exception {
            collector.collect(streamRecord.getValue().f1);
        }
    }

    /**
     * A simple in memory implementation Sampling, and with only one pass through the input
     * iteration whose size is unpredictable. The basic idea behind this sampler implementation is
     * to generate a random number for each input element as its weight, select the top K elements
     * with max weight. As the weights are generated randomly, so are the selected top K elements.
     * In the first phase, we generate random numbers as the weights for each element and select top
     * K elements as the output of each partitions. In the second phase, we select top K elements
     * from all the outputs of the first phase.
     *
     * <p>This implementation refers to the algorithm described in <a
     * href="researcher.ibm.com/files/us-dpwoodru/tw11.pdf">"Optimal Random Sampling from
     * Distributed Streams Revisited"</a>.
     */
    private static class Sampler<T> {

        private final int numSamples;
        private final Random random;
        private final PriorityQueue<Tuple2<Double, T>> queue;

        private int index = 0;
        private Tuple2<Double, T> smallest = null;

        /**
         * Create a new sampler with reservoir size and a supplied random number generator.
         *
         * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
         */
        Sampler(int numSamples, long seed) {
            Preconditions.checkArgument(numSamples >= 0, "numSamples should be non-negative.");
            this.numSamples = numSamples;
            this.random = new XORShiftRandom(seed);
            this.queue = new PriorityQueue<>(numSamples, Comparator.comparingDouble(o -> o.f0));
        }

        void collect(T rowData) {
            collect(random.nextDouble(), rowData);
        }

        void collect(double weight, T key) {
            if (index < numSamples) {
                // Fill the queue with first K elements from input.
                addQueue(weight, key);
            } else {
                // Remove the element with the smallest weight,
                // and append current element into the queue.
                if (weight > smallest.f0) {
                    queue.remove();
                    addQueue(weight, key);
                }
            }
            index++;
        }

        private void addQueue(double weight, T row) {
            queue.add(new Tuple2<>(weight, row));
            smallest = queue.peek();
        }

        Iterator<Tuple2<Double, T>> sample() {
            return queue.iterator();
        }
    }

    /** Contains integers and randomly get one. */
    private static class RandomList {

        private static final Random RANDOM = new Random();

        private final List<Integer> list = new ArrayList<>();

        public void add(int i) {
            list.add(i);
        }

        public int get() {
            return list.get(RANDOM.nextInt(list.size()));
        }
    }
}
