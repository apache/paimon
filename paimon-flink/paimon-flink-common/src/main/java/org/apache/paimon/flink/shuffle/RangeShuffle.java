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

package org.apache.paimon.flink.shuffle;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.types.InternalRowToSizeVisitor;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SerializableSupplier;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

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
            int localSampleSize,
            int globalSampleSize,
            int rangeNum,
            int outParallelism,
            RowType valueRowType,
            boolean isSortBySize) {
        Transformation<Tuple2<T, RowData>> input = inputDataStream.getTransformation();

        OneInputTransformation<Tuple2<T, RowData>, Tuple2<T, Integer>> keyInput =
                new OneInputTransformation<>(
                        input,
                        "ABSTRACT KEY AND SIZE",
                        new StreamMap<>(new KeyAndSizeExtractor<>(valueRowType, isSortBySize)),
                        new TupleTypeInfo<>(keyTypeInformation, BasicTypeInfo.INT_TYPE_INFO),
                        input.getParallelism());

        // 1. Fixed size sample in each partitions.
        OneInputTransformation<Tuple2<T, Integer>, Tuple3<Double, T, Integer>> localSample =
                new OneInputTransformation<>(
                        keyInput,
                        "LOCAL SAMPLE",
                        new LocalSampleOperator<>(localSampleSize),
                        new TupleTypeInfo<>(
                                BasicTypeInfo.DOUBLE_TYPE_INFO,
                                keyTypeInformation,
                                BasicTypeInfo.INT_TYPE_INFO),
                        keyInput.getParallelism());

        // 2. Collect all the samples and gather them into a sorted key range.
        OneInputTransformation<Tuple3<Double, T, Integer>, List<T>> sampleAndHistogram =
                new OneInputTransformation<>(
                        localSample,
                        "GLOBAL SAMPLE",
                        new GlobalSampleOperator<>(globalSampleSize, keyComparator, rangeNum),
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

    /** KeyAndSizeExtractor is responsible for extracting the sort key and row size. */
    public static class KeyAndSizeExtractor<T>
            extends RichMapFunction<Tuple2<T, RowData>, Tuple2<T, Integer>> {
        private final RowType rowType;
        private final boolean isSortBySize;
        private transient List<BiFunction<DataGetters, Integer, Integer>> fieldSizeCalculator;

        public KeyAndSizeExtractor(RowType rowType, boolean isSortBySize) {
            this.rowType = rowType;
            this.isSortBySize = isSortBySize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            InternalRowToSizeVisitor internalRowToSizeVisitor = new InternalRowToSizeVisitor();
            fieldSizeCalculator =
                    rowType.getFieldTypes().stream()
                            .map(dataType -> dataType.accept(internalRowToSizeVisitor))
                            .collect(Collectors.toList());
        }

        @Override
        public Tuple2<T, Integer> map(Tuple2<T, RowData> keyAndRowData) throws Exception {
            if (isSortBySize) {
                int size = 0;
                for (int i = 0; i < fieldSizeCalculator.size(); i++) {
                    size +=
                            fieldSizeCalculator
                                    .get(i)
                                    .apply(new FlinkRowWrapper(keyAndRowData.f1), i);
                }
                return new Tuple2<>(keyAndRowData.f0, size);
            } else {
                // when basing on quantity, we don't need the size of the data, so setting it to a
                // constant of 1 would be sufficient.
                return new Tuple2<>(keyAndRowData.f0, 1);
            }
        }
    }

    /**
     * LocalSampleOperator wraps the sample logic on the partition side (the first phase of
     * distributed sample algorithm). Outputs sampled weight with record.
     *
     * <p>See {@link Sampler}.
     */
    @Internal
    public static class LocalSampleOperator<T>
            extends TableStreamOperator<Tuple3<Double, T, Integer>>
            implements OneInputStreamOperator<Tuple2<T, Integer>, Tuple3<Double, T, Integer>>,
                    BoundedOneInput {

        private static final long serialVersionUID = 1L;

        private final int numSample;

        private transient Collector<Tuple3<Double, T, Integer>> collector;
        private transient Sampler<Tuple2<T, Integer>> sampler;

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
        public void processElement(StreamRecord<Tuple2<T, Integer>> streamRecord) throws Exception {
            sampler.collect(streamRecord.getValue());
        }

        @Override
        public void endInput() {
            Iterator<Tuple2<Double, Tuple2<T, Integer>>> sampled = sampler.sample();
            while (sampled.hasNext()) {
                Tuple2<Double, Tuple2<T, Integer>> next = sampled.next();

                collector.collect(new Tuple3<>(next.f0, next.f1.f0, next.f1.f1));
            }
        }
    }

    /**
     * Global sample for range partition. Inputs weight with record. Outputs list of sampled record.
     *
     * <p>See {@link Sampler}.
     */
    private static class GlobalSampleOperator<T> extends TableStreamOperator<List<T>>
            implements OneInputStreamOperator<Tuple3<Double, T, Integer>, List<T>>,
                    BoundedOneInput {

        private static final long serialVersionUID = 1L;

        private final int numSample;
        private final int rangesNum;
        private final SerializableSupplier<Comparator<T>> comparatorSupplier;

        private transient Comparator<T> keyComparator;
        private transient Collector<List<T>> collector;
        private transient Sampler<Tuple2<T, Integer>> sampler;

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
        public void processElement(StreamRecord<Tuple3<Double, T, Integer>> record)
                throws Exception {
            Tuple3<Double, T, Integer> tuple = record.getValue();
            sampler.collect(tuple.f0, new Tuple2<>(tuple.f1, tuple.f2));
        }

        @Override
        public void endInput() {
            Iterator<Tuple2<Double, Tuple2<T, Integer>>> sampled = sampler.sample();

            List<Tuple2<T, Integer>> sampledData = new ArrayList<>();

            while (sampled.hasNext()) {
                sampledData.add(sampled.next().f1);
            }

            sampledData.sort((o1, o2) -> keyComparator.compare(o1.f0, o2.f0));

            List<T> range;
            if (sampledData.isEmpty()) {
                range = new ArrayList<>();
            } else {
                range = Arrays.asList(allocateRangeBaseSize(sampledData, rangesNum));
            }

            collector.collect(range);
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

    @VisibleForTesting
    static <T> T[] allocateRangeBaseSize(List<Tuple2<T, Integer>> sampledData, int rangesNum) {
        int sampeNum = sampledData.size();
        int boundarySize = rangesNum - 1;
        @SuppressWarnings("unchecked")
        T[] boundaries = (T[]) new Object[boundarySize];

        if (!sampledData.isEmpty()) {
            long restSize = sampledData.stream().mapToLong(t -> (long) t.f1).sum();
            double stepRange = restSize / (double) rangesNum;

            int currentWeight = 0;
            int index = 0;

            for (int i = 0; i < boundarySize; i++) {
                while (currentWeight < stepRange && index < sampeNum) {
                    boundaries[i] = sampledData.get(Math.min(index, sampeNum - 1)).f0;
                    int sampleWeight = sampledData.get(index++).f1;
                    currentWeight += sampleWeight;
                    restSize -= sampleWeight;
                }

                currentWeight = 0;
                stepRange = restSize / (double) (rangesNum - i - 1);
            }
        }

        for (int i = 0; i < boundarySize; i++) {
            if (boundaries[i] == null) {
                boundaries[i] = sampledData.get(sampeNum - 1).f0;
            }
        }

        return boundaries;
    }
}
