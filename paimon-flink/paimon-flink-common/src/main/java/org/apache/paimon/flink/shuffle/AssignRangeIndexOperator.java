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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Comparator;
import java.util.List;

/**
 * This two-input-operator require a input with RangeBoundaries as broadcast input, and generate
 * Tuple2 which includes range index and record from the other input itself as output.
 */
@Internal
public class AssignRangeIndexOperator<T>
        extends TableStreamOperator<Tuple2<Integer, Pair<T, RowData>>>
        implements TwoInputStreamOperator<
                        List<T>, Pair<T, RowData>, Tuple2<Integer, Pair<T, RowData>>>,
                InputSelectable {

    private static final long serialVersionUID = 1L;

    private transient List<T> boundaries;
    private transient Collector<Tuple2<Integer, Pair<T, RowData>>> collector;

    private final Comparator<T> keyComparator;

    public AssignRangeIndexOperator(Comparator<T> comparator) {
        this.keyComparator = comparator;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.collector = new StreamRecordCollector<>(output);
    }

    @Override
    public void processElement1(StreamRecord<List<T>> streamRecord) throws Exception {
        this.boundaries = streamRecord.getValue();
    }

    @Override
    public void processElement2(StreamRecord<Pair<T, RowData>> streamRecord) throws Exception {
        if (boundaries == null) {
            throw new RuntimeException("There should be one data from the first input.");
        }
        Pair<T, RowData> row = streamRecord.getValue();
        collector.collect(new Tuple2<>(binarySearch(row.getLeft()), row));
    }

    @Override
    public InputSelection nextSelection() {
        return boundaries == null ? InputSelection.FIRST : InputSelection.ALL;
    }

    private int binarySearch(T key) {
        int low = 0;
        int high = this.boundaries.size() - 1;

        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final int result = keyComparator.compare(key, this.boundaries.get(mid));

            if (result > 0) {
                low = mid + 1;
            } else if (result < 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        // key not found, but the low index is the target
        // bucket, since the boundaries are the upper bound
        return low;
    }

    /** A {@link KeySelector} to select by f0 of tuple2. */
    public static class Tuple2KeySelector<T>
            implements KeySelector<Tuple2<Integer, Pair<T, RowData>>, Integer> {

        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(Tuple2<Integer, Pair<T, RowData>> tuple2) throws Exception {
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
