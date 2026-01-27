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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;

import java.util.List;

/** Assign first row id for each row through binary search. */
public class FirstRowIdAssigner extends RichMapFunction<RowData, Tuple2<Long, RowData>> {

    private final FirstRowIdLookup firstRowIdLookup;

    private final int rowIdFieldIndex;

    public FirstRowIdAssigner(List<Long> firstRowIds, RowType rowType) {
        this.firstRowIdLookup = new FirstRowIdLookup(firstRowIds);
        this.rowIdFieldIndex = rowType.getFieldNames().indexOf(SpecialFields.ROW_ID.name());
        Preconditions.checkState(this.rowIdFieldIndex >= 0, "Do not found _ROW_ID column.");
    }

    @Override
    public Tuple2<Long, RowData> map(RowData value) throws Exception {
        long rowId = value.getLong(rowIdFieldIndex);
        return new Tuple2<>(firstRowIdLookup.lookup(rowId), value);
    }

    /** The Key Selector to get firstRowId from tuple2. */
    public static class FirstRowIdKeySelector implements KeySelector<Tuple2<Long, RowData>, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long getKey(Tuple2<Long, RowData> value) throws Exception {
            return value.f0;
        }
    }

    /** The Partitioner to partition rows by their firstRowId. */
    public static class FirstRowIdPartitioner implements Partitioner<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public int partition(Long firstRowId, int numPartitions) {
            Preconditions.checkNotNull(firstRowId, "FirstRowId should not be null.");
            // Now we just simply floorMod the hash result of the firstRowId.
            // We could make it more balanced by considering the number of records of each row id
            // range.
            return floorMod(MurmurHashUtils.fmix(firstRowId), numPartitions);
        }

        /** For compatible with java-1.8. */
        private int floorMod(long x, int y) {
            return (int) (x - Math.floorDiv(x, (long) y) * y);
        }
    }
}
