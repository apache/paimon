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

package org.apache.paimon.stats;

import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedArray;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Converter for array of {@link SimpleColStats}. */
public class SimpleStatsEvolution {

    private final List<String> fieldNames;
    @Nullable private final int[] indexMapping;
    @Nullable private final CastFieldGetter[] castFieldGetters;

    private final Map<List<String>, int[]> indexMappings;

    private final GenericRow emptyValues;
    private final GenericArray emptyNullCounts;

    public SimpleStatsEvolution(
            RowType rowType,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castFieldGetters) {
        this.fieldNames = rowType.getFieldNames();
        this.indexMapping = indexMapping;
        this.castFieldGetters = castFieldGetters;
        this.indexMappings = new ConcurrentHashMap<>();
        this.emptyValues = new GenericRow(fieldNames.size());
        this.emptyNullCounts = new GenericArray(new Object[fieldNames.size()]);
    }

    public Result evolution(
            SimpleStats stats, @Nullable Long rowCount, @Nullable List<String> denseFields) {
        InternalRow minValues = stats.minValues();
        InternalRow maxValues = stats.maxValues();
        InternalArray nullCounts = stats.nullCounts();

        if (denseFields != null && denseFields.isEmpty()) {
            // optimize for empty dense fields
            minValues = emptyValues;
            maxValues = emptyValues;
            nullCounts = emptyNullCounts;
        } else if (denseFields != null) {
            int[] denseIndexMapping =
                    indexMappings.computeIfAbsent(
                            denseFields,
                            k -> fieldNames.stream().mapToInt(denseFields::indexOf).toArray());
            minValues = ProjectedRow.from(denseIndexMapping).replaceRow(minValues);
            maxValues = ProjectedRow.from(denseIndexMapping).replaceRow(maxValues);
            nullCounts = ProjectedArray.from(denseIndexMapping).replaceArray(nullCounts);
        }

        if (indexMapping != null) {
            minValues = ProjectedRow.from(indexMapping).replaceRow(minValues);
            maxValues = ProjectedRow.from(indexMapping).replaceRow(maxValues);

            if (rowCount == null) {
                throw new RuntimeException("Schema Evolution for stats needs row count.");
            }

            nullCounts = new NullCountsEvoArray(indexMapping, nullCounts, rowCount);
        }

        if (castFieldGetters != null) {
            minValues = CastedRow.from(castFieldGetters).replaceRow(minValues);
            maxValues = CastedRow.from(castFieldGetters).replaceRow(maxValues);
        }

        return new Result(minValues, maxValues, nullCounts);
    }

    /** Result to {@link SimpleStats} evolution. */
    public static class Result {

        private final InternalRow minValues;
        private final InternalRow maxValues;
        private final InternalArray nullCounts;

        public Result(InternalRow minValues, InternalRow maxValues, InternalArray nullCounts) {
            this.minValues = minValues;
            this.maxValues = maxValues;
            this.nullCounts = nullCounts;
        }

        public InternalRow minValues() {
            return minValues;
        }

        public InternalRow maxValues() {
            return maxValues;
        }

        public InternalArray nullCounts() {
            return nullCounts;
        }
    }

    private static class NullCountsEvoArray implements InternalArray {

        private final int[] indexMapping;
        private final InternalArray array;
        private final long notFoundValue;

        private NullCountsEvoArray(int[] indexMapping, InternalArray array, long notFoundValue) {
            this.indexMapping = indexMapping;
            this.array = array;
            this.notFoundValue = notFoundValue;
        }

        @Override
        public int size() {
            return indexMapping.length;
        }

        @Override
        public boolean isNullAt(int pos) {
            if (indexMapping[pos] < 0) {
                return false;
            }
            return array.isNullAt(indexMapping[pos]);
        }

        @Override
        public long getLong(int pos) {
            if (indexMapping[pos] < 0) {
                return notFoundValue;
            }
            return array.getLong(indexMapping[pos]);
        }

        // ============================= Unsupported Methods ================================

        @Override
        public boolean getBoolean(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte getByte(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public short getShort(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getInt(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BinaryString getString(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getBinary(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Variant getVariant(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalArray getArray(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalMap getMap(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean[] toBooleanArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] toByteArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short[] toShortArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int[] toIntArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long[] toLongArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float[] toFloatArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double[] toDoubleArray() {
            throw new UnsupportedOperationException();
        }
    }
}
