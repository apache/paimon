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

import org.apache.paimon.utils.TwoJoinedRow;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

/** Converter for array of {@link SimpleColStats}. */
public class SimpleStatsConverter {

    private final InternalRowSerializer serializer;

    @Nullable private final int[] indexMapping;
    @Nullable private final CastFieldGetter[] castFieldGetters;
    @Nullable private final int[] partitionMap;

    public SimpleStatsConverter(RowType type) {
        this(type, null, null);
    }

    public SimpleStatsConverter(RowType type, int[] partitionMap) {
        this(type, null, null, partitionMap);
    }

    public SimpleStatsConverter(
            RowType type,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castFieldGetters) {
        RowType safeType = toAllFieldsNullableRowType(type);
        this.serializer = new InternalRowSerializer(safeType);
        this.indexMapping = indexMapping;
        this.castFieldGetters = castFieldGetters;
        this.partitionMap = null;
    }

    public SimpleStatsConverter(
            RowType type,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castFieldGetters,
            @Nullable int[] partitionMap) {
        RowType safeType = toAllFieldsNullableRowType(type);
        this.serializer = new InternalRowSerializer(safeType);
        this.indexMapping = indexMapping;
        this.castFieldGetters = castFieldGetters;
        this.partitionMap = partitionMap;
    }

    public SimpleStats toBinary(SimpleColStats[] stats) {
        int rowFieldCount = stats.length;
        GenericRow minValues = new GenericRow(rowFieldCount);
        GenericRow maxValues = new GenericRow(rowFieldCount);
        Long[] nullCounts = new Long[rowFieldCount];
        for (int i = 0; i < rowFieldCount; i++) {
            minValues.setField(i, stats[i].min());
            maxValues.setField(i, stats[i].max());
            nullCounts[i] = stats[i].nullCount();
        }
        return new SimpleStats(
                serializer.toBinaryRow(minValues).copy(),
                serializer.toBinaryRow(maxValues).copy(),
                BinaryArray.fromLongArray(nullCounts));
    }

    public InternalRow evolution(BinaryRow values) {
        InternalRow row = values;
        if (indexMapping != null) {
            row = ProjectedRow.from(indexMapping).replaceRow(row);
        }

        if (castFieldGetters != null) {
            row = CastedRow.from(castFieldGetters).replaceRow(values);
        }

        return row;
    }

    public InternalRow evolutionWithPartition(BinaryRow values, BinaryRow partition) {
        InternalRow row = values;
        if (indexMapping != null) {
            row = ProjectedRow.from(indexMapping).replaceRow(row);
        }

        if (castFieldGetters != null) {
            row = CastedRow.from(castFieldGetters).replaceRow(values);
        }

        if (partitionMap != null) {
            row = TwoJoinedRow.from(partitionMap).replaceMainRow(values).replaceSecondRow(partition);
        }

        return row;
    }

    public InternalArray evolution(BinaryArray nullCounts, @Nullable Long rowCount) {
        if (indexMapping == null) {
            return nullCounts;
        }

        if (rowCount == null) {
            throw new RuntimeException("Schema Evolution for stats needs row count.");
        }

        return new NullCountsEvoArray(indexMapping, nullCounts, rowCount);
    }

    public InternalArray evolutionWithPartition(BinaryArray nullCounts, @Nullable Long rowCount) {
        if (indexMapping == null) {
            return nullCounts;
        }

        if (rowCount == null) {
            throw new RuntimeException("Schema Evolution for stats needs row count.");
        }

        return new NullCountsEvoArray(indexMapping, nullCounts, rowCount);
    }

    private static RowType toAllFieldsNullableRowType(RowType rowType) {
        // as stated in RollingFile.Writer#finish, col stats are not collected currently so
        // min/max values are all nulls
        return RowType.builder()
                .fields(
                        rowType.getFields().stream()
                                .map(f -> f.type().copy(true))
                                .toArray(DataType[]::new),
                        rowType.getFieldNames().toArray(new String[0]))
                .build();
    }

    private static class NullCountsEvoArray implements InternalArray {

        private final int[] indexMapping;
        private final InternalArray array;
        private final long notFoundValue;

        protected NullCountsEvoArray(int[] indexMapping, InternalArray array, long notFoundValue) {
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
