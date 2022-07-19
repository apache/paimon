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

package org.apache.flink.table.store.file.stats;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.format.FieldStats;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.table.store.file.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.flink.table.store.file.utils.SerializationUtils.serializeBinaryRow;

/**
 * A serialized row bytes to cache {@link FieldStats}.
 *
 * <p>TODO: {@link Predicate} get min and max from {@link BinaryRowData}, lazily deserialization.
 */
public class BinaryTableStats {

    private final BinaryRowData min;
    private final BinaryRowData max;
    private final long[] nullCounts;

    @Nullable private FieldStats[] cacheArray;

    public BinaryTableStats(BinaryRowData min, BinaryRowData max, long[] nullCounts) {
        this(min, max, nullCounts, null);
    }

    public BinaryTableStats(
            BinaryRowData min,
            BinaryRowData max,
            long[] nullCounts,
            @Nullable FieldStats[] cacheArray) {
        this.min = min;
        this.max = max;
        this.nullCounts = nullCounts;
        this.cacheArray = cacheArray;
    }

    public FieldStats[] fields(FieldStatsArraySerializer converter) {
        return fields(converter, null);
    }

    public FieldStats[] fields(FieldStatsArraySerializer converter, @Nullable Long rowCount) {
        if (cacheArray == null) {
            cacheArray = converter.fromBinary(this, rowCount);
        }
        return cacheArray;
    }

    public BinaryRowData min() {
        return min;
    }

    public BinaryRowData max() {
        return max;
    }

    public long[] nullCounts() {
        return nullCounts;
    }

    public RowData toRowData() {
        return GenericRowData.of(
                serializeBinaryRow(min), serializeBinaryRow(max), new GenericArrayData(nullCounts));
    }

    public static BinaryTableStats fromRowData(RowData row) {
        return new BinaryTableStats(
                deserializeBinaryRow(row.getBinary(0)),
                deserializeBinaryRow(row.getBinary(1)),
                row.getArray(2).toLongArray());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BinaryTableStats that = (BinaryTableStats) o;
        return Objects.equals(min, that.min)
                && Objects.equals(max, that.max)
                && Arrays.equals(nullCounts, that.nullCounts);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(min, max);
        result = 31 * result + Arrays.hashCode(nullCounts);
        return result;
    }
}
