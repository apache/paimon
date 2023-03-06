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

import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.GenericArray;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.data.InternalArray;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.format.FieldStats;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.table.store.file.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.flink.table.store.file.utils.SerializationUtils.serializeBinaryRow;
import static org.apache.flink.table.store.utils.Preconditions.checkNotNull;

/** A serialized row bytes to cache {@link FieldStats}. */
public class BinaryTableStats {

    @Nullable private InternalRow row;
    @Nullable private FieldStats[] cacheArray;
    @Nullable private BinaryRow cacheMin;
    @Nullable private BinaryRow cacheMax;
    @Nullable private Long[] cacheNullCounts;

    public BinaryTableStats(InternalRow row) {
        this.row = row;
    }

    public BinaryTableStats(BinaryRow cacheMin, BinaryRow cacheMax, Long[] cacheNullCounts) {
        this(cacheMin, cacheMax, cacheNullCounts, null);
    }

    public BinaryTableStats(
            BinaryRow cacheMin,
            BinaryRow cacheMax,
            Long[] cacheNullCounts,
            @Nullable FieldStats[] cacheArray) {
        this.cacheMin = cacheMin;
        this.cacheMax = cacheMax;
        this.cacheNullCounts = cacheNullCounts;
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

    public BinaryRow min() {
        if (cacheMin == null) {
            checkNotNull(row);
            cacheMin = deserializeBinaryRow(this.row.getBinary(0));
        }
        return cacheMin;
    }

    public BinaryRow max() {
        if (cacheMax == null) {
            checkNotNull(row);
            cacheMax = deserializeBinaryRow(this.row.getBinary(1));
        }
        return cacheMax;
    }

    public Long[] nullCounts() {
        if (cacheNullCounts == null) {
            checkNotNull(row);
            InternalArray internalArray = row.getArray(2);
            Long[] array = new Long[internalArray.size()];
            for (int i = 0; i < array.length; i++) {
                array[i] = internalArray.isNullAt(i) ? null : internalArray.getLong(i);
            }
            return array;
        }
        return cacheNullCounts;
    }

    public InternalRow toRowData() {
        return row == null
                ? GenericRow.of(
                        serializeBinaryRow(min()),
                        serializeBinaryRow(max()),
                        new GenericArray(nullCounts()))
                : row;
    }

    public static BinaryTableStats fromRowData(InternalRow row) {
        return new BinaryTableStats(row);
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
        return Objects.equals(min(), that.min())
                && Objects.equals(max(), that.max())
                && Arrays.equals(nullCounts(), that.nullCounts());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(min(), max());
        result = 31 * result + Arrays.hashCode(nullCounts());
        return result;
    }
}
