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

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;

/** Utility to merge {@link SimpleStats} from multiple files with the same stats schema. */
public class SimpleStatsMerger {

    private SimpleStatsMerger() {}

    public static SimpleStats merge(List<SimpleStats> statsList, RowType rowType) {
        return merge(statsList, rowType, null);
    }

    public static SimpleStats merge(
            List<SimpleStats> statsList, RowType rowType, @Nullable List<String> valueStatsCols) {
        if (statsList.isEmpty()) {
            return EMPTY_STATS;
        }

        RowType statsRowType = valueStatsCols == null ? rowType : rowType.project(valueStatsCols);
        int fieldCount = statsRowType.getFieldCount();
        InternalRowSerializer serializer = new InternalRowSerializer(statsRowType);
        InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            DataType fieldType = statsRowType.getTypeAt(i);
            fieldGetters[i] = InternalRow.createFieldGetter(fieldType, i);
        }

        Object[] minValues = new Object[fieldCount];
        Object[] maxValues = new Object[fieldCount];
        long[] nullCounts = new long[fieldCount];
        boolean[] minUnknown = new boolean[fieldCount];
        boolean[] maxUnknown = new boolean[fieldCount];
        boolean[] nullCountUnknown = new boolean[fieldCount];
        boolean[] hasValue = new boolean[fieldCount];

        for (SimpleStats stats : statsList) {
            if (stats.equals(EMPTY_STATS)) {
                continue;
            }
            InternalRow minRow = stats.minValues();
            InternalRow maxRow = stats.maxValues();
            InternalArray nullCountArray = stats.nullCounts();
            for (int i = 0; i < fieldCount; i++) {
                if (minRow.isNullAt(i)) {
                    minUnknown[i] = true;
                } else {
                    minValues[i] = pickMin(minValues[i], fieldGetters[i].getFieldOrNull(minRow));
                }
                if (maxRow.isNullAt(i)) {
                    maxUnknown[i] = true;
                } else {
                    maxValues[i] = pickMax(maxValues[i], fieldGetters[i].getFieldOrNull(maxRow));
                }
                if (nullCountArray.isNullAt(i)) {
                    nullCountUnknown[i] = true;
                } else {
                    nullCounts[i] += nullCountArray.getLong(i);
                }
                hasValue[i] =
                        hasValue[i]
                                || minUnknown[i]
                                || maxUnknown[i]
                                || nullCountUnknown[i]
                                || !minRow.isNullAt(i)
                                || !maxRow.isNullAt(i)
                                || !nullCountArray.isNullAt(i);
            }
        }

        boolean allEmpty = true;
        for (boolean value : hasValue) {
            if (value) {
                allEmpty = false;
                break;
            }
        }
        if (allEmpty) {
            return EMPTY_STATS;
        }

        Object[] mergedMinValues = new Object[fieldCount];
        Object[] mergedMaxValues = new Object[fieldCount];
        Long[] nullCountObjects = new Long[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            mergedMinValues[i] = minUnknown[i] ? null : minValues[i];
            mergedMaxValues[i] = maxUnknown[i] ? null : maxValues[i];
            nullCountObjects[i] = nullCountUnknown[i] ? null : nullCounts[i];
        }
        return new SimpleStats(
                serializer.toBinaryRow(GenericRow.of(mergedMinValues)).copy(),
                serializer.toBinaryRow(GenericRow.of(mergedMaxValues)).copy(),
                BinaryArray.fromLongArray(nullCountObjects));
    }

    @Nullable
    private static Object pickMin(@Nullable Object current, @Nullable Object candidate) {
        if (candidate == null) {
            return current;
        }
        if (current == null) {
            return candidate;
        }
        if (current instanceof Comparable && candidate instanceof Comparable) {
            Comparable<Object> currentComparable = (Comparable<Object>) current;
            return currentComparable.compareTo(candidate) <= 0 ? current : candidate;
        }
        return current;
    }

    @Nullable
    private static Object pickMax(@Nullable Object current, @Nullable Object candidate) {
        if (candidate == null) {
            return current;
        }
        if (current == null) {
            return candidate;
        }
        if (current instanceof Comparable && candidate instanceof Comparable) {
            Comparable<Object> currentComparable = (Comparable<Object>) current;
            return currentComparable.compareTo(candidate) >= 0 ? current : candidate;
        }
        return current;
    }

    public static boolean sameValueStatsCols(
            @Nullable List<String> left, @Nullable List<String> right) {
        if (left == null && right == null) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        return left.equals(right);
    }

    public static List<SimpleStats> collectValueStatsFromFiles(List<DataFileMeta> files) {
        List<SimpleStats> stats = new ArrayList<>(files.size());
        for (DataFileMeta file : files) {
            stats.add(file.valueStats());
        }
        return stats;
    }
}
