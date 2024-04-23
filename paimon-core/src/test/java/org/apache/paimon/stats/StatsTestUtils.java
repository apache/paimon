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
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Utils for stats related tests. */
public class StatsTestUtils {

    public static FieldStats[] convertWithoutSchemaEvolution(
            BinaryTableStats stats, RowType rowType) {
        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(rowType);
        Object[] mins = converter.convert(stats.minValues());
        Object[] maxs = converter.convert(stats.maxValues());
        FieldStats[] ret = new FieldStats[rowType.getFieldCount()];
        BinaryArray nulls = stats.nullCounts();
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new FieldStats(mins[i], maxs[i], nulls.isNullAt(i) ? null : nulls.getLong(i));
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public static <T> void checkRollingFileStats(
            FieldStats expected, List<T> actualObjects, Function<T, FieldStats> mapToStats) {
        List<FieldStats> actual = new ArrayList<>();
        for (T object : actualObjects) {
            actual.add(mapToStats.apply(object));
        }
        if (expected.minValue() instanceof Comparable) {
            Object actualMin = null;
            Object actualMax = null;
            for (FieldStats stats : actual) {
                if (stats.minValue() != null
                        && (actualMin == null
                                || ((Comparable<Object>) stats.minValue()).compareTo(actualMin)
                                        < 0)) {
                    actualMin = stats.minValue();
                }
                if (stats.maxValue() != null
                        && (actualMax == null
                                || ((Comparable<Object>) stats.maxValue()).compareTo(actualMax)
                                        > 0)) {
                    actualMax = stats.maxValue();
                }
            }
            assertThat(actualMin).isEqualTo(expected.minValue());
            assertThat(actualMax).isEqualTo(expected.maxValue());
        } else {
            for (FieldStats stats : actual) {
                assertThat(stats.minValue()).isNull();
                assertThat(stats.maxValue()).isNull();
            }
        }
        assertThat(actual.stream().mapToLong(FieldStats::nullCount).sum())
                .isEqualTo(expected.nullCount());
    }

    public static BinaryTableStats newEmptyTableStats() {
        return newEmptyTableStats(1);
    }

    public static BinaryTableStats newEmptyTableStats(int fieldCount) {
        FieldStatsArraySerializer statsConverter =
                new FieldStatsArraySerializer(RowType.of(new IntType()));
        FieldStats[] array = new FieldStats[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            array[i] = new FieldStats(null, null, 0L);
        }
        return statsConverter.toBinary(array);
    }

    public static BinaryTableStats newTableStats(int min, int max) {
        FieldStatsArraySerializer statsConverter =
                new FieldStatsArraySerializer(RowType.of(new IntType()));
        return statsConverter.toBinary(new FieldStats[] {new FieldStats(min, max, 0L)});
    }
}
