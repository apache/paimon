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
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Utils for stats related tests. */
public class StatsTestUtils {

    public static SimpleColStats[] convertWithoutSchemaEvolution(
            SimpleStats stats, RowType rowType) {
        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(rowType);
        Object[] mins = converter.convert(stats.minValues());
        Object[] maxs = converter.convert(stats.maxValues());
        SimpleColStats[] ret = new SimpleColStats[rowType.getFieldCount()];
        BinaryArray nulls = stats.nullCounts();
        for (int i = 0; i < ret.length; i++) {
            ret[i] =
                    new SimpleColStats(
                            mins[i], maxs[i], nulls.isNullAt(i) ? null : nulls.getLong(i));
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public static <T> void checkRollingFileStats(
            SimpleColStats expected,
            List<T> actualObjects,
            Function<T, SimpleColStats> mapToStats) {
        List<SimpleColStats> actual = new ArrayList<>();
        for (T object : actualObjects) {
            actual.add(mapToStats.apply(object));
        }
        if (expected.min() instanceof Comparable) {
            Object actualMin = null;
            Object actualMax = null;
            for (SimpleColStats stats : actual) {
                if (stats.min() != null
                        && (actualMin == null
                                || ((Comparable<Object>) stats.min()).compareTo(actualMin) < 0)) {
                    actualMin = stats.min();
                }
                if (stats.max() != null
                        && (actualMax == null
                                || ((Comparable<Object>) stats.max()).compareTo(actualMax) > 0)) {
                    actualMax = stats.max();
                }
            }
            assertThat(actualMin).isEqualTo(expected.min());
            assertThat(actualMax).isEqualTo(expected.max());
        } else {
            for (SimpleColStats stats : actual) {
                assertThat(stats.min()).isNull();
                assertThat(stats.max()).isNull();
            }
        }
        assertThat(actual.stream().mapToLong(SimpleColStats::nullCount).sum())
                .isEqualTo(expected.nullCount());
    }

    public static SimpleStats newEmptySimpleStats() {
        return newEmptySimpleStats(1);
    }

    public static SimpleStats newEmptySimpleStats(int fieldCount) {
        SimpleStatsConverter statsConverter = new SimpleStatsConverter(RowType.of(new IntType()));
        SimpleColStats[] array = new SimpleColStats[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            array[i] = new SimpleColStats(null, null, 0L);
        }
        return statsConverter.toBinaryAllMode(array);
    }

    public static SimpleStats newSimpleStats(int min, int max) {
        SimpleStatsConverter statsConverter = new SimpleStatsConverter(RowType.of(new IntType()));
        return statsConverter.toBinaryAllMode(
                new SimpleColStats[] {new SimpleColStats(min, max, 0L)});
    }
}
