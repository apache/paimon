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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SimpleStatsMerger}. */
public class SimpleStatsMergerTest {

    private static final RowType ROW_TYPE =
            RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.STRING());

    @Test
    public void testMergeIgnoresEmptyStatsContributors() {
        SimpleStats knownStats = stats(0, 10, 1L, 5, 8, 0L);
        SimpleStats merged =
                SimpleStatsMerger.merge(Arrays.asList(knownStats, EMPTY_STATS), ROW_TYPE, null);

        assertThat(merged).isNotEqualTo(EMPTY_STATS);
        InternalRow.FieldGetter minGetter = InternalRow.createFieldGetter(DataTypes.INT(), 0);
        InternalRow.FieldGetter maxGetter = InternalRow.createFieldGetter(DataTypes.INT(), 0);
        assertThat(minGetter.getFieldOrNull(merged.minValues())).isEqualTo(0);
        assertThat(maxGetter.getFieldOrNull(merged.maxValues())).isEqualTo(10);
    }

    @Test
    public void testMergeDenseStatsWithProjectedColumns() {
        RowType projectedType = ROW_TYPE.project(Arrays.asList("f0", "f2"));
        SimpleStats left = stats(projectedType, 0, 5, 0L, null, null, 1L);
        SimpleStats right = stats(projectedType, 3, 10, 2L, null, null, 0L);
        SimpleStats merged =
                SimpleStatsMerger.merge(
                        Arrays.asList(left, right), ROW_TYPE, Arrays.asList("f0", "f2"));

        InternalRow.FieldGetter minGetter = InternalRow.createFieldGetter(DataTypes.INT(), 0);
        InternalRow.FieldGetter maxGetter = InternalRow.createFieldGetter(DataTypes.INT(), 0);
        assertThat(minGetter.getFieldOrNull(merged.minValues())).isEqualTo(0);
        assertThat(maxGetter.getFieldOrNull(merged.maxValues())).isEqualTo(10);
        assertThat(merged.nullCounts().getLong(0)).isEqualTo(2L);
        assertThat(merged.nullCounts().getLong(1)).isEqualTo(1L);
    }

    @Test
    public void testMergePropagatesUnknownMinBound() {
        SimpleStatsConverter converter = new SimpleStatsConverter(ROW_TYPE);
        SimpleStats unknownMin =
                converter.toBinaryAllMode(
                        new SimpleColStats[] {
                            new SimpleColStats(null, 10, 0L),
                            new SimpleColStats(5, 8, 0L),
                            new SimpleColStats(null, null, 0L)
                        });
        SimpleStats known = stats(0, 10, 0L, 5, 8, 0L);

        SimpleStats merged =
                SimpleStatsMerger.merge(Arrays.asList(unknownMin, known), ROW_TYPE, null);

        assertThat(merged.minValues().isNullAt(0)).isTrue();
        assertThat(merged.maxValues().getInt(0)).isEqualTo(10);
    }

    @Test
    public void testMergePropagatesUnknownNullCount() {
        SimpleStatsConverter converter = new SimpleStatsConverter(ROW_TYPE);
        SimpleStats unknownNullCount =
                converter.toBinaryAllMode(
                        new SimpleColStats[] {
                            new SimpleColStats(0, 10, null),
                            new SimpleColStats(5, 8, 0L),
                            new SimpleColStats(null, null, 0L)
                        });
        SimpleStats known = stats(0, 10, 2L, 5, 8, 0L);

        SimpleStats merged =
                SimpleStatsMerger.merge(Arrays.asList(unknownNullCount, known), ROW_TYPE, null);

        assertThat(merged.nullCounts().isNullAt(0)).isTrue();
        assertThat(merged.nullCounts().getLong(1)).isEqualTo(0L);
    }

    private static SimpleStats stats(
            int min0, int max0, long null0, int min1, int max1, long null1) {
        InternalRowSerializer serializer = new InternalRowSerializer(ROW_TYPE);
        return new SimpleStats(
                serializer.toBinaryRow(GenericRow.of(min0, min1, null)).copy(),
                serializer.toBinaryRow(GenericRow.of(max0, max1, null)).copy(),
                BinaryArray.fromLongArray(new Long[] {null0, null1, 0L}));
    }

    private static SimpleStats stats(
            RowType rowType,
            Integer min0,
            Integer max0,
            Long null0,
            Integer min1,
            Integer max1,
            Long null1) {
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        return new SimpleStats(
                serializer.toBinaryRow(GenericRow.of(min0, min1)).copy(),
                serializer.toBinaryRow(GenericRow.of(max0, max1)).copy(),
                BinaryArray.fromLongArray(new Long[] {null0, null1}));
    }
}
