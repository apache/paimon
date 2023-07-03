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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BinaryTableStats}. */
public class BinaryTableFieldStatsTestCollector {
    @Test
    public void testBinaryTableStats() {
        List<Integer> minList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        List<Integer> maxList = Arrays.asList(11, 12, 13, 14, 15, 16, 17, 18, 19);
        Long[] nullCounts = new Long[] {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L};

        BinaryRow minRowData = binaryRow(minList);
        BinaryRow maxRowData = binaryRow(maxList);
        BinaryTableStats tableStats1 = new BinaryTableStats(minRowData, maxRowData, nullCounts);

        InternalRow rowData = tableStats1.toRowData();
        BinaryTableStats tableStats2 = new BinaryTableStats(rowData);
        assertThat(tableStats1.min()).isEqualTo(tableStats2.min());
        assertThat(tableStats1.max()).isEqualTo(tableStats2.max());
        assertThat(tableStats1.nullCounts()).isEqualTo(tableStats2.nullCounts());
        assertThat(tableStats1).isEqualTo(tableStats2);

        FieldStatsArraySerializer serializer =
                new FieldStatsArraySerializer(
                        RowType.of(
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType()));

        FieldStats[] fieldStatsArray1 = tableStats1.fields(serializer, 100L);
        FieldStats[] fieldStatsArray2 = tableStats2.fields(serializer, 100L);
        assertThat(fieldStatsArray1.length).isEqualTo(fieldStatsArray2.length).isEqualTo(9);
        assertThat(fieldStatsArray1).isEqualTo(fieldStatsArray2);
        for (int i = 0; i < fieldStatsArray1.length; i++) {
            FieldStats fieldStats = fieldStatsArray1[i];
            assertThat(fieldStats.minValue()).isEqualTo(1 + i);
            assertThat(fieldStats.maxValue()).isEqualTo(11 + i);
            assertThat(fieldStats.nullCount()).isEqualTo(i);
        }

        BinaryTableStats tableStats3 =
                new BinaryTableStats(minRowData, maxRowData, nullCounts, fieldStatsArray1);
        assertThat(tableStats3).isEqualTo(tableStats1).isEqualTo(tableStats2);
        assertThat(tableStats3.fields(serializer)).isEqualTo(fieldStatsArray2);
    }

    private BinaryRow binaryRow(List<Integer> valueList) {
        BinaryRow b = new BinaryRow(valueList.size());
        BinaryRowWriter writer = new BinaryRowWriter(b);
        for (int i = 0; i < valueList.size(); i++) {
            writer.writeInt(i, valueList.get(i));
        }
        writer.complete();
        return b;
    }
}
