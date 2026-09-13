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

package org.apache.paimon.sort.hilbert;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HilbertIndexer}. */
public class HilbertIndexerTest {

    @Test
    public void testBooleanValuesDistinctFromNull() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BOOLEAN(), DataTypes.BOOLEAN()},
                        new String[] {"a", "b"});
        HilbertIndexer indexer = new HilbertIndexer(rowType, Arrays.asList("a", "b"));
        indexer.open();

        // FALSE, TRUE and NULL are the only three states a boolean column has, and each has to
        // land on its own point of the curve. Pinning the exact curve position of each one also
        // pins the mapping itself (0 / 1 / the null sentinel), so an inverted mapping that keeps
        // the three distinct cannot slip through and desync this from the Spark UDF.
        byte[] falseIndex = indexer.index(booleanRow(false));
        byte[] trueIndex = indexer.index(booleanRow(true));
        byte[] nullIndex = indexer.index(booleanRow(null));

        assertThat(falseIndex).isEqualTo(HilbertIndexer.hilbertCurvePosBytes(new Long[] {0L, 0L}));
        assertThat(trueIndex).isEqualTo(HilbertIndexer.hilbertCurvePosBytes(new Long[] {1L, 1L}));
        assertThat(nullIndex)
                .isEqualTo(
                        HilbertIndexer.hilbertCurvePosBytes(
                                new Long[] {Long.MAX_VALUE, Long.MAX_VALUE}));
        assertThat(trueIndex).isNotEqualTo(nullIndex);
        assertThat(falseIndex).isNotEqualTo(nullIndex);
        assertThat(falseIndex).isNotEqualTo(trueIndex);
    }

    @Test
    public void testHighDimensionIndexKeepsAllBits() {
        // 9 dimensions: the 63*9-bit index needs 71 bytes; distinct points that differ
        // only in the low-order bits must stay distinct instead of being truncated away
        Long[][] points = {
            {0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L},
            {0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L},
        };
        byte[] first = HilbertIndexer.hilbertCurvePosBytes(points[0]);
        byte[] second = HilbertIndexer.hilbertCurvePosBytes(points[1]);
        assertThat(first).hasSize(71);
        assertThat(second).hasSize(71);
        assertThat(first).isNotEqualTo(second);

        // 16 dimensions: the top bit being set adds BigInteger's sign byte, so the width
        // must cover it or the low byte is truncated away
        Long[] highBits =
                new Long[] {
                    Long.MAX_VALUE, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L
                };
        Long[] highBitsVariant = highBits.clone();
        highBitsVariant[15] = 1L;
        byte[] highFirst = HilbertIndexer.hilbertCurvePosBytes(highBits);
        byte[] highSecond = HilbertIndexer.hilbertCurvePosBytes(highBitsVariant);
        assertThat(highFirst).hasSize(127);
        assertThat(highSecond).hasSize(127);
        assertThat(highFirst).isNotEqualTo(highSecond);

        // up to 8 dimensions keep the legacy 63-byte width, so existing indexes are stable
        assertThat(HilbertIndexer.hilbertCurvePosBytes(new Long[] {0L, 0L})).hasSize(63);
        assertThat(HilbertIndexer.hilbertCurvePosBytes(new Long[] {0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L}))
                .hasSize(63);
    }

    private static GenericRow booleanRow(Boolean value) {
        GenericRow row = new GenericRow(2);
        row.setField(0, value);
        row.setField(1, value);
        return row;
    }
}
