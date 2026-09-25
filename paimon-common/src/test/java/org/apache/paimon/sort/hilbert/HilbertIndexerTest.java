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

import org.davidmoten.hilbert.HilbertCurve;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

        // the width is 63*N/8 + 1 for every N, so a 2-dimension key is 16 bytes and an
        // 8-dimension one is 64 — the extra byte over the 63-byte magnitude is what makes
        // room for BigInteger's sign byte
        assertThat(HilbertIndexer.hilbertCurvePosBytes(new Long[] {0L, 0L})).hasSize(16);
        assertThat(HilbertIndexer.hilbertCurvePosBytes(new Long[] {0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L}))
                .hasSize(64);
    }

    /**
     * At 8 dimensions the index fills 63 bytes, so the top half of the space carries BigInteger's
     * sign byte and spills to 64. Truncating that back to 63 does not merely lose resolution: the
     * leading zero makes a large index sort below a smaller one.
     */
    @Test
    public void testEightDimensionKeysOrderLikeTheirIndex() {
        List<Long[]> points = new ArrayList<>();
        for (long i = 0; i < 24; i++) {
            // spread the points over the whole space so some land in the top half
            long v = Long.MAX_VALUE / 23 * i;
            points.add(new Long[] {v, v / 3, i, Long.MAX_VALUE - v, v / 7, i * 31, v / 11, i});
        }

        for (Long[] left : points) {
            for (Long[] right : points) {
                int indexOrder = index(left).compareTo(index(right));
                int keyOrder =
                        compareUnsigned(
                                HilbertIndexer.hilbertCurvePosBytes(left),
                                HilbertIndexer.hilbertCurvePosBytes(right));
                assertThat(Integer.signum(keyOrder))
                        .as(
                                "key order must follow index order for %s vs %s",
                                Arrays.toString(left), Arrays.toString(right))
                        .isEqualTo(Integer.signum(indexOrder));
            }
        }
    }

    /**
     * Sizes and inequalities are proxies for the property the width exists to guarantee: the key
     * carries the whole index. State it directly, at the two dimension counts where the index needs
     * its last byte the most.
     */
    @Test
    public void testKeyCarriesTheWholeIndex() {
        for (int dimensions : new int[] {8, 9}) {
            Long[] topOfSpace = new Long[dimensions];
            Arrays.fill(topOfSpace, Long.MAX_VALUE);
            Long[] oneLowBitOff = topOfSpace.clone();
            oneLowBitOff[dimensions - 1] = Long.MAX_VALUE - 1;

            for (Long[] point : new Long[][] {topOfSpace, oneLowBitOff}) {
                byte[] key = HilbertIndexer.hilbertCurvePosBytes(point);
                assertThat(new BigInteger(1, key))
                        .as("key must round-trip the index at %s dimensions", dimensions)
                        .isEqualTo(index(point));
            }
        }
    }

    private static BigInteger index(Long[] points) {
        long[] data = Arrays.stream(points).mapToLong(Long::longValue).toArray();
        return HilbertCurve.bits(63).dimensions(points.length).index(data);
    }

    private static int compareUnsigned(byte[] left, byte[] right) {
        assertThat(left).hasSameSizeAs(right);
        for (int i = 0; i < left.length; i++) {
            int cmp = Integer.compare(left[i] & 0xFF, right[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private static GenericRow booleanRow(Boolean value) {
        GenericRow row = new GenericRow(2);
        row.setField(0, value);
        row.setField(1, value);
        return row;
    }
}
