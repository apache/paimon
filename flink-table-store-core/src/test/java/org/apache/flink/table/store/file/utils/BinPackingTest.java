/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.store.file.utils;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Copied from apache iceberg. Test cases for {@link BinPacking}. */
public class BinPackingTest {
    @Test
    public void testBasicBinPacking() {
        // Should pack the first 2 values
        assertThat(list(list(1, 2), list(3), list(4), list(5)))
                .isEqualTo(pack(list(1, 2, 3, 4, 5), 3));

        // Should pack the first 2 values
        assertThat(list(list(1, 2), list(3), list(4), list(5)))
                .isEqualTo(pack(list(1, 2, 3, 4, 5), 5));

        // Should pack the first 3 values
        assertThat(list(list(1, 2, 3), list(4), list(5))).isEqualTo(pack(list(1, 2, 3, 4, 5), 6));

        // Should pack the first 3 values
        assertThat(list(list(1, 2, 3), list(4), list(5))).isEqualTo(pack(list(1, 2, 3, 4, 5), 8));

        // Should pack the first 3 values, last 2 values
        assertThat(list(list(1, 2, 3), list(4, 5))).isEqualTo(pack(list(1, 2, 3, 4, 5), 9));

        // Should pack the first 4 values
        assertThat(list(list(1, 2, 3, 4), list(5))).isEqualTo(pack(list(1, 2, 3, 4, 5), 10));

        // Should pack the first 4 values
        assertThat(list(list(1, 2, 3, 4), list(5))).isEqualTo(pack(list(1, 2, 3, 4, 5), 14));

        // Should pack the first 5 values
        assertThat(list(list(1, 2, 3, 4, 5))).isEqualTo(pack(list(1, 2, 3, 4, 5), 15));
    }

    @Test
    public void testReverseBinPackingSingleLookback() {
        // Should pack the first 2 values
        assertThat(list(list(1, 2), list(3), list(4), list(5)))
                .isEqualTo(packEnd(list(1, 2, 3, 4, 5), 3, 1));

        // Should pack the first 2 values
        assertThat(list(list(1, 2), list(3), list(4), list(5)))
                .isEqualTo(packEnd(list(1, 2, 3, 4, 5), 4, 1));

        // Should pack the second and third values
        assertThat(list(list(1), list(2, 3), list(4), list(5)))
                .isEqualTo(packEnd(list(1, 2, 3, 4, 5), 5, 1));

        // Should pack the first 3 values
        assertThat(list(list(1, 2, 3), list(4), list(5)))
                .isEqualTo(packEnd(list(1, 2, 3, 4, 5), 6, 1));

        // Should pack the first two pairs of values
        assertThat(list(list(1, 2), list(3, 4), list(5)))
                .isEqualTo(packEnd(list(1, 2, 3, 4, 5), 7, 1));

        // Should pack the first two pairs of values
        assertThat(list(list(1, 2), list(3, 4), list(5)))
                .isEqualTo(packEnd(list(1, 2, 3, 4, 5), 8, 1));

        // Should pack the first 3 values, last 2 values
        assertThat(list(list(1, 2, 3), list(4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 9, 1));

        // Should pack the first 3 values, last 2 values
        assertThat(list(list(1, 2, 3), list(4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 11, 1));

        // Should pack the first 3 values, last 2 values
        assertThat(list(list(1, 2), list(3, 4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 12, 1));

        // Should pack the last 4 values
        assertThat(list(list(1), list(2, 3, 4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 14, 1));

        // Should pack the first 5 values
        assertThat(list(list(1, 2, 3, 4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 15, 1));
    }

    @Test
    public void testReverseBinPackingUnlimitedLookback() {
        // Should pack the first 2 values
        assertThat(list(list(1, 2), list(3), list(4), list(5)))
                .isEqualTo(packEnd(list(1, 2, 3, 4, 5), 3));

        // Should pack 1 with 3
        assertThat(list(list(2), list(1, 3), list(4), list(5)))
                .isEqualTo(packEnd(list(1, 2, 3, 4, 5), 4));

        // Should pack 2,3 and 1,4
        assertThat(list(list(2, 3), list(1, 4), list(5)))
                .isEqualTo(packEnd(list(1, 2, 3, 4, 5), 5));

        // Should pack 2,4 and 1,5
        assertThat(list(list(3), list(2, 4), list(1, 5)))
                .isEqualTo(packEnd(list(1, 2, 3, 4, 5), 6));

        // Should pack 3,4 and 2,5
        assertThat(list(list(1), list(3, 4), list(2, 5)))
                .isEqualTo(packEnd(list(1, 2, 3, 4, 5), 7));

        // Should pack 1,2,3 and 3,5
        assertThat(list(list(1, 2, 4), list(3, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 8));

        // Should pack the first 3 values, last 2 values
        assertThat(list(list(1, 2, 3), list(4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 9));

        // Should pack 2,3 and 1,4,5
        assertThat(list(list(2, 3), list(1, 4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 10));

        // Should pack 1,3 and 2,4,5
        assertThat(list(list(1, 3), list(2, 4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 11));

        // Should pack 1,2 and 3,4,5
        assertThat(list(list(1, 2), list(3, 4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 12));

        // Should pack 1,2 and 3,4,5
        assertThat(list(list(2), list(1, 3, 4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 13));

        // Should pack the last 4 values
        assertThat(list(list(1), list(2, 3, 4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 14));

        // Should pack the first 5 values
        assertThat(list(list(1, 2, 3, 4, 5))).isEqualTo(packEnd(list(1, 2, 3, 4, 5), 15));
    }

    @Test
    public void testBinPackingLookBack() {
        // lookback state:
        // 1. [5]
        // 2. [5, 1]
        // 3. [5, 1], [5]
        // 4. [5, 1, 1], [5]
        // 5. [5, 1, 1], [5], [5]
        // 6. [5, 1, 1, 1], [5], [5]

        // Unlimited look-back: should merge ones into first bin
        assertThat(list(list(5, 1, 1, 1), list(5), list(5)))
                .isEqualTo(pack(list(5, 1, 5, 1, 5, 1), 8));

        // lookback state:
        // 1. [5]
        // 2. [5, 1]
        // 3. [5, 1], [5]
        // 4. [5, 1, 1], [5]
        // 5. [5], [5]          ([5, 1, 1] drops out of look-back)
        // 6. [5, 1], [5]

        // 2 bin look-back: should merge two ones into first bin
        assertThat(list(list(5, 1, 1), list(5, 1), list(5)))
                .isEqualTo(pack(list(5, 1, 5, 1, 5, 1), 8, 2));

        // lookback state:
        // 1. [5]
        // 2. [5, 1]
        // 3. [5]               ([5, 1] drops out of look-back)
        // 4. [5, 1]
        // 5. [5]               ([5, 1] #2 drops out of look-back)
        // 6. [5, 1]

        // 1 bin look-back: should merge ones with fives
        assertThat(list(list(5, 1), list(5, 1), list(5, 1)))
                .isEqualTo(pack(list(5, 1, 5, 1, 5, 1), 8, 1));

        // 2 bin look-back: should merge until targetWeight when largestBinFirst is enabled
        assertThat(list(list(36, 36, 36), list(128), list(36, 65), list(65)))
                .isEqualTo(pack(list(36, 36, 36, 36, 65, 65, 128), 128, 2, true));

        // 1 bin look-back: should merge until targetWeight when largestBinFirst is enabled
        assertThat(list(list(64, 64), list(128), list(32, 32, 32, 32)))
                .isEqualTo(pack(list(64, 64, 128, 32, 32, 32, 32), 128, 1, true));
    }

    private List<List<Integer>> pack(List<Integer> items, long targetWeight) {
        return pack(items, targetWeight, Integer.MAX_VALUE);
    }

    private List<List<Integer>> pack(List<Integer> items, long targetWeight, int lookback) {
        return pack(items, targetWeight, lookback, false);
    }

    private List<List<Integer>> pack(
            List<Integer> items, long targetWeight, int lookback, boolean largestBinFirst) {
        BinPacking.ListPacker<Integer> packer =
                new BinPacking.ListPacker<>(targetWeight, lookback, largestBinFirst);
        return packer.pack(items, Integer::longValue);
    }

    private List<List<Integer>> packEnd(List<Integer> items, long targetWeight) {
        return packEnd(items, targetWeight, Integer.MAX_VALUE);
    }

    private List<List<Integer>> packEnd(List<Integer> items, long targetWeight, int lookback) {
        BinPacking.ListPacker<Integer> packer =
                new BinPacking.ListPacker<>(targetWeight, lookback, false);
        return packer.packEnd(items, Integer::longValue);
    }

    private <T> List<T> list(T... items) {
        return Lists.newArrayList(items);
    }
}
