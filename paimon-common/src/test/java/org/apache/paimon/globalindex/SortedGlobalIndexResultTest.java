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

package org.apache.paimon.globalindex;

import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_FIRST;
import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SortedGlobalIndexResult}. */
public class SortedGlobalIndexResultTest {

    private static final Comparator<Object> INT_COMPARATOR =
            (left, right) -> Integer.compare((Integer) left, (Integer) right);

    @Test
    public void testMergeKeepsGlobalTopN() {
        SortedGlobalIndexResult first = result(NULLS_LAST, 3, entry(50, 5), entry(10, 1));
        SortedGlobalIndexResult second = result(NULLS_LAST, 3, entry(40, 4), entry(30, 3));

        GlobalIndexResult merged = first.or(second);

        assertThat(merged).isInstanceOf(SortedGlobalIndexResult.class);
        assertThat(merged.results()).containsExactlyInAnyOrder(3L, 4L, 5L);
    }

    @Test
    public void testMergeBreaksBoundaryTiesByRowId() {
        SortedGlobalIndexResult first = result(NULLS_LAST, 2, entry(20, 9), entry(10, 3));
        SortedGlobalIndexResult second = result(NULLS_LAST, 2, entry(10, 2), entry(5, 1));

        assertThat(first.or(second).results()).containsExactlyInAnyOrder(2L, 9L);
    }

    @Test
    public void testNullOrdering() {
        SortedGlobalIndexResult first = result(NULLS_FIRST, 2, entry(null, 2), entry(100, 4));
        SortedGlobalIndexResult second = result(NULLS_FIRST, 2, entry(null, 1), entry(200, 3));

        assertThat(first.or(second).results()).containsExactlyInAnyOrder(1L, 2L);

        first = result(NULLS_LAST, 2, entry(null, 2), entry(100, 4));
        second = result(NULLS_LAST, 2, entry(null, 1), entry(200, 3));

        assertThat(first.or(second).results()).containsExactlyInAnyOrder(3L, 4L);
    }

    @Test
    public void testOffsetPreservesSortKeys() {
        SortedGlobalIndexResult first = result(NULLS_LAST, 2, entry(20, 1), entry(10, 2));
        SortedGlobalIndexResult second = result(NULLS_LAST, 2, entry(30, 1)).offset(10);

        GlobalIndexResult merged = first.or(second);

        assertThat(merged).isInstanceOf(SortedGlobalIndexResult.class);
        assertThat(merged.results()).containsExactlyInAnyOrder(1L, 11L);
    }

    @Test
    public void testPlainResultUsesConservativeUnion() {
        SortedGlobalIndexResult sorted = result(NULLS_LAST, 1, entry(20, 1));
        RoaringNavigableMap64 unindexedRows = new RoaringNavigableMap64();
        unindexedRows.add(2);

        GlobalIndexResult merged = sorted.or(GlobalIndexResult.create(unindexedRows));

        assertThat(merged).isNotInstanceOf(SortedGlobalIndexResult.class);
        assertThat(merged.results()).containsExactlyInAnyOrder(1L, 2L);
    }

    private SortedGlobalIndexResult result(
            org.apache.paimon.predicate.SortValue.NullOrdering nullOrdering,
            int limit,
            SortedGlobalIndexResult.Entry... entries) {
        List<SortedGlobalIndexResult.Entry> candidates = Arrays.asList(entries);
        return SortedGlobalIndexResult.create(candidates, INT_COMPARATOR, nullOrdering, limit);
    }

    private SortedGlobalIndexResult.Entry entry(Integer key, long rowId) {
        return new SortedGlobalIndexResult.Entry(key, rowId);
    }
}
