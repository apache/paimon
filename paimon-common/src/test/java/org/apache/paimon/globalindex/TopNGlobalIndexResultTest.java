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

import org.apache.paimon.globalindex.btree.BTreeIndexReader.KeyRowIds;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_FIRST;
import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TopNGlobalIndexResult}. */
public class TopNGlobalIndexResultTest {

    private static final Comparator<Object> INT_COMPARATOR =
            (left, right) -> Integer.compare((Integer) left, (Integer) right);

    @Test
    public void testMergeKeepsGlobalTopN() {
        TopNGlobalIndexResult first = result(NULLS_LAST, 3, keyRowIds(50, 5), keyRowIds(10, 1));
        TopNGlobalIndexResult second = result(NULLS_LAST, 3, keyRowIds(40, 4), keyRowIds(30, 3));

        GlobalIndexResult merged = first.or(second);

        assertThat(merged).isInstanceOf(TopNGlobalIndexResult.class);
        assertThat(merged.results()).containsExactlyInAnyOrder(3L, 4L, 5L);
    }

    @Test
    public void testMergeKeepsGlobalAscendingTopN() {
        TopNGlobalIndexResult first =
                result(ASCENDING, NULLS_LAST, 3, keyRowIds(50, 5), keyRowIds(10, 1));
        TopNGlobalIndexResult second =
                result(ASCENDING, NULLS_LAST, 3, keyRowIds(40, 4), keyRowIds(30, 3));

        TopNGlobalIndexResult merged = (TopNGlobalIndexResult) first.or(second);

        assertThat(merged.results()).containsExactlyInAnyOrder(1L, 3L, 4L);
        assertThat(merged.keyRowIds()).extracting(KeyRowIds::key).containsExactly(10, 30, 40);
    }

    @Test
    public void testCannotMergeDifferentDirections() {
        TopNGlobalIndexResult ascending = result(ASCENDING, NULLS_LAST, 1, keyRowIds(10, 1));
        TopNGlobalIndexResult descending = result(DESCENDING, NULLS_LAST, 1, keyRowIds(10, 1));

        assertThatThrownBy(() -> ascending.or(descending))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Cannot merge sorted global index results with different sort directions.");
    }

    @Test
    public void testMergeBreaksBoundaryTiesByRowId() {
        TopNGlobalIndexResult first = result(NULLS_LAST, 2, keyRowIds(20, 9), keyRowIds(10, 3));
        TopNGlobalIndexResult second = result(NULLS_LAST, 2, keyRowIds(10, 2), keyRowIds(5, 1));

        assertThat(first.or(second).results()).containsExactlyInAnyOrder(2L, 9L);
    }

    @Test
    public void testMergeCombinesSameKeyAndLimitsByRowIdCardinality() {
        TopNGlobalIndexResult first = result(NULLS_LAST, 4, keyRowIds(20, 9, 3), keyRowIds(10, 1));
        TopNGlobalIndexResult second = result(NULLS_LAST, 4, keyRowIds(20, 2, 9), keyRowIds(15, 4));

        TopNGlobalIndexResult merged = (TopNGlobalIndexResult) first.or(second);

        assertThat(merged.results()).containsExactlyInAnyOrder(2L, 3L, 4L, 9L);
        assertThat(merged.keyRowIds()).hasSize(2);
        assertThat(merged.keyRowIds().get(0).key()).isEqualTo(20);
        assertThat(merged.keyRowIds().get(0).rowIds()).containsExactly(2L, 3L, 9L);
        assertThat(merged.keyRowIds().get(1).key()).isEqualTo(15);
        assertThat(merged.keyRowIds().get(1).rowIds()).containsExactly(4L);
    }

    @Test
    public void testSingleKeyIsLimitedByRowIdCardinality() {
        TopNGlobalIndexResult result =
                result(NULLS_LAST, 2, keyRowIds(20, 5, 3, 4), keyRowIds(10, 1));

        assertThat(result.keyRowIds()).hasSize(1);
        assertThat(result.keyRowIds().get(0).key()).isEqualTo(20);
        assertThat(result.keyRowIds().get(0).rowIds()).containsExactly(3L, 4L);
        assertThat(result.results()).containsExactlyInAnyOrder(3L, 4L);
    }

    @Test
    public void testNullOrdering() {
        TopNGlobalIndexResult first = result(NULLS_FIRST, 2, keyRowIds(null, 2), keyRowIds(100, 4));
        TopNGlobalIndexResult second =
                result(NULLS_FIRST, 2, keyRowIds(null, 1), keyRowIds(200, 3));

        assertThat(first.or(second).results()).containsExactlyInAnyOrder(1L, 2L);

        first = result(NULLS_LAST, 2, keyRowIds(null, 2), keyRowIds(100, 4));
        second = result(NULLS_LAST, 2, keyRowIds(null, 1), keyRowIds(200, 3));

        assertThat(first.or(second).results()).containsExactlyInAnyOrder(3L, 4L);
    }

    @Test
    public void testOffsetPreservesSortKeys() {
        TopNGlobalIndexResult first = result(NULLS_LAST, 2, keyRowIds(20, 1), keyRowIds(10, 2));
        TopNGlobalIndexResult second = result(NULLS_LAST, 2, keyRowIds(30, 1)).offset(10);

        GlobalIndexResult merged = first.or(second);

        assertThat(merged).isInstanceOf(TopNGlobalIndexResult.class);
        assertThat(merged.results()).containsExactlyInAnyOrder(1L, 11L);
    }

    @Test
    public void testPlainResultUsesConservativeUnion() {
        TopNGlobalIndexResult sorted = result(NULLS_LAST, 1, keyRowIds(20, 1));
        RoaringNavigableMap64 unindexedRows = new RoaringNavigableMap64();
        unindexedRows.add(2);

        GlobalIndexResult merged = sorted.or(GlobalIndexResult.create(unindexedRows));

        assertThat(merged).isNotInstanceOf(TopNGlobalIndexResult.class);
        assertThat(merged.results()).containsExactlyInAnyOrder(1L, 2L);
    }

    private TopNGlobalIndexResult result(
            org.apache.paimon.predicate.SortValue.NullOrdering nullOrdering,
            int limit,
            KeyRowIds... entries) {
        return result(DESCENDING, nullOrdering, limit, entries);
    }

    private TopNGlobalIndexResult result(
            org.apache.paimon.predicate.SortValue.SortDirection direction,
            org.apache.paimon.predicate.SortValue.NullOrdering nullOrdering,
            int limit,
            KeyRowIds... entries) {
        List<KeyRowIds> candidates = Arrays.asList(entries);
        return TopNGlobalIndexResult.create(
                candidates, INT_COMPARATOR, direction, nullOrdering, limit);
    }

    private KeyRowIds keyRowIds(Integer key, long... rowIds) {
        return new KeyRowIds(key, rowIds);
    }
}
