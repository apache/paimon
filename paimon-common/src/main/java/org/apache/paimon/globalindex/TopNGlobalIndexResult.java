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
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A bounded global index result which retains sort keys while merging TopN candidates.
 *
 * <p>Key groups are ordered by the requested sort direction and their row ids are ordered
 * ascending. Merging two compatible results combines equal keys and keeps only the globally best
 * {@code limit} row ids. Merging with a plain bitmap result falls back to a conservative bitmap
 * union because the other result has no sort keys.
 */
public final class TopNGlobalIndexResult implements GlobalIndexResult {

    private final List<KeyRowIds> keyRowIds;
    private final Comparator<Object> keyComparator;
    private final SortValue.SortDirection direction;
    private final SortValue.NullOrdering nullOrdering;
    private final int limit;
    private final RoaringNavigableMap64 results;

    private TopNGlobalIndexResult(
            List<KeyRowIds> keyRowIds,
            Comparator<Object> keyComparator,
            SortValue.SortDirection direction,
            SortValue.NullOrdering nullOrdering,
            int limit) {
        this.keyComparator = keyComparator;
        this.direction = direction;
        this.nullOrdering = nullOrdering;
        this.limit = limit;

        List<KeyRowIds> sorted = new ArrayList<>(keyRowIds);
        sorted.sort(keyRowIdsComparator());
        this.keyRowIds = Collections.unmodifiableList(mergeAndLimit(sorted));
        this.results = toBitmap(this.keyRowIds);
    }

    public static TopNGlobalIndexResult create(
            List<KeyRowIds> keyRowIds,
            Comparator<Object> keyComparator,
            SortValue.NullOrdering nullOrdering,
            int limit) {
        return create(
                keyRowIds, keyComparator, SortValue.SortDirection.DESCENDING, nullOrdering, limit);
    }

    public static TopNGlobalIndexResult create(
            List<KeyRowIds> keyRowIds,
            Comparator<Object> keyComparator,
            SortValue.SortDirection direction,
            SortValue.NullOrdering nullOrdering,
            int limit) {
        Preconditions.checkArgument(limit >= 0, "TopN limit must not be negative.");
        return new TopNGlobalIndexResult(keyRowIds, keyComparator, direction, nullOrdering, limit);
    }

    @Override
    public RoaringNavigableMap64 results() {
        return results;
    }

    @Override
    public TopNGlobalIndexResult offset(long startOffset) {
        if (startOffset == 0) {
            return this;
        }

        List<KeyRowIds> offsetKeyRowIds = new ArrayList<>(keyRowIds.size());
        for (KeyRowIds keyRowIds : keyRowIds) {
            long[] rowIds = keyRowIds.rowIds();
            long[] offsetRowIds = new long[rowIds.length];
            for (int i = 0; i < rowIds.length; i++) {
                offsetRowIds[i] = rowIds[i] + startOffset;
            }
            offsetKeyRowIds.add(new KeyRowIds(keyRowIds.key(), offsetRowIds));
        }
        return create(offsetKeyRowIds, keyComparator, direction, nullOrdering, limit);
    }

    @Override
    public GlobalIndexResult or(GlobalIndexResult other) {
        if (!(other instanceof TopNGlobalIndexResult)) {
            if (other.results().isEmpty()) {
                return this;
            }
            return GlobalIndexResult.super.or(other);
        }

        TopNGlobalIndexResult sortedOther = (TopNGlobalIndexResult) other;
        Preconditions.checkArgument(
                limit == sortedOther.limit,
                "Cannot merge sorted global index results with different TopN limits.");
        Preconditions.checkArgument(
                direction == sortedOther.direction,
                "Cannot merge sorted global index results with different sort directions.");
        Preconditions.checkArgument(
                nullOrdering == sortedOther.nullOrdering,
                "Cannot merge sorted global index results with different null ordering.");

        if (sortedOther.keyRowIds.isEmpty()) {
            return this;
        }
        if (keyRowIds.isEmpty()) {
            return sortedOther;
        }

        List<KeyRowIds> merged = new ArrayList<>(keyRowIds.size() + sortedOther.keyRowIds.size());
        merged.addAll(keyRowIds);
        merged.addAll(sortedOther.keyRowIds);
        return new TopNGlobalIndexResult(merged, keyComparator, direction, nullOrdering, limit);
    }

    private List<KeyRowIds> mergeAndLimit(List<KeyRowIds> sorted) {
        List<KeyRowIds> result = new ArrayList<>(Math.min(limit, sorted.size()));
        int remaining = limit;
        int position = 0;
        while (remaining > 0 && position < sorted.size()) {
            Object key = sorted.get(position).key();
            RoaringNavigableMap64 sameKeyRowIds = new RoaringNavigableMap64();
            do {
                for (long rowId : sorted.get(position).rowIds()) {
                    sameKeyRowIds.add(rowId);
                }
                position++;
            } while (position < sorted.size() && compareKeys(key, sorted.get(position).key()) == 0);

            int count = (int) Math.min((long) remaining, sameKeyRowIds.getLongCardinality());
            if (count > 0) {
                long[] limitedRowIds = new long[count];
                int index = 0;
                for (long rowId : sameKeyRowIds) {
                    limitedRowIds[index++] = rowId;
                    if (index == count) {
                        break;
                    }
                }
                result.add(new KeyRowIds(key, limitedRowIds));
                remaining -= count;
            }
        }
        return result;
    }

    private Comparator<KeyRowIds> keyRowIdsComparator() {
        return (left, right) -> compareKeys(left.key(), right.key());
    }

    private int compareKeys(@Nullable Object left, @Nullable Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return nullOrdering == SortValue.NullOrdering.NULLS_FIRST ? -1 : 1;
        }
        if (right == null) {
            return nullOrdering == SortValue.NullOrdering.NULLS_FIRST ? 1 : -1;
        }
        return direction == SortValue.SortDirection.ASCENDING
                ? keyComparator.compare(left, right)
                : keyComparator.compare(right, left);
    }

    private static RoaringNavigableMap64 toBitmap(List<KeyRowIds> keyRowIds) {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        for (KeyRowIds group : keyRowIds) {
            for (long rowId : group.rowIds()) {
                bitmap.add(rowId);
            }
        }
        return bitmap;
    }

    List<KeyRowIds> keyRowIds() {
        return keyRowIds;
    }
}
