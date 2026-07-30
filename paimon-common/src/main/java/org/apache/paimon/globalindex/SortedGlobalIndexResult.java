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

import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A bounded global index result which retains sort keys while merging TopN candidates.
 *
 * <p>Entries are ordered by key descending, null ordering and row id ascending. Merging two
 * compatible results keeps only the globally best {@code limit} entries. Merging with a plain
 * bitmap result falls back to a conservative bitmap union because the other result has no sort
 * keys.
 */
public final class SortedGlobalIndexResult implements GlobalIndexResult {

    private final List<Entry> entries;
    private final Comparator<Object> keyComparator;
    private final SortValue.NullOrdering nullOrdering;
    private final int limit;
    private final RoaringNavigableMap64 results;

    private SortedGlobalIndexResult(
            List<Entry> entries,
            Comparator<Object> keyComparator,
            SortValue.NullOrdering nullOrdering,
            int limit) {
        this.keyComparator = keyComparator;
        this.nullOrdering = nullOrdering;
        this.limit = limit;

        List<Entry> sorted = new ArrayList<>(entries);
        sorted.sort(entryComparator());
        this.entries = Collections.unmodifiableList(deduplicateAndLimit(sorted));
        this.results = toBitmap(this.entries);
    }

    public static SortedGlobalIndexResult create(
            List<Entry> entries,
            Comparator<Object> keyComparator,
            SortValue.NullOrdering nullOrdering,
            int limit) {
        Preconditions.checkArgument(limit >= 0, "TopN limit must not be negative.");
        return new SortedGlobalIndexResult(entries, keyComparator, nullOrdering, limit);
    }

    @Override
    public RoaringNavigableMap64 results() {
        return results;
    }

    @Override
    public SortedGlobalIndexResult offset(long startOffset) {
        if (startOffset == 0) {
            return this;
        }

        List<Entry> offsetEntries = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            offsetEntries.add(new Entry(entry.key(), entry.rowId() + startOffset));
        }
        return create(offsetEntries, keyComparator, nullOrdering, limit);
    }

    @Override
    public GlobalIndexResult or(GlobalIndexResult other) {
        if (other.results().isEmpty()) {
            return this;
        }
        if (entries.isEmpty()) {
            return other;
        }
        if (!(other instanceof SortedGlobalIndexResult)) {
            return GlobalIndexResult.super.or(other);
        }

        SortedGlobalIndexResult sortedOther = (SortedGlobalIndexResult) other;
        Preconditions.checkArgument(
                limit == sortedOther.limit,
                "Cannot merge sorted global index results with different TopN limits.");
        Preconditions.checkArgument(
                nullOrdering == sortedOther.nullOrdering,
                "Cannot merge sorted global index results with different null ordering.");

        List<Entry> merged =
                new ArrayList<>(Math.min(limit, entries.size() + sortedOther.entries.size()));
        Set<Long> seenRowIds = new HashSet<>();
        Comparator<Entry> comparator = entryComparator();
        int left = 0;
        int right = 0;
        while (merged.size() < limit
                && (left < entries.size() || right < sortedOther.entries.size())) {
            Entry candidate;
            if (right >= sortedOther.entries.size()
                    || left < entries.size()
                            && comparator.compare(entries.get(left), sortedOther.entries.get(right))
                                    <= 0) {
                candidate = entries.get(left++);
            } else {
                candidate = sortedOther.entries.get(right++);
            }
            if (seenRowIds.add(candidate.rowId())) {
                merged.add(candidate);
            }
        }
        return new SortedGlobalIndexResult(merged, keyComparator, nullOrdering, limit);
    }

    private List<Entry> deduplicateAndLimit(List<Entry> sorted) {
        List<Entry> result = new ArrayList<>(Math.min(limit, sorted.size()));
        Set<Long> seenRowIds = new HashSet<>();
        for (Entry entry : sorted) {
            if (seenRowIds.add(entry.rowId())) {
                result.add(entry);
                if (result.size() == limit) {
                    break;
                }
            }
        }
        return result;
    }

    private Comparator<Entry> entryComparator() {
        return (left, right) -> {
            int keyComparison = compareKeys(left.key(), right.key());
            return keyComparison == 0 ? Long.compare(left.rowId(), right.rowId()) : keyComparison;
        };
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
        return keyComparator.compare(right, left);
    }

    private static RoaringNavigableMap64 toBitmap(List<Entry> entries) {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        for (Entry entry : entries) {
            bitmap.add(entry.rowId());
        }
        return bitmap;
    }

    /** One sortable TopN candidate. */
    public static final class Entry {

        @Nullable private final Object key;
        private final long rowId;

        public Entry(@Nullable Object key, long rowId) {
            this.key = key;
            this.rowId = rowId;
        }

        @Nullable
        public Object key() {
            return key;
        }

        public long rowId() {
            return rowId;
        }
    }
}
