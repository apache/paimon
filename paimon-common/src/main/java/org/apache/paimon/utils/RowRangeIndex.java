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

package org.apache.paimon.utils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Index over exact row ranges or unresolved candidates for file-group pruning. */
public class RowRangeIndex {

    private final List<Range> ranges;
    private final long[] starts;
    private final long[] ends;
    @Nullable private final RoaringNavigableMap64 fileGroupCandidates;

    private RowRangeIndex(List<Range> ranges, @Nullable RoaringNavigableMap64 fileGroupCandidates) {
        this.ranges = ranges;
        this.fileGroupCandidates = fileGroupCandidates;
        this.starts = new long[ranges.size()];
        this.ends = new long[ranges.size()];
        for (int i = 0; i < ranges.size(); i++) {
            Range range = ranges.get(i);
            starts[i] = range.from;
            ends[i] = range.to;
        }
    }

    /** Holds the candidate bitmap for the scan planner to resolve against complete file groups. */
    public static RowRangeIndex forFileGroupPruning(RoaringNavigableMap64 candidates) {
        return new RowRangeIndex(Collections.emptyList(), checkNotNull(candidates));
    }

    /** Returns the unresolved bitmap if this index was created for file-group pruning. */
    @Nullable
    public RoaringNavigableMap64 fileGroupCandidates() {
        return fileGroupCandidates;
    }

    /** Builds an index from a bitmap, skipping sorting and merging for already ordered ranges. */
    public static RowRangeIndex fromBitmap(RoaringNavigableMap64 bitmap) {
        return fromBitmapRanges(bitmap.toRangeList());
    }

    public static Optional<RowRangeIndex> tryFromBitmap(
            RoaringNavigableMap64 bitmap, int maxRanges) {
        return bitmap.tryToRangeList(maxRanges).map(RowRangeIndex::fromBitmapRanges);
    }

    /** Builds a bounded index over the given sorted, merged, non-negative row ranges. */
    public static Optional<RowRangeIndex> tryFromBitmap(
            RoaringNavigableMap64 bitmap, int maxRanges, List<Range> scope) {
        return bitmap.tryToRangeList(maxRanges, scope).map(RowRangeIndex::fromBitmapRanges);
    }

    private static RowRangeIndex fromBitmapRanges(List<Range> ranges) {
        // Bitmap order is unsigned; mixed signs still require signed sorting and merging.
        if (ranges.size() > 1 && ranges.get(0).from > ranges.get(ranges.size() - 1).from) {
            return create(ranges);
        }
        return new RowRangeIndex(ranges, null);
    }

    public static RowRangeIndex create(List<Range> ranges) {
        return create(ranges, true);
    }

    public static RowRangeIndex create(List<Range> ranges, boolean mergeAdjacent) {
        checkArgument(ranges != null, "Ranges cannot be null");
        return new RowRangeIndex(Range.sortAndMergeOverlap(ranges, mergeAdjacent), null);
    }

    public List<Range> ranges() {
        requireExactRanges();
        return Collections.unmodifiableList(ranges);
    }

    public boolean intersects(long start, long end) {
        requireExactRanges();
        int candidate = lowerBound(ends, start);
        return candidate < starts.length && starts[candidate] <= end;
    }

    public boolean contains(Range range) {
        requireExactRanges();
        int candidate = lowerBound(ends, range.from);
        return candidate < starts.length
                && starts[candidate] <= range.from
                && ends[candidate] >= range.to;
    }

    public boolean containsExactly(Range range) {
        requireExactRanges();
        int candidate = lowerBound(starts, range.from);
        return candidate < starts.length
                && starts[candidate] == range.from
                && ends[candidate] == range.to;
    }

    public List<Range> intersectedRanges(long start, long end) {
        requireExactRanges();
        int left = lowerBound(ends, start);
        if (left >= ranges.size()) {
            return Collections.emptyList();
        }

        int right = lowerBound(ends, end);
        if (right >= ranges.size()) {
            right = ranges.size() - 1;
        }

        if (starts[left] > end) {
            return Collections.emptyList();
        }

        List<Range> expected = new ArrayList<>();
        Range from = ranges.get(left);
        expected.add(new Range(Math.max(start, from.from), Math.min(end, from.to)));

        int length = right - left - 1;
        if (length > 0) {
            expected.addAll(ranges.subList(left + 1, left + 1 + length));
        }

        if (right != left) {
            Range to = ranges.get(right);
            if (to.from <= end) {
                expected.add(new Range(Math.max(start, to.from), Math.min(end, to.to)));
            }
        }
        return expected;
    }

    private void requireExactRanges() {
        checkState(fileGroupCandidates == null, "Resolve file groups before accessing row ranges.");
    }

    private static int lowerBound(long[] sorted, long target) {
        int left = 0;
        int right = sorted.length;
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (sorted[mid] < target) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }
}
