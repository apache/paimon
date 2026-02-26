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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Index for row ranges. */
public class RowRangeIndex {

    private final List<Range> ranges;
    private final long[] starts;
    private final long[] ends;

    private RowRangeIndex(List<Range> ranges) {
        this.ranges = ranges;
        this.starts = new long[ranges.size()];
        this.ends = new long[ranges.size()];
        for (int i = 0; i < ranges.size(); i++) {
            Range range = ranges.get(i);
            starts[i] = range.from;
            ends[i] = range.to;
        }
    }

    public static RowRangeIndex create(List<Range> ranges) {
        checkArgument(ranges != null, "Ranges cannot be null");
        return new RowRangeIndex(Range.sortAndMergeOverlap(ranges, true));
    }

    public List<Range> ranges() {
        return Collections.unmodifiableList(ranges);
    }

    public boolean intersects(long start, long end) {
        int candidate = lowerBound(ends, start);
        return candidate < starts.length && starts[candidate] <= end;
    }

    public List<Range> intersectedRanges(long start, long end) {
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
