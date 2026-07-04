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
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Index for row ranges. */
public abstract class RowRangeIndex {

    public static RowRangeIndex create(List<Range> ranges) {
        return create(ranges, true);
    }

    public static RowRangeIndex create(List<Range> ranges, boolean mergeAdjacent) {
        checkArgument(ranges != null, "Ranges cannot be null");
        return new RangeListRowRangeIndex(Range.sortAndMergeOverlap(ranges, mergeAdjacent));
    }

    public static RowRangeIndex fromBitmap(RoaringNavigableMap64 rowIds) {
        checkArgument(rowIds != null, "Row IDs cannot be null");
        return new BitmapRowRangeIndex(rowIds);
    }

    public RowRangeIndex intersect(RowRangeIndex other) {
        checkArgument(other != null, "Other row range index cannot be null");

        List<Range> ranges = new ArrayList<>();
        other.forEachRange(
                (start, end) ->
                        forEachIntersectedRange(
                                start, end, (from, to) -> ranges.add(new Range(from, to))));
        return create(ranges);
    }

    public List<Range> toRangeList() {
        List<Range> ranges = new ArrayList<>();
        forEachRange((from, to) -> ranges.add(new Range(from, to)));
        return ranges;
    }

    public abstract boolean intersects(long start, long end);

    public abstract boolean contains(Range range);

    public abstract boolean containsExactly(Range range);

    public abstract void forEachIntersectedRange(long start, long end, LongRangeConsumer consumer);

    abstract void forEachRange(LongRangeConsumer consumer);
}
