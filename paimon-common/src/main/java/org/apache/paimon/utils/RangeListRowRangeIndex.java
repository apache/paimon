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

import java.util.List;

/** Row range index backed by a sorted range list. */
class RangeListRowRangeIndex extends RowRangeIndex {

    private final List<Range> ranges;
    private final RoaringNavigableMap64 rowIds;

    RangeListRowRangeIndex(List<Range> ranges) {
        this.ranges = ranges;
        this.rowIds = RoaringNavigableMap64.fromRanges(ranges);
    }

    @Override
    public boolean intersects(long start, long end) {
        return rowIds.intersects(new Range(start, end));
    }

    @Override
    public boolean contains(Range range) {
        return RowRangeIndexUtils.contains(ranges, range);
    }

    @Override
    public boolean containsExactly(Range range) {
        int candidate = RowRangeIndexUtils.lowerBoundByStart(ranges, range.from);
        if (candidate >= ranges.size()) {
            return false;
        }

        Range candidateRange = ranges.get(candidate);
        return candidateRange.from == range.from && candidateRange.to == range.to;
    }

    @Override
    public void forEachIntersectedRange(long start, long end, LongRangeConsumer consumer) {
        if (start > end) {
            return;
        }

        int left = RowRangeIndexUtils.lowerBoundByEnd(ranges, start);
        if (left >= ranges.size()) {
            return;
        }

        int right = RowRangeIndexUtils.lowerBoundByEnd(ranges, end);
        if (right >= ranges.size()) {
            right = ranges.size() - 1;
        }

        if (ranges.get(left).from > end) {
            return;
        }

        Range from = ranges.get(left);
        consumer.accept(Math.max(start, from.from), Math.min(end, from.to));

        for (int i = left + 1; i < right; i++) {
            Range range = ranges.get(i);
            consumer.accept(range.from, range.to);
        }

        if (right != left) {
            Range to = ranges.get(right);
            if (to.from <= end) {
                consumer.accept(Math.max(start, to.from), Math.min(end, to.to));
            }
        }
    }

    @Override
    void forEachRange(LongRangeConsumer consumer) {
        for (Range range : ranges) {
            consumer.accept(range.from, range.to);
        }
    }
}
