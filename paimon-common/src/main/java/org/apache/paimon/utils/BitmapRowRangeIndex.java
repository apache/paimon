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

/** Row range index backed by a row-id bitmap. */
class BitmapRowRangeIndex extends RowRangeIndex {

    private final RoaringNavigableMap64 rowIds;

    BitmapRowRangeIndex(RoaringNavigableMap64 rowIds) {
        this.rowIds = rowIds;
    }

    @Override
    public boolean intersects(long start, long end) {
        return rowIds.intersects(new Range(start, end));
    }

    @Override
    public boolean contains(Range range) {
        return rowIds.containsRange(range);
    }

    @Override
    public boolean containsExactly(Range range) {
        if (!rowIds.containsRange(range)) {
            return false;
        }

        boolean leftOpen = range.from == Long.MIN_VALUE || !rowIds.contains(range.from - 1);
        boolean rightOpen = range.to == Long.MAX_VALUE || !rowIds.contains(range.to + 1);
        return leftOpen && rightOpen;
    }

    @Override
    public void forEachIntersectedRange(long start, long end, LongRangeConsumer consumer) {
        rowIds.forEachIntersectedRange(start, end, consumer);
    }

    @Override
    void forEachRange(LongRangeConsumer consumer) {
        rowIds.forEachIntersectedRange(0, Long.MAX_VALUE, consumer);
    }
}
