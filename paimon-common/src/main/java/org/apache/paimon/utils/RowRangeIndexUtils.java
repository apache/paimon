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

/** Utilities for row range index implementations. */
class RowRangeIndexUtils {

    private RowRangeIndexUtils() {}

    static boolean contains(List<Range> ranges, Range range) {
        int candidate = lowerBoundByEnd(ranges, range.from);
        if (candidate >= ranges.size()) {
            return false;
        }

        Range candidateRange = ranges.get(candidate);
        return candidateRange.from <= range.from && candidateRange.to >= range.to;
    }

    static int lowerBoundByStart(List<Range> sorted, long target) {
        int left = 0;
        int right = sorted.size();
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (sorted.get(mid).from < target) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    static int lowerBoundByEnd(List<Range> sorted, long target) {
        int left = 0;
        int right = sorted.size();
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (sorted.get(mid).to < target) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }
}
