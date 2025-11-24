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

import java.util.Objects;

/** Range represents from (inclusive) and to (inclusive). */
public class Range {

    public final long from;
    public final long to;

    // Creates a range of [from, to] (from and to are inclusive; empty ranges are not valid)
    public Range(long from, long to) {
        assert from <= to;
        this.from = from;
        this.to = to;
    }

    public long count() {
        return to - from + 1;
    }

    public Range addOffset(long offset) {
        return new Range(from + offset, to + offset);
    }

    public boolean isBefore(Range other) {
        return to < other.from;
    }

    public boolean isAfter(Range other) {
        return from > other.to;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Range range = (Range) o;
        return from == range.from && to == range.to;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public String toString() {
        return "[" + from + ", " + to + ']';
    }

    // Returns the union of the two ranges or null if there are elements between them.
    public static Range union(Range left, Range right) {
        if (left.from <= right.from) {
            if (left.to + 1 >= right.from) {
                return new Range(left.from, Math.max(left.to, right.to));
            }
        } else if (right.to + 1 >= left.from) {
            return new Range(right.from, Math.max(left.to, right.to));
        }
        return null;
    }

    // Returns the intersection of the two ranges of null if they are not overlapped.
    public static Range intersection(Range left, Range right) {
        if (left.from <= right.from) {
            if (left.to >= right.from) {
                return new Range(right.from, Math.min(left.to, right.to));
            }
        } else if (right.to >= left.from) {
            return new Range(left.from, Math.min(left.to, right.to));
        }
        return null;
    }

    public static boolean intersect(long start1, long end1, long start2, long end2) {
        long intersectionStart = Math.max(start1, start2);
        long intersectionEnd = Math.min(end1, end2);
        return intersectionStart <= intersectionEnd;
    }
}
