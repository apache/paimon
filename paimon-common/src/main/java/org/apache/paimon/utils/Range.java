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

import java.util.Iterator;

/** Define a range with start (include) and end (exclude). */
public class Range {

    private final long start;
    private final long end;

    public Range(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public boolean overlaps(Range other) {
        return this.start < other.end && other.start < this.end;
    }

    public long offset(long value) {
        if (value < start || value >= end) {
            return -1;
        }
        return value - start;
    }

    public long count() {
        return end - start;
    }

    public static Range of(long start, long end) {
        return new Range(start, end);
    }

    public Iterator<Long> iterator() {
        return new Iterator<Long>() {
            private long current = start;

            @Override
            public boolean hasNext() {
                return current < end;
            }

            @Override
            public Long next() {
                return current++;
            }
        };
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(start);
        result = 31 * result + Long.hashCode(end);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Range range = (Range) obj;
        return start == range.start && end == range.end;
    }
}
