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

package org.apache.paimon.table.source;

import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An immutable, exact set of physical bucket IDs for {@link ReadBuilder#withBucketFilter}.
 *
 * <p>The domain is the entire signed integer range, including negative bucket IDs. It is
 * independent of a table's current bucket count. Set operations do not implement SQL null
 * semantics; callers must account for those when constructing the predicate.
 *
 * <p>Sorted, disjoint inclusive ranges avoid enumerating large bucket domains. Range endpoints use
 * longs so that arithmetic at either integer boundary cannot overflow.
 */
public final class PhysicalBucketPredicate implements Filter<Integer>, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Range DOMAIN = new Range(Integer.MIN_VALUE, Integer.MAX_VALUE);
    private static final PhysicalBucketPredicate ALL =
            new PhysicalBucketPredicate(Collections.singletonList(DOMAIN));
    private static final PhysicalBucketPredicate NONE =
            new PhysicalBucketPredicate(Collections.emptyList());

    private final List<Range> ranges;

    private PhysicalBucketPredicate(List<Range> ranges) {
        this.ranges = Collections.unmodifiableList(Range.sortAndMergeOverlap(ranges, true));
    }

    public static PhysicalBucketPredicate all() {
        return ALL;
    }

    public static PhysicalBucketPredicate none() {
        return NONE;
    }

    public static PhysicalBucketPredicate equal(int bucket) {
        return range(bucket, bucket);
    }

    public static PhysicalBucketPredicate lessThan(int bucket) {
        return range(Integer.MIN_VALUE, (long) bucket - 1);
    }

    public static PhysicalBucketPredicate lessOrEqual(int bucket) {
        return range(Integer.MIN_VALUE, bucket);
    }

    public static PhysicalBucketPredicate greaterThan(int bucket) {
        return range((long) bucket + 1, Integer.MAX_VALUE);
    }

    public static PhysicalBucketPredicate greaterOrEqual(int bucket) {
        return range(bucket, Integer.MAX_VALUE);
    }

    public static PhysicalBucketPredicate in(int... buckets) {
        List<Range> ranges = new ArrayList<>(buckets.length);
        for (int bucket : buckets) {
            ranges.add(new Range(bucket, bucket));
        }
        return new PhysicalBucketPredicate(ranges);
    }

    private static PhysicalBucketPredicate range(long from, long to) {
        return from > to
                ? NONE
                : new PhysicalBucketPredicate(Collections.singletonList(new Range(from, to)));
    }

    /** Returns the exact intersection without changing either predicate. */
    public PhysicalBucketPredicate intersect(PhysicalBucketPredicate other) {
        return new PhysicalBucketPredicate(Range.and(ranges, other.ranges));
    }

    /** Returns the exact union without changing either predicate. */
    public PhysicalBucketPredicate union(PhysicalBucketPredicate other) {
        List<Range> union = new ArrayList<>(ranges);
        union.addAll(other.ranges);
        return new PhysicalBucketPredicate(union);
    }

    /** Returns the complement relative to the entire signed integer domain. */
    public PhysicalBucketPredicate negate() {
        return new PhysicalBucketPredicate(DOMAIN.exclude(ranges));
    }

    @Override
    public boolean test(Integer bucket) {
        return bucket != null && mayContain(bucket, bucket);
    }

    /**
     * Returns whether the inclusive range contains any matching bucket. Invalid bounds are
     * conservatively retained, since they cannot prove that a manifest is disjoint.
     */
    public boolean mayContain(int minBucket, int maxBucket) {
        if (minBucket > maxBucket) {
            return true;
        }
        int low = 0;
        int high = ranges.size() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Range range = ranges.get(mid);
            if (range.to < minBucket) {
                low = mid + 1;
            } else if (range.from > maxBucket) {
                high = mid - 1;
            } else {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PhysicalBucketPredicate
                && ranges.equals(((PhysicalBucketPredicate) obj).ranges);
    }

    @Override
    public int hashCode() {
        return ranges.hashCode();
    }

    @Override
    public String toString() {
        return "PhysicalBucketPredicate" + ranges;
    }
}
