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

import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Global index result represents row ids as a list of ranges.
 *
 * <p>Invariant: The list returned by {@link #results()} must be sorted by range start position and
 * contain no overlapping ranges.
 */
public interface GlobalIndexResult {

    /**
     * Returns the list of ranges representing row ids.
     *
     * <p>Please make sure that: the returned list must be sorted by range start position and
     * contain no overlapping ranges.
     */
    List<Range> results();

    static GlobalIndexResult createEmpty() {
        return Collections::emptyList;
    }

    /**
     * Returns the intersection of this result and the other result.
     *
     * <p>Assumes both inputs have sorted, non-overlapping ranges. Complexity: O(n+m).
     */
    default GlobalIndexResult and(GlobalIndexResult other) {
        List<Range> list1 = this.results();
        List<Range> list2 = other.results();
        List<Range> result = new ArrayList<>();

        int i = 0, j = 0;
        while (i < list1.size() && j < list2.size()) {
            Range r1 = list1.get(i);
            Range r2 = list2.get(j);

            Range intersection = Range.intersection(r1, r2);
            if (intersection != null) {
                result.add(intersection);
            }

            // Move pointer for the range that ends first
            if (r1.to <= r2.to) {
                i++;
            } else {
                j++;
            }
        }

        return () -> result;
    }

    /**
     * Returns the union of this result and the other result.
     *
     * <p>Assumes both inputs have sorted, non-overlapping ranges. Complexity: O(n+m).
     */
    default GlobalIndexResult or(GlobalIndexResult other) {
        List<Range> list1 = this.results();
        List<Range> list2 = other.results();
        List<Range> result = new ArrayList<>();

        int i = 0, j = 0;
        Range current = null;

        while (i < list1.size() || j < list2.size()) {
            Range next;
            if (i >= list1.size()) {
                next = list2.get(j++);
            } else if (j >= list2.size()) {
                next = list1.get(i++);
            } else {
                // Pick the range with smaller start position
                if (list1.get(i).from <= list2.get(j).from) {
                    next = list1.get(i++);
                } else {
                    next = list2.get(j++);
                }
            }

            if (current == null) {
                current = next;
            } else {
                Range union = Range.union(current, next);
                if (union != null) {
                    current = union;
                } else {
                    result.add(current);
                    current = next;
                }
            }
        }

        if (current != null) {
            result.add(current);
        }

        return () -> result;
    }
}
