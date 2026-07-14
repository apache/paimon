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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Index for row-range mappings. */
final class RowRangeMappingIndex {

    private final List<Mapping> mappings;
    private final long[] oldEnds;

    private RowRangeMappingIndex(List<Mapping> mappings) {
        this.mappings = mappings;
        this.oldEnds = new long[mappings.size()];
        for (int i = 0; i < mappings.size(); i++) {
            Mapping mapping = mappings.get(i);
            oldEnds[i] = mapping.oldEnd;
        }
    }

    static RowRangeMappingIndex create(List<Mapping> mappings) {
        checkArgument(mappings != null, "Row range mappings cannot be null.");
        checkArgument(!mappings.isEmpty(), "Row range mappings cannot be empty.");

        List<Mapping> sorted = new ArrayList<>(mappings);
        Collections.sort(sorted, Comparator.comparingLong(mapping -> mapping.oldStart));
        Mapping previous = null;
        for (Mapping mapping : sorted) {
            checkArgument(
                    mapping.oldStart <= mapping.oldEnd,
                    "Invalid old row range [%s, %s].",
                    mapping.oldStart,
                    mapping.oldEnd);
            if (previous != null) {
                checkArgument(
                        previous.oldEnd < mapping.oldStart,
                        "Old row range mappings cannot overlap.");
            }
            previous = mapping;
        }
        return new RowRangeMappingIndex(Collections.unmodifiableList(sorted));
    }

    static Mapping mapping(long oldStart, long oldEnd, long newStart) {
        return new Mapping(oldStart, oldEnd, newStart);
    }

    RowRangeMappingIndex shiftNewStarts(long offset) {
        List<Mapping> shifted = new ArrayList<>(mappings.size());
        for (Mapping mapping : mappings) {
            shifted.add(
                    mapping(
                            mapping.oldStart,
                            mapping.oldEnd,
                            Math.addExact(mapping.newStart, offset)));
        }
        return create(shifted);
    }

    Optional<Range> map(Range oldRange) {
        checkArgument(oldRange != null, "Old row range cannot be null.");
        checkArgument(oldRange.from <= oldRange.to, "Invalid old row range %s.", oldRange);

        long cursor = oldRange.from;
        long newFrom = Long.MIN_VALUE;
        long newTo = Long.MIN_VALUE;
        boolean mapped = false;

        for (int i = lowerBound(oldEnds, cursor); i < mappings.size(); i++) {
            Mapping mapping = mappings.get(i);
            if (mapping.oldStart > cursor) {
                break;
            }

            long segmentTo = Math.min(mapping.oldEnd, oldRange.to);
            long segmentNewFrom = mapping.newStart + cursor - mapping.oldStart;
            long segmentNewTo = mapping.newStart + segmentTo - mapping.oldStart;

            if (!mapped) {
                newFrom = segmentNewFrom;
                mapped = true;
            } else if (newTo + 1 != segmentNewFrom) {
                return Optional.empty();
            }
            newTo = segmentNewTo;
            cursor = segmentTo + 1;
            if (cursor > oldRange.to) {
                break;
            }
        }

        if (cursor <= oldRange.to) {
            return Optional.empty();
        }
        return Optional.of(new Range(newFrom, newTo));
    }

    boolean overlaps(Range oldRange) {
        checkArgument(oldRange != null, "Old row range cannot be null.");
        checkArgument(oldRange.from <= oldRange.to, "Invalid old row range %s.", oldRange);

        int index = lowerBound(oldEnds, oldRange.from);
        return index < mappings.size() && mappings.get(index).oldStart <= oldRange.to;
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

    static final class Mapping {
        private final long oldStart;
        private final long oldEnd;
        private final long newStart;

        private Mapping(long oldStart, long oldEnd, long newStart) {
            this.oldStart = oldStart;
            this.oldEnd = oldEnd;
            this.newStart = newStart;
        }
    }
}
