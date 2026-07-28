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

    private final long[] oldStarts;
    private final long[] oldEnds;
    private final long[] newStarts;
    private final long newStartOffset;

    private RowRangeMappingIndex(
            long[] oldStarts, long[] oldEnds, long[] newStarts, long newStartOffset) {
        this.oldStarts = oldStarts;
        this.oldEnds = oldEnds;
        this.newStarts = newStarts;
        this.newStartOffset = newStartOffset;
    }

    static RowRangeMappingIndex create(List<Mapping> mappings) {
        checkArgument(mappings != null, "Row range mappings cannot be null.");
        checkArgument(!mappings.isEmpty(), "Row range mappings cannot be empty.");

        List<Mapping> sorted = new ArrayList<>(mappings);
        Collections.sort(sorted, Comparator.comparingLong(mapping -> mapping.oldStart));
        long[] oldStarts = new long[sorted.size()];
        long[] oldEnds = new long[sorted.size()];
        long[] newStarts = new long[sorted.size()];
        for (int i = 0; i < sorted.size(); i++) {
            Mapping mapping = sorted.get(i);
            oldStarts[i] = mapping.oldStart;
            oldEnds[i] = mapping.oldEnd;
            newStarts[i] = mapping.newStart;
        }
        validate(oldStarts, oldEnds, newStarts);
        return new RowRangeMappingIndex(oldStarts, oldEnds, newStarts, 0L);
    }

    /** Creates an index by taking ownership of the three arrays. */
    static RowRangeMappingIndex createFromOwnedArrays(
            long[] oldStarts, long[] oldEnds, long[] newStarts) {
        checkArgument(oldStarts != null, "Old row range starts cannot be null.");
        checkArgument(oldEnds != null, "Old row range ends cannot be null.");
        checkArgument(newStarts != null, "New row range starts cannot be null.");

        validate(oldStarts, oldEnds, newStarts);
        return new RowRangeMappingIndex(oldStarts, oldEnds, newStarts, 0L);
    }

    private static void validate(long[] oldStarts, long[] oldEnds, long[] newStarts) {
        checkArgument(
                oldStarts.length == oldEnds.length && oldStarts.length == newStarts.length,
                "Row range mapping arrays must have the same length.");
        checkArgument(oldStarts.length > 0, "Row range mappings cannot be empty.");

        for (int i = 0; i < oldStarts.length; i++) {
            checkArgument(
                    oldStarts[i] <= oldEnds[i],
                    "Invalid old row range [%s, %s].",
                    oldStarts[i],
                    oldEnds[i]);
            if (i > 0) {
                checkArgument(
                        oldEnds[i - 1] < oldStarts[i],
                        "Old row range mappings cannot overlap or be out of order.");
            }
        }
    }

    static Mapping mapping(long oldStart, long oldEnd, long newStart) {
        return new Mapping(oldStart, oldEnd, newStart);
    }

    RowRangeMappingIndex shiftNewStarts(long offset) {
        long combinedOffset;
        try {
            combinedOffset = Math.addExact(newStartOffset, offset);
        } catch (ArithmeticException ignored) {
            // The offsets themselves may overflow even though every shifted row ID remains valid.
            // Materialize the effective starts in that uncommon case.
            long[] shiftedNewStarts = new long[newStarts.length];
            for (int i = 0; i < newStarts.length; i++) {
                shiftedNewStarts[i] =
                        Math.addExact(Math.addExact(newStarts[i], newStartOffset), offset);
            }
            return new RowRangeMappingIndex(oldStarts, oldEnds, shiftedNewStarts, 0L);
        }

        // Preserve the old eager implementation's contract: shift fails immediately if any
        // individual new row ID overflows, even when that mapping is never queried later.
        for (long newStart : newStarts) {
            Math.addExact(Math.addExact(newStart, newStartOffset), offset);
        }
        return new RowRangeMappingIndex(oldStarts, oldEnds, newStarts, combinedOffset);
    }

    Optional<Range> map(Range oldRange) {
        checkArgument(oldRange != null, "Old row range cannot be null.");
        checkArgument(oldRange.from <= oldRange.to, "Invalid old row range %s.", oldRange);

        long cursor = oldRange.from;
        long newFrom = Long.MIN_VALUE;
        long newTo = Long.MIN_VALUE;
        boolean mapped = false;

        for (int i = lowerBound(oldEnds, cursor); i < oldStarts.length; i++) {
            if (oldStarts[i] > cursor) {
                break;
            }

            long segmentTo = Math.min(oldEnds[i], oldRange.to);
            long shiftedNewStart = Math.addExact(newStarts[i], newStartOffset);
            long segmentNewFrom =
                    Math.addExact(shiftedNewStart, Math.subtractExact(cursor, oldStarts[i]));
            long segmentNewTo =
                    Math.addExact(shiftedNewStart, Math.subtractExact(segmentTo, oldStarts[i]));

            if (!mapped) {
                newFrom = segmentNewFrom;
                mapped = true;
            } else if (Math.addExact(newTo, 1L) != segmentNewFrom) {
                return Optional.empty();
            }
            newTo = segmentNewTo;
            if (segmentTo == oldRange.to) {
                return Optional.of(new Range(newFrom, newTo));
            }
            cursor = Math.addExact(segmentTo, 1L);
        }

        return Optional.empty();
    }

    boolean overlaps(Range oldRange) {
        checkArgument(oldRange != null, "Old row range cannot be null.");
        checkArgument(oldRange.from <= oldRange.to, "Invalid old row range %s.", oldRange);

        int index = lowerBound(oldEnds, oldRange.from);
        return index < oldStarts.length && oldStarts[index] <= oldRange.to;
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
