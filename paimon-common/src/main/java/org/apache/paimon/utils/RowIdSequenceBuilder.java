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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class RowIdSequenceBuilder {

    private final List<Range> allExistingRanges;

    private List<Range> current = new ArrayList<>();
    private boolean noMore = false;
    private int pageNum = 0;
    private long startPos;
    private long currentPos; // exclusive

    public RowIdSequenceBuilder(Collection<Range> rangesUnsorted) {
        Preconditions.checkArgument(
                !rangesUnsorted.isEmpty(), "RowIdSequenceBuilder must have at least one range.");
        List<Range> tempRanges =
                rangesUnsorted.stream()
                        .sorted(Comparator.comparingLong(Range::getStart))
                        .collect(Collectors.toList());

        this.allExistingRanges = new ArrayList<>();

        for (Range range : tempRanges) {
            if (allExistingRanges.isEmpty()) {
                allExistingRanges.add(range);
            } else {
                Range lastRange = allExistingRanges.get(allExistingRanges.size() - 1);
                if (lastRange.getEnd() == range.getStart()) {
                    // merge with the last range
                    allExistingRanges.set(
                            allExistingRanges.size() - 1,
                            Range.of(lastRange.getStart(), range.getEnd()));
                } else if (!lastRange.overlaps(range)) {
                    // add a new range
                    allExistingRanges.add(range);
                } else {
                    throw new IllegalArgumentException(
                            "RowIdSequenceBuilder cannot handle overlapping ranges: "
                                    + lastRange
                                    + " and "
                                    + range);
                }
            }
        }

        this.startPos = allExistingRanges.get(0).getStart();
        this.currentPos = startPos;
    }

    public void more() {
        if (noMore) {
            throw new IllegalStateException("No more ranges to process.");
        }
        Range current = allExistingRanges.get(pageNum);
        currentPos++;
        if (currentPos == current.getEnd()) {
            this.current.add(
                    startPos == current.getStart() ? current : Range.of(startPos, currentPos));

            pageNum++;
            if (pageNum < allExistingRanges.size()) {
                current = allExistingRanges.get(pageNum);
                startPos = current.getStart();
                currentPos = startPos;
            } else {
                startPos = currentPos;
                // No more ranges to process
                noMore = true;
            }
        }
    }

    public @Nullable RowIdSequence build() {
        if (currentPos != startPos) {
            current.add(Range.of(startPos, currentPos));
            startPos = currentPos;
        }
        RowIdSequence rowIdSequence = new RowIdSequence(current);
        current = new ArrayList<>();
        return rowIdSequence;
    }
}
