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

package org.apache.paimon.predicate;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * A range of rows to read from a single data split, expressed as a 0-based global effective-row
 * position range {@code [startInclusive, endInclusive]} (both endpoints inclusive) across the
 * concatenated effective rows of all files in the split.
 *
 * <p>"Effective row" means the row is actually returned (deletion-vector-deleted rows are not
 * counted). When there is no deletion vector, the effective-row position equals the physical row
 * index. When a deletion vector exists, the reader maps effective-row positions to physical row
 * indices internally (see {@code RangeSkipReader} / selection bitmap), so the user always reasons
 * in the continuous effective-row space.
 *
 * <p>This is used to support range queries that read a contiguous slice of rows (e.g. AI training
 * data slicing by effective sample count). The reader skips row groups / pages outside the range
 * without decoding them when the underlying format supports it; otherwise it falls back to a naive
 * skip+limit over the output stream.
 *
 * <p>The semantics is the 0-based effective-row position space, <b>not</b> the logical {@code
 * firstRowId} space and <b>not</b> the row-id space used by {@code IndexedSplit.rowRanges()}.
 * {@code ROW_ID = firstRowId + returnedPosition()} is unaffected.
 *
 * <p>Note: this is distinct from {@link org.apache.paimon.utils.Range}, which is a row-id interval
 * used by the global-index / {@code IndexedSplit} path. {@code RowRange} is an effective-row
 * position interval used by range-query reads.
 *
 * @since 1.1.0
 */
public final class RowRange implements Serializable {

    private static final long serialVersionUID = 1L;

    /** From the 0th effective row to the end of the split (no upper bound). */
    public static final RowRange FULL = new RowRange(0, Long.MAX_VALUE);

    /** Empty range: used as a per-file intersection result meaning "skip this file". */
    public static final RowRange EMPTY = new RowRange(0, -1);

    private final long startInclusive;
    private final long endInclusive;

    private RowRange(long startInclusive, long endInclusive) {
        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
    }

    /**
     * Creates a row range {@code [startInclusive, endInclusive]} (both endpoints inclusive).
     *
     * @param startInclusive the first effective-row position to read (inclusive), must be {@code >=
     *     0}
     * @param endInclusive the last effective-row position to read (inclusive), must be {@code >=
     *     startInclusive}; {@link Long#MAX_VALUE} means "to the end of the split"
     */
    public static RowRange of(long startInclusive, long endInclusive) {
        if (startInclusive < 0) {
            throw new IllegalArgumentException(
                    "startInclusive must be >= 0, but is " + startInclusive);
        }
        if (endInclusive != Long.MAX_VALUE && endInclusive < startInclusive) {
            throw new IllegalArgumentException(
                    "endInclusive must be >= startInclusive (or Long.MAX_VALUE), but startInclusive="
                            + startInclusive
                            + ", endInclusive="
                            + endInclusive);
        }
        return new RowRange(startInclusive, endInclusive);
    }

    public long startInclusive() {
        return startInclusive;
    }

    public long endInclusive() {
        return endInclusive;
    }

    /** Whether this range selects no rows at all. */
    public boolean isEmpty() {
        return endInclusive < startInclusive;
    }

    /**
     * Number of rows covered by this range ({@code end - start + 1}); {@link Long#MAX_VALUE} if
     * unbounded.
     */
    public long count() {
        if (endInclusive == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return endInclusive - startInclusive + 1;
    }

    /**
     * Translate a split-global {@link RowRange} into a per-file local {@link RowRange} over the
     * file's own effective-row space.
     *
     * <p>For a file occupying the global effective-row interval {@code [fileStartGlobal,
     * fileStartGlobal + fileRowCount - 1]}, the local range is the intersection: {@code localStart
     * = max(start - fileStartGlobal, 0)}, {@code localEnd = min(end - fileStartGlobal, fileRowCount
     * - 1)} (with {@link Long#MAX_VALUE} end left unbounded before the clamp).
     *
     * @param globalRange the split-global range, or {@code null} when no range is set
     * @param fileStartGlobal the global effective-row position of the first row in this file
     *     (accumulated across preceding files in the split)
     * @param fileRowCount the effective row count of this file (physical row count when there is no
     *     deletion vector)
     * @return the per-file local range; {@code null} if {@code globalRange} is null (read the whole
     *     file); {@link #EMPTY} if the file lies entirely outside the range (skip the file)
     */
    @Nullable
    public static RowRange localOf(
            @Nullable RowRange globalRange, long fileStartGlobal, long fileRowCount) {
        if (globalRange == null) {
            return null;
        }
        long start = Math.max(globalRange.startInclusive() - fileStartGlobal, 0);
        long end =
                Math.min(
                        globalRange.endInclusive() == Long.MAX_VALUE
                                ? Long.MAX_VALUE
                                : globalRange.endInclusive() - fileStartGlobal,
                        fileRowCount - 1);
        if (start >= fileRowCount || end < 0 || end < start) {
            return RowRange.EMPTY;
        }
        return RowRange.of(start, end);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowRange rowRange = (RowRange) o;
        return startInclusive == rowRange.startInclusive && endInclusive == rowRange.endInclusive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startInclusive, endInclusive);
    }

    @Override
    public String toString() {
        return "RowRange[" + startInclusive + ", " + endInclusive + "]";
    }
}
