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
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Work limits shared by the index readers of one scalar batch scan. */
public final class GlobalIndexScanBudget {

    public static final GlobalIndexScanBudget UNLIMITED = new GlobalIndexScanBudget(0, 0, 0);

    @Nullable private final AtomicLong indexBytes;
    @Nullable private final AtomicLong rowIds;
    private final int maxSelectionRanges;

    public GlobalIndexScanBudget(long maxIndexBytes, long maxRowIds, int maxSelectionRanges) {
        checkArgument(maxIndexBytes >= 0, "Maximum index read size must be non-negative.");
        checkArgument(maxRowIds >= 0, "Maximum row ID count must be non-negative.");
        checkArgument(
                maxSelectionRanges >= 0, "Maximum selection range count must be non-negative.");
        this.indexBytes = maxIndexBytes == 0 ? null : new AtomicLong(maxIndexBytes);
        this.rowIds = maxRowIds == 0 ? null : new AtomicLong(maxRowIds);
        this.maxSelectionRanges = maxSelectionRanges;
    }

    public boolean hasIndexByteLimit() {
        return indexBytes != null;
    }

    public long remainingIndexBytes() {
        return indexBytes == null ? Long.MAX_VALUE : indexBytes.get();
    }

    /** Checks an estimate before opening leaf blocks. Actual reads reserve the budget. */
    public void checkIndexBytes(long estimatedBytes) {
        if (estimatedBytes > remainingIndexBytes()) {
            throw new Exceeded("index bytes");
        }
    }

    public void reserveIndexBytes(long bytes) {
        reserve(indexBytes, bytes, "index bytes");
    }

    public void reserveRowIds(long count) {
        reserve(rowIds, count, "row IDs");
    }

    public boolean hasRowIdLimit() {
        return rowIds != null;
    }

    public GlobalIndexResult offset(GlobalIndexResult result, long offset) {
        if (rowIds != null && offset != 0) {
            reserveRowIds(result.results().getLongCardinality());
        }
        return result.offset(offset);
    }

    public int maxSelectionRanges() {
        return maxSelectionRanges;
    }

    List<Range> toRanges(RoaringNavigableMap64 bitmap) {
        return maxSelectionRanges == 0
                ? bitmap.toRangeList()
                : bitmap.tryToRangeList(maxSelectionRanges)
                        .orElseThrow(() -> new Exceeded("row-ID ranges"));
    }

    private static void reserve(@Nullable AtomicLong remaining, long amount, String unit) {
        if (remaining == null || amount == 0) {
            return;
        }
        checkArgument(amount > 0, "Index scan work must be non-negative.");
        long available;
        do {
            available = remaining.get();
            if (amount > available) {
                throw new Exceeded(unit);
            }
        } while (!remaining.compareAndSet(available, available - amount));
    }

    public static boolean isExceeded(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof Exceeded) {
                return true;
            }
        }
        return false;
    }

    /** A declined lookup may fall back to evaluating the predicate on the data. */
    private static final class Exceeded extends RuntimeException {
        private Exceeded(String unit) {
            super("Scalar global index scan exceeded the budget for " + unit, null, false, false);
        }
    }
}
