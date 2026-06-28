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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.operation.commit.RowTrackingCommitUtils;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import java.util.function.Supplier;

/**
 * Tracks sequence number range for rows written to a data file.
 *
 * <p>Two modes of operation:
 *
 * <ul>
 *   <li><b>Non-row-tracking mode</b> (no {@code _SEQUENCE_NUMBER} field in schema): sequence
 *       numbers are generated monotonically by a {@link LongCounter}. min = counter - rowCount, max
 *       = counter - 1.
 *   <li><b>Row-tracking mode</b> (schema contains {@code _SEQUENCE_NUMBER}): sequence numbers are
 *       extracted from each row. min/max are computed from actual row values. Null sequence numbers
 *       are tracked and cause max() to return 0, which signals {@link
 *       RowTrackingCommitUtils#assignSnapshotId} to assign the current snapshot ID.
 * </ul>
 */
public class RowDataFileSequenceNumberTracker {
    private final LongCounter seqNumCounter;
    private final Supplier<Long> recordCountSupplier;
    private final int seqNumberFieldIndex;
    private long minSeqNumber;
    private long maxSeqNumber;
    private boolean hasNullSeqNumber;

    public RowDataFileSequenceNumberTracker(
            RowType writeSchema,
            Supplier<LongCounter> seqNumCounterSupplier,
            Supplier<Long> recordCountSupplier) {
        this.seqNumCounter = seqNumCounterSupplier.get();
        this.recordCountSupplier = recordCountSupplier;
        this.seqNumberFieldIndex = writeSchema.getFieldIndex(SpecialFields.SEQUENCE_NUMBER.name());
        this.minSeqNumber = Long.MAX_VALUE;
        this.maxSeqNumber = Long.MIN_VALUE;
        this.hasNullSeqNumber = false;
    }

    /** Returns the minimum sequence number for the file. */
    public long min() {
        if (seqNumberFieldIndex == -1) {
            return seqNumCounter.getValue() - recordCountSupplier.get();
        }
        // minSeqNumber stays at Long.MAX_VALUE when all records have null sequence numbers.
        // Returning 0 triggers RowTrackingCommitUtils.assignSnapshotId() to use snapshot ID.
        return minSeqNumber == Long.MAX_VALUE ? 0 : minSeqNumber;
    }

    /** Returns the maximum sequence number for the file. */
    public long max() {
        if (seqNumberFieldIndex == -1) {
            return seqNumCounter.getValue() - 1;
        }
        // When hasNullSeqNumber is true, some records have null sequence numbers.
        // Returning 0 triggers RowTrackingCommitUtils.assignSnapshotId() to use snapshot ID for
        // max.
        return hasNullSeqNumber ? 0 : maxSeqNumber;
    }

    /**
     * Processes one row: increments the sequence counter, in row-tracking mode, extracts and tracks
     * the sequence number from the row.
     */
    public void update(InternalRow row) {
        seqNumCounter.add(1L);

        if (seqNumberFieldIndex != -1 && !row.isNullAt(seqNumberFieldIndex)) {
            // If sequence number field exists, extract min/max from row data
            long seqNum = row.getLong(seqNumberFieldIndex);
            minSeqNumber = Math.min(minSeqNumber, seqNum);
            maxSeqNumber = Math.max(maxSeqNumber, seqNum);
        } else if (seqNumberFieldIndex != -1) {
            // Manifest will calculate the correct max based on snapshot id
            hasNullSeqNumber = true;
        }
    }
}
