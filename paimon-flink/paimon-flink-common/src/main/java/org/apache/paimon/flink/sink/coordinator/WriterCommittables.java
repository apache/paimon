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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.annotation.VisibleForTesting;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Per-subtask buffer of {@link CheckpointCommittables} received by the coordinator, indexed by
 * checkpoint id.
 */
public class WriterCommittables {

    private static final Logger LOG = LoggerFactory.getLogger(WriterCommittables.class);
    private static final long END_INPUT_CHECKPOINT_ID = Long.MAX_VALUE;

    private long maxCheckpointId;
    private final NavigableMap<Long, CheckpointCommittables> committablesPerCheckpoint;
    private CheckpointCommittables endInputCommittables;
    private long endInputCoveredBy = -1;

    @VisibleForTesting
    WriterCommittables(long maxCheckpointId, List<CheckpointCommittables> entries) {
        this.maxCheckpointId = maxCheckpointId;
        this.committablesPerCheckpoint = new TreeMap<>();
        for (CheckpointCommittables entry : entries) {
            if (entry.checkpointId() == END_INPUT_CHECKPOINT_ID) {
                if (endInputCommittables != null) {
                    throw new IllegalStateException(
                            "Invalid input committables, duplicate end input entry");
                }
                endInputCommittables = entry;
                endInputCoveredBy = maxCheckpointId;
            } else if (entry.checkpointId() > maxCheckpointId) {
                throw new IllegalStateException(
                        "Invalid input committables, max checkpoint id should not be less than "
                                + "checkpoint id of committables, max checkpoint is "
                                + maxCheckpointId
                                + ", entry checkpoint is "
                                + entry.checkpointId());
            } else if (committablesPerCheckpoint.containsKey(entry.checkpointId())) {
                throw new IllegalStateException(
                        "Invalid input committables, duplicate checkpoint id "
                                + entry.checkpointId());
            } else {
                committablesPerCheckpoint.put(entry.checkpointId(), entry);
            }
        }
    }

    @VisibleForTesting
    WriterCommittables(CheckpointCommittables entry) {
        this.maxCheckpointId =
                entry.checkpointId() == END_INPUT_CHECKPOINT_ID ? -1 : entry.checkpointId();
        this.committablesPerCheckpoint = new TreeMap<>();
        if (entry.checkpointId() == END_INPUT_CHECKPOINT_ID) {
            endInputCommittables = entry;
        } else {
            committablesPerCheckpoint.put(entry.checkpointId(), entry);
        }
    }

    public NavigableMap<Long, CheckpointCommittables> getCommittablesPerCheckpoint() {
        return committablesPerCheckpoint;
    }

    public NavigableMap<Long, CheckpointCommittables> getCommittablesBeforeCheckpoint(
            long checkpointId, boolean inclusive) {
        return committablesPerCheckpoint.headMap(checkpointId, inclusive);
    }

    public void clearCommittablesBeforeCheckpoint(long checkpointId, boolean inclusive) {
        if (checkpointId > maxCheckpointId || (checkpointId == maxCheckpointId && inclusive)) {
            maxCheckpointId = -1;
            committablesPerCheckpoint.clear();
        } else {
            committablesPerCheckpoint.headMap(checkpointId, inclusive).clear();
        }
    }

    public void reset() {
        maxCheckpointId = -1;
        committablesPerCheckpoint.clear();
        endInputCommittables = null;
        endInputCoveredBy = -1;
    }

    public void mergeWith(WriterCommittables other) {
        if (other.maxCheckpointId >= 0 && other.maxCheckpointId <= maxCheckpointId) {
            throw new IllegalStateException(
                    "It must merge later checkpoint committables, however current checkpoint id is "
                            + maxCheckpointId
                            + ", to be merged checkpoint id is "
                            + other.maxCheckpointId);
        }
        if (other.maxCheckpointId >= 0) {
            maxCheckpointId = other.maxCheckpointId;
            if (endInputCommittables != null && endInputCoveredBy < 0) {
                endInputCoveredBy = other.maxCheckpointId;
            }
        }
        for (Map.Entry<Long, CheckpointCommittables> entry :
                other.getCommittablesPerCheckpoint().entrySet()) {
            if (committablesPerCheckpoint.containsKey(entry.getKey())) {
                LOG.error(
                        "Subtask committables should not contain {}, the current committables are "
                                + "{}, to be merged committables are {}",
                        entry.getKey(),
                        committablesPerCheckpoint,
                        other.getCommittablesPerCheckpoint());
                throw new IllegalStateException(
                        "Subtask contains repeated committables of checkpoint " + entry.getKey());
            }
            committablesPerCheckpoint.put(entry.getKey(), entry.getValue());
        }
        if (other.endInputCommittables != null) {
            // End input can be replayed after restore. It is a terminal slot, not a normal
            // checkpoint entry, so replacing it cannot duplicate file committables.
            endInputCommittables = other.endInputCommittables;
            endInputCoveredBy = Math.max(endInputCoveredBy, other.endInputCoveredBy);
        }
    }

    public long getMaxCheckpointId() {
        return maxCheckpointId;
    }

    public boolean hasEndInput() {
        return endInputCommittables != null;
    }

    public boolean isEndInputCoveredBy(long checkpointId) {
        return endInputCommittables != null
                && endInputCoveredBy >= 0
                && endInputCoveredBy <= checkpointId;
    }

    public CheckpointCommittables getEndInputCommittables() {
        return endInputCommittables;
    }

    public void clearEndInputCommittables() {
        endInputCommittables = null;
        endInputCoveredBy = -1;
    }

    /** Retains only the terminal entry from a region-failover restore. */
    public void restoreEndInput(WriterCommittables restored) {
        if (restored.endInputCommittables != null) {
            endInputCommittables = restored.endInputCommittables;
            endInputCoveredBy = restored.endInputCoveredBy;
        }
    }

    /**
     * Returns the watermark this subtask reported for {@code checkpointId}. Falls back to {@link
     * Long#MIN_VALUE} when the subtask has no entry for that checkpoint — that "no observed
     * watermark" sentinel is what {@link
     * org.apache.paimon.flink.sink.CoordinatorCommittingRowDataStoreWriteOperator} emits at
     * barriers before any watermark is seen, and matches {@code CommitterOperator}'s initial value.
     * Aggregation across subtasks (per-checkpoint min, future idle handling) belongs to the
     * coordinator, but the per-subtask policy for missing entries lives here.
     */
    public long watermarkAt(long checkpointId) {
        CheckpointCommittables entry = committablesPerCheckpoint.get(checkpointId);
        if (entry != null) {
            return entry.watermark();
        }
        return checkpointId == END_INPUT_CHECKPOINT_ID && endInputCommittables != null
                ? endInputCommittables.watermark()
                : Long.MIN_VALUE;
    }

    /**
     * Returns whether this subtask was idle at {@code checkpointId}. Falls back to {@code false}
     * (ACTIVE) when the subtask has no entry — equivalent to "channel exists but has never reported
     * a watermark yet", which Flink's {@code StatusWatermarkValve} also treats as ACTIVE.
     */
    public boolean isIdleAt(long checkpointId) {
        CheckpointCommittables entry = committablesPerCheckpoint.get(checkpointId);
        return entry != null
                ? entry.idle()
                : checkpointId == END_INPUT_CHECKPOINT_ID
                        && endInputCommittables != null
                        && endInputCommittables.idle();
    }

    @Override
    public String toString() {
        return String.format(
                "WriterCommittables{maxCheckpointId=%d, endInputCoveredBy=%d, committables=%s, endInput=%s}",
                maxCheckpointId,
                endInputCoveredBy,
                committablesPerCheckpoint,
                endInputCommittables);
    }

    public static WriterCommittables from(
            CommittableEvent event, TypeSerializer<CheckpointCommittables> serializer)
            throws IOException {
        CheckpointCommittables entry = event.deserialize(serializer);
        if (entry.checkpointId() != event.getCheckpointId()) {
            throw new IllegalStateException(
                    "CommittableEvent checkpoint id "
                            + event.getCheckpointId()
                            + " does not match payload checkpoint id "
                            + entry.checkpointId());
        }
        return new WriterCommittables(entry);
    }

    public static WriterCommittables fromRestore(
            RestoredCommittableEvent event, TypeSerializer<CheckpointCommittables> serializer)
            throws IOException {
        return new WriterCommittables(
                event.getRestoredCheckpointId(), event.deserialize(serializer));
    }
}
