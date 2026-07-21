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

    private long maxCheckpointId;
    private final NavigableMap<Long, CheckpointCommittables> committablesPerCheckpoint;

    @VisibleForTesting
    WriterCommittables(long maxCheckpointId, List<CheckpointCommittables> entries) {
        this.maxCheckpointId = maxCheckpointId;
        this.committablesPerCheckpoint = new TreeMap<>();
        for (CheckpointCommittables entry : entries) {
            if (entry.checkpointId() > maxCheckpointId) {
                throw new IllegalStateException(
                        "Invalid input committables, max checkpoint id should not be less than "
                                + "checkpoint id of committables, max checkpoint is "
                                + maxCheckpointId
                                + ", entry checkpoint is "
                                + entry.checkpointId());
            }
            if (committablesPerCheckpoint.containsKey(entry.checkpointId())) {
                throw new IllegalStateException(
                        "Invalid input committables, duplicate checkpoint id "
                                + entry.checkpointId());
            }
            committablesPerCheckpoint.put(entry.checkpointId(), entry);
        }
    }

    @VisibleForTesting
    WriterCommittables(CheckpointCommittables entry) {
        this.maxCheckpointId = entry.checkpointId();
        this.committablesPerCheckpoint = new TreeMap<>();
        committablesPerCheckpoint.put(entry.checkpointId(), entry);
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
            reset();
        } else {
            committablesPerCheckpoint.headMap(checkpointId, inclusive).clear();
        }
    }

    public void reset() {
        maxCheckpointId = -1;
        committablesPerCheckpoint.clear();
    }

    public void mergeWith(WriterCommittables other) {
        if (other.maxCheckpointId <= maxCheckpointId) {
            throw new IllegalStateException(
                    "It must merge later checkpoint committables, however current checkpoint id is "
                            + maxCheckpointId
                            + ", to be merged checkpoint id is "
                            + other.maxCheckpointId);
        }
        maxCheckpointId = other.maxCheckpointId;
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
    }

    public long getMaxCheckpointId() {
        return maxCheckpointId;
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
        return entry == null ? Long.MIN_VALUE : entry.watermark();
    }

    /**
     * Returns whether this subtask was idle at {@code checkpointId}. Falls back to {@code false}
     * (ACTIVE) when the subtask has no entry — equivalent to "channel exists but has never reported
     * a watermark yet", which Flink's {@code StatusWatermarkValve} also treats as ACTIVE.
     */
    public boolean isIdleAt(long checkpointId) {
        CheckpointCommittables entry = committablesPerCheckpoint.get(checkpointId);
        return entry != null && entry.idle();
    }

    @Override
    public String toString() {
        return String.format(
                "WriterCommittables{maxCheckpointId=%d, committables=%s}",
                maxCheckpointId, committablesPerCheckpoint);
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
