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
import org.apache.paimon.flink.sink.Committable;

import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Buffer of {@link Committable}s received from one writer subtask, indexed by checkpoint id. Lives
 * inside the coordinator and is consumed when a checkpoint is ready to commit.
 */
public class WriterCommittables {

    private static final Logger LOG = LoggerFactory.getLogger(WriterCommittables.class);

    private long maxCheckpointId;
    private final NavigableMap<Long, List<Committable>> committablesPerCheckpoint;

    @VisibleForTesting
    WriterCommittables(long maxCheckpointId, List<Committable> committables) {
        this.maxCheckpointId = maxCheckpointId;
        this.committablesPerCheckpoint = new TreeMap<>();

        if (!committables.isEmpty()) {
            groupByCheckpoint(committablesPerCheckpoint, committables);
            if (maxCheckpointId < committablesPerCheckpoint.lastKey()) {
                throw new IllegalStateException(
                        "Invalid input committables, max checkpoint id should not be less than "
                                + "checkpoint id of committables, max checkpoint is "
                                + maxCheckpointId
                                + ", committables are "
                                + committables);
            }
        }
    }

    public NavigableMap<Long, List<Committable>> getCommittablesPerCheckpoint() {
        return committablesPerCheckpoint;
    }

    public NavigableMap<Long, List<Committable>> getCommittablesBeforeCheckpoint(
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
        for (Map.Entry<Long, List<Committable>> entry :
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

    @Override
    public String toString() {
        return String.format(
                "WriterCommittables{maxCheckpointId=%d, committables=%s}",
                maxCheckpointId, committablesPerCheckpoint);
    }

    public static WriterCommittables from(
            CommittableEvent event, ListSerializer<Committable> serializer) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(event.getSerialized());
        return new WriterCommittables(event.getCheckpointId(), serializer.deserialize(in));
    }

    @VisibleForTesting
    static void groupByCheckpoint(
            Map<Long, List<Committable>> grouped, Collection<Committable> committables) {
        for (Committable c : committables) {
            grouped.computeIfAbsent(c.checkpointId(), k -> new ArrayList<>()).add(c);
        }
    }
}
