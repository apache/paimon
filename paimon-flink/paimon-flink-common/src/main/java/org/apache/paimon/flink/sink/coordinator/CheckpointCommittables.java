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

import org.apache.paimon.flink.sink.Committable;

import java.util.List;

/**
 * Committables produced for one checkpoint together with the watermark associated with it. The
 * checkpoint id is intrinsic to the payload so both the writer-side persistence and the
 * coordinator-side per-subtask buffer can share this type without an external key.
 */
public class CheckpointCommittables {

    private final long checkpointId;
    private final List<Committable> committables;
    private final long watermark;

    public CheckpointCommittables(
            long checkpointId, List<Committable> committables, long watermark) {
        this.checkpointId = checkpointId;
        this.committables = committables;
        this.watermark = watermark;
    }

    public long checkpointId() {
        return checkpointId;
    }

    public List<Committable> committables() {
        return committables;
    }

    public long watermark() {
        return watermark;
    }

    public int size() {
        return committables.size();
    }

    public boolean isEmpty() {
        return committables.isEmpty();
    }

    @Override
    public String toString() {
        return String.format(
                "CheckpointCommittables{checkpointId=%d, watermark=%d, committables=%s}",
                checkpointId, watermark, committables);
    }
}
