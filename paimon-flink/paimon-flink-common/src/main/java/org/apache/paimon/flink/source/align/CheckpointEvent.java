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

package org.apache.paimon.flink.source.align;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Objects;

/**
 * Event sent from {@link AlignedContinuousFileSplitEnumerator} to {@link AlignedSourceReader} to
 * notify the checkpointId that is about to be triggered.
 */
public class CheckpointEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final long checkpointId;

    public CheckpointEvent(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointEvent that = (CheckpointEvent) o;
        return checkpointId == that.checkpointId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpointId);
    }

    @Override
    public String toString() {
        return "CheckpointEvent{" + "checkpointId=" + checkpointId + '}';
    }
}
