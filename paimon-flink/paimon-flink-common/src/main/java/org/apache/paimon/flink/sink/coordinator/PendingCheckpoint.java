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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** Pending data and state for one checkpoint in PWC. */
class PendingCheckpoint {

    private final long checkpointId;
    private final Map<Integer, SubtaskFileInfo> fileInfos;
    private boolean staged;

    PendingCheckpoint(long checkpointId) {
        this.checkpointId = checkpointId;
        this.fileInfos = new HashMap<>();
    }

    long checkpointId() {
        return checkpointId;
    }

    boolean receive(int subtask, FileInfoRequest request, List<Committable> committables) {
        SubtaskFileInfo previous = fileInfos.get(subtask);
        if (previous != null) {
            if (previous.request().samePayload(request)) {
                return false;
            }
            throw new IllegalStateException(
                    String.format(
                            "Different FileInfoRequest received for checkpoint %d subtask %d.",
                            checkpointId, subtask));
        }

        fileInfos.put(subtask, new SubtaskFileInfo(request, committables));
        return true;
    }

    void removeSubtask(int subtask) {
        fileInfos.remove(subtask);
    }

    boolean isEmpty() {
        return fileInfos.isEmpty();
    }

    boolean staged() {
        return staged;
    }

    void markStaged() {
        staged = true;
    }

    List<SubtaskFileInfo> fileInfos() {
        return new ArrayList<>(fileInfos.values());
    }

    TreeSet<Integer> receivedSubtasks() {
        return new TreeSet<>(fileInfos.keySet());
    }

    List<Committable> allCommittables() {
        List<Committable> result = new ArrayList<>();
        for (Integer subtask : receivedSubtasks()) {
            result.addAll(fileInfos.get(subtask).committables());
        }
        return result;
    }

    List<Committable> committablesAfter(long checkpointId) {
        List<Committable> result = new ArrayList<>();
        for (Committable committable : allCommittables()) {
            if (committable.checkpointId() > checkpointId) {
                result.add(committable);
            }
        }
        return result;
    }
}
