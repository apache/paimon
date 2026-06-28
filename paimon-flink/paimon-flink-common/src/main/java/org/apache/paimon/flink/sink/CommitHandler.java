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

package org.apache.paimon.flink.sink;

import org.apache.paimon.flink.sink.coordinator.CommitCompleteEvent;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;

/** Handles writer-side interactions with commit coordinator. */
public class CommitHandler {

    public static final CommitHandler EMPTY = new CommitHandler();

    public void initialize(
            StateInitializationContext context, int subtaskId, int attemptNumber, String commitUser)
            throws Exception {}

    public void processWatermark(long watermark) {}

    public void snapshot(StateSnapshotContext context) throws Exception {}

    public void handleCommittables(long checkpointId) {}

    public boolean requiresStableCommitUser() {
        return false;
    }

    public boolean collect(Committable committable) {
        return false;
    }

    public boolean handleOperatorEvent(OperatorEvent event) {
        return event instanceof CommitCompleteEvent;
    }
}
