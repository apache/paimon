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

/** Result of a PWC commit attempt. */
class CommitResult {

    static final CommitResult NONE = new CommitResult(false, 0, -1, false);

    private final boolean committed;
    private final int committedCount;
    private final long checkpointId;
    private final boolean restoredCommit;

    CommitResult(boolean committed, int committedCount, long checkpointId, boolean restoredCommit) {
        this.committed = committed;
        this.committedCount = committedCount;
        this.checkpointId = checkpointId;
        this.restoredCommit = restoredCommit;
    }

    boolean committed() {
        return committed;
    }

    int committedCount() {
        return committedCount;
    }

    long checkpointId() {
        return checkpointId;
    }

    boolean restoredCommit() {
        return restoredCommit;
    }
}
