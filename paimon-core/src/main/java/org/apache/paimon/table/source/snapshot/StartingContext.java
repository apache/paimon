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

package org.apache.paimon.table.source.snapshot;

/** That contains some information that will be used out of StartingScanner. */
public class StartingContext {
    /**
     * Notice: The snapshot ID is the initial one corresponding to the StartScanner configuration,
     * not necessarily the snapshot ID at the time of the actual scan. E.g, in
     * ContinuousFromSnapshotFullStartingScanner, this snapshot ID used in the first scan is the
     * bigger one between the configured one and the earliest one.
     */
    private final Long snapshotId;

    private final Boolean scanFullSnapshot;

    public StartingContext(Long snapshotId, Boolean scanFullSnapshot) {
        this.snapshotId = snapshotId;
        this.scanFullSnapshot = scanFullSnapshot;
    }

    public Long getSnapshotId() {
        return this.snapshotId;
    }

    public Boolean getScanFullSnapshot() {
        return this.scanFullSnapshot;
    }

    public static final StartingContext EMPTY = new StartingContext(1L, false);
}
