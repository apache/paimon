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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * {@link StartingScanner} for the {@link CoreOptions.StartupMode#FROM_TIMESTAMP} startup mode of a
 * batch read.
 */
public class StaticFromTimestampStartingScanner extends ReadPlanStartingScanner {

    private static final Logger LOG =
            LoggerFactory.getLogger(StaticFromTimestampStartingScanner.class);

    private final long startupMillis;

    public StaticFromTimestampStartingScanner(SnapshotManager snapshotManager, long startupMillis) {
        super(snapshotManager);
        this.startupMillis = startupMillis;
        Snapshot snapshot = timeTravelToTimestamp(snapshotManager, startupMillis);
        if (snapshot != null) {
            this.startingSnapshotId = snapshot.id();
        }
    }

    @Override
    public SnapshotReader configure(SnapshotReader snapshotReader) {
        if (startingSnapshotId == null) {
            LOG.debug(
                    "There is currently no snapshot earlier than or equal to timestamp[{}]",
                    startupMillis);
            return null;
        }
        return snapshotReader.withMode(ScanMode.ALL).withSnapshot(startingSnapshotId);
    }

    @Nullable
    public static Snapshot timeTravelToTimestamp(SnapshotManager snapshotManager, long timestamp) {
        return snapshotManager.earlierOrEqualTimeMills(timestamp);
    }
}
