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

package org.apache.flink.table.store.table.source.snapshot;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.table.DataTable;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

/** {@link SnapshotEnumerator} for batch read. */
public class StaticDataFileSnapshotEnumerator implements SnapshotEnumerator {

    private final SnapshotManager snapshotManager;
    private final DataTableScan scan;
    private final StartingScanner startingScanner;

    private boolean hasNext;

    public StaticDataFileSnapshotEnumerator(
            FileIO fileIO, Path tablePath, DataTableScan scan, StartingScanner startingScanner) {
        this.snapshotManager = new SnapshotManager(fileIO, tablePath);
        this.scan = scan;
        this.startingScanner = startingScanner;

        this.hasNext = true;
    }

    @Nullable
    @Override
    public DataTableScan.DataFilePlan enumerate() {
        if (hasNext) {
            hasNext = false;
            return startingScanner.getPlan(snapshotManager, scan);
        } else {
            return null;
        }
    }

    // ------------------------------------------------------------------------
    //  static create methods
    // ------------------------------------------------------------------------

    public static StaticDataFileSnapshotEnumerator create(DataTable table, DataTableScan scan) {
        CoreOptions.StartupMode startupMode = table.options().startupMode();
        StartingScanner startingScanner;
        if (startupMode == CoreOptions.StartupMode.LATEST_FULL
                || startupMode == CoreOptions.StartupMode.LATEST) {
            startingScanner = new FullStartingScanner();
        } else if (startupMode == CoreOptions.StartupMode.COMPACTED_FULL) {
            startingScanner = new CompactedStartingScanner();
        } else if (startupMode == CoreOptions.StartupMode.FROM_TIMESTAMP) {
            Long startupMillis = table.options().scanTimestampMills();
            Preconditions.checkNotNull(
                    startupMillis,
                    String.format(
                            "%s can not be null when you use %s for %s",
                            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                            CoreOptions.StartupMode.FROM_TIMESTAMP,
                            CoreOptions.SCAN_MODE.key()));
            startingScanner = new StaticFromTimestampStartingScanner(startupMillis);
        } else if (startupMode == CoreOptions.StartupMode.FROM_SNAPSHOT) {
            Long snapshotId = table.options().scanSnapshotId();
            Preconditions.checkNotNull(
                    snapshotId,
                    String.format(
                            "%s can not be null when you use %s for %s",
                            CoreOptions.SCAN_SNAPSHOT_ID.key(),
                            CoreOptions.StartupMode.FROM_SNAPSHOT,
                            CoreOptions.SCAN_MODE.key()));
            startingScanner = new StaticFromSnapshotStartingScanner(snapshotId);
        } else {
            throw new UnsupportedOperationException("Unknown startup mode " + startupMode.name());
        }

        return new StaticDataFileSnapshotEnumerator(
                table.fileIO(), table.location(), scan, startingScanner);
    }

    // ------------------------------------------------------------------------
    //  factory interface
    // ------------------------------------------------------------------------

    /** Factory to create {@link StaticDataFileSnapshotEnumerator}. */
    public interface Factory extends Serializable {

        StaticDataFileSnapshotEnumerator create(DataTable table, DataTableScan scan);
    }
}
