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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.table.source.DataTableScan;

/** {@link DataFileSnapshotEnumerator} implementation for static snapshot enumerator. */
public class StaticSnapshotEnumerator extends DataFileSnapshotEnumerator {
    public StaticSnapshotEnumerator(Path tablePath, DataTableScan scan) {
        super(tablePath, scan, null);
    }

    @Override
    protected DataTableScan.DataFilePlan tryFirstEnumerate() {
        Long startingSnapshotId =
                scan.options().readCompacted()
                        ? snapshotManager.latestCompactedSnapshotId()
                        : snapshotManager.latestSnapshotId();
        if (startingSnapshotId == null) {
            LOG.debug(
                    "There is currently no snapshot or compacted snapshot. Wait for the snapshot generation.");
            return null;
        }

        return scan.withSnapshot(startingSnapshotId).plan();
    }

    @Override
    protected DataTableScan.DataFilePlan nextEnumerate() {
        throw new UnsupportedOperationException(
                "Unsupported operation in static snapshot enumerator.");
    }
}
