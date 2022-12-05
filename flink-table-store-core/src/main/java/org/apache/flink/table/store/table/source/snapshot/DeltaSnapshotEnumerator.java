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
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.operation.ScanKind;
import org.apache.flink.table.store.table.source.DataTableScan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * A {@link DataFileSnapshotEnumerator} which scans incremental changes in {@link
 * Snapshot#deltaManifestList()} for each newly created snapshots.
 */
public class DeltaSnapshotEnumerator extends DataFileSnapshotEnumerator {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaSnapshotEnumerator.class);

    public DeltaSnapshotEnumerator(
            Path tablePath,
            DataTableScan scan,
            CoreOptions.LogStartupMode startupMode,
            @Nullable Long startupMillis,
            @Nullable Long nextSnapshotId) {
        super(tablePath, scan, startupMode, startupMillis, nextSnapshotId);
    }

    @Override
    protected boolean shouldReadSnapshot(Snapshot snapshot) {
        if (snapshot.commitKind() == Snapshot.CommitKind.APPEND) {
            return true;
        }

        LOG.debug(
                "Next snapshot id {} is not APPEND, but is {}, check next one.",
                snapshot.id(),
                snapshot.commitKind());
        return false;
    }

    @Override
    protected DataTableScan.DataFilePlan getPlan(DataTableScan scan) {
        return scan.withKind(ScanKind.DELTA).plan();
    }
}
