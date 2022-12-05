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
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.source.DataTableScan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** Abstract class for all {@link SnapshotEnumerator}s which enumerate record related data files. */
public abstract class DataFileSnapshotEnumerator implements SnapshotEnumerator {

    protected static final Logger LOG = LoggerFactory.getLogger(DataFileSnapshotEnumerator.class);

    protected final SnapshotManager snapshotManager;
    protected final DataTableScan scan;

    protected @Nullable Long nextSnapshotId;

    public DataFileSnapshotEnumerator(
            Path tablePath, DataTableScan scan, @Nullable Long nextSnapshotId) {
        this.snapshotManager = new SnapshotManager(tablePath);
        this.scan = scan;

        this.nextSnapshotId = nextSnapshotId;
    }

    @Override
    public DataTableScan.DataFilePlan enumerate() {
        if (nextSnapshotId == null) {
            return tryFirstEnumerate();
        } else {
            return nextEnumerate();
        }
    }

    protected abstract DataTableScan.DataFilePlan tryFirstEnumerate();

    protected abstract DataTableScan.DataFilePlan nextEnumerate();

    public static DataFileSnapshotEnumerator create(
            FileStoreTable table, DataTableScan scan, Long nextSnapshotId) {
        Path location = table.location();
        CoreOptions.LogStartupMode startupMode = table.options().logStartupMode();
        Long startupMillis = table.options().logScanTimestampMills();

        switch (table.options().changelogProducer()) {
            case NONE:
                return new DeltaSnapshotEnumerator(
                        location, scan, startupMode, startupMillis, nextSnapshotId);
            case INPUT:
                return new InputChangelogSnapshotEnumerator(
                        location, scan, startupMode, startupMillis, nextSnapshotId);
            case FULL_COMPACTION:
                return new FullCompactionChangelogSnapshotEnumerator(
                        location,
                        scan,
                        table.options().numLevels() - 1,
                        startupMode,
                        startupMillis,
                        nextSnapshotId);
            default:
                throw new UnsupportedOperationException(
                        "Unknown changelog producer " + table.options().changelogProducer().name());
        }
    }

    public static DataFileSnapshotEnumerator create(FileStoreTable table, DataTableScan scan) {
        return new StaticSnapshotEnumerator(table.location(), scan);
    }
}
