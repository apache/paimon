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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.annotation.VisibleForTesting;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.operation.ScanKind;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.table.source.snapshot.CompactedStartingScanner;
import org.apache.flink.table.store.table.source.snapshot.ContinuousFromSnapshotStartingScanner;
import org.apache.flink.table.store.table.source.snapshot.ContinuousFromTimestampStartingScanner;
import org.apache.flink.table.store.table.source.snapshot.ContinuousLatestStartingScanner;
import org.apache.flink.table.store.table.source.snapshot.FullStartingScanner;
import org.apache.flink.table.store.table.source.snapshot.SnapshotSplitReader;
import org.apache.flink.table.store.table.source.snapshot.StartingScanner;
import org.apache.flink.table.store.table.source.snapshot.StaticFromSnapshotStartingScanner;
import org.apache.flink.table.store.table.source.snapshot.StaticFromTimestampStartingScanner;
import org.apache.flink.table.store.utils.Filter;
import org.apache.flink.table.store.utils.Preconditions;

/** An abstraction layer above {@link FileStoreScan} to provide input split generation. */
public abstract class AbstractDataTableScan implements DataTableScan {

    private final CoreOptions options;
    protected final SnapshotSplitReader snapshotSplitReader;

    protected AbstractDataTableScan(CoreOptions options, SnapshotSplitReader snapshotSplitReader) {
        this.options = options;
        this.snapshotSplitReader = snapshotSplitReader;
    }

    @Override
    public AbstractDataTableScan withSnapshot(long snapshotId) {
        snapshotSplitReader.withSnapshot(snapshotId);
        return this;
    }

    @Override
    public AbstractDataTableScan withFilter(Predicate predicate) {
        snapshotSplitReader.withFilter(predicate);
        return this;
    }

    @Override
    public AbstractDataTableScan withKind(ScanKind scanKind) {
        snapshotSplitReader.withKind(scanKind);
        return this;
    }

    @Override
    public AbstractDataTableScan withLevelFilter(Filter<Integer> levelFilter) {
        snapshotSplitReader.withLevelFilter(levelFilter);
        return this;
    }

    @VisibleForTesting
    public AbstractDataTableScan withBucket(int bucket) {
        snapshotSplitReader.withBucket(bucket);
        return this;
    }

    public CoreOptions options() {
        return options;
    }

    protected StartingScanner createStartingScanner(boolean isStreaming) {
        CoreOptions.StartupMode startupMode = options.startupMode();
        switch (startupMode) {
            case LATEST_FULL:
                return new FullStartingScanner();
            case LATEST:
                return isStreaming
                        ? new ContinuousLatestStartingScanner()
                        : new FullStartingScanner();
            case COMPACTED_FULL:
                return new CompactedStartingScanner();
            case FROM_TIMESTAMP:
                Long startupMillis = options.scanTimestampMills();
                Preconditions.checkNotNull(
                        startupMillis,
                        String.format(
                                "%s can not be null when you use %s for %s",
                                CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                                CoreOptions.StartupMode.FROM_TIMESTAMP,
                                CoreOptions.SCAN_MODE.key()));
                return isStreaming
                        ? new ContinuousFromTimestampStartingScanner(startupMillis)
                        : new StaticFromTimestampStartingScanner(startupMillis);
            case FROM_SNAPSHOT:
                Long snapshotId = options.scanSnapshotId();
                Preconditions.checkNotNull(
                        snapshotId,
                        String.format(
                                "%s can not be null when you use %s for %s",
                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                CoreOptions.StartupMode.FROM_SNAPSHOT,
                                CoreOptions.SCAN_MODE.key()));
                return isStreaming
                        ? new ContinuousFromSnapshotStartingScanner(snapshotId)
                        : new StaticFromSnapshotStartingScanner(snapshotId);
            default:
                throw new UnsupportedOperationException(
                        "Unknown startup mode " + startupMode.name());
        }
    }
}
