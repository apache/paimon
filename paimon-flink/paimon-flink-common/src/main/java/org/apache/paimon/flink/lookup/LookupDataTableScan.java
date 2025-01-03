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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.table.source.DataTableStreamScan;
import org.apache.paimon.table.source.snapshot.AllDeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.BoundedChecker;
import org.apache.paimon.table.source.snapshot.FollowUpScanner;
import org.apache.paimon.table.source.snapshot.FullStartingScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.apache.paimon.CoreOptions.StartupMode;
import static org.apache.paimon.flink.lookup.LookupFileStoreTable.LookupStreamScanMode;

/**
 * {@link org.apache.paimon.table.source.StreamTableScan} implementation for lookup streaming
 * planning.
 */
public class LookupDataTableScan extends DataTableStreamScan {

    private static final Logger LOG = LoggerFactory.getLogger(LookupDataTableScan.class);

    private final StartupMode startupMode;
    private final LookupStreamScanMode lookupScanMode;

    public LookupDataTableScan(
            CoreOptions options,
            SnapshotReader snapshotReader,
            SnapshotManager snapshotManager,
            boolean supportStreamingReadOverwrite,
            DefaultValueAssigner defaultValueAssigner,
            LookupStreamScanMode lookupScanMode) {
        super(
                options,
                snapshotReader,
                snapshotManager,
                supportStreamingReadOverwrite,
                defaultValueAssigner);
        this.startupMode = options.startupMode();
        this.lookupScanMode = lookupScanMode;
        dropStats();
    }

    @Override
    @Nullable
    protected SnapshotReader.Plan handleOverwriteSnapshot(Snapshot snapshot) {
        SnapshotReader.Plan plan = super.handleOverwriteSnapshot(snapshot);
        if (plan != null) {
            return plan;
        }
        LOG.info("Dim table found OVERWRITE snapshot {}, reopen.", snapshot.id());
        throw new ReopenException();
    }

    @Override
    protected StartingScanner createStartingScanner(boolean isStreaming) {
        return startupMode != CoreOptions.StartupMode.COMPACTED_FULL
                ? new FullStartingScanner(snapshotReader.snapshotManager())
                : super.createStartingScanner(isStreaming);
    }

    @Override
    protected FollowUpScanner createFollowUpScanner() {
        switch (lookupScanMode) {
            case CHANGELOG:
                return super.createFollowUpScanner();
            case FILE_MONITOR:
                return new AllDeltaFollowUpScanner();
            case COMPACT_DELTA_MONITOR:
                return new CompactionDiffFollowUpScanner();
            default:
                throw new UnsupportedOperationException(
                        "Unknown lookup stream scan mode: " + lookupScanMode.name());
        }
    }

    @Override
    protected BoundedChecker createBoundedChecker() {
        return BoundedChecker.neverEnd(); // dim table should never end
    }
}
