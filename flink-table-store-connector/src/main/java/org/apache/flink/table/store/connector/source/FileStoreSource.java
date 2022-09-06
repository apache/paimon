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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.table.store.connector.source.PendingSplitsCheckpoint.INVALID_SNAPSHOT;
import static org.apache.flink.util.Preconditions.checkArgument;

/** {@link Source} of file store. */
public class FileStoreSource
        implements Source<RowData, FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;

    private final boolean isContinuous;

    private final long discoveryInterval;

    private final boolean latestContinuous;

    @Nullable private final int[][] projectedFields;

    @Nullable private final Predicate predicate;

    public FileStoreSource(
            FileStoreTable table,
            boolean isContinuous,
            long discoveryInterval,
            boolean latestContinuous,
            @Nullable int[][] projectedFields,
            @Nullable Predicate predicate) {
        this.table = table;
        this.isContinuous = isContinuous;
        this.discoveryInterval = discoveryInterval;
        this.latestContinuous = latestContinuous;
        this.projectedFields = projectedFields;
        this.predicate = predicate;
    }

    @Override
    public Boundedness getBoundedness() {
        return isContinuous ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, FileStoreSourceSplit> createReader(SourceReaderContext context) {
        TableRead read = table.newRead();
        if (projectedFields != null) {
            read.withProjection(projectedFields);
        }
        if (predicate != null) {
            read.withFilter(predicate);
        }
        return new FileStoreSourceReader(context, read);
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> createEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context) {
        return restoreEnumerator(context, null);
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        SnapshotManager snapshotManager = table.snapshotManager();
        TableScan scan = table.newScan();
        if (predicate != null) {
            scan.withFilter(predicate);
        }

        Long snapshotId;
        Collection<FileStoreSourceSplit> splits;
        if (checkpoint == null) {
            // first, create new enumerator, plan splits
            if (latestContinuous) {
                checkArgument(
                        isContinuous,
                        "The latest continuous can only be true when isContinuous is true.");
                snapshotId = snapshotManager.latestSnapshotId();
                splits = new ArrayList<>();
            } else {
                TableScan.Plan plan = scan.plan();
                snapshotId = plan.snapshotId;
                splits = new FileStoreSourceSplitGenerator().createSplits(plan);
            }
        } else {
            // restore from checkpoint
            snapshotId = checkpoint.currentSnapshotId();
            if (snapshotId == INVALID_SNAPSHOT) {
                snapshotId = null;
            }
            splits = checkpoint.splits();
        }

        // create enumerator from snapshotId and splits
        if (isContinuous) {
            long currentSnapshot = snapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : snapshotId;
            return new ContinuousFileSplitEnumerator(
                    context,
                    table.location(),
                    scan.withIncremental(true), // the subsequent planning is all incremental
                    splits,
                    currentSnapshot,
                    discoveryInterval);
        } else {
            Snapshot snapshot = snapshotId == null ? null : snapshotManager.snapshot(snapshotId);
            return new StaticFileStoreSplitEnumerator(context, snapshot, splits);
        }
    }

    @Override
    public FileStoreSourceSplitSerializer getSplitSerializer() {
        return new FileStoreSourceSplitSerializer();
    }

    @Override
    public PendingSplitsCheckpointSerializer getEnumeratorCheckpointSerializer() {
        return new PendingSplitsCheckpointSerializer(getSplitSerializer());
    }
}
