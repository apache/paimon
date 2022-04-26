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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.utils.PartitionedManifestMeta;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.table.store.connector.source.PendingSplitsCheckpoint.INVALID_SNAPSHOT;
import static org.apache.flink.util.Preconditions.checkArgument;

/** {@link Source} of file store. */
public class FileStoreSource
        implements Source<RowData, FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final long serialVersionUID = 1L;

    private final FileStore fileStore;

    private final boolean valueCountMode;

    private final boolean isContinuous;

    private final long discoveryInterval;

    private final boolean latestContinuous;

    @Nullable private final int[][] projectedFields;

    @Nullable private final Predicate partitionPredicate;

    @Nullable private final Predicate fieldPredicate;

    /**
     * The partitioned manifest meta collected at planning phase when manual compaction is
     * triggered.
     */
    @Nullable private final PartitionedManifestMeta specifiedPartManifests;

    public FileStoreSource(
            FileStore fileStore,
            boolean valueCountMode,
            boolean isContinuous,
            long discoveryInterval,
            boolean latestContinuous,
            @Nullable int[][] projectedFields,
            @Nullable Predicate partitionPredicate,
            @Nullable Predicate fieldPredicate,
            @Nullable PartitionedManifestMeta specifiedPartManifests) {
        this.fileStore = fileStore;
        this.valueCountMode = valueCountMode;
        this.isContinuous = isContinuous;
        this.discoveryInterval = discoveryInterval;
        this.latestContinuous = latestContinuous;
        this.projectedFields = projectedFields;
        this.partitionPredicate = partitionPredicate;
        this.fieldPredicate = fieldPredicate;
        this.specifiedPartManifests = specifiedPartManifests;
    }

    @Override
    public Boundedness getBoundedness() {
        return isContinuous ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, FileStoreSourceSplit> createReader(SourceReaderContext context) {
        FileStoreRead read = fileStore.newRead();

        if (isContinuous) {
            read.withDropDelete(false);
        }

        int[][] valueCountModeProjects = null;
        if (projectedFields != null) {
            if (valueCountMode) {
                // push projection to file store for better performance under continuous read mode,
                // because the merge cannot be performed anyway
                if (isContinuous) {
                    read.withKeyProjection(projectedFields);
                } else {
                    valueCountModeProjects = projectedFields;
                }
            } else {
                read.withValueProjection(projectedFields);
            }
        }

        return new FileStoreSourceReader(context, read, valueCountMode, valueCountModeProjects);
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
        FileStoreScan scan = fileStore.newScan();

        if (specifiedPartManifests != null) {
            return new StaticFileStoreSplitEnumerator(
                    context,
                    scan.snapshot(specifiedPartManifests.getSnapshotId()),
                    new FileStoreSourceSplitGenerator()
                            .createSplits(specifiedPartManifests.getManifestEntries()));
        }

        if (partitionPredicate != null) {
            scan.withPartitionFilter(partitionPredicate);
        }
        if (fieldPredicate != null) {
            if (valueCountMode) {
                scan.withKeyFilter(fieldPredicate);
            } else {
                scan.withValueFilter(fieldPredicate);
            }
        }

        Long snapshotId;
        Collection<FileStoreSourceSplit> splits;
        if (checkpoint == null) {
            // first, create new enumerator, plan splits
            if (latestContinuous) {
                checkArgument(
                        isContinuous,
                        "The latest continuous can only be true when isContinuous is true.");
                snapshotId = scan.latestSnapshot();
                splits = new ArrayList<>();
            } else {
                FileStoreScan.Plan plan = scan.plan();
                snapshotId = plan.snapshotId();
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
                    scan.withIncremental(true), // the subsequent planning is all incremental
                    splits,
                    currentSnapshot,
                    discoveryInterval);
        } else {
            Snapshot snapshot = snapshotId == null ? null : scan.snapshot(snapshotId);
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

    @VisibleForTesting
    @Nullable
    PartitionedManifestMeta getSpecifiedPartManifests() {
        return specifiedPartManifests;
    }
}
