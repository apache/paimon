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
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.Predicate;

import javax.annotation.Nullable;

import java.util.Collection;

import static org.apache.flink.table.store.connector.source.PendingSplitsCheckpoint.INVALID_SNAPSHOT;

/** {@link Source} of file store. */
public class FileStoreSource
        implements Source<RowData, FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final long serialVersionUID = 1L;

    private final FileStore fileStore;

    private final boolean valueCountMode;

    private final boolean isContinuous;

    private final long discoveryInterval;

    @Nullable private final int[][] projectedFields;

    @Nullable private final Predicate partitionPredicate;

    @Nullable private final Predicate fieldPredicate;

    public FileStoreSource(
            FileStore fileStore,
            boolean valueCountMode,
            boolean isContinuous,
            long discoveryInterval,
            @Nullable int[][] projectedFields,
            @Nullable Predicate partitionPredicate,
            final Predicate fieldPredicate) {
        this.fileStore = fileStore;
        this.valueCountMode = valueCountMode;
        this.isContinuous = isContinuous;
        this.discoveryInterval = discoveryInterval;
        this.projectedFields = projectedFields;
        this.partitionPredicate = partitionPredicate;
        this.fieldPredicate = fieldPredicate;
    }

    @Override
    public Boundedness getBoundedness() {
        // TODO supports streaming reading for file store
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, FileStoreSourceSplit> createReader(SourceReaderContext context) {
        FileStoreRead read = fileStore.newRead();

        if (isContinuous) {
            read.withDropDelete(false);
        }

        if (projectedFields != null) {
            if (valueCountMode) {
                // TODO when isContinuous is false, don't project keys, and add key projection to
                // split reader
                read.withKeyProjection(projectedFields);
            } else {
                read.withValueProjection(projectedFields);
            }
        }

        return new FileStoreSourceReader(context, read, valueCountMode);
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
            FileStoreScan.Plan plan = scan.plan();
            snapshotId = plan.snapshotId();
            splits = new FileStoreSourceSplitGenerator().createSplits(plan);
        } else {
            snapshotId = checkpoint.currentSnapshotId();
            if (snapshotId == INVALID_SNAPSHOT) {
                snapshotId = null;
            }
            splits = checkpoint.splits();
        }

        if (isContinuous) {
            long currentSnapshot = snapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : snapshotId;
            return new ContinuousFileSplitEnumerator(
                    context,
                    scan.withIncremental(true),
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
        return new FileStoreSourceSplitSerializer(
                fileStore.partitionType(), fileStore.keyType(), fileStore.valueType());
    }

    @Override
    public PendingSplitsCheckpointSerializer getEnumeratorCheckpointSerializer() {
        return new PendingSplitsCheckpointSerializer(getSplitSerializer());
    }
}
