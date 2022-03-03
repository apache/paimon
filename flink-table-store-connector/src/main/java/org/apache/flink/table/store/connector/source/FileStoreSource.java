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
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import static org.apache.flink.table.store.utils.ProjectionUtils.project;
import static org.apache.flink.util.Preconditions.checkArgument;

/** {@link Source} of file store. */
public class FileStoreSource
        implements Source<RowData, FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final long serialVersionUID = 1L;

    private final FileStore fileStore;

    private final RowType rowType;

    private final int[] partitions;

    private final int[] primaryKeys;

    @Nullable private final int[][] projectedFields;

    @Nullable private final Predicate partitionPredicate;

    @Nullable private final Predicate fieldsPredicate;

    public FileStoreSource(
            FileStore fileStore,
            RowType rowType,
            int[] partitions,
            int[] primaryKeys,
            @Nullable int[][] projectedFields,
            @Nullable Predicate partitionPredicate,
            @Nullable Predicate fieldsPredicate) {
        this.fileStore = fileStore;
        this.rowType = rowType;
        this.partitions = partitions;
        this.primaryKeys = primaryKeys;
        this.projectedFields = projectedFields;
        this.partitionPredicate = partitionPredicate;
        this.fieldsPredicate = fieldsPredicate;
    }

    @Override
    public Boundedness getBoundedness() {
        // TODO supports streaming reading for file store
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, FileStoreSourceSplit> createReader(SourceReaderContext context) {
        FileStoreRead read = fileStore.newRead();
        if (projectedFields != null) {
            if (primaryKeys.length == 0) {
                read.withKeyProjection(projectedFields);
            } else {
                read.withValueProjection(projectedFields);
            }
        }
        return new FileStoreSourceReader(context, read, primaryKeys.length == 0);
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> createEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context) {
        FileStoreScan scan = fileStore.newScan();
        if (partitionPredicate != null) {
            scan.withPartitionFilter(partitionPredicate);
        }
        if (fieldsPredicate != null) {
            if (primaryKeys.length == 0) {
                scan.withKeyFilter(fieldsPredicate);
            } else {
                scan.withValueFilter(fieldsPredicate);
            }
        }
        return new StaticFileStoreSplitEnumerator(context, scan);
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        checkArgument(checkpoint.nextSnapshotId() == -1);
        return new StaticFileStoreSplitEnumerator(context, checkpoint.splits());
    }

    @Override
    public FileStoreSourceSplitSerializer getSplitSerializer() {
        return new FileStoreSourceSplitSerializer(
                project(rowType, partitions), project(rowType, primaryKeys), rowType);
    }

    @Override
    public PendingSplitsCheckpointSerializer getEnumeratorCheckpointSerializer() {
        return new PendingSplitsCheckpointSerializer(getSplitSerializer());
    }
}
