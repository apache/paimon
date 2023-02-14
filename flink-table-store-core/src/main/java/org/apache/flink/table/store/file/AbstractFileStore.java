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

package org.apache.flink.table.store.file;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.annotation.VisibleForTesting;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.operation.FileStoreCommitImpl;
import org.apache.flink.table.store.file.operation.FileStoreExpireImpl;
import org.apache.flink.table.store.file.operation.PartitionExpire;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.types.RowType;

import java.time.Duration;
import java.util.Comparator;

/**
 * Base {@link FileStore} implementation.
 *
 * @param <T> type of record to read and write.
 */
public abstract class AbstractFileStore<T> implements FileStore<T> {

    protected final FileIO fileIO;
    protected final SchemaManager schemaManager;
    protected final long schemaId;
    protected final CoreOptions options;
    protected final RowType partitionType;

    public AbstractFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            long schemaId,
            CoreOptions options,
            RowType partitionType) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schemaId = schemaId;
        this.options = options;
        this.partitionType = partitionType;
    }

    public FileStorePathFactory pathFactory() {
        return new FileStorePathFactory(
                options.path(),
                partitionType,
                options.partitionDefaultName(),
                options.fileFormat().getFormatIdentifier());
    }

    @Override
    public SnapshotManager snapshotManager() {
        return new SnapshotManager(fileIO, options.path());
    }

    @VisibleForTesting
    public ManifestFile.Factory manifestFileFactory() {
        return new ManifestFile.Factory(
                fileIO,
                schemaManager,
                schemaId,
                partitionType,
                options.manifestFormat(),
                pathFactory(),
                options.manifestTargetSize().getBytes());
    }

    @VisibleForTesting
    public ManifestList.Factory manifestListFactory() {
        return new ManifestList.Factory(
                fileIO, partitionType, options.manifestFormat(), pathFactory());
    }

    @Override
    public RowType partitionType() {
        return partitionType;
    }

    @Override
    public CoreOptions options() {
        return options;
    }

    @Override
    public FileStoreCommitImpl newCommit(String commitUser) {
        return new FileStoreCommitImpl(
                fileIO,
                schemaId,
                commitUser,
                partitionType,
                pathFactory(),
                snapshotManager(),
                manifestFileFactory(),
                manifestListFactory(),
                newScan(),
                options.bucket(),
                options.manifestTargetSize(),
                options.manifestMergeMinCount(),
                newKeyComparator());
    }

    @Override
    public FileStoreExpireImpl newExpire() {
        return new FileStoreExpireImpl(
                fileIO,
                options.snapshotNumRetainMin(),
                options.snapshotNumRetainMax(),
                options.snapshotTimeRetain().toMillis(),
                pathFactory(),
                snapshotManager(),
                manifestFileFactory(),
                manifestListFactory());
    }

    public abstract Comparator<InternalRow> newKeyComparator();

    @Override
    public PartitionExpire newPartitionExpire(String commitUser) {
        Duration partitionExpireTime = options.partitionExpireTime();
        if (partitionExpireTime == null || partitionType().getFieldCount() == 0) {
            return null;
        }

        return new PartitionExpire(
                partitionType(),
                partitionExpireTime,
                options.partitionExpireCheckInterval(),
                options.partitionTimestampPattern(),
                options.partitionTimestampFormatter(),
                newScan(),
                newCommit(commitUser));
    }
}
