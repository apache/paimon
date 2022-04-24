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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.connector.file.table.RowDataPartitionComputer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.mergetree.sst.SstPathFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.table.store.file.utils.SnapshotFinder.SNAPSHOT_PREFIX;

/** Factory which produces {@link Path}s for each type of files. */
@ThreadSafe
public class FileStorePathFactory {

    private final Path root;
    private final String uuid;
    private final RowDataPartitionComputer partitionComputer;
    private final int maxRetry;

    private final AtomicInteger manifestFileCount;
    private final AtomicInteger manifestListCount;

    public FileStorePathFactory(Path root, int maxRetry) {
        this(
                root,
                RowType.of(),
                FileSystemConnectorOptions.PARTITION_DEFAULT_NAME.defaultValue(),
                maxRetry);
    }

    // for tables without partition, partitionType should be a row type with 0 columns (not null)
    public FileStorePathFactory(
            Path root, RowType partitionType, String defaultPartValue, int maxRetry) {
        this.root = root;
        this.uuid = UUID.randomUUID().toString();

        String[] partitionColumns = partitionType.getFieldNames().toArray(new String[0]);
        this.partitionComputer =
                new RowDataPartitionComputer(
                        defaultPartValue,
                        partitionColumns,
                        partitionType.getFields().stream()
                                .map(f -> LogicalTypeDataTypeConverter.toDataType(f.getType()))
                                .toArray(DataType[]::new),
                        partitionColumns);

        this.manifestFileCount = new AtomicInteger(0);
        this.manifestListCount = new AtomicInteger(0);
        this.maxRetry = maxRetry;
    }

    public Path newManifestFile() {
        return new Path(
                root + "/manifest/manifest-" + uuid + "-" + manifestFileCount.getAndIncrement());
    }

    public Path newManifestList() {
        return new Path(
                root
                        + "/manifest/manifest-list-"
                        + uuid
                        + "-"
                        + manifestListCount.getAndIncrement());
    }

    public Path toManifestFilePath(String manifestFileName) {
        return new Path(root + "/manifest/" + manifestFileName);
    }

    public Path toManifestListPath(String manifestListName) {
        return new Path(root + "/manifest/" + manifestListName);
    }

    public Path toSnapshotPath(long id) {
        return new Path(root + "/snapshot/" + SNAPSHOT_PREFIX + id);
    }

    public Path toTmpSnapshotPath(long id) {
        return new Path(root + "/snapshot/." + SNAPSHOT_PREFIX + id + "-" + UUID.randomUUID());
    }

    public SstPathFactory createSstPathFactory(BinaryRowData partition, int bucket) {
        return new SstPathFactory(root, getPartitionString(partition), bucket);
    }

    /** IMPORTANT: This method is NOT THREAD SAFE. */
    public String getPartitionString(BinaryRowData partition) {
        return PartitionPathUtils.generatePartitionPath(
                partitionComputer.generatePartValues(
                        Preconditions.checkNotNull(
                                partition, "Partition row data is null. This is unexpected.")));
    }

    public Path snapshotDirectory() {
        return new Path(root + "/snapshot");
    }

    @Nullable
    public Long latestSnapshotId() {
        try {
            return SnapshotFinder.findLatest(snapshotDirectory(), maxRetry);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
    }

    @VisibleForTesting
    public String uuid() {
        return uuid;
    }

    /** Cache for storing {@link SstPathFactory}s. */
    public static class SstPathFactoryCache {

        private final FileStorePathFactory pathFactory;
        private final Map<BinaryRowData, Map<Integer, SstPathFactory>> cache;

        public SstPathFactoryCache(FileStorePathFactory pathFactory) {
            this.pathFactory = pathFactory;
            this.cache = new HashMap<>();
        }

        public SstPathFactory getSstPathFactory(BinaryRowData partition, int bucket) {
            return cache.compute(partition, (p, m) -> m == null ? new HashMap<>() : m)
                    .compute(
                            bucket,
                            (b, f) ->
                                    f == null
                                            ? pathFactory.createSstPathFactory(partition, bucket)
                                            : f);
        }
    }
}
