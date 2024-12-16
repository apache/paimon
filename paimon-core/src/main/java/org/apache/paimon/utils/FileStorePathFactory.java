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

package org.apache.paimon.utils;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.TablePathProvider;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Factory which produces {@link Path}s for manifest files. */
@ThreadSafe
public class FileStorePathFactory {

    public static final String BUCKET_PATH_PREFIX = "bucket-";

    private final Path root;
    private final String uuid;
    private final InternalRowPartitionComputer partitionComputer;
    private final String formatIdentifier;
    private final String dataFilePrefix;
    private final String changelogFilePrefix;
    private final boolean fileSuffixIncludeCompression;
    private final String fileCompression;

    @Nullable private final String dataFilePathDirectory;

    private final AtomicInteger manifestFileCount;
    private final AtomicInteger manifestListCount;
    private final AtomicInteger indexManifestCount;
    private final AtomicInteger indexFileCount;
    private final AtomicInteger statsFileCount;
    private final TablePathProvider tablePathProvider;

    public FileStorePathFactory(
            TablePathProvider tablePathProvider,
            RowType partitionType,
            String defaultPartValue,
            String formatIdentifier,
            String dataFilePrefix,
            String changelogFilePrefix,
            boolean legacyPartitionName,
            boolean fileSuffixIncludeCompression,
            String fileCompression,
            @Nullable String dataFilePathDirectory) {
        this.dataFilePathDirectory = dataFilePathDirectory;
        this.uuid = UUID.randomUUID().toString();

        this.partitionComputer =
                getPartitionComputer(partitionType, defaultPartValue, legacyPartitionName);
        this.formatIdentifier = formatIdentifier;
        this.dataFilePrefix = dataFilePrefix;
        this.changelogFilePrefix = changelogFilePrefix;
        this.fileSuffixIncludeCompression = fileSuffixIncludeCompression;
        this.fileCompression = fileCompression;

        this.manifestFileCount = new AtomicInteger(0);
        this.manifestListCount = new AtomicInteger(0);
        this.indexManifestCount = new AtomicInteger(0);
        this.indexFileCount = new AtomicInteger(0);
        this.statsFileCount = new AtomicInteger(0);
        this.tablePathProvider = tablePathProvider;
        this.root = tablePathProvider.getTableWritePath();
    }

    public Path root() {
        return root;
    }

    @VisibleForTesting
    public static InternalRowPartitionComputer getPartitionComputer(
            RowType partitionType, String defaultPartValue, boolean legacyPartitionName) {
        String[] partitionColumns = partitionType.getFieldNames().toArray(new String[0]);
        return new InternalRowPartitionComputer(
                defaultPartValue, partitionType, partitionColumns, legacyPartitionName);
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

    public DataFilePathFactory createDataFilePathFactory(BinaryRow partition, int bucket) {
        return new DataFilePathFactory(
                tablePathProvider.getDataFileExternalPath(),
                relativeDataFilePath(partition, bucket),
                formatIdentifier,
                dataFilePrefix,
                changelogFilePrefix,
                fileSuffixIncludeCompression,
                fileCompression);
    }

    public Path relativeDataFilePath(BinaryRow partition, int bucket) {
        Path relativeDataFile = tablePathProvider.getReleativeTableWritePath();
        if (dataFilePathDirectory != null) {
            relativeDataFile = new Path(relativeDataFile, dataFilePathDirectory);
        }
        return new Path(relativeDataFile + "/" + relativePartitionAndBucketPath(partition, bucket));
    }

    public Path bucketPath(BinaryRow partition, int bucket) {
        return new Path(root, relativeBucketPath(partition, bucket));
    }

    public Path relativeBucketPath(BinaryRow partition, int bucket) {
        Path relativeBucketPath = new Path(BUCKET_PATH_PREFIX + bucket);
        String partitionPath = getPartitionString(partition);
        if (!partitionPath.isEmpty()) {
            relativeBucketPath = new Path(partitionPath, relativeBucketPath);
        }
        if (dataFilePathDirectory != null) {
            relativeBucketPath = new Path(dataFilePathDirectory, relativeBucketPath);
        }
        return relativeBucketPath;
    }

    /** IMPORTANT: This method is NOT THREAD SAFE. */
    public String getPartitionString(BinaryRow partition) {
        return PartitionPathUtils.generatePartitionPath(
                partitionComputer.generatePartValues(
                        Preconditions.checkNotNull(
                                partition, "Partition row data is null. This is unexpected.")));
    }

    public List<Path> getHierarchicalPartitionPath(BinaryRow partition) {
        return PartitionPathUtils.generateHierarchicalPartitionPaths(
                        partitionComputer.generatePartValues(
                                Preconditions.checkNotNull(
                                        partition,
                                        "Partition binary row is null. This is unexpected.")))
                .stream()
                .map(p -> new Path(root + "/" + p))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    public String uuid() {
        return uuid;
    }

    public PathFactory manifestFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestFile();
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestFilePath(fileName);
            }
        };
    }

    public PathFactory manifestListFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestList();
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestListPath(fileName);
            }
        };
    }

    public PathFactory indexManifestFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return new Path(
                        root
                                + "/manifest/index-manifest-"
                                + uuid
                                + "-"
                                + indexManifestCount.getAndIncrement());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(root + "/manifest/" + fileName);
            }
        };
    }

    public PathFactory indexFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return new Path(
                        root + "/index/index-" + uuid + "-" + indexFileCount.getAndIncrement());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(root + "/index/" + fileName);
            }
        };
    }

    public PathFactory statsFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return new Path(
                        root
                                + "/statistics/stats-"
                                + uuid
                                + "-"
                                + statsFileCount.getAndIncrement());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(root + "/statistics/" + fileName);
            }
        };
    }
}
