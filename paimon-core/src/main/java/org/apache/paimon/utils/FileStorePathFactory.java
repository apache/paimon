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
import org.apache.paimon.fs.ExternalPathProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFilePathFactory;
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

    public static final String MANIFEST_PATH = "manifest";
    public static final String MANIFEST_PREFIX = "manifest-";
    public static final String MANIFEST_LIST_PREFIX = "manifest-list-";
    public static final String INDEX_MANIFEST_PREFIX = "index-manifest-";

    public static final String INDEX_PATH = "index";
    public static final String INDEX_PREFIX = "index-";

    public static final String STATISTICS_PATH = "statistics";
    public static final String STATISTICS_PREFIX = "stat-";

    public static final String BUCKET_PATH_PREFIX = "bucket-";

    // this is the table schema root path
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
    private final List<Path> externalPaths;

    public FileStorePathFactory(
            Path root,
            RowType partitionType,
            String defaultPartValue,
            String formatIdentifier,
            String dataFilePrefix,
            String changelogFilePrefix,
            boolean legacyPartitionName,
            boolean fileSuffixIncludeCompression,
            String fileCompression,
            @Nullable String dataFilePathDirectory,
            List<Path> externalPaths) {
        this.root = root;
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
        this.externalPaths = externalPaths;
    }

    public Path root() {
        return root;
    }

    public Path manifestPath() {
        return new Path(root, MANIFEST_PATH);
    }

    public Path indexPath() {
        return new Path(root, INDEX_PATH);
    }

    public Path statisticsPath() {
        return new Path(root, STATISTICS_PATH);
    }

    public Path dataFilePath() {
        if (dataFilePathDirectory != null) {
            return new Path(root, dataFilePathDirectory);
        }
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
        return toManifestFilePath(
                MANIFEST_PREFIX + uuid + "-" + manifestFileCount.getAndIncrement());
    }

    public Path newManifestList() {
        return toManifestListPath(
                MANIFEST_LIST_PREFIX + uuid + "-" + manifestListCount.getAndIncrement());
    }

    public Path toManifestFilePath(String manifestFileName) {
        return new Path(manifestPath(), manifestFileName);
    }

    public Path toManifestListPath(String manifestListName) {
        return new Path(manifestPath(), manifestListName);
    }

    public DataFilePathFactory createDataFilePathFactory(BinaryRow partition, int bucket) {
        return new DataFilePathFactory(
                bucketPath(partition, bucket),
                formatIdentifier,
                dataFilePrefix,
                changelogFilePrefix,
                fileSuffixIncludeCompression,
                fileCompression,
                createExternalPathProvider(partition, bucket));
    }

    @Nullable
    private ExternalPathProvider createExternalPathProvider(BinaryRow partition, int bucket) {
        if (externalPaths == null || externalPaths.isEmpty()) {
            return null;
        }

        return new ExternalPathProvider(externalPaths, relativeBucketPath(partition, bucket));
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

    // @TODO, need to be changed
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
                return toPath(
                        INDEX_MANIFEST_PREFIX + uuid + "-" + indexManifestCount.getAndIncrement());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(manifestPath(), fileName);
            }
        };
    }

    public PathFactory indexFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return toPath(INDEX_PREFIX + uuid + "-" + indexFileCount.getAndIncrement());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(indexPath(), fileName);
            }
        };
    }

    public PathFactory statsFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return toPath(STATISTICS_PREFIX + uuid + "-" + statsFileCount.getAndIncrement());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(statisticsPath(), fileName);
            }
        };
    }
}
