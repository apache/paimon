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

package org.apache.paimon.index;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.IndexFilePathFactories;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;

/** Handle index files. */
public class IndexFileHandler {

    private final FileIO fileIO;
    private final SnapshotManager snapshotManager;
    private final IndexManifestFile indexManifestFile;
    private final IndexFilePathFactories pathFactories;
    private final MemorySize dvTargetFileSize;
    private final boolean dvBitmap64;
    @Nullable private CacheMetrics cacheMetrics;
    private final boolean enableDVMetaCache;

    public IndexFileHandler(
            FileIO fileIO,
            SnapshotManager snapshotManager,
            IndexManifestFile indexManifestFile,
            IndexFilePathFactories pathFactories,
            MemorySize dvTargetFileSize,
            boolean dvBitmap64,
            boolean enableDVMetaCache) {
        this.fileIO = fileIO;
        this.snapshotManager = snapshotManager;
        this.pathFactories = pathFactories;
        this.indexManifestFile = indexManifestFile;
        this.dvTargetFileSize = dvTargetFileSize;
        this.dvBitmap64 = dvBitmap64;
        this.enableDVMetaCache = enableDVMetaCache;
    }

    public boolean isEnableDVMetaCache() {
        return enableDVMetaCache;
    }

    public HashIndexFile hashIndex(BinaryRow partition, int bucket) {
        return new HashIndexFile(fileIO, pathFactories.get(partition, bucket));
    }

    public DeletionVectorsIndexFile dvIndex(BinaryRow partition, int bucket) {
        return new DeletionVectorsIndexFile(
                fileIO, pathFactories.get(partition, bucket), dvTargetFileSize, dvBitmap64);
    }

    public Optional<IndexFileMeta> scanHashIndex(
            Snapshot snapshot, BinaryRow partition, int bucket) {
        List<IndexFileMeta> result = scan(snapshot, HASH_INDEX, partition, bucket);
        if (result.size() > 1) {
            throw new IllegalArgumentException(
                    "Find multiple hash index files for one bucket: " + result);
        }
        return result.isEmpty() ? Optional.empty() : Optional.of(result.get(0));
    }

    public void withCacheMetrics(@Nullable CacheMetrics cacheMetrics) {
        this.cacheMetrics = cacheMetrics;
    }

    @Nullable
    // Construct DataFile -> DeletionFile based on IndexFileMeta
    public Map<String, DeletionFile> extractDeletionFileByMeta(
            BinaryRow partition, Integer bucket, IndexFileMeta fileMeta) {
        if (fileMeta.dvRanges() != null && fileMeta.dvRanges().size() > 0) {
            Map<String, DeletionFile> result = new HashMap<>();
            for (DeletionVectorMeta dvMeta : fileMeta.dvRanges().values()) {
                result.put(
                        dvMeta.dataFileName(),
                        new DeletionFile(
                                dvIndex(partition, bucket).path(fileMeta).toString(),
                                dvMeta.offset(),
                                dvMeta.length(),
                                dvMeta.cardinality()));
            }
            return result;
        }
        return null;
    }

    // Scan DV index file of given partition buckets
    // returns <DataFile: DeletionFile> map grouped by partition and bucket
    public Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> scanDVIndex(
            Snapshot snapshot, Set<Pair<BinaryRow, Integer>> partitionBuckets) {
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> partitionFileMetas =
                scan(
                        snapshot,
                        DELETION_VECTORS_INDEX,
                        partitionBuckets.stream().map(Pair::getLeft).collect(Collectors.toSet()));
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> result = new HashMap<>();
        partitionBuckets.forEach(
                entry -> {
                    List<IndexFileMeta> fileMetas = partitionFileMetas.get(entry);
                    if (fileMetas != null) {
                        fileMetas.forEach(
                                meta -> {
                                    Map<String, DeletionFile> dvMetas =
                                            extractDeletionFileByMeta(
                                                    entry.getLeft(), entry.getRight(), meta);
                                    if (dvMetas != null) {
                                        result.computeIfAbsent(entry, k -> new HashMap<>())
                                                .putAll(dvMetas);
                                    }
                                });
                    }
                });
        return result;
    }

    // Scan DV Meta Cache first, if not exist, scan DV index file, returns the exact deletion file
    // of the specified partition/buckets
    public Map<String, DeletionFile> scanDVIndexWithCache(
            Snapshot snapshot, BinaryRow partition, Integer bucket) {
        if (snapshot == null || snapshot.indexManifest() == null) {
            return Collections.emptyMap();
        }
        // read from cache
        String indexManifestName = snapshot.indexManifest();
        Map<String, DeletionFile> result =
                indexManifestFile.readFromDVMetaCache(indexManifestName, partition, bucket);
        if (result != null) {
            if (cacheMetrics != null) {
                cacheMetrics.increaseHitObject();
            }
            return result;
        }
        if (cacheMetrics != null) {
            cacheMetrics.increaseMissedObject();
        }
        // If miss, read the whole partition's deletion files
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> partitionFileMetas =
                scan(
                        snapshot,
                        DELETION_VECTORS_INDEX,
                        new HashSet<>(Collections.singletonList(partition)));
        // for each bucket, extract deletion files, and fill meta cache
        for (Map.Entry<Pair<BinaryRow, Integer>, List<IndexFileMeta>> entry :
                partitionFileMetas.entrySet()) {
            Pair<BinaryRow, Integer> partitionBucket = entry.getKey();
            List<IndexFileMeta> fileMetas = entry.getValue();
            if (entry.getValue() != null) {
                Map<String, DeletionFile> bucketDeletionFiles = new HashMap<>();
                fileMetas.forEach(
                        meta -> {
                            Map<String, DeletionFile> bucketDVMetas =
                                    extractDeletionFileByMeta(
                                            partitionBucket.getLeft(),
                                            partitionBucket.getRight(),
                                            meta);
                            if (bucketDVMetas != null) {
                                bucketDeletionFiles.putAll(bucketDVMetas);
                            }
                        });
                // bucketDeletionFiles can be empty
                indexManifestFile.fillDVMetaCache(
                        indexManifestName,
                        partitionBucket.getLeft(),
                        partitionBucket.getRight(),
                        bucketDeletionFiles);
                if (partitionBucket.getRight() != null
                        && partitionBucket.getLeft() != null
                        && partitionBucket.getRight().equals(bucket)
                        && partitionBucket.getLeft().equals(partition)) {
                    result = bucketDeletionFiles;
                }
            }
        }
        if (result == null) {
            result = Collections.emptyMap();
        }
        return result;
    }

    public List<IndexManifestEntry> scan(String indexType) {
        return scan(snapshotManager.latestSnapshot(), indexType);
    }

    public List<IndexManifestEntry> scan(Snapshot snapshot, String indexType) {
        if (snapshot == null) {
            return Collections.emptyList();
        }
        String indexManifest = snapshot.indexManifest();
        if (indexManifest == null) {
            return Collections.emptyList();
        }

        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry file : indexManifestFile.read(indexManifest)) {
            if (file.indexFile().indexType().equals(indexType)) {
                result.add(file);
            }
        }
        return result;
    }

    public List<IndexFileMeta> scan(
            Snapshot snapshot, String indexType, BinaryRow partition, int bucket) {
        List<IndexFileMeta> result = new ArrayList<>();
        for (IndexManifestEntry file : scanEntries(snapshot, indexType, partition)) {
            if (file.bucket() == bucket) {
                result.add(file.indexFile());
            }
        }
        return result;
    }

    public Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> scan(
            long snapshot, String indexType, Set<BinaryRow> partitions) {
        return scan(snapshotManager.snapshot(snapshot), indexType, partitions);
    }

    public Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> scan(
            Snapshot snapshot, String indexType, Set<BinaryRow> partitions) {
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> result = new HashMap<>();
        for (IndexManifestEntry file : scanEntries(snapshot, indexType, partitions)) {
            result.computeIfAbsent(Pair.of(file.partition(), file.bucket()), k -> new ArrayList<>())
                    .add(file.indexFile());
        }
        return result;
    }

    public List<IndexManifestEntry> scanEntries() {
        Snapshot snapshot = snapshotManager.latestSnapshot();
        if (snapshot == null || snapshot.indexManifest() == null) {
            return Collections.emptyList();
        }

        return indexManifestFile.read(snapshot.indexManifest());
    }

    public List<IndexManifestEntry> scanEntries(String indexType, BinaryRow partition) {
        Snapshot snapshot = snapshotManager.latestSnapshot();
        if (snapshot == null) {
            return Collections.emptyList();
        }

        return scanEntries(snapshot, indexType, partition);
    }

    public List<IndexManifestEntry> scanEntries(
            Snapshot snapshot, String indexType, BinaryRow partition) {
        return scanEntries(snapshot, indexType, Collections.singleton(partition));
    }

    public List<IndexManifestEntry> scanEntries(
            Snapshot snapshot, String indexType, Set<BinaryRow> partitions) {
        List<IndexManifestEntry> manifestEntries = scan(snapshot, indexType);
        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry file : manifestEntries) {
            if (partitions.contains(file.partition())) {
                result.add(file);
            }
        }
        return result;
    }

    public Path filePath(IndexManifestEntry entry) {
        return pathFactories.get(entry.partition(), entry.bucket()).toPath(entry.indexFile());
    }

    public boolean existsManifest(String indexManifest) {
        return indexManifestFile.exists(indexManifest);
    }

    public List<IndexManifestEntry> readManifest(String indexManifest) {
        return indexManifestFile.read(indexManifest);
    }

    public List<IndexManifestEntry> readManifestWithIOException(String indexManifest)
            throws IOException {
        return indexManifestFile.readWithIOException(indexManifest);
    }

    private IndexFile indexFile(IndexManifestEntry entry) {
        IndexFileMeta file = entry.indexFile();
        switch (file.indexType()) {
            case HASH_INDEX:
                return hashIndex(entry.partition(), entry.bucket());
            case DELETION_VECTORS_INDEX:
                return dvIndex(entry.partition(), entry.bucket());
            default:
                throw new IllegalArgumentException("Unknown index type: " + file.indexType());
        }
    }

    public boolean existsIndexFile(IndexManifestEntry file) {
        return indexFile(file).exists(file.indexFile());
    }

    public void deleteIndexFile(IndexManifestEntry entry) {
        indexFile(entry).delete(entry.indexFile());
    }

    public void deleteManifest(String indexManifest) {
        indexManifestFile.delete(indexManifest);
    }

    public Map<String, DeletionVector> readAllDeletionVectors(
            BinaryRow partition, int bucket, List<IndexFileMeta> fileMetas) {
        return dvIndex(partition, bucket).readAllDeletionVectors(fileMetas);
    }

    public Map<String, DeletionVector> readAllDeletionVectors(IndexManifestEntry entry) {
        return dvIndex(entry.partition(), entry.bucket()).readAllDeletionVectors(entry.indexFile());
    }
}
