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
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.IndexFilePathFactories;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    public IndexFileHandler(
            FileIO fileIO,
            SnapshotManager snapshotManager,
            IndexManifestFile indexManifestFile,
            IndexFilePathFactories pathFactories,
            MemorySize dvTargetFileSize,
            boolean dvBitmap64) {
        this.fileIO = fileIO;
        this.snapshotManager = snapshotManager;
        this.pathFactories = pathFactories;
        this.indexManifestFile = indexManifestFile;
        this.dvTargetFileSize = dvTargetFileSize;
        this.dvBitmap64 = dvBitmap64;
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

    public List<IndexManifestEntry> scan(
            Snapshot snapshot, Filter<IndexManifestEntry> readTFilter) {
        if (snapshot == null) {
            return Collections.emptyList();
        }
        String indexManifest = snapshot.indexManifest();
        if (indexManifest == null) {
            return Collections.emptyList();
        }

        return indexManifestFile.read(indexManifest, null, Filter.alwaysTrue(), readTFilter);
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

    public Path indexManifestFilePath(String indexManifest) {
        return indexManifestFile.indexManifestFilePath(indexManifest);
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
