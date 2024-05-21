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
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.utils.IntIterator;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SnapshotManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;

/** Handle index files. */
public class IndexFileHandler {

    private final SnapshotManager snapshotManager;
    private final PathFactory pathFactory;
    private final IndexManifestFile indexManifestFile;
    private final HashIndexFile hashIndex;
    private final DeletionVectorsIndexFile deletionVectorsIndex;

    public IndexFileHandler(
            SnapshotManager snapshotManager,
            PathFactory pathFactory,
            IndexManifestFile indexManifestFile,
            HashIndexFile hashIndex,
            DeletionVectorsIndexFile deletionVectorsIndex) {
        this.snapshotManager = snapshotManager;
        this.pathFactory = pathFactory;
        this.indexManifestFile = indexManifestFile;
        this.hashIndex = hashIndex;
        this.deletionVectorsIndex = deletionVectorsIndex;
    }

    public DeletionVectorsIndexFile deletionVectorsIndex() {
        return this.deletionVectorsIndex;
    }

    public List<IndexManifestEntry> scan() {
        Snapshot snapshot = snapshotManager.latestSnapshot();
        String indexManifest = snapshot.indexManifest();
        if (indexManifest == null) {
            return Collections.emptyList();
        }
        return indexManifestFile.read(indexManifest);
    }

    public Optional<IndexFileMeta> scanHashIndex(long snapshotId, BinaryRow partition, int bucket) {
        List<IndexFileMeta> result = scan(snapshotId, HASH_INDEX, partition, bucket);
        if (result.size() > 1) {
            throw new IllegalArgumentException(
                    "Find multiple hash index files for one bucket: " + result);
        }
        return result.isEmpty() ? Optional.empty() : Optional.of(result.get(0));
    }

    public List<IndexFileMeta> scan(
            long snapshotId, String indexType, BinaryRow partition, int bucket) {
        List<IndexManifestEntry> entries = scan(snapshotId, indexType, partition);
        List<IndexFileMeta> result = new ArrayList<>();
        for (IndexManifestEntry file : entries) {
            if (file.bucket() == bucket) {
                result.add(file.indexFile());
            }
        }
        return result;
    }

    public List<IndexManifestEntry> scan(String indexType, BinaryRow partition) {
        Long snapshot = snapshotManager.latestSnapshotId();
        if (snapshot == null) {
            return Collections.emptyList();
        }

        return scan(snapshot, indexType, partition);
    }

    public List<IndexManifestEntry> scan(long snapshotId, String indexType, BinaryRow partition) {
        Snapshot snapshot = snapshotManager.snapshot(snapshotId);
        String indexManifest = snapshot.indexManifest();
        if (indexManifest == null) {
            return Collections.emptyList();
        }

        List<IndexManifestEntry> allFiles = indexManifestFile.read(indexManifest);
        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry file : allFiles) {
            if (file.indexFile().indexType().equals(indexType)
                    && file.partition().equals(partition)) {
                result.add(file);
            }
        }

        return result;
    }

    public Path filePath(IndexFileMeta file) {
        return pathFactory.toPath(file.fileName());
    }

    public List<Integer> readHashIndexList(IndexFileMeta file) {
        return IntIterator.toIntList(readHashIndex(file));
    }

    public IntIterator readHashIndex(IndexFileMeta file) {
        if (!file.indexType().equals(HASH_INDEX)) {
            throw new IllegalArgumentException("Input file is not hash index: " + file.indexType());
        }

        try {
            return hashIndex.read(file.fileName());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public IndexFileMeta writeHashIndex(int[] ints) {
        return writeHashIndex(ints.length, IntIterator.create(ints));
    }

    public IndexFileMeta writeHashIndex(int size, IntIterator iterator) {
        String file;
        try {
            file = hashIndex.write(iterator);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new IndexFileMeta(HASH_INDEX, file, hashIndex.fileSize(file), size);
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

    public boolean existsIndexFile(IndexManifestEntry file) {
        return hashIndex.exists(file.indexFile().fileName());
    }

    public void deleteIndexFile(IndexManifestEntry file) {
        hashIndex.delete(file.indexFile().fileName());
    }

    public void deleteManifest(String indexManifest) {
        indexManifestFile.delete(indexManifest);
    }

    public Map<String, DeletionVector> readAllDeletionVectors(List<IndexFileMeta> fileMetas) {
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        for (IndexFileMeta indexFile : fileMetas) {
            if (!indexFile.indexType().equals(DELETION_VECTORS_INDEX)) {
                throw new IllegalArgumentException(
                        "Input file is not deletion vectors index " + indexFile.indexType());
            }
            deletionVectors.putAll(deletionVectorsIndex.readAllDeletionVectors(indexFile));
        }
        return deletionVectors;
    }

    public IndexFileMeta writeDeletionVectorsIndex(Map<String, DeletionVector> deletionVectors) {
        Pair<String, LinkedHashMap<String, Pair<Integer, Integer>>> pair =
                deletionVectorsIndex.write(deletionVectors);
        return new IndexFileMeta(
                DELETION_VECTORS_INDEX,
                pair.getLeft(),
                deletionVectorsIndex.fileSize(pair.getLeft()),
                deletionVectors.size(),
                pair.getRight());
    }
}
