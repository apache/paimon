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
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.utils.IntIterator;
import org.apache.paimon.utils.SnapshotManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;

/** Handle index files. */
public class IndexFileHandler {

    private final SnapshotManager snapshotManager;
    private final IndexManifestFile indexManifestFile;
    private final HashIndexFile hashIndex;

    public IndexFileHandler(
            SnapshotManager snapshotManager,
            IndexManifestFile indexManifestFile,
            HashIndexFile hashIndex) {
        this.snapshotManager = snapshotManager;
        this.indexManifestFile = indexManifestFile;
        this.hashIndex = hashIndex;
    }

    public Optional<IndexFileMeta> scan(
            long snapshotId, String indexType, BinaryRow partition, int bucket) {
        List<IndexManifestEntry> entries = scan(snapshotId, indexType, partition);
        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry file : entries) {
            if (file.bucket() == bucket) {
                result.add(file);
            }
        }
        if (result.size() > 1) {
            throw new IllegalArgumentException(
                    "Find multiple index files for one bucket: " + result);
        }
        return result.isEmpty() ? Optional.empty() : Optional.of(result.get(0).indexFile());
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
            throw new IllegalArgumentException("Index manifest is null in snapshot: " + snapshot);
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
}
