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

package org.apache.paimon.deletionvectors.append;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionFileKey;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.source.DeletionFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;

/** A {@link BaseAppendDeleteFileMaintainer} of unaware bucket append table. */
public class AppendDeleteFileMaintainer implements BaseAppendDeleteFileMaintainer {

    private final DeletionVectorsIndexFile dvIndexFile;

    private final BinaryRow partition;
    private final Map<DeletionFileKey, DeletionFile> dataFileToDeletionFile;
    private final Map<String, IndexManifestEntry> indexNameToEntry;
    private final Map<String, Map<DeletionFileKey, DeletionFile>> indexFileToDeletionFiles;
    private final Map<DeletionFileKey, String> dataFileToIndexFile;
    private final Set<String> touchedIndexFiles;
    private final Map<DeletionFileKey, DeletionVector> deletionVectors;

    AppendDeleteFileMaintainer(
            DeletionVectorsIndexFile dvIndexFile,
            BinaryRow partition,
            List<IndexManifestEntry> manifestEntries,
            Map<DeletionFileKey, DeletionFile> deletionFiles) {
        this.dvIndexFile = dvIndexFile;
        this.partition = partition;
        this.dataFileToDeletionFile = new HashMap<>(deletionFiles);
        this.deletionVectors = new HashMap<>();

        this.indexNameToEntry = new HashMap<>();
        for (IndexManifestEntry entry : manifestEntries) {
            indexNameToEntry.put(entry.indexFile().fileName(), entry);
        }

        this.indexFileToDeletionFiles = new HashMap<>();
        this.dataFileToIndexFile = new HashMap<>();
        for (DeletionFileKey dataFile : dataFileToDeletionFile.keySet()) {
            DeletionFile deletionFile = dataFileToDeletionFile.get(dataFile);
            String indexFileName = new Path(deletionFile.path()).getName();
            indexFileToDeletionFiles
                    .computeIfAbsent(indexFileName, k -> new HashMap<>())
                    .put(dataFile, deletionFile);
            dataFileToIndexFile.put(dataFile, indexFileName);
        }
        this.touchedIndexFiles = new HashSet<>();
    }

    @Override
    public BinaryRow getPartition() {
        return this.partition;
    }

    @Override
    public int getBucket() {
        return UNAWARE_BUCKET;
    }

    public DeletionFile getDeletionFile(String dataFile) {
        return getDeletionFile(DeletionFileKey.ofFileName(dataFile));
    }

    public DeletionFile getDeletionFile(DeletionFileKey key) {
        return this.dataFileToDeletionFile.get(key);
    }

    public void putDeletionFile(String dataFile, DeletionFile deletionFile) {
        this.dataFileToDeletionFile.put(DeletionFileKey.ofFileName(dataFile), deletionFile);
    }

    public DeletionVector getDeletionVector(String dataFile) {
        return getDeletionVector(DeletionFileKey.ofFileName(dataFile));
    }

    public DeletionVector getDeletionVector(DeletionFileKey key) {
        DeletionFile deletionFile = getDeletionFile(key);
        if (deletionFile != null) {
            return dvIndexFile.readDeletionVector(deletionFile);
        }
        return null;
    }

    public DeletionFile notifyRemovedDeletionVector(String dataFile) {
        return notifyRemovedDeletionVector(DeletionFileKey.ofFileName(dataFile));
    }

    public DeletionFile notifyRemovedDeletionVector(DeletionFileKey key) {
        if (dataFileToIndexFile.containsKey(key)) {
            String indexFileName = dataFileToIndexFile.get(key);
            touchedIndexFiles.add(indexFileName);
            if (indexFileToDeletionFiles.containsKey(indexFileName)) {
                return indexFileToDeletionFiles.get(indexFileName).remove(key);
            }
        }
        return null;
    }

    @Override
    public void notifyNewDeletionVector(DeletionFileKey key, DeletionVector deletionVector) {
        DeletionFile previous = notifyRemovedDeletionVector(key);
        if (previous != null) {
            deletionVector.merge(dvIndexFile.readDeletionVector(previous));
        }
        deletionVectors.put(key, deletionVector);
    }

    @Override
    public List<IndexManifestEntry> persist() {
        List<IndexManifestEntry> result = writeUnchangedDeletionVector();
        dvIndexFile.writeWithRolling(deletionVectors).stream()
                .map(this::toAddEntry)
                .forEach(result::add);
        return result;
    }

    private IndexManifestEntry toAddEntry(IndexFileMeta file) {
        return new IndexManifestEntry(FileKind.ADD, partition, UNAWARE_BUCKET, file);
    }

    public String getIndexFilePath(String dataFile) {
        DeletionFile deletionFile = getDeletionFile(dataFile);
        return deletionFile == null ? null : deletionFile.path();
    }

    @VisibleForTesting
    List<IndexManifestEntry> writeUnchangedDeletionVector() {
        List<IndexManifestEntry> newIndexEntries = new ArrayList<>();
        for (String indexFile : indexFileToDeletionFiles.keySet()) {
            if (touchedIndexFiles.contains(indexFile)) {
                IndexManifestEntry oldEntry = indexNameToEntry.get(indexFile);

                // write unchanged deletion vector.
                Map<DeletionFileKey, DeletionFile> dataFileToDeletionFiles =
                        indexFileToDeletionFiles.get(indexFile);
                if (!dataFileToDeletionFiles.isEmpty()) {
                    List<IndexFileMeta> newIndexFiles =
                            dvIndexFile.writeWithRolling(
                                    dvIndexFile.readDeletionVector(dataFileToDeletionFiles));
                    newIndexFiles.forEach(
                            newIndexFile -> newIndexEntries.add(toAddEntry(newIndexFile)));
                }

                // mark the touched index file as removed.
                newIndexEntries.add(oldEntry.toDeleteEntry());
            }
        }
        return newIndexEntries;
    }
}
