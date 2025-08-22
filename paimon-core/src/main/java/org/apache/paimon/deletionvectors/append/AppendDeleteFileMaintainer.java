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
    private final Map<String, DeletionFile> dataFileToDeletionFile;
    private final Map<String, IndexManifestEntry> indexNameToEntry;
    private final Map<String, Map<String, DeletionFile>> indexFileToDeletionFiles;
    private final Map<String, String> dataFileToIndexFile;
    private final Set<String> touchedIndexFiles;
    private final Map<String, DeletionVector> deletionVectors;

    AppendDeleteFileMaintainer(
            DeletionVectorsIndexFile dvIndexFile,
            BinaryRow partition,
            List<IndexManifestEntry> manifestEntries,
            Map<String, DeletionFile> deletionFiles) {
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
        for (String dataFile : deletionFiles.keySet()) {
            DeletionFile deletionFile = deletionFiles.get(dataFile);
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
        return this.dataFileToDeletionFile.get(dataFile);
    }

    public void putDeletionFile(String dataFile, DeletionFile deletionFile) {
        this.dataFileToDeletionFile.put(dataFile, deletionFile);
    }

    public DeletionVector getDeletionVector(String dataFile) {
        DeletionFile deletionFile = getDeletionFile(dataFile);
        if (deletionFile != null) {
            return dvIndexFile.readDeletionVector(deletionFile);
        }
        return null;
    }

    public DeletionFile notifyRemovedDeletionVector(String dataFile) {
        if (dataFileToIndexFile.containsKey(dataFile)) {
            String indexFileName = dataFileToIndexFile.get(dataFile);
            touchedIndexFiles.add(indexFileName);
            if (indexFileToDeletionFiles.containsKey(indexFileName)) {
                return indexFileToDeletionFiles.get(indexFileName).remove(dataFile);
            }
        }
        return null;
    }

    @Override
    public void notifyNewDeletionVector(String dataFile, DeletionVector deletionVector) {
        DeletionFile previous = notifyRemovedDeletionVector(dataFile);
        if (previous != null) {
            deletionVector.merge(dvIndexFile.readDeletionVector(previous));
        }
        deletionVectors.put(dataFile, deletionVector);
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
                Map<String, DeletionFile> dataFileToDeletionFiles =
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
