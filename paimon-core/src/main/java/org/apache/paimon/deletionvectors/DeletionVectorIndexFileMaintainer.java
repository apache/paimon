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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
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
import java.util.stream.Collectors;

/** DeletionVectorIndexFileMaintainer. */
public class DeletionVectorIndexFileMaintainer {

    private final IndexFileHandler indexFileHandler;

    private final BinaryRow partition;
    private final int bucket;
    private final Map<String, IndexManifestEntry> indexNameToEntry = new HashMap<>();

    private final Map<String, Map<String, DeletionFile>> indexFileToDeletionFiles = new HashMap<>();
    private final Map<String, String> dataFileToIndexFile = new HashMap<>();

    private final Set<String> touchedIndexFiles = new HashSet<>();

    private final DeletionVectorsMaintainer maintainer;

    // the key of dataFileToDeletionFiles is the relative path again table's location.
    public DeletionVectorIndexFileMaintainer(
            IndexFileHandler indexFileHandler,
            Long snapshotId,
            BinaryRow partition,
            int bucket,
            boolean restore) {
        this.indexFileHandler = indexFileHandler;
        this.partition = partition;
        this.bucket = bucket;
        if (restore) {
            this.maintainer =
                    new DeletionVectorsMaintainer.Factory(indexFileHandler)
                            .createOrRestore(snapshotId, partition, bucket);
        } else {
            this.maintainer = new DeletionVectorsMaintainer.Factory(indexFileHandler).create();
        }
        Map<String, DeletionFile> dataFileToDeletionFiles =
                indexFileHandler.scanDVIndex(snapshotId, partition, bucket);
        init(dataFileToDeletionFiles);
    }

    @VisibleForTesting
    public void init(Map<String, DeletionFile> dataFileToDeletionFiles) {
        List<String> touchedIndexFileNames =
                dataFileToDeletionFiles.values().stream()
                        .map(deletionFile -> new Path(deletionFile.path()).getName())
                        .distinct()
                        .collect(Collectors.toList());
        indexFileHandler.scanEntries().stream()
                .filter(
                        indexManifestEntry ->
                                touchedIndexFileNames.contains(
                                        indexManifestEntry.indexFile().fileName()))
                .forEach(entry -> indexNameToEntry.put(entry.indexFile().fileName(), entry));

        for (String dataFile : dataFileToDeletionFiles.keySet()) {
            DeletionFile deletionFile = dataFileToDeletionFiles.get(dataFile);
            String indexFileName = new Path(deletionFile.path()).getName();
            if (!indexFileToDeletionFiles.containsKey(indexFileName)) {
                indexFileToDeletionFiles.put(indexFileName, new HashMap<>());
            }
            indexFileToDeletionFiles.get(indexFileName).put(dataFile, deletionFile);
            dataFileToIndexFile.put(dataFile, indexFileName);
        }
    }

    public BinaryRow getPartition() {
        return this.partition;
    }

    public int getBucket() {
        return this.bucket;
    }

    public void notifyDeletionFiles(String dataFile, DeletionVector deletionVector) {
        DeletionVectorsIndexFile deletionVectorsIndexFile = indexFileHandler.deletionVectorsIndex();
        DeletionFile previous = null;
        if (dataFileToIndexFile.containsKey(dataFile)) {
            String indexFileName = dataFileToIndexFile.get(dataFile);
            touchedIndexFiles.add(indexFileName);
            if (indexFileToDeletionFiles.containsKey(indexFileName)) {
                previous = indexFileToDeletionFiles.get(indexFileName).remove(dataFile);
            }
        }
        if (previous != null) {
            deletionVector.merge(deletionVectorsIndexFile.readDeletionVector(dataFile, previous));
        }
        maintainer.notifyNewDeletion(dataFile, deletionVector);
    }

    public void notifyDeletionFiles(Map<String, DeletionFile> dataFileToDeletionFiles) {
        for (String dataFile : dataFileToDeletionFiles.keySet()) {
            DeletionFile deletionFile = dataFileToDeletionFiles.get(dataFile);
            String indexFileName = new Path(deletionFile.path()).getName();
            touchedIndexFiles.add(indexFileName);
            if (indexFileToDeletionFiles.containsKey(indexFileName)) {
                indexFileToDeletionFiles.get(indexFileName).remove(dataFile);
            }
        }
    }

    public List<IndexManifestEntry> persist() {
        List<IndexManifestEntry> result = writeUnchangedDeletionVector();
        List<IndexManifestEntry> newIndexFileEntries =
                maintainer.writeDeletionVectorsIndex().stream()
                        .map(
                                fileMeta ->
                                        new IndexManifestEntry(
                                                FileKind.ADD, partition, bucket, fileMeta))
                        .collect(Collectors.toList());
        result.addAll(newIndexFileEntries);
        return result;
    }

    public List<IndexManifestEntry> writeUnchangedDeletionVector() {
        DeletionVectorsIndexFile deletionVectorsIndexFile = indexFileHandler.deletionVectorsIndex();
        List<IndexManifestEntry> newIndexEntries = new ArrayList<>();
        for (String indexFile : indexFileToDeletionFiles.keySet()) {
            if (touchedIndexFiles.contains(indexFile)) {
                IndexManifestEntry oldEntry = indexNameToEntry.get(indexFile);

                // write unchanged deletion vector.
                Map<String, DeletionFile> dataFileToDeletionFiles =
                        indexFileToDeletionFiles.get(indexFile);
                if (!dataFileToDeletionFiles.isEmpty()) {
                    List<IndexFileMeta> newIndexFiles =
                            indexFileHandler.writeDeletionVectorsIndex(
                                    deletionVectorsIndexFile.readDeletionVector(
                                            dataFileToDeletionFiles));
                    newIndexFiles.forEach(
                            newIndexFile -> {
                                newIndexEntries.add(
                                        new IndexManifestEntry(
                                                FileKind.ADD,
                                                oldEntry.partition(),
                                                oldEntry.bucket(),
                                                newIndexFile));
                            });
                }

                // mark the touched index file as removed.
                newIndexEntries.add(oldEntry.toDeleteEntry());
            }
        }
        return newIndexEntries;
    }
}
