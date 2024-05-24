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

    private final Map<String, IndexManifestEntry> indexNameToEntry = new HashMap<>();

    private final Map<String, Map<String, DeletionFile>> indexFileToDeletionFiles = new HashMap<>();

    private final Set<String> touchedIndexFiles = new HashSet<>();

    public DeletionVectorIndexFileMaintainer(
            IndexFileHandler indexFileHandler, Map<String, DeletionFile> dataFileToDeletionFiles) {
        this.indexFileHandler = indexFileHandler;
        List<String> touchedIndexFileNames =
                dataFileToDeletionFiles.values().stream()
                        .map(deletionFile -> new Path(deletionFile.path()).getName())
                        .distinct()
                        .collect(Collectors.toList());
        indexFileHandler.scan().stream()
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
        }
    }

    public void notifyDeletionFiles(Map<String, DeletionFile> dataFileToDeletionFiles) {
        for (String dataFile : dataFileToDeletionFiles.keySet()) {
            DeletionFile deletionFile = dataFileToDeletionFiles.get(dataFile);
            String indexFileName = new Path(deletionFile.path()).getName();
            touchedIndexFiles.add(indexFileName);
            if (indexFileToDeletionFiles.containsKey(indexFileName)) {
                indexFileToDeletionFiles.get(indexFileName).remove(dataFile);
                if (indexFileToDeletionFiles.get(indexFileName).isEmpty()) {
                    indexFileToDeletionFiles.remove(indexFileName);
                    indexNameToEntry.remove(indexFileName);
                }
            }
        }
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
                    IndexFileMeta newIndexFile =
                            indexFileHandler.writeDeletionVectorsIndex(
                                    deletionVectorsIndexFile.readDeletionVector(
                                            dataFileToDeletionFiles));
                    newIndexEntries.add(
                            new IndexManifestEntry(
                                    FileKind.ADD,
                                    oldEntry.partition(),
                                    oldEntry.bucket(),
                                    newIndexFile));
                }

                // mark the touched index file as removed.
                newIndexEntries.add(oldEntry.toDeleteEntry());
            }
        }
        return newIndexEntries;
    }
}
