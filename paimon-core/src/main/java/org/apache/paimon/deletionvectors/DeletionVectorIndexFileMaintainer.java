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

    private final Map<String, List<DeletionFileWithDataFile>> indexFileToDataFiles =
            new HashMap<>();

    private final Set<String> touchedIndexFiles = new HashSet<>();

    public DeletionVectorIndexFileMaintainer(
            IndexFileHandler indexFileHandler, List<DeletionFileWithDataFile> files) {
        this.indexFileHandler = indexFileHandler;
        List<String> touchedIndexFileNames =
                files.stream()
                        .map(file -> new Path(file.deletionFile().path()).getName())
                        .distinct()
                        .collect(Collectors.toList());
        indexFileHandler.scan().stream()
                .filter(
                        indexManifestEntry ->
                                touchedIndexFileNames.contains(
                                        indexManifestEntry.indexFile().fileName()))
                .forEach(entry -> indexNameToEntry.put(entry.indexFile().fileName(), entry));

        for (DeletionFileWithDataFile ddFile : files) {
            String indexFileName = new Path(ddFile.deletionFile().path()).getName();
            if (!indexFileToDataFiles.containsKey(indexFileName)) {
                indexFileToDataFiles.put(indexFileName, new ArrayList<>());
            }
            indexFileToDataFiles.get(indexFileName).add(ddFile);
        }
    }

    public void notifyDeletionFiles(List<DeletionFileWithDataFile> ddFiles) {
        for (DeletionFileWithDataFile ddFile : ddFiles) {
            String indexFileName = new Path(ddFile.deletionFile().path()).getName();
            touchedIndexFiles.add(indexFileName);
            if (indexFileToDataFiles.containsKey(indexFileName)) {
                indexFileToDataFiles.get(indexFileName).remove(ddFile);
            }
        }
    }

    public void notifyIndexFiles(List<String> indexFiles) {
        List<String> indexFileNames =
                indexFiles.stream()
                        .map(indexFile -> new Path(indexFile).getName())
                        .collect(Collectors.toList());
        touchedIndexFiles.addAll(indexFileNames);
        for (String indexFileName : indexFileNames) {
            indexFileToDataFiles.remove(indexFileName);
        }
    }

    public List<IndexManifestEntry> writeUnchangedDeletionVector() {
        DeletionVectorsIndexFile deletionVectorsIndexFile = indexFileHandler.deletionVectorsIndex();
        List<IndexManifestEntry> newIndexEntries = new ArrayList<>();
        for (String indexFile : indexFileToDataFiles.keySet()) {
            if (touchedIndexFiles.contains(indexFile)) {
                IndexManifestEntry oldEntry = indexNameToEntry.get(indexFile);

                // write unchanged deletion vector.
                List<DeletionFileWithDataFile> ddFiles = indexFileToDataFiles.get(indexFile);
                if (!ddFiles.isEmpty()) {
                    IndexFileMeta newIndexFile =
                            indexFileHandler.writeDeletionVectorsIndex(
                                    deletionVectorsIndexFile.readDeletionVector(ddFiles));
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
