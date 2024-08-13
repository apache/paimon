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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A maintainer to maintain deletion files for append table, the core methods:
 *
 * <ul>
 *   <li>{@link #notifyDeletionFiles}: Mark the deletion of data files, create new deletion vectors.
 *   <li>{@link #persist}:
 *   <li>Version 3: Introduced in paimon 0.4. Add "baseRecordCount" field, "deltaRecordCount" field
 *       and "changelogRecordCount" field.
 * </ul>
 */
public class AppendDeletionFileMaintainer {

    private final IndexFileHandler indexFileHandler;

    private final BinaryRow partition;
    private final int bucket;
    private final Map<String, IndexManifestEntry> indexNameToEntry = new HashMap<>();

    private final Map<String, Map<String, DeletionFile>> indexFileToDeletionFiles = new HashMap<>();
    private final Map<String, String> dataFileToIndexFile = new HashMap<>();

    private final Set<String> touchedIndexFiles = new HashSet<>();

    private final DeletionVectorsMaintainer maintainer;

    // the key of dataFileToDeletionFiles is the relative path again table's location.
    private AppendDeletionFileMaintainer(
            IndexFileHandler indexFileHandler,
            BinaryRow partition,
            int bucket,
            Map<String, DeletionFile> deletionFiles,
            DeletionVectorsMaintainer maintainer) {
        this.indexFileHandler = indexFileHandler;
        this.partition = partition;
        this.bucket = bucket;
        this.maintainer = maintainer;
        init(deletionFiles);
    }

    public static AppendDeletionFileMaintainer forBucketedAppend(
            IndexFileHandler indexFileHandler,
            @Nullable Long snapshotId,
            BinaryRow partition,
            int bucket) {
        Map<String, DeletionFile> deletionFiles =
                indexFileHandler.scanDVIndex(snapshotId, partition, bucket);
        checkArgument(deletionFiles.size() <= 1, "bucket should only have one deletion file.");
        // bucket should have only one deletion file, so here we should read old deletion vectors,
        // overwrite the entire deletion file of the bucket when writing deletes.
        DeletionVectorsMaintainer maintainer =
                new DeletionVectorsMaintainer.Factory(indexFileHandler).restore(deletionFiles);
        return new AppendDeletionFileMaintainer(
                indexFileHandler, partition, bucket, deletionFiles, maintainer);
    }

    public static AppendDeletionFileMaintainer forUnawareAppend(
            IndexFileHandler indexFileHandler, @Nullable Long snapshotId, BinaryRow partition) {
        Map<String, DeletionFile> deletionFiles =
                indexFileHandler.scanDVIndex(snapshotId, partition, UNAWARE_BUCKET);
        // the deletion of data files is independent
        // just create an empty maintainer
        DeletionVectorsMaintainer maintainer =
                new DeletionVectorsMaintainer.Factory(indexFileHandler).create();
        return new AppendDeletionFileMaintainer(
                indexFileHandler, partition, UNAWARE_BUCKET, deletionFiles, maintainer);
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

    @VisibleForTesting
    List<IndexManifestEntry> writeUnchangedDeletionVector() {
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
                            newIndexFile ->
                                    newIndexEntries.add(
                                            new IndexManifestEntry(
                                                    FileKind.ADD,
                                                    oldEntry.partition(),
                                                    oldEntry.bucket(),
                                                    newIndexFile)));
                }

                // mark the touched index file as removed.
                newIndexEntries.add(oldEntry.toDeleteEntry());
            }
        }
        return newIndexEntries;
    }
}
