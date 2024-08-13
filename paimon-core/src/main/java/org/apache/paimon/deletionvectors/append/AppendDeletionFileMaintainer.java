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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.source.DeletionFile;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;

/**
 * A maintainer to maintain deletion files for append table, the core methods:
 *
 * <ul>
 *   <li>{@link #notifyNewDeletionVector}: Mark the deletion of data files, create new deletion
 *       vectors.
 *   <li>{@link #persist}: persist deletion files to commit.
 * </ul>
 */
public abstract class AppendDeletionFileMaintainer {

    protected final IndexFileHandler indexFileHandler;

    protected final BinaryRow partition;
    protected final Map<String, DeletionFile> dataFileToDeletionFile;
    protected final Map<String, IndexManifestEntry> indexNameToEntry = new HashMap<>();

    protected final Map<String, Map<String, DeletionFile>> indexFileToDeletionFiles =
            new HashMap<>();
    protected final Map<String, String> dataFileToIndexFile = new HashMap<>();

    protected final Set<String> touchedIndexFiles = new HashSet<>();

    protected final DeletionVectorsMaintainer maintainer;

    AppendDeletionFileMaintainer(
            IndexFileHandler indexFileHandler,
            BinaryRow partition,
            Map<String, DeletionFile> deletionFiles,
            DeletionVectorsMaintainer maintainer) {
        this.indexFileHandler = indexFileHandler;
        this.partition = partition;
        this.dataFileToDeletionFile = deletionFiles;
        this.maintainer = maintainer;
        init(this.dataFileToDeletionFile);
    }

    public void init(Map<String, DeletionFile> dataFileToDeletionFile) {
        List<String> touchedIndexFileNames =
                dataFileToDeletionFile.values().stream()
                        .map(deletionFile -> new Path(deletionFile.path()).getName())
                        .distinct()
                        .collect(Collectors.toList());
        indexFileHandler.scanEntries().stream()
                .filter(
                        indexManifestEntry ->
                                touchedIndexFileNames.contains(
                                        indexManifestEntry.indexFile().fileName()))
                .forEach(entry -> indexNameToEntry.put(entry.indexFile().fileName(), entry));

        for (String dataFile : dataFileToDeletionFile.keySet()) {
            DeletionFile deletionFile = dataFileToDeletionFile.get(dataFile);
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

    public DeletionFile getDeletionFile(String dataFile) {
        return this.dataFileToDeletionFile.get(dataFile);
    }

    /** In compaction operation, notify that a deletion file of a data file is dropped. */
    public DeletionFile notify(String dataFile) {
        if (dataFileToIndexFile.containsKey(dataFile)) {
            String indexFileName = dataFileToIndexFile.get(dataFile);
            touchedIndexFiles.add(indexFileName);
            if (indexFileToDeletionFiles.containsKey(indexFileName)) {
                return indexFileToDeletionFiles.get(indexFileName).remove(dataFile);
            }
        }
        return null;
    }

    public IndexFileMeta getIndexFile(String dataFile) {
        DeletionFile deletionFile = getDeletionFile(dataFile);
        if (deletionFile == null) {
            return null;
        } else {
            IndexManifestEntry entry =
                    this.indexNameToEntry.get(new Path(deletionFile.path()).getName());
            return entry == null ? null : entry.indexFile();
        }
    }

    public List<IndexManifestEntry> persist() {
        List<IndexManifestEntry> result = writeUnchangedDeletionVector();
        List<IndexManifestEntry> newIndexFileEntries =
                maintainer.writeDeletionVectorsIndex().stream()
                        .map(
                                fileMeta ->
                                        new IndexManifestEntry(
                                                FileKind.ADD, partition, getBucket(), fileMeta))
                        .collect(Collectors.toList());
        result.addAll(newIndexFileEntries);
        return result;
    }

    public abstract int getBucket();

    public abstract void notifyNewDeletionVector(String dataFile, DeletionVector deletionVector);

    abstract List<IndexManifestEntry> writeUnchangedDeletionVector();

    public static AppendDeletionFileMaintainer forBucketedAppend(
            IndexFileHandler indexFileHandler,
            @Nullable Long snapshotId,
            BinaryRow partition,
            int bucket) {
        // bucket should have only one deletion file, so here we should read old deletion vectors,
        // overwrite the entire deletion file of the bucket when writing deletes.
        DeletionVectorsMaintainer maintainer =
                new DeletionVectorsMaintainer.Factory(indexFileHandler)
                        .createOrRestore(snapshotId, partition, bucket);
        Map<String, DeletionFile> deletionFiles =
                indexFileHandler.scanDVIndex(snapshotId, partition, bucket);
        return new BucketedAppendDeletionFileMaintainer(
                indexFileHandler, partition, bucket, deletionFiles, maintainer);
    }

    public static AppendDeletionFileMaintainer forUnawareAppend(
            IndexFileHandler indexFileHandler, @Nullable Long snapshotId, BinaryRow partition) {
        // the deletion of data files is independent
        // just create an empty maintainer
        DeletionVectorsMaintainer maintainer =
                new DeletionVectorsMaintainer.Factory(indexFileHandler).create();
        Map<String, DeletionFile> deletionFiles =
                indexFileHandler.scanDVIndex(snapshotId, partition, UNAWARE_BUCKET);
        return new UnawareAppendDeletionFileMaintainer(
                indexFileHandler, partition, deletionFiles, maintainer);
    }
}
