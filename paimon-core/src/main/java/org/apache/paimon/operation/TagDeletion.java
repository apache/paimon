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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.utils.FileStorePathFactory;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.operation.DeletionUtils.addMergedDataFiles;
import static org.apache.paimon.operation.DeletionUtils.containsDataFile;
import static org.apache.paimon.operation.DeletionUtils.readEntries;

/**
 * Delete tag files. This class doesn't check changelog files because they are handled by {@link
 * SnapshotDeletion}.
 */
public class TagDeletion {

    private final FileIO fileIO;
    private final FileStorePathFactory pathFactory;
    private final ManifestList manifestList;
    private final ManifestFile manifestFile;
    private final IndexFileHandler indexFileHandler;
    @Nullable private final Integer scanManifestParallelism;

    public TagDeletion(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestList manifestList,
            ManifestFile manifestFile,
            IndexFileHandler indexFileHandler,
            @Nullable Integer scanManifestParallelism) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.manifestList = manifestList;
        this.manifestFile = manifestFile;
        this.indexFileHandler = indexFileHandler;
        this.scanManifestParallelism = scanManifestParallelism;
    }

    public void collectSkippedDataFiles(
            Map<BinaryRow, Map<Integer, Set<String>>> skipped, Snapshot snapshot) {
        addMergedDataFiles(skipped, snapshot, manifestList, manifestFile, scanManifestParallelism);
    }

    public Set<String> collectManifestSkippingSet(Snapshot snapshot) {
        return DeletionUtils.collectManifestSkippingSet(snapshot, manifestList, indexFileHandler);
    }

    public void deleteDataFiles(
            Snapshot taggedSnapshot,
            Map<BinaryRow, Map<Integer, Set<String>>> skipped,
            Map<BinaryRow, Set<Integer>> deletionBuckets) {
        // delete data files
        Iterable<ManifestEntry> entries =
                readEntries(
                        taggedSnapshot.dataManifests(manifestList),
                        manifestFile,
                        scanManifestParallelism);
        for (ManifestEntry entry : ManifestEntry.mergeEntries(entries)) {
            if (!containsDataFile(skipped, entry)) {
                Path bucketPath = pathFactory.bucketPath(entry.partition(), entry.bucket());
                fileIO.deleteQuietly(new Path(bucketPath, entry.file().fileName()));
                for (String file : entry.file().extraFiles()) {
                    fileIO.deleteQuietly(new Path(bucketPath, file));
                }

                deletionBuckets
                        .computeIfAbsent(entry.partition(), p -> new HashSet<>())
                        .add(entry.bucket());
            }
        }
    }

    public void deleteManifestFiles(Snapshot taggedSnapshot, Set<String> skipped) {
        for (ManifestFileMeta manifest : taggedSnapshot.dataManifests(manifestList)) {
            String fileName = manifest.fileName();
            if (!skipped.contains(fileName)) {
                manifestFile.delete(fileName);
            }
        }

        // delete manifest lists
        manifestList.delete(taggedSnapshot.baseManifestList());
        manifestList.delete(taggedSnapshot.deltaManifestList());

        // delete index files
        String indexManifest = taggedSnapshot.indexManifest();
        // check exists, it may have been deleted by other snapshots
        if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
            for (IndexManifestEntry entry : indexFileHandler.readManifest(indexManifest)) {
                if (!skipped.contains(entry.indexFile().fileName())) {
                    indexFileHandler.deleteIndexFile(entry);
                }
            }

            if (!skipped.contains(indexManifest)) {
                indexFileHandler.deleteManifest(indexManifest);
            }
        }
    }

    public void tryDeleteDirectories(Map<BinaryRow, Set<Integer>> deletionBuckets) {
        DeletionUtils.tryDeleteDirectories(deletionBuckets, pathFactory, fileIO);
    }
}
