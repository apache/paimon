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
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.operation.DeletionUtils.containsDataFile;
import static org.apache.paimon.operation.DeletionUtils.indexDataFiles;
import static org.apache.paimon.operation.DeletionUtils.readEntries;
import static org.apache.paimon.operation.DeletionUtils.tryDeleteDirectories;

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

    private final SnapshotManager snapshotManager;
    private final List<Snapshot> taggedSnapshots;

    private final Map<BinaryRow, Map<Integer, Set<String>>> skippedDataFiles;
    private final Set<String> skippedManifests;

    public TagDeletion(
            FileIO fileIO,
            Path tablePath,
            FileStorePathFactory pathFactory,
            ManifestList manifestList,
            ManifestFile manifestFile,
            IndexFileHandler indexFileHandler) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.manifestList = manifestList;
        this.manifestFile = manifestFile;
        this.indexFileHandler = indexFileHandler;

        this.snapshotManager = new SnapshotManager(fileIO, tablePath);
        this.taggedSnapshots = new TagManager(fileIO, tablePath).taggedSnapshots();

        this.skippedDataFiles = new HashMap<>();
        this.skippedManifests = new HashSet<>();
    }

    /** Delete unused data files, manifest files and empty data file directories of tag. */
    public void delete(Snapshot taggedSnapshot) {
        // collect from the earliest snapshot's base
        Snapshot earliest = snapshotManager.snapshot(snapshotManager.earliestSnapshotId());
        collectSkip(earliest.baseManifests(manifestList));

        // collect from the neighbor tags
        int index = findIndex(taggedSnapshot);
        if (index - 1 >= 0) {
            collectSkip(taggedSnapshots.get(index - 1).dataManifests(manifestList));
        }
        if (index + 1 < taggedSnapshots.size()) {
            collectSkip(taggedSnapshots.get(index + 1).dataManifests(manifestList));
        }

        // delete data files and empty directories
        deleteDataFiles(taggedSnapshot);
        // delete manifests
        deleteManifestFiles(taggedSnapshot);
    }

    private void collectSkip(List<ManifestFileMeta> manifestFileMetas) {
        Iterable<ManifestEntry> entries = readEntries(manifestFileMetas, manifestFile);
        indexDataFiles(skippedDataFiles, ManifestEntry.mergeEntries(entries));
        skippedManifests.addAll(
                manifestFileMetas.stream()
                        .map(ManifestFileMeta::fileName)
                        .collect(Collectors.toSet()));
    }

    private void deleteDataFiles(Snapshot taggedSnapshot) {
        // delete data files
        Map<BinaryRow, Set<Integer>> deletionBuckets = new HashMap<>();
        Iterable<ManifestEntry> entries =
                readEntries(taggedSnapshot.dataManifests(manifestList), manifestFile);
        for (ManifestEntry entry : ManifestEntry.mergeEntries(entries)) {
            if (!containsDataFile(skippedDataFiles, entry)) {
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

        // delete empty data file directories
        tryDeleteDirectories(deletionBuckets, pathFactory, fileIO);
    }

    private void deleteManifestFiles(Snapshot taggedSnapshot) {
        for (ManifestFileMeta manifest : taggedSnapshot.dataManifests(manifestList)) {
            String fileName = manifest.fileName();
            if (!skippedManifests.contains(fileName)) {
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
                indexFileHandler.deleteIndexFile(entry);
            }
            indexFileHandler.deleteManifest(indexManifest);
        }
    }

    private int findIndex(Snapshot taggedSnapshot) {
        for (int i = 0; i < taggedSnapshots.size(); i++) {
            if (taggedSnapshot.id() == taggedSnapshots.get(i).id()) {
                return i;
            }
        }
        throw new RuntimeException(
                String.format(
                        "Didn't find tag with snapshot id '%s'.This is unexpected.",
                        taggedSnapshot.id()));
    }
}
