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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.TagManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/** Delete snapshot files. */
public class SnapshotDeletion extends FileDeletionBase {

    /** Used to record which tag is cached in tagged snapshots list. */
    private int cachedTagIndex = -1;

    /** Used to cache data files used by current tag. */
    private final Map<BinaryRow, Map<Integer, Set<String>>> cachedTagDataFiles = new HashMap<>();

    public SnapshotDeletion(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestFile manifestFile,
            ManifestList manifestList,
            IndexFileHandler indexFileHandler) {
        super(fileIO, pathFactory, manifestFile, manifestList, indexFileHandler);
    }

    @Override
    public void cleanUnusedDataFiles(Snapshot snapshot, Predicate<ManifestEntry> skipper) {
        doCleanUnusedDataFile(tryReadManifestEntries(snapshot.deltaManifestList()), skipper);
    }

    @Override
    public void cleanUnusedManifests(Snapshot snapshot, Set<String> skippingSet) {
        cleanUnusedManifests(snapshot, skippingSet, true);
    }

    @VisibleForTesting
    void doCleanUnusedDataFile(
            Iterable<ManifestEntry> dataFileLog, Predicate<ManifestEntry> skipper) {
        // we cannot delete a data file directly when we meet a DELETE entry, because that
        // file might be upgraded
        // data file path -> (original manifest entry, extra file paths)
        Map<Path, Pair<ManifestEntry, List<Path>>> dataFileToDelete = new HashMap<>();
        for (ManifestEntry entry : dataFileLog) {
            Path bucketPath = pathFactory.bucketPath(entry.partition(), entry.bucket());
            Path dataFilePath = new Path(bucketPath, entry.file().fileName());
            switch (entry.kind()) {
                case ADD:
                    dataFileToDelete.remove(dataFilePath);
                    break;
                case DELETE:
                    List<Path> extraFiles = new ArrayList<>(entry.file().extraFiles().size());
                    for (String file : entry.file().extraFiles()) {
                        extraFiles.add(new Path(bucketPath, file));
                    }
                    dataFileToDelete.put(dataFilePath, Pair.of(entry, extraFiles));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + entry.kind().name());
            }
        }

        List<Path> actualDataFileToDelete = new ArrayList<>();
        dataFileToDelete.forEach(
                (path, pair) -> {
                    ManifestEntry entry = pair.getLeft();
                    // check whether we should skip the data file
                    if (!skipper.test(entry)) {
                        // delete data files
                        actualDataFileToDelete.add(path);
                        actualDataFileToDelete.addAll(pair.getRight());

                        recordDeletionBuckets(entry);
                    }
                });
        deleteFiles(actualDataFileToDelete, fileIO::deleteQuietly);
    }

    /**
     * Delete added file in the manifest list files. Added files marked as "ADD" in manifests.
     *
     * @param manifestListName name of manifest list
     */
    public void deleteAddedDataFiles(String manifestListName) {
        deleteAddedDataFiles(tryReadManifestEntries(manifestListName));
    }

    public void deleteAddedDataFiles(Iterable<ManifestEntry> manifestEntries) {
        List<Path> dataFileToDelete = new ArrayList<>();
        for (ManifestEntry entry : manifestEntries) {
            if (entry.kind() == FileKind.ADD) {
                dataFileToDelete.add(
                        new Path(
                                pathFactory.bucketPath(entry.partition(), entry.bucket()),
                                entry.file().fileName()));
                recordDeletionBuckets(entry);
            }
        }
        deleteFiles(dataFileToDelete, fileIO::deleteQuietly);
    }

    public Predicate<ManifestEntry> dataFileSkipper(
            List<Snapshot> taggedSnapshots, long expiringSnapshotId) {
        int index = TagManager.findPreviousTag(taggedSnapshots, expiringSnapshotId);
        // refresh tag data files
        if (index >= 0 && cachedTagIndex != index) {
            cachedTagIndex = index;
            cachedTagDataFiles.clear();
            addMergedDataFiles(cachedTagDataFiles, taggedSnapshots.get(index));
        }

        return entry -> index >= 0 && containsDataFile(cachedTagDataFiles, entry);
    }
}
