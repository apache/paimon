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
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.utils.FileStorePathFactory;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** TODO WIP. */
public abstract class FileDeletionBase {

    private static final Logger LOG = LoggerFactory.getLogger(FileDeletionBase.class);

    protected final FileIO fileIO;
    protected final FileStorePathFactory pathFactory;
    protected final ManifestFile manifestFile;
    protected final ManifestList manifestList;
    protected final IndexFileHandler indexFileHandler;
    protected final Map<BinaryRow, Set<Integer>> deletionBuckets;

    public FileDeletionBase(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestFile manifestFile,
            ManifestList manifestList,
            IndexFileHandler indexFileHandler) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.manifestFile = manifestFile;
        this.manifestList = manifestList;
        this.indexFileHandler = indexFileHandler;

        this.deletionBuckets = new HashMap<>();
    }

    /**
     * Clean data files that will not be used anymore in the snapshot.
     *
     * @param snapshot {@link Snapshot} that will be cleaned
     * @param skipper if the test result of a data file is true, it will be skipped when deleting;
     *     else it will be deleted
     */
    public abstract void cleanUnusedDataFiles(Snapshot snapshot, Predicate<ManifestEntry> skipper);

    /**
     * Clean metadata files that will not be used anymore of a snapshot, including data manifests,
     * index manifests and manifest lists.
     *
     * @param snapshot {@link Snapshot} that will be cleaned
     * @param skippingSnapshots manifests that still used by these snapshots will be skipped
     */
    public abstract void cleanUnusedManifests(Snapshot snapshot, List<Snapshot> skippingSnapshots);

    /** Try to delete data directories that may be empty after data file deletion. */
    public void cleanDataDirectories() {
        if (deletionBuckets.isEmpty()) {
            return;
        }

        // All directory paths are deduplicated and sorted by hierarchy level
        Map<Integer, Set<Path>> deduplicate = new HashMap<>();
        for (Map.Entry<BinaryRow, Set<Integer>> entry : deletionBuckets.entrySet()) {
            // try to delete bucket directories
            for (Integer bucket : entry.getValue()) {
                tryDeleteEmptyDirectory(pathFactory.bucketPath(entry.getKey(), bucket));
            }

            List<Path> hierarchicalPaths = pathFactory.getHierarchicalPartitionPath(entry.getKey());
            int hierarchies = hierarchicalPaths.size();
            if (hierarchies == 0) {
                continue;
            }

            if (tryDeleteEmptyDirectory(hierarchicalPaths.get(hierarchies - 1))) {
                // deduplicate high level partition directories
                for (int hierarchy = 0; hierarchy < hierarchies - 1; hierarchy++) {
                    Path path = hierarchicalPaths.get(hierarchy);
                    deduplicate.computeIfAbsent(hierarchy, i -> new HashSet<>()).add(path);
                }
            }
        }

        // from deepest to shallowest
        for (int hierarchy = deduplicate.size() - 1; hierarchy >= 0; hierarchy--) {
            deduplicate.get(hierarchy).forEach(this::tryDeleteEmptyDirectory);
        }

        deletionBuckets.clear();
    }

    protected void recordDeletionBuckets(ManifestEntry entry) {
        deletionBuckets
                .computeIfAbsent(entry.partition(), p -> new HashSet<>())
                .add(entry.bucket());
    }

    protected void cleanUnusedManifests(
            Snapshot snapshot, List<Snapshot> skippingSnapshots, boolean deleteChangelog) {
        Set<String> skippingSet = manifestSkippingSet(skippingSnapshots);

        // clean base and delta manifests
        List<ManifestFileMeta> toExpireManifests = new ArrayList<>();
        toExpireManifests.addAll(tryReadManifestList(snapshot.baseManifestList()));
        toExpireManifests.addAll(tryReadManifestList(snapshot.deltaManifestList()));
        for (ManifestFileMeta manifest : toExpireManifests) {
            String fileName = manifest.fileName();
            if (!skippingSet.contains(fileName)) {
                manifestFile.delete(fileName);
                // to avoid other snapshots trying to delete again
                skippingSet.add(fileName);
            }
        }

        if (!skippingSet.contains(snapshot.baseManifestList())) {
            manifestList.delete(snapshot.baseManifestList());
        }
        if (!skippingSet.contains(snapshot.deltaManifestList())) {
            manifestList.delete(snapshot.deltaManifestList());
        }

        // clean changelog manifests
        if (deleteChangelog && snapshot.changelogManifestList() != null) {
            for (ManifestFileMeta manifest :
                    tryReadManifestList(snapshot.changelogManifestList())) {
                manifestFile.delete(manifest.fileName());
            }
            manifestList.delete(snapshot.changelogManifestList());
        }

        // clean index manifests
        String indexManifest = snapshot.indexManifest();
        // check exists, it may have been deleted by other snapshots
        if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
            for (IndexManifestEntry entry : indexFileHandler.readManifest(indexManifest)) {
                if (!skippingSet.contains(entry.indexFile().fileName())) {
                    indexFileHandler.deleteIndexFile(entry);
                }
            }

            if (!skippingSet.contains(indexManifest)) {
                indexFileHandler.deleteManifest(indexManifest);
            }
        }
    }

    protected Iterable<ManifestEntry> tryReadManifestEntries(String manifestListName) {
        return readManifestEntries(tryReadManifestList(manifestListName));
    }

    protected Iterable<ManifestEntry> tryReadDataManifestEntries(Snapshot snapshot) {
        return readManifestEntries(tryReadDataManifests(snapshot));
    }

    private Iterable<ManifestEntry> readManifestEntries(List<ManifestFileMeta> manifestFileMetas) {
        Queue<String> files =
                manifestFileMetas.stream()
                        .map(ManifestFileMeta::fileName)
                        .collect(Collectors.toCollection(LinkedList::new));
        return Iterables.concat(
                (Iterable<Iterable<ManifestEntry>>)
                        () ->
                                new Iterator<Iterable<ManifestEntry>>() {
                                    @Override
                                    public boolean hasNext() {
                                        return files.size() > 0;
                                    }

                                    @Override
                                    public Iterable<ManifestEntry> next() {
                                        String file = files.poll();
                                        try {
                                            return manifestFile.read(file);
                                        } catch (Exception e) {
                                            LOG.warn("Failed to read manifest file " + file, e);
                                            return Collections.emptyList();
                                        }
                                    }
                                });
    }

    /** Changelogs were not checked. Let the subclass determine whether to delete them. */
    private Set<String> manifestSkippingSet(List<Snapshot> skippingSnapshots) {
        Set<String> skippingSet = new HashSet<>();
        for (Snapshot snapshot : skippingSnapshots) {
            // data manifests
            skippingSet.add(snapshot.baseManifestList());
            skippingSet.add(snapshot.deltaManifestList());
            snapshot.dataManifests(manifestList).stream()
                    .map(ManifestFileMeta::fileName)
                    .forEach(skippingSet::add);

            // index manifests
            String indexManifest = snapshot.indexManifest();
            if (indexManifest != null) {
                skippingSet.add(indexManifest);
                indexFileHandler.readManifest(indexManifest).stream()
                        .map(IndexManifestEntry::indexFile)
                        .map(IndexFileMeta::fileName)
                        .forEach(skippingSet::add);
            }
        }

        return skippingSet;
    }

    /**
     * It is possible that a job was killed during expiration and some manifest files have been
     * deleted, so if the clean methods need to read manifests of a snapshot to be cleaned, use this
     * method instead of calling {@link Snapshot#dataManifests} directly.
     */
    private List<ManifestFileMeta> tryReadManifestList(String manifestListName) {
        try {
            return manifestList.read(manifestListName);
        } catch (Exception e) {
            LOG.warn("Failed to read manifest list file " + manifestListName, e);
            return Collections.emptyList();
        }
    }

    /**
     * Try to read base and delta manifest lists at one time. If failed to read either list, the
     * result will be empty to avoid error merging result.
     */
    private List<ManifestFileMeta> tryReadDataManifests(Snapshot snapshot) {
        List<ManifestFileMeta> manifestFileMetas = new ArrayList<>();

        try {
            manifestFileMetas.addAll(manifestList.read(snapshot.baseManifestList()));
        } catch (Exception e) {
            LOG.warn("Failed to read manifest list file " + snapshot.baseManifestList(), e);
            return Collections.emptyList();
        }

        try {
            manifestFileMetas.addAll(manifestList.read(snapshot.deltaManifestList()));
        } catch (Exception e) {
            LOG.warn("Failed to read manifest list file " + snapshot.deltaManifestList(), e);
            return Collections.emptyList();
        }

        return manifestFileMetas;
    }

    private boolean tryDeleteEmptyDirectory(Path path) {
        try {
            fileIO.delete(path, false);
            return true;
        } catch (IOException e) {
            LOG.debug("Failed to delete directory '{}'. Check whether it is empty.", path);
            return false;
        }
    }
}
