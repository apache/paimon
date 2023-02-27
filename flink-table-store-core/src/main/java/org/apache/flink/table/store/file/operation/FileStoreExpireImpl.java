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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.store.annotation.VisibleForTesting;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link FileStoreExpire}. It retains a certain number or period of
 * latest snapshots.
 *
 * <p>NOTE: This implementation will keep at least one snapshot so that users will not accidentally
 * clear all snapshots.
 *
 * <p>TODO: add concurrent tests.
 */
public class FileStoreExpireImpl implements FileStoreExpire {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreExpireImpl.class);

    private final FileIO fileIO;
    private final int numRetainedMin;
    // snapshots exceeding any constraint will be expired
    private final int numRetainedMax;
    private final long millisRetained;

    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;

    private Lock lock;

    public FileStoreExpireImpl(
            FileIO fileIO,
            int numRetainedMin,
            int numRetainedMax,
            long millisRetained,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory) {
        this.fileIO = fileIO;
        this.numRetainedMin = numRetainedMin;
        this.numRetainedMax = numRetainedMax;
        this.millisRetained = millisRetained;
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.manifestFile = manifestFileFactory.create();
        this.manifestList = manifestListFactory.create();
    }

    @Override
    public FileStoreExpire withLock(Lock lock) {
        this.lock = lock;
        return this;
    }

    @Override
    public void expire() {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return;
        }

        long currentMillis = System.currentTimeMillis();

        Long earliest = snapshotManager.earliestSnapshotId();
        if (earliest == null) {
            return;
        }

        // find the earliest snapshot to retain
        for (long id = Math.max(latestSnapshotId - numRetainedMax + 1, earliest);
                id <= latestSnapshotId - numRetainedMin;
                id++) {
            if (snapshotManager.snapshotExists(id)
                    && currentMillis - snapshotManager.snapshot(id).timeMillis()
                            <= millisRetained) {
                // within time threshold, can assume that all snapshots after it are also within
                // the threshold
                expireUntil(earliest, id);
                return;
            }
        }

        // no snapshot can be retained, expire until there are only numRetainedMin snapshots left
        expireUntil(earliest, latestSnapshotId - numRetainedMin + 1);
    }

    private void expireUntil(long earliestId, long endExclusiveId) {
        if (endExclusiveId <= earliestId) {
            // No expire happens:
            // write the hint file in order to see the earliest snapshot directly next time
            // should avoid duplicate writes when the file exists
            if (snapshotManager.readHint(SnapshotManager.EARLIEST) == null) {
                writeEarliestHint(endExclusiveId);
            }

            // fast exit
            return;
        }

        // find first snapshot to expire
        long beginInclusiveId = earliestId;
        for (long id = endExclusiveId - 1; id >= earliestId; id--) {
            if (!snapshotManager.snapshotExists(id)) {
                // only latest snapshots are retained, as we cannot find this snapshot, we can
                // assume that all snapshots preceding it have been removed
                beginInclusiveId = id + 1;
                break;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Snapshot expire range is [" + beginInclusiveId + ", " + endExclusiveId + ")");
        }

        // delete merge tree files
        // deleted merge tree files in a snapshot are not used by the next snapshot, so the range of
        // id should be (beginInclusiveId, endExclusiveId]
        for (long id = beginInclusiveId + 1; id <= endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete merge tree files not used by snapshot #" + id);
            }
            Snapshot snapshot = snapshotManager.snapshot(id);
            expireMergeTreeFiles(snapshot.deltaManifestList());
        }

        // delete changelog files
        for (long id = beginInclusiveId; id < endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete changelog files from snapshot #" + id);
            }
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot.changelogManifestList() != null) {
                expireChangelogFiles(snapshot.changelogManifestList());
            }
        }

        // delete manifests
        Snapshot exclusiveSnapshot = snapshotManager.snapshot(endExclusiveId);
        Set<ManifestFileMeta> manifestsInUse =
                new HashSet<>(exclusiveSnapshot.dataManifests(manifestList));
        // to avoid deleting twice
        Set<ManifestFileMeta> deletedManifests = new HashSet<>();
        for (long id = beginInclusiveId; id < endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete manifests in snapshot #" + id);
            }

            Snapshot toExpire = snapshotManager.snapshot(id);
            // cannot call `toExpire.dataManifests` directly, it is possible that a job is
            // killed during expiration, so some manifest files may have been deleted
            List<ManifestFileMeta> toExpireManifests = new ArrayList<>();
            toExpireManifests.addAll(tryReadManifestList(toExpire.baseManifestList()));
            toExpireManifests.addAll(tryReadManifestList(toExpire.deltaManifestList()));

            // delete manifest
            for (ManifestFileMeta manifest : toExpireManifests) {
                if (!manifestsInUse.contains(manifest) && !deletedManifests.contains(manifest)) {
                    manifestFile.delete(manifest.fileName());
                    deletedManifests.add(manifest);
                }
            }
            if (toExpire.changelogManifestList() != null) {
                for (ManifestFileMeta manifest :
                        tryReadManifestList(toExpire.changelogManifestList())) {
                    manifestFile.delete(manifest.fileName());
                }
            }

            // delete manifest lists
            manifestList.delete(toExpire.baseManifestList());
            manifestList.delete(toExpire.deltaManifestList());
            if (toExpire.changelogManifestList() != null) {
                manifestList.delete(toExpire.changelogManifestList());
            }

            // delete snapshot
            fileIO.deleteQuietly(snapshotManager.snapshotPath(id));
        }

        writeEarliestHint(endExclusiveId);
    }

    private void expireMergeTreeFiles(String manifestListName) {
        expireMergeTreeFiles(getManifestEntriesFromManifestList(manifestListName));
    }

    @VisibleForTesting
    void expireMergeTreeFiles(Iterable<ManifestEntry> dataFileLog) {
        // we cannot delete a data file directly when we meet a DELETE entry, because that
        // file might be upgraded
        Map<Path, List<Path>> dataFileToDelete = new HashMap<>();
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
                    dataFileToDelete.put(dataFilePath, extraFiles);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + entry.kind().name());
            }
        }
        dataFileToDelete.forEach(
                (path, extraFiles) -> {
                    fileIO.deleteQuietly(path);
                    extraFiles.forEach(fileIO::deleteQuietly);
                });
    }

    private void expireChangelogFiles(String manifestListName) {
        for (ManifestEntry changelogEntry : getManifestEntriesFromManifestList(manifestListName)) {
            fileIO.deleteQuietly(
                    new Path(
                            pathFactory.bucketPath(
                                    changelogEntry.partition(), changelogEntry.bucket()),
                            changelogEntry.file().fileName()));
        }
    }

    private Iterable<ManifestEntry> getManifestEntriesFromManifestList(String manifestListName) {
        List<String> manifestFiles =
                tryReadManifestList(manifestListName).stream()
                        .map(ManifestFileMeta::fileName)
                        .collect(Collectors.toList());
        Queue<String> files = new LinkedList<>(manifestFiles);
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

    private List<ManifestFileMeta> tryReadManifestList(String manifestListName) {
        try {
            return manifestList.read(manifestListName);
        } catch (Exception e) {
            LOG.warn("Failed to read manifest list file " + manifestListName, e);
            return Collections.emptyList();
        }
    }

    private void writeEarliestHint(long earliest) {
        // update earliest hint file

        Callable<Void> callable =
                () -> {
                    snapshotManager.commitEarliestHint(earliest);
                    return null;
                };

        try {
            if (lock != null) {
                lock.runWithLock(callable);
            } else {
                callable.call();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
