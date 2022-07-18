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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
            int numRetainedMin,
            int numRetainedMax,
            long millisRetained,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory) {
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

        Long earliest;
        try {
            earliest = snapshotManager.findEarliest();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest snapshot id", e);
        }
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

        // delete data files
        // deleted data files in a snapshot are not used by that snapshot, so the range of id should
        // be (beginInclusiveId, endExclusiveId]
        for (long id = beginInclusiveId + 1; id <= endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete data files in snapshot #" + id);
            }

            List<String> manifestFiles =
                    manifestList.read(snapshotManager.snapshot(id).deltaManifestList()).stream()
                            .map(ManifestFileMeta::fileName)
                            .collect(Collectors.toList());
            Iterable<ManifestEntry> dataFileLog = manifestFile.readManifestFiles(manifestFiles);
            expireDataFiles(dataFileLog);
        }

        // delete manifests
        Snapshot exclusiveSnapshot = snapshotManager.snapshot(endExclusiveId);
        Set<ManifestFileMeta> manifestsInUse =
                new HashSet<>(exclusiveSnapshot.readAllManifests(manifestList));
        // to avoid deleting twice
        Set<ManifestFileMeta> deletedManifests = new HashSet<>();
        for (long id = beginInclusiveId; id < endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete manifests in snapshot #" + id);
            }

            Snapshot toExpire = snapshotManager.snapshot(id);

            for (ManifestFileMeta manifest : toExpire.readAllManifests(manifestList)) {
                if (!manifestsInUse.contains(manifest) && !deletedManifests.contains(manifest)) {
                    manifestFile.delete(manifest.fileName());
                    deletedManifests.add(manifest);
                }
            }

            // delete manifest lists
            manifestList.delete(toExpire.baseManifestList());
            manifestList.delete(toExpire.deltaManifestList());

            // delete snapshot
            FileUtils.deleteOrWarn(snapshotManager.snapshotPath(id));
        }

        writeEarliestHint(endExclusiveId);
    }

    @VisibleForTesting
    void expireDataFiles(Iterable<ManifestEntry> dataFileLog) {
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
                    FileUtils.deleteOrWarn(path);
                    extraFiles.forEach(FileUtils::deleteOrWarn);
                });
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
