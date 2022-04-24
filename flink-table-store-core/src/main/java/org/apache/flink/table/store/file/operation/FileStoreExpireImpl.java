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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.sst.SstPathFactory;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.SnapshotFinder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

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
    // max retry times to discover the earliest snapshot
    private final int maxRetry;
    private final long millisRetained;

    private final FileStorePathFactory pathFactory;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;

    private Lock lock;

    public FileStoreExpireImpl(
            int numRetainedMin,
            int numRetainedMax,
            int maxRetry,
            long millisRetained,
            FileStorePathFactory pathFactory,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory) {
        this.numRetainedMin = numRetainedMin;
        this.numRetainedMax = numRetainedMax;
        this.maxRetry = maxRetry;
        this.millisRetained = millisRetained;
        this.pathFactory = pathFactory;
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
        Long latestSnapshotId = pathFactory.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return;
        }

        long currentMillis = System.currentTimeMillis();

        Long earliest;
        try {
            earliest = SnapshotFinder.findEarliest(pathFactory.snapshotDirectory(), maxRetry);
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
            Path snapshotPath = pathFactory.toSnapshotPath(id);
            try {
                if (snapshotPath.getFileSystem().exists(snapshotPath)
                        && currentMillis - Snapshot.fromPath(snapshotPath).timeMillis()
                                <= millisRetained) {
                    // within time threshold, can assume that all snapshots after it are also within
                    // the threshold
                    expireUntil(earliest, id);
                    return;
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to determine if snapshot #" + id + " still exists", e);
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
            Path hint = new Path(pathFactory.snapshotDirectory(), SnapshotFinder.EARLIEST);
            try {
                if (!hint.getFileSystem().exists(hint)) {
                    writeEarliestHint(endExclusiveId);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            // fast exit
            return;
        }

        // find first snapshot to expire
        long beginInclusiveId = earliestId;
        for (long id = endExclusiveId - 1; id >= earliestId; id--) {
            Path snapshotPath = pathFactory.toSnapshotPath(id);
            try {
                if (!snapshotPath.getFileSystem().exists(snapshotPath)) {
                    // only latest snapshots are retained, as we cannot find this snapshot, we can
                    // assume that all snapshots preceding it have been removed
                    beginInclusiveId = id + 1;
                    break;
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to determine if snapshot #" + id + " still exists", e);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Snapshot expire range is [" + beginInclusiveId + ", " + endExclusiveId + ")");
        }

        // delete sst files
        FileStorePathFactory.SstPathFactoryCache sstPathFactoryCache =
                new FileStorePathFactory.SstPathFactoryCache(pathFactory);
        // deleted sst files in a snapshot are not used by that snapshot, so the range of id should
        // be (beginInclusiveId, endExclusiveId]
        for (long id = beginInclusiveId + 1; id <= endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete sst files in snapshot #" + id);
            }

            Snapshot toExpire = Snapshot.fromPath(pathFactory.toSnapshotPath(id));
            List<ManifestFileMeta> deltaManifests = manifestList.read(toExpire.deltaManifestList());

            // we cannot delete an sst file directly when we meet a DELETE entry, because that
            // file might be upgraded
            Set<Path> sstToDelete = new HashSet<>();
            for (ManifestFileMeta meta : deltaManifests) {
                for (ManifestEntry entry : manifestFile.read(meta.fileName())) {
                    SstPathFactory sstPathFactory =
                            sstPathFactoryCache.getSstPathFactory(
                                    entry.partition(), entry.bucket());
                    Path sstPath = sstPathFactory.toPath(entry.file().fileName());
                    switch (entry.kind()) {
                        case ADD:
                            sstToDelete.remove(sstPath);
                            break;
                        case DELETE:
                            sstToDelete.add(sstPath);
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unknown value kind " + entry.kind().name());
                    }
                }
            }
            for (Path sst : sstToDelete) {
                FileUtils.deleteOrWarn(sst);
            }
        }

        // delete manifests
        Snapshot exclusiveSnapshot = Snapshot.fromPath(pathFactory.toSnapshotPath(endExclusiveId));
        Set<ManifestFileMeta> manifestsInUse =
                new HashSet<>(exclusiveSnapshot.readAllManifests(manifestList));
        // to avoid deleting twice
        Set<ManifestFileMeta> deletedManifests = new HashSet<>();
        for (long id = beginInclusiveId; id < endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete manifests in snapshot #" + id);
            }

            Snapshot toExpire = Snapshot.fromPath(pathFactory.toSnapshotPath(id));

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
            FileUtils.deleteOrWarn(pathFactory.toSnapshotPath(id));
        }

        writeEarliestHint(endExclusiveId);
    }

    private void writeEarliestHint(long earliest) {
        // update earliest hint file

        Callable<Void> callable =
                () -> {
                    SnapshotFinder.commitEarliestHint(pathFactory.snapshotDirectory(), earliest);
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
