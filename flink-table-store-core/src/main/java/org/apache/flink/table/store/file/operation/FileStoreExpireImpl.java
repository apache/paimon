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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Default implementation of {@link FileStoreExpire}. It retains a certain number or period of
 * latest snapshots.
 *
 * <p>NOTE: This implementation will keep at least one snapshot so that users will not accidentally
 * clear all snapshots.
 */
public class FileStoreExpireImpl implements FileStoreExpire {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreExpireImpl.class);

    // snapshots exceeding any constraint will be expired
    private final int numRetained;
    private final long millisRetained;

    private final FileStorePathFactory pathFactory;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;

    public FileStoreExpireImpl(
            int numRetained,
            long millisRetained,
            FileStorePathFactory pathFactory,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory) {
        this.numRetained = numRetained;
        this.millisRetained = millisRetained;
        this.pathFactory = pathFactory;
        this.manifestFile = manifestFileFactory.create();
        this.manifestList = manifestListFactory.create();
    }

    @Override
    public void expire() {
        Long latestSnapshotId = pathFactory.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return;
        }

        long currentMillis = System.currentTimeMillis();

        // find the earliest snapshot to retain
        // TODO Here id will start from 1, we need to optimize the method of finding the minimum
        // snapshot
        for (long id = Math.max(latestSnapshotId - numRetained + 1, Snapshot.FIRST_SNAPSHOT_ID);
                id <= latestSnapshotId;
                id++) {
            Path snapshotPath = pathFactory.toSnapshotPath(id);
            try {
                if (snapshotPath.getFileSystem().exists(snapshotPath)
                        && currentMillis - Snapshot.fromPath(snapshotPath).timeMillis()
                                <= millisRetained) {
                    // within time threshold, can assume that all snapshots after it are also within
                    // the threshold
                    expireUntil(id);
                    return;
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to determine if snapshot #" + id + " still exists", e);
            }
        }

        // no snapshot can be retained, expire all but last one
        expireUntil(latestSnapshotId);
    }

    private void expireUntil(long endExclusiveId) {
        if (endExclusiveId <= Snapshot.FIRST_SNAPSHOT_ID) {
            // fast exit
            return;
        }

        // find first snapshot to expire
        long beginInclusiveId = Snapshot.FIRST_SNAPSHOT_ID;
        for (long id = endExclusiveId - 1; id >= Snapshot.FIRST_SNAPSHOT_ID; id--) {
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
    }
}
