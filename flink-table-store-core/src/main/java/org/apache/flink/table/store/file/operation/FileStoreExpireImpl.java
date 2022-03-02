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

        // find earliest snapshot to retain
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

    private void expireUntil(long exclusiveId) {
        if (exclusiveId <= Snapshot.FIRST_SNAPSHOT_ID) {
            // fast exit
            return;
        }

        Snapshot exclusiveSnapshot = Snapshot.fromPath(pathFactory.toSnapshotPath(exclusiveId));

        // manifests are only added and deleted once
        Set<ManifestFileMeta> manifestsInUse = new HashSet<>();
        manifestsInUse.addAll(manifestList.read(exclusiveSnapshot.previousChanges()));
        manifestsInUse.addAll(manifestList.read(exclusiveSnapshot.newChanges()));

        FileStorePathFactory.SstPathFactoryCache sstPathFactoryCache =
                new FileStorePathFactory.SstPathFactoryCache(pathFactory);
        // we cannot delete manifest file directly because other snapshots to be expired might
        // be using this as well
        Set<ManifestFileMeta> manifestsToDelete = new HashSet<>();

        for (long id = exclusiveId - 1; id >= Snapshot.FIRST_SNAPSHOT_ID; id--) {
            Path snapshotPath = pathFactory.toSnapshotPath(id);
            try {
                if (!snapshotPath.getFileSystem().exists(snapshotPath)) {
                    // only latest snapshots are retained, as we cannot find this snapshot, we can
                    // assume that all snapshots preceding it have been removed
                    break;
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to determine if snapshot #" + id + " still exists", e);
            }

            Snapshot toExpire = Snapshot.fromPath(pathFactory.toSnapshotPath(id));
            List<ManifestFileMeta> previousChanges = manifestList.read(toExpire.previousChanges());
            List<ManifestFileMeta> newChanges = manifestList.read(toExpire.newChanges());

            // we cannot delete an sst file directly when we meet a DELETE entry, because that
            // file might be upgraded
            Set<Path> sstToDelete = new HashSet<>();
            for (ManifestFileMeta meta : newChanges) {
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

            // collect useless manifests
            for (ManifestFileMeta manifest : previousChanges) {
                if (!manifestsInUse.contains(manifest)) {
                    manifestsToDelete.add(manifest);
                }
            }
            for (ManifestFileMeta manifest : newChanges) {
                if (!manifestsInUse.contains(manifest)) {
                    manifestsToDelete.add(manifest);
                }
            }

            // delete manifest lists
            manifestList.delete(toExpire.previousChanges());
            manifestList.delete(toExpire.newChanges());

            // delete snapshot
            FileUtils.deleteOrWarn(pathFactory.toSnapshotPath(id));
        }

        // actual delete of manifest files
        for (ManifestFileMeta manifest : manifestsToDelete) {
            manifestFile.delete(manifest.fileName());
        }
    }
}
