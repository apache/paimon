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
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.sst.SstPathFactory;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;

import java.io.IOException;
import java.util.HashSet;
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
    private final ManifestList manifestList;
    private final FileStoreScan scan;

    public FileStoreExpireImpl(
            int numRetained,
            long millisRetained,
            FileStorePathFactory pathFactory,
            ManifestList.Factory manifestListFactory,
            FileStoreScan scan) {
        this.numRetained = numRetained;
        this.millisRetained = millisRetained;
        this.pathFactory = pathFactory;
        this.manifestList = manifestListFactory.create();
        this.scan = scan;
    }

    @Override
    public void expire() {
        Long latestSnapshotId = pathFactory.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return;
        }

        // binary search for the last snapshot to expire
        long currentMillis = System.currentTimeMillis();
        long head = Math.max(Snapshot.FIRST_SNAPSHOT_ID - 1, latestSnapshotId - numRetained);
        long tail = latestSnapshotId;
        while (head < tail) {
            long mid = (head + tail + 1) / 2;
            Path snapshotPath = pathFactory.toSnapshotPath(mid);
            try {
                if (!snapshotPath.getFileSystem().exists(snapshotPath)
                        || currentMillis - Snapshot.fromPath(snapshotPath).timeMillis()
                                > millisRetained) {
                    head = mid;
                } else {
                    tail = mid - 1;
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to determine if snapshot #" + mid + " still exists", e);
            }
        }

        // determine the exact expire range, we also keep at least 1 snapshot not to expire
        long lastSnapshotIdToExpire = Math.min(head, latestSnapshotId - 1);
        long firstSnapshotIdToExpire = Snapshot.FIRST_SNAPSHOT_ID;
        for (long id = lastSnapshotIdToExpire; id >= Snapshot.FIRST_SNAPSHOT_ID; id--) {
            Path snapshotPath = pathFactory.toSnapshotPath(id);
            try {
                if (!snapshotPath.getFileSystem().exists(snapshotPath)) {
                    // only latest snapshots are retained, as we cannot find this snapshot, we can
                    // assume that all snapshots preceding it have been removed
                    firstSnapshotIdToExpire = id + 1;
                    break;
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to determine if snapshot #" + id + " still exists", e);
            }
        }

        // expire each snapshot
        for (long id = firstSnapshotIdToExpire; id <= lastSnapshotIdToExpire; id++) {
            expire(id);
        }
    }

    private void expire(long snapshotId) {
        Snapshot toExpire = Snapshot.fromPath(pathFactory.toSnapshotPath(snapshotId));
        Snapshot nextSnapshot = Snapshot.fromPath(pathFactory.toSnapshotPath(snapshotId + 1));

        // if sst file in only used in snapshot to expire but not in next snapshot we can delete it
        // because each sst file will only be added and deleted once
        Set<Path> sstInUse = new HashSet<>();
        FileStorePathFactory.SstPathFactoryCache sstPathFactoryCache =
                new FileStorePathFactory.SstPathFactoryCache(pathFactory);
        for (ManifestEntry entry : scan.withSnapshot(nextSnapshot.id()).plan().files()) {
            SstPathFactory sstPathFactory =
                    sstPathFactoryCache.getSstPathFactory(entry.partition(), entry.bucket());
            sstInUse.add(sstPathFactory.toPath(entry.file().fileName()));
        }
        for (ManifestEntry entry : scan.withSnapshot(toExpire.id()).plan().files()) {
            SstPathFactory sstPathFactory =
                    sstPathFactoryCache.getSstPathFactory(entry.partition(), entry.bucket());
            Path sstPath = sstPathFactory.toPath(entry.file().fileName());
            if (!sstInUse.contains(sstPath)) {
                FileUtils.deleteOrWarn(sstPath);
            }
        }

        // delete manifest file not used by the following snapshots
        Set<ManifestFileMeta> manifestsInUse =
                new HashSet<>(manifestList.read(nextSnapshot.manifestList()));
        for (ManifestFileMeta manifest : manifestList.read(toExpire.manifestList())) {
            if (!manifestsInUse.contains(manifest)) {
                FileUtils.deleteOrWarn(pathFactory.toManifestFilePath(manifest.fileName()));
            }
        }

        // delete manifest list
        manifestList.delete(toExpire.manifestList());

        // delete snapshot file
        FileUtils.deleteOrWarn(pathFactory.toSnapshotPath(snapshotId));
    }
}
