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

package org.apache.paimon.operation.write;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.List;

/** {@link WriteRestore} to restore files directly from file system. */
public class FileSystemWriteRestore implements WriteRestore {

    private final SnapshotManager snapshotManager;
    private final FileStoreScan scan;

    public FileSystemWriteRestore(
            CoreOptions options, SnapshotManager snapshotManager, FileStoreScan scan) {
        this.snapshotManager = snapshotManager;
        this.scan = scan;
        if (options.manifestDeleteFileDropStats()) {
            if (this.scan != null) {
                this.scan.dropStats();
            }
        }
    }

    @Override
    public long latestCommittedIdentifier(String user) {
        return snapshotManager
                .latestSnapshotOfUserFromFilesystem(user)
                .map(Snapshot::commitIdentifier)
                .orElse(Long.MIN_VALUE);
    }

    @Override
    public RestoreFiles restore(BinaryRow partition, int bucket) {
        // NOTE: don't use snapshotManager.latestSnapshot() here,
        // because we don't want to flood the catalog with high concurrency
        Snapshot snapshot = snapshotManager.latestSnapshotFromFileSystem();
        if (snapshot == null) {
            return RestoreFiles.empty();
        }

        List<DataFileMeta> restoreFiles = new ArrayList<>();
        List<ManifestEntry> entries =
                scan.withSnapshot(snapshot).withPartitionBucket(partition, bucket).plan().files();
        Integer totalBuckets = WriteRestore.extractDataFiles(entries, restoreFiles);
        return new RestoreFiles(snapshot, restoreFiles, totalBuckets);
    }
}
