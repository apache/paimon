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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/** {@link WriteRestore} to restore files directly from file system. */
public class FileSystemWriteRestore implements WriteRestore {

    private final SnapshotManager snapshotManager;
    private final FileStoreScan scan;
    private final IndexFileHandler indexFileHandler;

    public FileSystemWriteRestore(
            CoreOptions options,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            IndexFileHandler indexFileHandler) {
        this.snapshotManager = snapshotManager;
        this.scan = scan;
        this.indexFileHandler = indexFileHandler;
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
    public RestoreFiles restoreFiles(
            BinaryRow partition,
            int bucket,
            boolean scanDynamicBucketIndex,
            boolean scanDeleteVectorsIndex) {
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

        IndexFileMeta dynamicBucketIndex = null;
        if (scanDynamicBucketIndex) {
            dynamicBucketIndex =
                    indexFileHandler.scanHashIndex(snapshot, partition, bucket).orElse(null);
        }

        List<IndexFileMeta> deleteVectorsIndex = null;
        if (scanDeleteVectorsIndex) {
            deleteVectorsIndex =
                    indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX, partition, bucket);
        }

        return new RestoreFiles(
                snapshot, totalBuckets, restoreFiles, dynamicBucketIndex, deleteVectorsIndex);
    }
}
