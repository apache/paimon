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

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.mergetree.MergeTree;
import org.apache.flink.table.store.file.mergetree.MergeTreeFactory;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordWriter;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/** Default implementation of {@link FileStoreWrite}. */
public class FileStoreWriteImpl implements FileStoreWrite {

    private final FileStorePathFactory pathFactory;
    private final MergeTreeFactory mergeTreeFactory;
    private final FileStoreScan scan;

    public FileStoreWriteImpl(
            FileStorePathFactory pathFactory,
            MergeTreeFactory mergeTreeFactory,
            FileStoreScan scan) {
        this.pathFactory = pathFactory;
        this.mergeTreeFactory = mergeTreeFactory;
        this.scan = scan;
    }

    @Override
    public RecordWriter createWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        Long latestSnapshotId = pathFactory.latestSnapshotId();
        if (latestSnapshotId == null) {
            return createEmptyWriter(partition, bucket, compactExecutor);
        } else {
            MergeTree mergeTree = mergeTreeFactory.create(partition, bucket, compactExecutor);
            return mergeTree.createWriter(
                    scan.withSnapshot(latestSnapshotId)
                            .withPartitionFilter(Collections.singletonList(partition))
                            .withBucket(bucket).plan().files().stream()
                            .map(ManifestEntry::file)
                            .collect(Collectors.toList()));
        }
    }

    @Override
    public RecordWriter createEmptyWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        MergeTree mergeTree = mergeTreeFactory.create(partition, bucket, compactExecutor);
        return mergeTree.createWriter(Collections.emptyList());
    }
}
