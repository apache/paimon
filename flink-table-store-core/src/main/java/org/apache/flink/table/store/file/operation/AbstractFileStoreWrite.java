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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * Base {@link FileStoreWrite} implementation.
 *
 * @param <T> type of record to write.
 */
public abstract class AbstractFileStoreWrite<T> implements FileStoreWrite<T> {

    private final RowType partitionType;
    private final SnapshotManager snapshotManager;
    private final FileStoreScan scan;
    private final int expectNumBucket;

    protected AbstractFileStoreWrite(
            RowType partitionType,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            int expectNumBucket) {
        this.partitionType = partitionType;
        this.snapshotManager = snapshotManager;
        this.scan = scan;
        this.expectNumBucket = expectNumBucket;
    }

    protected List<DataFileMeta> scanExistingFileMetas(BinaryRowData partition, int bucket) {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        List<DataFileMeta> existingFileMetas = Lists.newArrayList();
        if (latestSnapshotId != null) {
            // Concat all the DataFileMeta of existing files into existingFileMetas.
            List<ManifestEntry> manifestEntries =
                    scan.withSnapshot(latestSnapshotId)
                            .withPartitionFilter(Collections.singletonList(partition))
                            .withBucket(bucket)
                            .plan()
                            .files();
            for (ManifestEntry entry : manifestEntries) {
                if (entry.totalBuckets() != expectNumBucket) {
                    String partInfo =
                            partitionType.getFieldCount() > 0
                                    ? "partition "
                                            + FileStorePathFactory.getPartitionComputer(
                                                            partitionType,
                                                            FileStorePathFactory
                                                                    .PARTITION_DEFAULT_NAME
                                                                    .defaultValue())
                                                    .generatePartValues(entry.partition())
                                    : "table";
                    throw new TableException(
                            String.format(
                                    "Try to write %s with a new bucket num %d, but the previous bucket num is %d. "
                                            + "Please switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.",
                                    partInfo, expectNumBucket, entry.totalBuckets()));
                }

                existingFileMetas.add(entry.file());
            }
        }
        return existingFileMetas;
    }

    protected long getMaxSequenceNumber(List<DataFileMeta> fileMetas) {
        return fileMetas.stream()
                .map(DataFileMeta::maxSequenceNumber)
                .max(Long::compare)
                .orElse(-1L);
    }
}
