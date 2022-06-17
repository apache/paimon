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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.AppendOnlyWriter;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFilePathFactory;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.mergetree.compact.CompactResult;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/** {@link FileStoreWrite} for {@link org.apache.flink.table.store.file.AppendOnlyFileStore}. */
public class AppendOnlyFileStoreWrite extends AbstractFileStoreWrite<RowData> {

    private final long schemaId;
    private final RowType rowType;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;
    private final long targetFileSize;

    public AppendOnlyFileStoreWrite(
            long schemaId,
            RowType rowType,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            long targetFileSize) {
        super(snapshotManager, scan);
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.fileFormat = fileFormat;
        this.pathFactory = pathFactory;
        this.targetFileSize = targetFileSize;
    }

    @Override
    public RecordWriter<RowData> createWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        return createWriter(
                partition, bucket, getMaxSequenceNumber(scanExistingFileMetas(partition, bucket)));
    }

    @Override
    public RecordWriter<RowData> createEmptyWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        return createWriter(partition, bucket, -1L);
    }

    @Override
    public Callable<CompactResult> createCompactWriter(
            BinaryRowData partition, int bucket, @Nullable List<DataFileMeta> compactFiles) {
        throw new UnsupportedOperationException(
                "Currently append only write mode does not support compaction.");
    }

    private RecordWriter<RowData> createWriter(
            BinaryRowData partition, int bucket, long maxSeqNum) {
        DataFilePathFactory factory = pathFactory.createDataFilePathFactory(partition, bucket);
        return new AppendOnlyWriter(
                schemaId, fileFormat, targetFileSize, rowType, maxSeqNum, factory);
    }
}
