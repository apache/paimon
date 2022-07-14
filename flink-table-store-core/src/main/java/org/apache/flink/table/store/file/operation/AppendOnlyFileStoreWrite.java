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

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.data.AppendOnlyCompactManager;
import org.apache.flink.table.store.file.data.AppendOnlyWriter;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFilePathFactory;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/** {@link FileStoreWrite} for {@link org.apache.flink.table.store.file.AppendOnlyFileStore}. */
public class AppendOnlyFileStoreWrite extends AbstractFileStoreWrite<RowData> {

    private final AppendOnlyFileStoreRead read;
    private final long schemaId;
    private final RowType rowType;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;
    private final long targetFileSize;
    private final int minFileNum;
    private final int maxFileNum;
    private final boolean commitForceCompact;

    public AppendOnlyFileStoreWrite(
            AppendOnlyFileStoreRead read,
            long schemaId,
            RowType rowType,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            long targetFileSize,
            int minFileNum,
            int maxFileNum,
            boolean commitForceCompact) {
        super(snapshotManager, scan);
        this.read = read;
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.fileFormat = fileFormat;
        this.pathFactory = pathFactory;
        this.targetFileSize = targetFileSize;
        this.maxFileNum = maxFileNum;
        this.minFileNum = minFileNum;
        this.commitForceCompact = commitForceCompact;
    }

    @Override
    public RecordWriter<RowData> createWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        return createWriter(
                partition, bucket, scanExistingFileMetas(partition, bucket), compactExecutor);
    }

    @Override
    public RecordWriter<RowData> createEmptyWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        return createWriter(partition, bucket, Collections.emptyList(), compactExecutor);
    }

    @Override
    public Callable<CompactResult> createCompactWriter(
            BinaryRowData partition, int bucket, @Nullable List<DataFileMeta> compactFiles) {
        if (compactFiles == null) {
            compactFiles = scanExistingFileMetas(partition, bucket);
        }
        return new AppendOnlyCompactManager.IterativeCompactTask(
                compactFiles,
                targetFileSize,
                minFileNum,
                maxFileNum,
                compactRewriter(partition, bucket),
                pathFactory.createDataFilePathFactory(partition, bucket));
    }

    private RecordWriter<RowData> createWriter(
            BinaryRowData partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            ExecutorService compactExecutor) {
        // let writer and compact manager hold the same reference
        // and make restore files mutable to update
        LinkedList<DataFileMeta> restored = new LinkedList<>(restoredFiles);
        DataFilePathFactory factory = pathFactory.createDataFilePathFactory(partition, bucket);
        return new AppendOnlyWriter(
                schemaId,
                fileFormat,
                targetFileSize,
                rowType,
                restored,
                new AppendOnlyCompactManager(
                        compactExecutor,
                        restored,
                        minFileNum,
                        maxFileNum,
                        targetFileSize,
                        compactRewriter(partition, bucket)),
                commitForceCompact,
                factory);
    }

    private AppendOnlyCompactManager.CompactRewriter compactRewriter(
            BinaryRowData partition, int bucket) {
        return toCompact -> {
            if (toCompact.isEmpty()) {
                return Collections.emptyList();
            }
            AppendOnlyWriter.RowRollingWriter rewriter =
                    AppendOnlyWriter.RowRollingWriter.createRollingRowWriter(
                            schemaId,
                            fileFormat,
                            targetFileSize,
                            rowType,
                            pathFactory.createDataFilePathFactory(partition, bucket),
                            new LongCounter(toCompact.get(0).minSequenceNumber()));
            return rewriter.write(
                    new RecordReaderIterator<>(
                            read.createReader(new Split(partition, bucket, toCompact, false))));
        };
    }
}
