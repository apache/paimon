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
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.append.AppendOnlyCompactManager;
import org.apache.flink.table.store.file.append.AppendOnlyWriter;
import org.apache.flink.table.store.file.compact.CompactManager;
import org.apache.flink.table.store.file.compact.NoopCompactManager;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.DataFilePathFactory;
import org.apache.flink.table.store.file.io.RowDataRollingFileWriter;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.table.store.file.io.DataFileMeta.getMaxSequenceNumber;

/** {@link FileStoreWrite} for {@link org.apache.flink.table.store.file.AppendOnlyFileStore}. */
public class AppendOnlyFileStoreWrite extends AbstractFileStoreWrite<RowData> {

    private final AppendOnlyFileStoreRead read;
    private final long schemaId;
    private final RowType rowType;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;
    private final long targetFileSize;
    private final int compactionMinFileNum;
    private final int compactionMaxFileNum;
    private final boolean commitForceCompact;
    private final boolean skipCompaction;

    public AppendOnlyFileStoreWrite(
            AppendOnlyFileStoreRead read,
            long schemaId,
            String commitUser,
            RowType rowType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options) {
        super(commitUser, snapshotManager, scan);
        this.read = read;
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.fileFormat = options.fileFormat();
        this.pathFactory = pathFactory;
        this.targetFileSize = options.targetFileSize();
        this.compactionMinFileNum = options.compactionMinFileNum();
        this.compactionMaxFileNum = options.compactionMaxFileNum();
        this.commitForceCompact = options.commitForceCompact();
        this.skipCompaction = options.writeCompactionSkip();
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

    private RecordWriter<RowData> createWriter(
            BinaryRowData partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            ExecutorService compactExecutor) {
        // let writer and compact manager hold the same reference
        // and make restore files mutable to update
        LinkedList<DataFileMeta> restored = new LinkedList<>(restoredFiles);
        DataFilePathFactory factory = pathFactory.createDataFilePathFactory(partition, bucket);
        CompactManager compactManager =
                skipCompaction
                        ? new NoopCompactManager()
                        : new AppendOnlyCompactManager(
                                compactExecutor,
                                restored,
                                compactionMinFileNum,
                                compactionMaxFileNum,
                                targetFileSize,
                                compactRewriter(partition, bucket),
                                factory);
        return new AppendOnlyWriter(
                schemaId,
                fileFormat,
                targetFileSize,
                rowType,
                getMaxSequenceNumber(restored),
                compactManager,
                commitForceCompact,
                factory);
    }

    private AppendOnlyCompactManager.CompactRewriter compactRewriter(
            BinaryRowData partition, int bucket) {
        return toCompact -> {
            if (toCompact.isEmpty()) {
                return Collections.emptyList();
            }
            RowDataRollingFileWriter rewriter =
                    new RowDataRollingFileWriter(
                            schemaId,
                            fileFormat,
                            targetFileSize,
                            rowType,
                            pathFactory.createDataFilePathFactory(partition, bucket),
                            new LongCounter(toCompact.get(0).minSequenceNumber()));
            rewriter.write(
                    new RecordReaderIterator<>(
                            read.createReader(new DataSplit(partition, bucket, toCompact, false))));
            rewriter.close();
            return rewriter.result();
        };
    }
}
