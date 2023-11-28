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

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.append.AppendOnlyCompactManager;
import org.apache.paimon.append.AppendOnlyWriter;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StatsCollectorFactories;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.io.DataFileMeta.getMaxSequenceNumber;

/** {@link FileStoreWrite} for {@link AppendOnlyFileStore}. */
public class AppendOnlyFileStoreWrite extends MemoryFileStoreWrite<InternalRow> {

    private final FileIO fileIO;
    private final AppendOnlyFileStoreRead read;
    private final long schemaId;
    private final RowType rowType;
    private final FileStorePathFactory pathFactory;
    private final FieldStatsCollector.Factory[] statsCollectors;

    private boolean skipCompaction;
    private BucketMode bucketMode = BucketMode.FIXED;

    public AppendOnlyFileStoreWrite(
            FileIO fileIO,
            AppendOnlyFileStoreRead read,
            long schemaId,
            String commitUser,
            RowType rowType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            String tableName) {
        super(commitUser, snapshotManager, scan, options, null, tableName, pathFactory);
        this.fileIO = fileIO;
        this.read = read;
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.pathFactory = pathFactory;
        this.skipCompaction = options.writeOnly();
        this.statsCollectors =
                StatsCollectorFactories.createStatsFactories(options, rowType.getFieldNames());
    }

    @Override
    protected RecordWriter<InternalRow> createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor) {
        // let writer and compact manager hold the same reference
        // and make restore files mutable to update
        long maxSequenceNumber = getMaxSequenceNumber(restoredFiles);
        DataFilePathFactory factory = pathFactory.createDataFilePathFactory(partition, bucket);
        CompactManager compactManager =
                skipCompaction
                        ? new NoopCompactManager()
                        : new AppendOnlyCompactManager(
                                compactExecutor,
                                restoredFiles,
                                options.compactionMinFileNum(),
                                options.compactionMaxFileNum(),
                                options.targetFileSize(),
                                compactRewriter(partition, bucket),
                                getCompactionMetrics(partition, bucket));

        return new AppendOnlyWriter(
                fileIO,
                ioManager,
                schemaId,
                options.fileFormat(),
                options.targetFileSize(),
                rowType,
                maxSequenceNumber,
                compactManager,
                options.commitForceCompact(),
                factory,
                restoreIncrement,
                options.useWriteBufferForAppend(),
                bufferSpillable(),
                options.fileCompression(),
                statsCollectors,
                getWriterMetrics(partition, bucket));
    }

    @VisibleForTesting
    public boolean bufferSpillable() {
        return options.writeBufferSpillable(fileIO.isObjectStore(), isStreamingMode);
    }

    public AppendOnlyCompactManager.CompactRewriter compactRewriter(
            BinaryRow partition, int bucket) {
        return toCompact -> {
            if (toCompact.isEmpty()) {
                return Collections.emptyList();
            }
            RowDataRollingFileWriter rewriter =
                    new RowDataRollingFileWriter(
                            fileIO,
                            schemaId,
                            options.fileFormat(),
                            options.targetFileSize(),
                            rowType,
                            pathFactory.createDataFilePathFactory(partition, bucket),
                            new LongCounter(toCompact.get(0).minSequenceNumber()),
                            options.fileCompression(),
                            statsCollectors);
            try {
                rewriter.write(
                        new RecordReaderIterator<>(
                                read.createReader(
                                        DataSplit.builder()
                                                .withPartition(partition)
                                                .withBucket(bucket)
                                                .withDataFiles(toCompact)
                                                .build())));
            } finally {
                rewriter.close();
            }
            return rewriter.result();
        };
    }

    public AppendOnlyFileStoreWrite withBucketMode(BucketMode bucketMode) {
        // AppendOnlyFileStoreWrite is sensitive with bucket mode. It will act difference in
        // unaware-bucket mode (no compaction and force empty-writer).
        this.bucketMode = bucketMode;
        if (bucketMode == BucketMode.UNAWARE) {
            super.withIgnorePreviousFiles(true);
            skipCompaction = true;
        }
        return this;
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePrevious) {
        // in unaware bucket mode, we need all writers to be empty
        super.withIgnorePreviousFiles(ignorePrevious || bucketMode == BucketMode.UNAWARE);
    }
}
