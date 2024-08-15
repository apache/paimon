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
import org.apache.paimon.append.AppendOnlyWriter;
import org.apache.paimon.append.BucketedAppendCompactManager;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOExceptionSupplier;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/** {@link FileStoreWrite} for {@link AppendOnlyFileStore}. */
public class AppendOnlyFileStoreWrite extends MemoryFileStoreWrite<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(AppendOnlyFileStoreWrite.class);

    private final FileIO fileIO;
    private final RawFileSplitRead read;
    private final long schemaId;
    private final RowType rowType;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;
    private final long targetFileSize;
    private final int compactionMinFileNum;
    private final int compactionMaxFileNum;
    private final boolean commitForceCompact;
    private final String fileCompression;
    private final String spillCompression;
    private final boolean useWriteBuffer;
    private final boolean spillable;
    private final MemorySize maxDiskSize;
    private final SimpleColStatsCollector.Factory[] statsCollectors;
    private final FileIndexOptions fileIndexOptions;

    private boolean forceBufferSpill = false;
    private boolean skipCompaction;
    private BucketMode bucketMode = BucketMode.HASH_FIXED;

    public AppendOnlyFileStoreWrite(
            FileIO fileIO,
            RawFileSplitRead read,
            long schemaId,
            String commitUser,
            RowType rowType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            String tableName) {
        super(commitUser, snapshotManager, scan, options, null, null, tableName);
        this.fileIO = fileIO;
        this.read = read;
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.fileFormat = options.fileFormat();
        this.pathFactory = pathFactory;
        this.targetFileSize = options.targetFileSize(false);
        this.compactionMinFileNum = options.compactionMinFileNum();
        this.compactionMaxFileNum = options.compactionMaxFileNum().orElse(5);
        this.commitForceCompact = options.commitForceCompact();
        this.skipCompaction = options.writeOnly();
        this.fileCompression = options.fileCompression();
        this.spillCompression = options.spillCompression();
        this.useWriteBuffer = options.useWriteBufferForAppend();
        this.spillable = options.writeBufferSpillable(fileIO.isObjectStore(), isStreamingMode);
        this.maxDiskSize = options.writeBufferSpillDiskSize();
        this.statsCollectors =
                StatsCollectorFactories.createStatsFactories(options, rowType.getFieldNames());
        this.fileIndexOptions = options.indexColumnsOptions();
    }

    @Override
    protected RecordWriter<InternalRow> createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable DeletionVectorsMaintainer ignore) {
        CompactManager compactManager =
                skipCompaction
                        ? new NoopCompactManager()
                        : new BucketedAppendCompactManager(
                                compactExecutor,
                                restoredFiles,
                                compactionMinFileNum,
                                compactionMaxFileNum,
                                targetFileSize,
                                toCompact -> compactRewrite(partition, bucket, toCompact),
                                compactionMetrics == null
                                        ? null
                                        : compactionMetrics.createReporter(partition, bucket));

        return new AppendOnlyWriter(
                fileIO,
                ioManager,
                schemaId,
                fileFormat,
                targetFileSize,
                rowType,
                restoredMaxSeqNumber,
                compactManager,
                // it is only for new files, no dv
                files -> createFilesIterator(partition, bucket, files, null),
                commitForceCompact,
                pathFactory.createDataFilePathFactory(partition, bucket),
                restoreIncrement,
                useWriteBuffer || forceBufferSpill,
                spillable || forceBufferSpill,
                fileCompression,
                spillCompression,
                statsCollectors,
                maxDiskSize,
                fileIndexOptions,
                options.asyncFileWrite());
    }

    /** TODO remove this, and pass deletion vectors. */
    public List<DataFileMeta> compactRewrite(
            BinaryRow partition, int bucket, List<DataFileMeta> toCompact) throws Exception {
        return compactRewrite(partition, bucket, toCompact, null);
    }

    public List<DataFileMeta> compactRewrite(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> toCompact,
            @Nullable List<IOExceptionSupplier<DeletionVector>> dvFactories)
            throws Exception {
        if (toCompact.isEmpty()) {
            return Collections.emptyList();
        }
        Exception collectedExceptions = null;
        RowDataRollingFileWriter rewriter =
                createRollingFileWriter(
                        partition,
                        bucket,
                        new LongCounter(toCompact.get(0).minSequenceNumber()),
                        FileSource.COMPACT);
        try {
            rewriter.write(createFilesIterator(partition, bucket, toCompact, dvFactories));
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                rewriter.close();
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }

        if (collectedExceptions != null) {
            throw collectedExceptions;
        }
        return rewriter.result();
    }

    private RowDataRollingFileWriter createRollingFileWriter(
            BinaryRow partition, int bucket, LongCounter seqNumCounter, FileSource fileSource) {
        return new RowDataRollingFileWriter(
                fileIO,
                schemaId,
                fileFormat,
                targetFileSize,
                rowType,
                pathFactory.createDataFilePathFactory(partition, bucket),
                seqNumCounter,
                fileCompression,
                statsCollectors,
                fileIndexOptions,
                fileSource,
                options.asyncFileWrite());
    }

    private RecordReaderIterator<InternalRow> createFilesIterator(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<IOExceptionSupplier<DeletionVector>> dvFactories)
            throws IOException {
        return new RecordReaderIterator<>(read.createReader(partition, bucket, files, dvFactories));
    }

    public AppendOnlyFileStoreWrite withBucketMode(BucketMode bucketMode) {
        // AppendOnlyFileStoreWrite is sensitive with bucket mode. It will act difference in
        // unaware-bucket mode (no compaction and force empty-writer).
        this.bucketMode = bucketMode;
        if (bucketMode == BucketMode.BUCKET_UNAWARE) {
            super.withIgnorePreviousFiles(true);
            skipCompaction = true;
        }
        return this;
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePrevious) {
        // in unaware bucket mode, we need all writers to be empty
        super.withIgnorePreviousFiles(ignorePrevious || bucketMode == BucketMode.BUCKET_UNAWARE);
    }

    @Override
    protected void forceBufferSpill() throws Exception {
        if (ioManager == null) {
            return;
        }
        forceBufferSpill = true;
        LOG.info(
                "Force buffer spill for append-only file store write, writer number is: {}",
                writers.size());
        for (Map<Integer, WriterContainer<InternalRow>> bucketWriters : writers.values()) {
            for (WriterContainer<InternalRow> writerContainer : bucketWriters.values()) {
                ((AppendOnlyWriter) writerContainer.writer).toBufferedWriter();
            }
        }
    }
}
