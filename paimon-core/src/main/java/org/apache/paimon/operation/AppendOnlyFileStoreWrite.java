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
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.statistics.SimpleColStatsCollector;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/** {@link FileStoreWrite} for {@link AppendOnlyFileStore}. */
public abstract class AppendOnlyFileStoreWrite extends MemoryFileStoreWrite<InternalRow>
        implements BundleFileStoreWriter {

    private static final Logger LOG = LoggerFactory.getLogger(AppendOnlyFileStoreWrite.class);

    private final FileIO fileIO;
    private final RawFileSplitRead read;
    private final long schemaId;
    private final RowType rowType;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;

    private final SimpleColStatsCollector.Factory[] statsCollectors;
    private final FileIndexOptions fileIndexOptions;
    private boolean forceBufferSpill = false;

    public AppendOnlyFileStoreWrite(
            FileIO fileIO,
            RawFileSplitRead read,
            long schemaId,
            RowType rowType,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            @Nullable DeletionVectorsMaintainer.Factory dvMaintainerFactory,
            String tableName) {
        super(snapshotManager, scan, options, partitionType, null, dvMaintainerFactory, tableName);
        this.fileIO = fileIO;
        this.read = read;
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.fileFormat = options.fileFormat();
        this.pathFactory = pathFactory;

        this.statsCollectors =
                StatsCollectorFactories.createStatsFactories(options, rowType.getFieldNames());
        this.fileIndexOptions = options.indexColumnsOptions();
    }

    @Override
    protected RecordWriter<InternalRow> createWriter(
            @Nullable Long snapshotId,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable DeletionVectorsMaintainer dvMaintainer) {
        return new AppendOnlyWriter(
                fileIO,
                ioManager,
                schemaId,
                fileFormat,
                options.targetFileSize(false),
                rowType,
                restoredMaxSeqNumber,
                getCompactManager(partition, bucket, restoredFiles, compactExecutor, dvMaintainer),
                // it is only for new files, no dv
                files -> createFilesIterator(partition, bucket, files, null),
                options.commitForceCompact(),
                pathFactory.createDataFilePathFactory(partition, bucket),
                restoreIncrement,
                options.useWriteBufferForAppend() || forceBufferSpill,
                options.writeBufferSpillable(fileIO.isObjectStore(), isStreamingMode)
                        || forceBufferSpill,
                options.fileCompression(),
                options.spillCompressOptions(),
                statsCollectors,
                options.writeBufferSpillDiskSize(),
                fileIndexOptions,
                options.asyncFileWrite(),
                options.statsDenseStore());
    }

    protected abstract CompactManager getCompactManager(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            ExecutorService compactExecutor,
            @Nullable DeletionVectorsMaintainer dvMaintainer);

    public List<DataFileMeta> compactRewrite(
            BinaryRow partition,
            int bucket,
            @Nullable Function<String, DeletionVector> dvFactory,
            List<DataFileMeta> toCompact)
            throws Exception {
        if (toCompact.isEmpty()) {
            return Collections.emptyList();
        }
        Exception collectedExceptions = null;
        RowDataRollingFileWriter rewriter =
                createRollingFileWriter(
                        partition, bucket, new LongCounter(toCompact.get(0).minSequenceNumber()));
        List<IOExceptionSupplier<DeletionVector>> dvFactories = null;
        if (dvFactory != null) {
            dvFactories = new ArrayList<>(toCompact.size());
            for (DataFileMeta file : toCompact) {
                dvFactories.add(() -> dvFactory.apply(file.fileName()));
            }
        }
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
            BinaryRow partition, int bucket, LongCounter seqNumCounter) {
        return new RowDataRollingFileWriter(
                fileIO,
                schemaId,
                fileFormat,
                options.targetFileSize(false),
                rowType,
                pathFactory.createDataFilePathFactory(partition, bucket),
                seqNumCounter,
                options.fileCompression(),
                statsCollectors,
                fileIndexOptions,
                FileSource.COMPACT,
                options.asyncFileWrite(),
                options.statsDenseStore());
    }

    private RecordReaderIterator<InternalRow> createFilesIterator(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<IOExceptionSupplier<DeletionVector>> dvFactories)
            throws IOException {
        return new RecordReaderIterator<>(read.createReader(partition, bucket, files, dvFactories));
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

    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle)
            throws Exception {
        WriterContainer<InternalRow> container = getWriterWrapper(partition, bucket);
        ((AppendOnlyWriter) container.writer).writeBundle(bundle);
    }
}
