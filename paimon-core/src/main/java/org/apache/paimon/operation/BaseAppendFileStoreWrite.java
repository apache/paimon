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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.apache.paimon.format.FileFormat.fileFormat;
import static org.apache.paimon.utils.StatsCollectorFactories.createStatsFactories;

/** {@link FileStoreWrite} for {@link AppendOnlyFileStore}. */
public abstract class BaseAppendFileStoreWrite extends MemoryFileStoreWrite<InternalRow>
        implements BundleFileStoreWriter {

    private static final Logger LOG = LoggerFactory.getLogger(BaseAppendFileStoreWrite.class);

    private final FileIO fileIO;
    private final RawFileSplitRead readForCompact;
    private final long schemaId;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;
    private final FileIndexOptions fileIndexOptions;
    private final RowType rowType;

    private RowType writeType;
    private @Nullable List<String> writeCols;
    private boolean forceBufferSpill = false;

    public BaseAppendFileStoreWrite(
            FileIO fileIO,
            RawFileSplitRead readForCompact,
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
        this.readForCompact = readForCompact;
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.writeType = rowType;
        this.writeCols = null;
        this.fileFormat = fileFormat(options);
        this.pathFactory = pathFactory;

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
            @Nullable DeletionVectorsMaintainer dvMaintainer) {
        return new AppendOnlyWriter(
                fileIO,
                ioManager,
                schemaId,
                fileFormat,
                options.targetFileSize(false),
                writeType,
                writeCols,
                restoredMaxSeqNumber,
                getCompactManager(partition, bucket, restoredFiles, compactExecutor, dvMaintainer),
                // it is only for new files, no dv
                files -> createFilesIterator(partition, bucket, files, null),
                options.commitForceCompact(),
                pathFactory.createDataFilePathFactory(partition, bucket),
                restoreIncrement,
                options.useWriteBufferForAppend() || forceBufferSpill,
                options.writeBufferSpillable() || forceBufferSpill,
                options.fileCompression(),
                options.spillCompressOptions(),
                statsCollectors(),
                options.writeBufferSpillDiskSize(),
                fileIndexOptions,
                options.asyncFileWrite(),
                options.statsDenseStore());
    }

    @Override
    public void withWriteType(RowType writeType) {
        this.writeType = writeType;
        int fullCount = rowType.getFieldCount();
        List<String> fullNames = rowType.getFieldNames();
        this.writeCols = writeType.getFieldNames();
        // optimize writeCols to null in following cases:
        // 1. writeType contains all columns
        // 2. writeType contains all columns and append _ROW_ID cols
        if (writeCols.size() >= fullCount && writeCols.subList(0, fullCount).equals(fullNames)) {
            writeCols = null;
        }
    }

    private SimpleColStatsCollector.Factory[] statsCollectors() {
        return createStatsFactories(options.statsMode(), options, writeType.getFieldNames());
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
        Map<String, IOExceptionSupplier<DeletionVector>> dvFactories = null;
        if (dvFactory != null) {
            dvFactories = new HashMap<>();
            for (DataFileMeta file : toCompact) {
                dvFactories.put(file.fileName(), () -> dvFactory.apply(file.fileName()));
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
                writeType,
                pathFactory.createDataFilePathFactory(partition, bucket),
                seqNumCounter,
                options.fileCompression(),
                statsCollectors(),
                fileIndexOptions,
                FileSource.COMPACT,
                options.asyncFileWrite(),
                options.statsDenseStore(),
                rowType.equals(writeType) ? null : writeType.getFieldNames());
    }

    private RecordReaderIterator<InternalRow> createFilesIterator(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable Map<String, IOExceptionSupplier<DeletionVector>> dvFactories)
            throws IOException {
        return new RecordReaderIterator<>(
                readForCompact.createReader(partition, bucket, files, dvFactories));
    }

    @Override
    protected void forceBufferSpill() throws Exception {
        if (ioManager == null) {
            return;
        }
        if (forceBufferSpill) {
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
