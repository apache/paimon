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

package org.apache.paimon.append;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.io.SingleFileWriter.AbortExecutor;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Test comment. */
public class RollingFileWriterWithBlob implements RollingFileWriter<InternalRow, DataFileMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(RollingFileWriterWithBlob.class);

    private final Supplier<MappedWriter<SingleFileWriter<InternalRow, DataFileMeta>, DataFileMeta>>
            writerFactory;
    private final MappedWriter<RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>>
            blobWriter;
    private final long targetFileSize;
    private final List<AbortExecutor> closedWriters;
    private final List<DataFileMeta> results;

    private MappedWriter<SingleFileWriter<InternalRow, DataFileMeta>, DataFileMeta> currentWriter =
            null;
    private long recordCount = 0;
    private boolean closed = false;

    public RollingFileWriterWithBlob(
            FileIO fileIO,
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            LongCounter seqNumCounter,
            String fileCompression,
            StatsCollectorFactories statsCollectorFactories,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore) {
        Pair<RowType, RowType> typeWithBlob = writeSchema.splitBlob();
        RowType normalRowType = typeWithBlob.getLeft();
        RowType blobType = typeWithBlob.getRight();
        List<String> normalColumnNames = normalRowType.getFieldNames();
        final int[] projection = writeSchema.projectIndexes(normalColumnNames);
        this.writerFactory =
                () -> {
                    RowDataFileWriter rowDataFileWriter =
                            new RowDataFileWriter(
                                    fileIO,
                                    RollingFileWriter.createFileWriterContext(
                                            fileFormat,
                                            normalRowType,
                                            statsCollectorFactories.statsCollectors(
                                                    normalColumnNames),
                                            fileCompression),
                                    pathFactory.newPath(),
                                    normalRowType,
                                    schemaId,
                                    seqNumCounter,
                                    fileIndexOptions,
                                    fileSource,
                                    asyncFileWrite,
                                    statsDenseStore,
                                    pathFactory.isExternalPath(),
                                    normalColumnNames);
                    return new MappedWriter<>(rowDataFileWriter, projection);
                };

        BlobFileFormat blobFileFormat = new BlobFileFormat();
        List<String> blobNames = blobType.getFieldNames();
        checkArgument(blobNames.size() == 1, "Limit only one blob fields in one paimon table yet.");
        final int[] blobProjection = writeSchema.projectIndexes(blobNames);
        Supplier<? extends SingleFileWriter<InternalRow, DataFileMeta>> blobWriterSupplier =
                () ->
                        new RowDataFileWriter(
                                fileIO,
                                RollingFileWriter.createFileWriterContext(
                                        blobFileFormat,
                                        blobType,
                                        new SimpleColStatsCollector.Factory[] {
                                            NoneSimpleColStatsCollector::new
                                        },
                                        "none"),
                                pathFactory.newBlobPath(),
                                blobType,
                                schemaId,
                                seqNumCounter,
                                new FileIndexOptions(),
                                fileSource,
                                asyncFileWrite,
                                statsDenseStore,
                                pathFactory.isExternalPath(),
                                blobNames);
        this.blobWriter =
                new MappedWriter<>(
                        new RollingFileWriterImpl<>(blobWriterSupplier, targetFileSize),
                        blobProjection);

        this.targetFileSize = targetFileSize;
        this.results = new ArrayList<>();
        this.closedWriters = new ArrayList<>();
    }

    @VisibleForTesting
    public long targetFileSize() {
        return targetFileSize;
    }

    private boolean rollingFile(boolean forceCheck) throws IOException {
        return currentWriter
                .writer()
                .reachTargetSize(
                        forceCheck || recordCount % CHECK_ROLLING_RECORD_CNT == 0, targetFileSize);
    }

    @Override
    public void write(InternalRow row) throws IOException {
        try {
            // Open the current writer if write the first record or roll over happen before.
            if (currentWriter == null) {
                openCurrentWriter();
            }

            currentWriter.write(row);
            blobWriter.write(row);
            recordCount += 1;

            if (rollingFile(false)) {
                closeCurrentWriter();
            }
        } catch (Throwable e) {
            LOG.warn(
                    "Exception occurs when writing file "
                            + (currentWriter == null ? null : currentWriter.writer().path())
                            + ". Cleaning up.",
                    e);
            abort();
            throw e;
        }
    }

    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        // TODO: support bundle projection
        for (InternalRow row : bundle) {
            write(row);
        }
    }

    private void openCurrentWriter() {
        currentWriter = writerFactory.get();
    }

    private void closeCurrentWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }

        currentWriter.close();
        closedWriters.add(currentWriter.writer().abortExecutor());
        DataFileMeta mainDataFileMeta = currentWriter.result();

        blobWriter.close();
        List<DataFileMeta> blobFileMetas = blobWriter.result();
        List<DataFileMeta> blobTaggedMetas = new ArrayList<>();
        for (int i = 0; i < blobFileMetas.size() - 1; i++) {
            blobTaggedMetas.add(
                    blobFileMetas.get(i).assignFileTag(DataFileMeta.FileTag.BLOB_ENTRY));
        }
        blobTaggedMetas.add(
                blobFileMetas
                        .get(blobFileMetas.size() - 1)
                        .assignFileTag(DataFileMeta.FileTag.BLOB_TAIL));

        if (mainDataFileMeta.rowCount()
                != blobTaggedMetas.stream().mapToLong(DataFileMeta::rowCount).sum()) {
            throw new IllegalStateException(
                    "The row count of main file and blob files does not match. "
                            + "Main file: "
                            + mainDataFileMeta
                            + ", blob files: "
                            + blobTaggedMetas);
        }

        results.add(mainDataFileMeta);
        results.addAll(blobTaggedMetas);
        currentWriter = null;
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public void abort() {
        if (currentWriter != null) {
            currentWriter.abort();
        }
        for (AbortExecutor abortExecutor : closedWriters) {
            abortExecutor.abort();
        }
        blobWriter.abort();
    }

    @Override
    public List<DataFileMeta> result() {
        Preconditions.checkState(closed, "Cannot access the results unless close all writers.");
        return results;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            closeCurrentWriter();
        } catch (IOException e) {
            LOG.warn(
                    "Exception occurs when writing file "
                            + currentWriter.writer().path()
                            + ". Cleaning up.",
                    e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }
}
