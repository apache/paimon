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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.FileWriterAbortExecutor;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A rolling file writer that handles both normal data and blob data. This writer creates separate
 * files for normal columns and blob columns, managing their lifecycle and ensuring consistency
 * between them.
 *
 * <pre>
 * For example,
 * given a table schema with normal columns (id INT, name STRING) and a blob column (data BLOB),
 * this writer will create separate files for (id, name) and (data).
 * It will roll files based on the specified target file size, ensuring that both normal and blob
 * files are rolled simultaneously.
 *
 * Every time a file is rolled, the writer will close the current normal data file and blob data files,
 * so one normal data file may correspond to multiple blob data files.
 *
 * Normal file1: f1.parquet may including (b1.blob, b2.blob, b3.blob)
 * Normal file2: f1-2.parquet may including (b4.blob, b5.blob)
 *
 * </pre>
 */
public class RollingBlobFileWriter implements RollingFileWriter<InternalRow, DataFileMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(RollingBlobFileWriter.class);

    /** Constant for checking rolling condition periodically. */
    private static final long CHECK_ROLLING_RECORD_CNT = 1000L;

    /** Expected number of blob fields in a table. */
    private static final int EXPECTED_BLOB_FIELD_COUNT = 1;

    // Core components
    private final Supplier<
                    PeojectedFileWriter<SingleFileWriter<InternalRow, DataFileMeta>, DataFileMeta>>
            writerFactory;
    private final Supplier<
                    PeojectedFileWriter<
                            RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>>>
            blobWriterFactory;
    private final long targetFileSize;
    private final long blobTargetFileSize;

    // State management
    private final List<FileWriterAbortExecutor> closedWriters;
    private final List<DataFileMeta> results;
    private PeojectedFileWriter<SingleFileWriter<InternalRow, DataFileMeta>, DataFileMeta>
            currentWriter;
    private PeojectedFileWriter<
                    RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>>
            blobWriter;
    private long recordCount = 0;
    private boolean closed = false;

    public RollingBlobFileWriter(
            FileIO fileIO,
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            long blobTargetFileSize,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            LongCounter seqNumCounter,
            String fileCompression,
            StatsCollectorFactories statsCollectorFactories,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore) {

        // Initialize basic fields
        this.targetFileSize = targetFileSize;
        this.blobTargetFileSize = blobTargetFileSize;
        this.results = new ArrayList<>();
        this.closedWriters = new ArrayList<>();

        // Split schema into normal and blob parts
        Pair<RowType, RowType> typeWithBlob = BlobType.splitBlob(writeSchema);
        RowType normalRowType = typeWithBlob.getLeft();
        RowType blobType = typeWithBlob.getRight();

        // Initialize writer factory for normal data
        this.writerFactory =
                createNormalWriterFactory(
                        fileIO,
                        schemaId,
                        fileFormat,
                        normalRowType,
                        writeSchema,
                        pathFactory,
                        seqNumCounter,
                        fileCompression,
                        statsCollectorFactories,
                        fileIndexOptions,
                        fileSource,
                        asyncFileWrite,
                        statsDenseStore);

        // Initialize blob writer
        this.blobWriterFactory =
                () ->
                        createBlobWriter(
                                fileIO,
                                schemaId,
                                blobType,
                                writeSchema,
                                pathFactory,
                                seqNumCounter,
                                fileSource,
                                asyncFileWrite,
                                statsDenseStore,
                                blobTargetFileSize);
    }

    /** Creates a factory for normal data writers. */
    private static Supplier<
                    PeojectedFileWriter<SingleFileWriter<InternalRow, DataFileMeta>, DataFileMeta>>
            createNormalWriterFactory(
                    FileIO fileIO,
                    long schemaId,
                    FileFormat fileFormat,
                    RowType normalRowType,
                    RowType writeSchema,
                    DataFilePathFactory pathFactory,
                    LongCounter seqNumCounter,
                    String fileCompression,
                    StatsCollectorFactories statsCollectorFactories,
                    FileIndexOptions fileIndexOptions,
                    FileSource fileSource,
                    boolean asyncFileWrite,
                    boolean statsDenseStore) {

        List<String> normalColumnNames = normalRowType.getFieldNames();
        int[] projectionNormalFields = writeSchema.projectIndexes(normalColumnNames);

        return () -> {
            RowDataFileWriter rowDataFileWriter =
                    new RowDataFileWriter(
                            fileIO,
                            RollingFileWriter.createFileWriterContext(
                                    fileFormat,
                                    normalRowType,
                                    statsCollectorFactories.statsCollectors(normalColumnNames),
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
            return new PeojectedFileWriter<>(rowDataFileWriter, projectionNormalFields);
        };
    }

    /** Creates a blob writer for handling blob data. */
    private static PeojectedFileWriter<
                    RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>>
            createBlobWriter(
                    FileIO fileIO,
                    long schemaId,
                    RowType blobType,
                    RowType writeSchema,
                    DataFilePathFactory pathFactory,
                    LongCounter seqNumCounter,
                    FileSource fileSource,
                    boolean asyncFileWrite,
                    boolean statsDenseStore,
                    long targetFileSize) {

        BlobFileFormat blobFileFormat = new BlobFileFormat();
        List<String> blobNames = blobType.getFieldNames();

        // Validate blob field count
        checkArgument(
                blobNames.size() == EXPECTED_BLOB_FIELD_COUNT,
                "Limit exactly one blob fields in one paimon table yet.");

        int[] blobProjection = writeSchema.projectIndexes(blobNames);
        return new PeojectedFileWriter<>(
                new RollingFileWriterImpl<>(
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
                                        blobNames),
                        targetFileSize),
                blobProjection);
    }

    /**
     * Writes a single row to both normal and blob writers. Automatically handles file rolling when
     * target size is reached.
     *
     * @param row The row to write
     * @throws IOException if writing fails
     */
    @Override
    public void write(InternalRow row) throws IOException {
        try {
            if (currentWriter == null) {
                currentWriter = writerFactory.get();
            }
            if (blobWriter == null) {
                blobWriter = blobWriterFactory.get();
            }
            currentWriter.write(row);
            blobWriter.write(row);
            recordCount++;

            if (rollingFile()) {
                closeCurrentWriter();
            }
        } catch (Throwable e) {
            handleWriteException(e);
            throw e;
        }
    }

    /** Handles write exceptions by logging and cleaning up resources. */
    private void handleWriteException(Throwable e) {
        String filePath = (currentWriter == null) ? null : currentWriter.writer().path().toString();
        LOG.warn("Exception occurs when writing file {}. Cleaning up.", filePath, e);
        abort();
    }

    /**
     * Writes a bundle of records by iterating through each row.
     *
     * @param bundle The bundle of records to write
     * @throws IOException if writing fails
     */
    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        // TODO: support bundle projection
        for (InternalRow row : bundle) {
            write(row);
        }
    }

    /**
     * Returns the total number of records written.
     *
     * @return the record count
     */
    @Override
    public long recordCount() {
        return recordCount;
    }

    /**
     * Aborts all writers and cleans up resources. This method should be called when an error occurs
     * during writing.
     */
    @Override
    public void abort() {
        if (currentWriter != null) {
            currentWriter.abort();
            currentWriter = null;
        }
        for (FileWriterAbortExecutor abortExecutor : closedWriters) {
            abortExecutor.abort();
        }
        if (blobWriter != null) {
            blobWriter.abort();
            blobWriter = null;
        }
    }

    /** Checks if the current file should be rolled based on size and record count. */
    private boolean rollingFile() throws IOException {
        return currentWriter
                .writer()
                .reachTargetSize(recordCount % CHECK_ROLLING_RECORD_CNT == 0, targetFileSize);
    }

    /**
     * Closes the current writer and processes the results. Validates consistency between main and
     * blob files.
     *
     * @throws IOException if closing fails
     */
    private void closeCurrentWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }

        // Close main writer and get metadata
        DataFileMeta mainDataFileMeta = closeMainWriter();

        // Close blob writer and process blob metadata
        List<DataFileMeta> blobMetas = closeBlobWriter();

        // Validate consistency between main and blob files
        validateFileConsistency(mainDataFileMeta, blobMetas);

        // Add results to the results list
        results.add(mainDataFileMeta);
        results.addAll(blobMetas);

        // Reset current writer
        currentWriter = null;
    }

    /** Closes the main writer and returns its metadata. */
    private DataFileMeta closeMainWriter() throws IOException {
        currentWriter.close();
        closedWriters.add(currentWriter.writer().abortExecutor());
        return currentWriter.result();
    }

    /** Closes the blob writer and processes blob metadata with appropriate tags. */
    private List<DataFileMeta> closeBlobWriter() throws IOException {
        if (blobWriter == null) {
            return Collections.emptyList();
        }
        blobWriter.close();
        List<DataFileMeta> results = blobWriter.result();
        blobWriter = null;
        return results;
    }

    /** Validates that the row counts match between main and blob files. */
    private void validateFileConsistency(
            DataFileMeta mainDataFileMeta, List<DataFileMeta> blobTaggedMetas) {
        long mainRowCount = mainDataFileMeta.rowCount();
        long blobRowCount = blobTaggedMetas.stream().mapToLong(DataFileMeta::rowCount).sum();

        if (mainRowCount != blobRowCount) {
            throw new IllegalStateException(
                    String.format(
                            "This is a bug: The row count of main file and blob files does not match. "
                                    + "Main file: %s (row count: %d), blob files: %s (total row count: %d)",
                            mainDataFileMeta, mainRowCount, blobTaggedMetas, blobRowCount));
        }
    }

    /**
     * Returns the list of file metadata for all written files. This method can only be called after
     * the writer has been closed.
     *
     * @return list of file metadata
     * @throws IllegalStateException if the writer is not closed
     */
    @Override
    public List<DataFileMeta> result() {
        Preconditions.checkState(closed, "Cannot access the results unless close all writers.");
        return results;
    }

    /**
     * Closes the writer and finalizes all files. This method ensures proper cleanup and validation
     * of written data.
     *
     * @throws IOException if closing fails
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            closeCurrentWriter();
        } catch (IOException e) {
            handleCloseException(e);
            throw e;
        } finally {
            closed = true;
        }
    }

    /** Handles exceptions that occur during closing. */
    private void handleCloseException(IOException e) {
        String filePath = (currentWriter == null) ? null : currentWriter.writer().path().toString();
        LOG.warn("Exception occurs when writing file {}. Cleaning up.", filePath, e);
        abort();
    }
}
