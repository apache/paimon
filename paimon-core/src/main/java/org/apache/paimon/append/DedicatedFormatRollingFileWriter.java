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
import org.apache.paimon.operation.BlobFileContext;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.types.BlobType.fieldNamesInBlobFile;
import static org.apache.paimon.types.VectorType.fieldNamesInVectorFile;
import static org.apache.paimon.types.VectorType.fieldsInVectorFile;

/**
 * A rolling file writer that supports dedicated file formats for different column types. This
 * writer creates separate files for normal columns, blob columns, vector columns, and external
 * storage blob columns, managing their lifecycle and ensuring consistency between them.
 *
 * <p>The writer handles four types of data:
 *
 * <ul>
 *   <li><b>Normal columns</b>: Stored in the main data file using the configured file format (e.g.,
 *       parquet)
 *   <li><b>Blob columns</b>: Stored in dedicated blob files within blob file format
 *   <li><b>Vector columns</b>: Stored in dedicated vector files using a specialized format (e.g.,
 *       lance)
 *   <li><b>External storage blob columns</b>: Stored in external storage locations for blob data
 * </ul>
 *
 * <p>File rolling is triggered based on the target file size for each type. When rolling occurs,
 * all active writers are closed together to maintain consistency. One normal data file may
 * correspond to multiple blob/vector files.
 *
 * <pre>
 * Example file structure:
 *   Normal file: f1.parquet (id, name columns)
 *   Blob files:  blob/b1.blob, blob/b2.blob (data BLOB column)
 *   Vector file: vector/v1.lance (embedding VECTOR column)
 *   External blob: stored in external storage path
 *
 * Rolling behavior:
 *   Normal file1: f1.parquet -&gt; blob files (b1.blob, b2.blob)
 *   Normal file2: f2.parquet -&gt; blob files (b3.blob)
 * </pre>
 *
 * <p>This writer is primarily used for append-only tables that require specialized storage formats
 * for certain column types (multimodal data, vector embeddings, etc.).
 */
public class DedicatedFormatRollingFileWriter
        implements RollingFileWriter<InternalRow, DataFileMeta> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DedicatedFormatRollingFileWriter.class);

    /** Constant for checking rolling condition periodically. */
    private static final long CHECK_ROLLING_RECORD_CNT = 1000L;

    // Core components
    private final Supplier<
                    ProjectedFileWriter<SingleFileWriter<InternalRow, DataFileMeta>, DataFileMeta>>
            writerFactory;
    private final @Nullable Supplier<MultipleBlobFileWriter> blobWriterFactory;
    private final @Nullable Supplier<
                    ProjectedFileWriter<
                            RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>>>
            vectorStoreWriterFactory;
    private final long targetFileSize;
    private final @Nullable ExternalStorageBlobWriter externalStorageBlobWriter;

    // State management
    private final List<FileWriterAbortExecutor> closedWriters;
    private final List<DataFileMeta> results;

    private ProjectedFileWriter<SingleFileWriter<InternalRow, DataFileMeta>, DataFileMeta>
            currentWriter;
    private MultipleBlobFileWriter blobWriter;
    private ProjectedFileWriter<
                    RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>>
            vectorStoreWriter;
    private long recordCount = 0;
    private boolean closed = false;

    public DedicatedFormatRollingFileWriter(
            FileIO fileIO,
            long schemaId,
            FileFormat fileFormat,
            @Nullable FileFormat vectorFileFormat,
            long targetFileSize,
            long blobTargetFileSize,
            long vectorTargetFileSize,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            Supplier<LongCounter> seqNumCounterSupplier,
            String fileCompression,
            StatsCollectorFactories statsCollectorFactories,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean statsDenseStore,
            @Nullable BlobFileContext context) {
        // Initialize basic fields
        this.targetFileSize = targetFileSize;
        this.results = new ArrayList<>();
        this.closedWriters = new ArrayList<>();

        // blob write does not need async write, cause blob write is I/O-intensive, not
        // CPU-intensive process.
        // Async write will cost 64MB more memory per write task, blob writes get low
        // benefit from async write, but cost a lot.
        boolean asyncFileWrite = false;

        Set<String> fieldsInDedicatedFile =
                fieldNamesInVectorFile(writeSchema, vectorFileFormat != null);
        if (context != null) {
            fieldsInDedicatedFile.addAll(
                    fieldNamesInBlobFile(writeSchema, context.blobDescriptorFields()));
        }
        List<DataField> fieldsInNormalFile = new ArrayList<>();
        for (DataField field : writeSchema.getFields()) {
            if (!fieldsInDedicatedFile.contains(field.name())) {
                fieldsInNormalFile.add(field);
            }
        }

        this.writerFactory =
                createNormalWriterFactory(
                        fileIO,
                        schemaId,
                        fileFormat,
                        fieldsInNormalFile,
                        writeSchema,
                        pathFactory,
                        seqNumCounterSupplier,
                        fileCompression,
                        statsCollectorFactories,
                        fileIndexOptions,
                        fileSource,
                        asyncFileWrite,
                        statsDenseStore);

        if (context != null) {
            this.blobWriterFactory =
                    () ->
                            new MultipleBlobFileWriter(
                                    fileIO,
                                    schemaId,
                                    writeSchema,
                                    pathFactory,
                                    seqNumCounterSupplier,
                                    fileSource,
                                    asyncFileWrite,
                                    statsDenseStore,
                                    blobTargetFileSize,
                                    context.blobConsumer(),
                                    context.blobDescriptorFields());
        } else {
            this.blobWriterFactory = null;
        }

        if (context != null && !context.blobExternalStorageFields().isEmpty()) {
            this.externalStorageBlobWriter =
                    new ExternalStorageBlobWriter(
                            fileIO,
                            schemaId,
                            writeSchema,
                            context.blobExternalStorageFields(),
                            context.blobExternalStoragePath(),
                            pathFactory,
                            seqNumCounterSupplier,
                            fileSource,
                            asyncFileWrite,
                            statsDenseStore,
                            blobTargetFileSize);
        } else {
            this.externalStorageBlobWriter = null;
        }

        // Initialize vector-store writer
        List<DataField> fieldsInVectorFile =
                fieldsInVectorFile(writeSchema, vectorFileFormat != null);
        if (!fieldsInVectorFile.isEmpty()) {
            List<String> vectorFieldNames =
                    fieldsInVectorFile.stream().map(DataField::name).collect(Collectors.toList());
            this.vectorStoreWriterFactory =
                    () ->
                            createVectorStoreWriter(
                                    fileIO,
                                    schemaId,
                                    vectorFileFormat,
                                    fieldsInVectorFile,
                                    writeSchema,
                                    pathFactory,
                                    seqNumCounterSupplier,
                                    fileCompression,
                                    statsCollectorFactories.statsCollectors(vectorFieldNames),
                                    fileSource,
                                    asyncFileWrite,
                                    statsDenseStore,
                                    vectorTargetFileSize);
        } else {
            this.vectorStoreWriterFactory = null;
        }
    }

    /** Creates a factory for normal data writers. */
    private static Supplier<
                    ProjectedFileWriter<SingleFileWriter<InternalRow, DataFileMeta>, DataFileMeta>>
            createNormalWriterFactory(
                    FileIO fileIO,
                    long schemaId,
                    FileFormat fileFormat,
                    List<DataField> fieldsInNormalFile,
                    RowType writeSchema,
                    DataFilePathFactory pathFactory,
                    Supplier<LongCounter> seqNumCounterSupplier,
                    String fileCompression,
                    StatsCollectorFactories statsCollectorFactories,
                    FileIndexOptions fileIndexOptions,
                    FileSource fileSource,
                    boolean asyncFileWrite,
                    boolean statsDenseStore) {
        RowType normalRowType = new RowType(fieldsInNormalFile);
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
                            seqNumCounterSupplier,
                            fileIndexOptions,
                            fileSource,
                            asyncFileWrite,
                            statsDenseStore,
                            pathFactory.isExternalPath(),
                            normalColumnNames);
            return new ProjectedFileWriter<>(rowDataFileWriter, projectionNormalFields);
        };
    }

    /** Creates a vector-store writer for handling vector-store data. */
    private static ProjectedFileWriter<
                    RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>>
            createVectorStoreWriter(
                    FileIO fileIO,
                    long schemaId,
                    FileFormat vectorFileFormat,
                    List<DataField> fieldsInVectorStore,
                    RowType writeSchema,
                    DataFilePathFactory pathFactory,
                    Supplier<LongCounter> seqNumCounterSupplier,
                    String fileCompression,
                    SimpleColStatsCollector.Factory[] statsCollectors,
                    FileSource fileSource,
                    boolean asyncFileWrite,
                    boolean statsDenseStore,
                    long targetFileSize) {
        RowType vectorStoreRowType = new RowType(fieldsInVectorStore);
        List<String> vectorStoreColumnNames = vectorStoreRowType.getFieldNames();
        int[] vectorStoreProjection = writeSchema.projectIndexes(vectorStoreColumnNames);
        return new ProjectedFileWriter<>(
                new RollingFileWriterImpl<>(
                        () ->
                                new RowDataFileWriter(
                                        fileIO,
                                        RollingFileWriter.createFileWriterContext(
                                                vectorFileFormat,
                                                vectorStoreRowType,
                                                statsCollectors,
                                                fileCompression),
                                        pathFactory.newVectorPath(),
                                        vectorStoreRowType,
                                        schemaId,
                                        seqNumCounterSupplier,
                                        new FileIndexOptions(),
                                        fileSource,
                                        asyncFileWrite,
                                        statsDenseStore,
                                        pathFactory.isExternalPath(),
                                        vectorStoreColumnNames),
                        targetFileSize),
                vectorStoreProjection);
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
            // Transform descriptor BLOB fields backed by external storage first.
            InternalRow transformedRow =
                    externalStorageBlobWriter != null
                            ? externalStorageBlobWriter.transformRow(row)
                            : row;

            if (currentWriter == null) {
                currentWriter = writerFactory.get();
            }
            if ((blobWriter == null) && (blobWriterFactory != null)) {
                blobWriter = blobWriterFactory.get();
            }
            if ((vectorStoreWriter == null) && (vectorStoreWriterFactory != null)) {
                vectorStoreWriter = vectorStoreWriterFactory.get();
            }
            if (blobWriter != null) {
                blobWriter.write(transformedRow);
            }
            if (vectorStoreWriter != null) {
                vectorStoreWriter.write(transformedRow);
            }
            currentWriter.write(transformedRow);
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
        if (vectorStoreWriter != null) {
            vectorStoreWriter.abort();
            vectorStoreWriter = null;
        }
        if (externalStorageBlobWriter != null) {
            externalStorageBlobWriter.abort();
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

        // Close vector-store writer and process vector-store metadata
        List<DataFileMeta> vectorStoreMetas = closeVectorStoreWriter();

        // Validate consistency between main and blob files
        validateFileConsistency(mainDataFileMeta, blobMetas, vectorStoreMetas);

        // Add results to the results list
        results.add(mainDataFileMeta);
        results.addAll(blobMetas);
        results.addAll(vectorStoreMetas);

        // Reset current writer
        currentWriter = null;
    }

    /** Closes the main writer and returns its metadata. */
    private DataFileMeta closeMainWriter() throws IOException {
        currentWriter.close();
        currentWriter.writer().abortExecutor().ifPresent(closedWriters::add);
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

    /** Closes the vector-store writer and processes blob metadata with appropriate tags. */
    private List<DataFileMeta> closeVectorStoreWriter() throws IOException {
        if (vectorStoreWriter == null) {
            return Collections.emptyList();
        }
        vectorStoreWriter.close();
        List<DataFileMeta> results = vectorStoreWriter.result();
        vectorStoreWriter = null;
        return results;
    }

    /** Validates that the row counts match between main and blob files. */
    private void validateFileConsistency(
            DataFileMeta mainDataFileMeta,
            List<DataFileMeta> blobTaggedMetas,
            List<DataFileMeta> vectorStoreMetas) {
        long mainRowCount = mainDataFileMeta.rowCount();

        Map<String, Long> blobRowCounts = new HashMap<>();
        for (DataFileMeta file : blobTaggedMetas) {
            long count = file.rowCount();
            blobRowCounts.compute(file.writeCols().get(0), (k, v) -> v == null ? count : v + count);
        }
        long vectorStoreRowCount =
                vectorStoreMetas.stream().mapToLong(DataFileMeta::rowCount).sum();

        for (String blobFieldName : blobRowCounts.keySet()) {
            long blobRowCount = blobRowCounts.get(blobFieldName);
            if (mainRowCount != blobRowCount) {
                throw new IllegalStateException(
                        String.format(
                                "This is a bug: The row count of main file and blob file does not match. "
                                        + "Main file: %s (row count: %d), blob field name: %s (row count: %d)",
                                mainDataFileMeta, mainRowCount, blobFieldName, blobRowCount));
            }
        }
        if (!vectorStoreMetas.isEmpty() && (mainRowCount != vectorStoreRowCount)) {
            throw new IllegalStateException(
                    String.format(
                            "This is a bug: The row count of main file and vector-store files does not match. "
                                    + "Main file: %s (row count: %d), vector-store files: %s (total row count: %d)",
                            mainDataFileMeta, mainRowCount, vectorStoreMetas, vectorStoreRowCount));
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
            if (externalStorageBlobWriter != null) {
                externalStorageBlobWriter.close();
            }
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
