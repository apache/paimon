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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.casting.FallbackMappingRow;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.UriReader;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A writer for descriptor BLOB fields that write raw data to a configured external storage path.
 *
 * <p>For each configured field, this writer writes BLOB data to the external storage path using
 * {@link BlobFileFormat} (standard blob format with CRC32 and index), then replaces the BLOB value
 * with a {@link BlobDescriptor}.
 *
 * <p>The external storage path is configured via {@link CoreOptions#BLOB_EXTERNAL_STORAGE_PATH}.
 * Internally, each field writer reuses the same {@link ProjectedFileWriter} + {@link
 * RollingFileWriterImpl} infrastructure as {@link MultipleBlobFileWriter}, with rolling support.
 * The produced {@link DataFileMeta} is discarded since these files live outside Paimon's
 * management.
 */
public class ExternalStorageBlobWriter implements Closeable {

    private final List<ExternalStorageBlobFieldWriter> fieldWriters;
    private final UriReader uriReader;
    private final GenericRow overrideRow;
    private final FallbackMappingRow resultRow;

    public ExternalStorageBlobWriter(
            FileIO fileIO,
            long schemaId,
            RowType writeSchema,
            Set<String> externalStorageFields,
            String externalStoragePath,
            DataFilePathFactory pathFactory,
            Supplier<LongCounter> seqNumCounterSupplier,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            long targetFileSize) {
        checkNotNull(
                externalStoragePath,
                "'%s' must be set when '%s' is configured.",
                CoreOptions.BLOB_EXTERNAL_STORAGE_PATH.key(),
                CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key());

        this.fieldWriters =
                createFieldWriters(
                        fileIO,
                        schemaId,
                        writeSchema,
                        externalStorageFields,
                        externalStoragePath,
                        pathFactory,
                        seqNumCounterSupplier,
                        fileSource,
                        asyncFileWrite,
                        statsDenseStore,
                        targetFileSize);
        this.uriReader = UriReader.fromFile(fileIO);

        int fieldCount = writeSchema.getFieldCount();
        this.overrideRow = new GenericRow(fieldCount);
        // identity mappings: position i maps to position i
        this.resultRow = new FallbackMappingRow(IntStream.range(0, fieldCount).toArray());
    }

    /**
     * Transform a row by writing BLOB data for configured fields to the external storage path and
     * replacing them with {@link BlobDescriptor} references.
     *
     * @return a new row with configured fields replaced, or the original row if none configured
     */
    public InternalRow transformRow(InternalRow row) throws IOException {
        if (fieldWriters.isEmpty()) {
            return row;
        }

        // clear all override positions so non-overridden fields fall back to delegate
        for (ExternalStorageBlobFieldWriter fw : fieldWriters) {
            overrideRow.setField(fw.fieldIndex(), null);
        }

        for (ExternalStorageBlobFieldWriter fw : fieldWriters) {
            BlobDescriptor descriptor = fw.writeAndReplace(row);
            if (descriptor != null) {
                overrideRow.setField(fw.fieldIndex(), Blob.fromDescriptor(uriReader, descriptor));
            }
        }

        // override row as main (non-null wins), original row as fallback
        return resultRow.replace(overrideRow, row);
    }

    @Override
    public void close() throws IOException {
        for (ExternalStorageBlobFieldWriter fw : fieldWriters) {
            fw.close();
        }
    }

    public void abort() {
        for (ExternalStorageBlobFieldWriter fw : fieldWriters) {
            fw.abort();
        }
    }

    // ------------------------------ Helper methods ------------------------------

    private static List<ExternalStorageBlobFieldWriter> createFieldWriters(
            FileIO fileIO,
            long schemaId,
            RowType writeSchema,
            Set<String> externalStorageFields,
            String externalStoragePath,
            DataFilePathFactory pathFactory,
            Supplier<LongCounter> seqNumCounterSupplier,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            long targetFileSize) {
        List<ExternalStorageBlobFieldWriter> writers = new ArrayList<>();
        for (DataField field : writeSchema.getFields()) {
            if (field.type().getTypeRoot() == DataTypeRoot.BLOB
                    && externalStorageFields.contains(field.name())) {
                writers.add(
                        createFieldWriter(
                                fileIO,
                                schemaId,
                                writeSchema,
                                field.name(),
                                externalStoragePath,
                                pathFactory,
                                seqNumCounterSupplier,
                                fileSource,
                                asyncFileWrite,
                                statsDenseStore,
                                targetFileSize));
            }
        }
        return writers;
    }

    private static ExternalStorageBlobFieldWriter createFieldWriter(
            FileIO fileIO,
            long schemaId,
            RowType writeSchema,
            String fieldName,
            String externalStoragePath,
            DataFilePathFactory pathFactory,
            Supplier<LongCounter> seqNumCounterSupplier,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            long targetFileSize) {
        int fieldIndex = writeSchema.getFieldIndex(fieldName);
        ExternalStorageBlobFieldWriter fieldWriter = new ExternalStorageBlobFieldWriter(fieldIndex);

        BlobFileFormat blobFileFormat = new BlobFileFormat();
        blobFileFormat.setWriteConsumer(fieldWriter);

        RowType projectedType = writeSchema.project(fieldName);
        fieldWriter.setWriter(
                new ProjectedFileWriter<>(
                        createRollingBlobWriter(
                                fileIO,
                                schemaId,
                                blobFileFormat,
                                projectedType,
                                fieldName,
                                externalStoragePath,
                                pathFactory,
                                seqNumCounterSupplier,
                                fileSource,
                                asyncFileWrite,
                                statsDenseStore,
                                targetFileSize),
                        writeSchema.projectIndexes(singletonList(fieldName))));

        return fieldWriter;
    }

    private static RollingFileWriterImpl<InternalRow, DataFileMeta> createRollingBlobWriter(
            FileIO fileIO,
            long schemaId,
            BlobFileFormat blobFileFormat,
            RowType projectedType,
            String fieldName,
            String externalStoragePath,
            DataFilePathFactory pathFactory,
            Supplier<LongCounter> seqNumCounterSupplier,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            long targetFileSize) {
        return new RollingFileWriterImpl<>(
                () ->
                        new RowDataFileWriter(
                                fileIO,
                                RollingFileWriter.createFileWriterContext(
                                        blobFileFormat,
                                        projectedType,
                                        new SimpleColStatsCollector.Factory[] {
                                            NoneSimpleColStatsCollector::new
                                        },
                                        "none"),
                                pathFactory.newExternalStorageBlobPath(externalStoragePath),
                                projectedType,
                                schemaId,
                                seqNumCounterSupplier,
                                new FileIndexOptions(),
                                fileSource,
                                asyncFileWrite,
                                statsDenseStore,
                                false,
                                singletonList(fieldName)),
                targetFileSize);
    }

    // ------------------------------ Inner class ------------------------------

    /**
     * Writes one descriptor BLOB field backed by external storage using {@link ProjectedFileWriter}
     * with rolling support. Implements {@link BlobConsumer} to directly capture the {@link
     * BlobDescriptor} produced by {@link BlobFileFormat} after each write. The produced {@link
     * DataFileMeta} is discarded since the files are written outside the table location.
     */
    private static class ExternalStorageBlobFieldWriter implements BlobConsumer, Closeable {

        private final int fieldIndex;
        private ProjectedFileWriter<
                        RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>>
                writer;

        /** The descriptor captured from the last {@link #accept} call. */
        @Nullable private BlobDescriptor lastDescriptor;

        ExternalStorageBlobFieldWriter(int fieldIndex) {
            this.fieldIndex = fieldIndex;
        }

        void setWriter(
                ProjectedFileWriter<
                                RollingFileWriterImpl<InternalRow, DataFileMeta>,
                                List<DataFileMeta>>
                        writer) {
            this.writer = writer;
        }

        int fieldIndex() {
            return fieldIndex;
        }

        @Override
        public boolean accept(String blobFieldName, BlobDescriptor blobDescriptor) {
            this.lastDescriptor = blobDescriptor;
            return true;
        }

        /**
         * Write the BLOB value from {@code src} to the external storage path.
         *
         * @return the {@link BlobDescriptor} for the written data, or null if the field is null
         */
        @Nullable
        BlobDescriptor writeAndReplace(InternalRow src) throws IOException {
            writer.write(src);
            return lastDescriptor;
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }

        void abort() {
            writer.abort();
        }
    }
}
