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
import org.apache.paimon.format.blob.VideoFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.FileWriterAbortExecutor;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.BlobFileContext;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.apache.paimon.types.BlobType.fieldsInBlobFile;

/** A blob file writer that writes blob files. */
public class MultipleBlobFileWriter implements Closeable {

    private final List<BlobProjectedFileWriter> blobWriters;

    public MultipleBlobFileWriter(
            FileIO fileIO,
            long schemaId,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            Supplier<LongCounter> seqNumCounterSupplier,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            long targetFileSize,
            BlobFileContext context) {
        RowType blobRowType =
                new RowType(fieldsInBlobFile(writeSchema, context.blobInlineFields()));
        this.blobWriters = new ArrayList<>();
        for (String blobFieldName : blobRowType.getFieldNames()) {
            boolean video = context.videoFrameFields().contains(blobFieldName);
            FileFormat blobFileFormat;
            if (video) {
                if (context.blobConsumer() != null) {
                    throw new IllegalArgumentException(
                            "BlobConsumer is not supported for video frame field '"
                                    + blobFieldName
                                    + "'.");
                }
                VideoFileFormat format = new VideoFileFormat(context.copyBufferSize());
                format.setWriteNullOnMissingFile(context.writeNullOnMissingFile());
                format.setWriteNullOnFetchFailure(context.writeNullOnFetchFailure());
                format.setBlobFetchMetricReporter(context.blobFetchMetricReporter());
                blobFileFormat = format;
            } else {
                BlobFileFormat format = new BlobFileFormat(false, context.copyBufferSize());
                format.setWriteConsumer(context.blobConsumer());
                format.setWriteNullOnMissingFile(context.writeNullOnMissingFile());
                format.setWriteNullOnFetchFailure(context.writeNullOnFetchFailure());
                format.setBlobFetchMetricReporter(context.blobFetchMetricReporter());
                blobFileFormat = format;
            }
            RowType fieldType = writeSchema.project(blobFieldName);
            Supplier<RowDataFileWriter> writerFactory =
                    () ->
                            new RowDataFileWriter(
                                    fileIO,
                                    RollingFileWriter.createFileWriterContext(
                                            blobFileFormat,
                                            fieldType,
                                            new SimpleColStatsCollector.Factory[] {
                                                NoneSimpleColStatsCollector::new
                                            },
                                            "none"),
                                    video ? pathFactory.newVideoPath() : pathFactory.newBlobPath(),
                                    fieldType,
                                    schemaId,
                                    seqNumCounterSupplier,
                                    new FileIndexOptions(),
                                    fileSource,
                                    asyncFileWrite,
                                    statsDenseStore,
                                    pathFactory.isExternalPath(),
                                    singletonList(blobFieldName),
                                    null,
                                    null);
            RollingFileWriterImpl<InternalRow, DataFileMeta> rollingWriter =
                    video
                            ? new VideoRollingFileWriter<>(writerFactory, targetFileSize)
                            : new RollingFileWriterImpl<>(
                                    writerFactory, targetFileSize, Long.MAX_VALUE);
            blobWriters.add(
                    new BlobProjectedFileWriter(
                            rollingWriter,
                            writeSchema.projectIndexes(singletonList(blobFieldName))));
        }
    }

    public void write(InternalRow row) throws IOException {
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            blobWriter.write(row);
        }
    }

    public void abort() {
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            blobWriter.abort();
        }
    }

    @Override
    public void close() throws IOException {
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            blobWriter.close();
        }
    }

    public List<DataFileMeta> result() throws IOException {
        List<DataFileMeta> results = new ArrayList<>();
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            results.addAll(blobWriter.result());
        }
        return results;
    }

    List<FileWriterAbortExecutor> drainAbortExecutors() {
        List<FileWriterAbortExecutor> abortExecutors = new ArrayList<>();
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            abortExecutors.addAll(blobWriter.drainAbortExecutors());
        }
        return abortExecutors;
    }

    private static class BlobProjectedFileWriter
            extends ProjectedFileWriter<
                    RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>> {
        public BlobProjectedFileWriter(
                RollingFileWriterImpl<InternalRow, DataFileMeta> writer, int[] projection) {
            super(writer, projection);
        }

        private List<FileWriterAbortExecutor> drainAbortExecutors() {
            return writer().drainAbortExecutors();
        }
    }
}
