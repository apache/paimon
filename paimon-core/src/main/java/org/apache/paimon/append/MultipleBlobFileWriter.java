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

import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
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

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

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
            @Nullable BlobConsumer blobConsumer,
            Set<String> blobStoredDescriptorFields) {
        RowType blobRowType =
                BlobType.splitBlob(writeSchema, blobStoredDescriptorFields).getRight();
        this.blobWriters = new ArrayList<>();
        for (String blobFieldName : blobRowType.getFieldNames()) {
            BlobFileFormat blobFileFormat = new BlobFileFormat();
            blobFileFormat.setWriteConsumer(blobConsumer);
            blobWriters.add(
                    new BlobProjectedFileWriter(
                            () ->
                                    new RowDataFileWriter(
                                            fileIO,
                                            RollingFileWriter.createFileWriterContext(
                                                    blobFileFormat,
                                                    writeSchema.project(blobFieldName),
                                                    new SimpleColStatsCollector.Factory[] {
                                                        NoneSimpleColStatsCollector::new
                                                    },
                                                    "none"),
                                            pathFactory.newBlobPath(),
                                            writeSchema.project(blobFieldName),
                                            schemaId,
                                            seqNumCounterSupplier,
                                            new FileIndexOptions(),
                                            fileSource,
                                            asyncFileWrite,
                                            statsDenseStore,
                                            pathFactory.isExternalPath(),
                                            singletonList(blobFieldName)),
                            targetFileSize,
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

    private static class BlobProjectedFileWriter
            extends ProjectedFileWriter<
                    RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>> {
        public BlobProjectedFileWriter(
                Supplier<? extends SingleFileWriter<InternalRow, DataFileMeta>> writerFactory,
                long targetFileSize,
                int[] projection) {
            super(new RollingFileWriterImpl<>(writerFactory, targetFileSize), projection);
        }
    }
}
