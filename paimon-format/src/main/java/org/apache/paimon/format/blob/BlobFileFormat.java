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

package org.apache.paimon.format.blob;

import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.EmptyStatsExtractor;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link FileFormat} for blob file. */
public class BlobFileFormat extends FileFormat {

    private final boolean blobAsDescriptor;
    private final int copyBufferSize;
    private boolean writeNullOnMissingFile;
    private boolean writeNullOnFetchFailure;
    private BlobFetchMetricReporter blobFetchMetricReporter = BlobFetchMetricReporter.NOOP;

    @Nullable public BlobConsumer writeConsumer;

    public BlobFileFormat() {
        this(false);
    }

    public BlobFileFormat(boolean blobAsDescriptor) {
        this(blobAsDescriptor, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
    }

    public BlobFileFormat(boolean blobAsDescriptor, int copyBufferSize) {
        super(BlobFileFormatFactory.IDENTIFIER);
        this.blobAsDescriptor = blobAsDescriptor;
        this.copyBufferSize = copyBufferSize;
    }

    public static boolean isBlobFile(String fileName) {
        return fileName.endsWith("." + BlobFileFormatFactory.IDENTIFIER);
    }

    public void setWriteNullOnMissingFile(boolean writeNullOnMissingFile) {
        this.writeNullOnMissingFile = writeNullOnMissingFile;
    }

    public void setWriteNullOnFetchFailure(boolean writeNullOnFetchFailure) {
        this.writeNullOnFetchFailure = writeNullOnFetchFailure;
    }

    public void setWriteConsumer(@Nullable BlobConsumer writeConsumer) {
        this.writeConsumer = writeConsumer;
    }

    public void setBlobFetchMetricReporter(BlobFetchMetricReporter blobFetchMetricReporter) {
        this.blobFetchMetricReporter = blobFetchMetricReporter;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        return new BlobFormatReaderFactory(blobAsDescriptor, projectedRowType);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new BlobFormatWriterFactory(type);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        checkArgument(rowType.getFieldCount() == 1, "BlobFileFormat only support one field.");
        checkArgument(
                BlobElementSerializerFactory.supports(rowType.getField(0).type()),
                "BlobFileFormat only supports BLOB, ARRAY<BLOB>, or supported MAP<X, BLOB> types.");
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        // return a empty stats extractor to avoid stats calculation
        return Optional.of(new EmptyStatsExtractor());
    }

    private class BlobFormatWriterFactory implements FormatWriterFactory {

        private final String blobFieldName;
        private final BlobElementSerializer elementSerializer;

        private BlobFormatWriterFactory(RowType type) {
            checkArgument(type.getFieldCount() == 1, "BlobFormatWriter only support one field.");
            this.blobFieldName = type.getFieldNames().get(0);
            this.elementSerializer = BlobElementSerializerFactory.create(type.getTypeAt(0));
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression) {
            BlobElementSerializer.Writer elementWriter =
                    elementSerializer.createWriter(
                            out,
                            blobFieldName,
                            writeConsumer,
                            writeNullOnMissingFile,
                            writeNullOnFetchFailure,
                            blobFetchMetricReporter,
                            copyBufferSize);
            return new BlobFormatWriter(out, writeConsumer, elementWriter);
        }
    }

    private static class BlobFormatReaderFactory implements FormatReaderFactory {

        private final boolean blobAsDescriptor;
        private final int fieldCount;
        private final int blobIndex;
        private final BlobElementSerializer elementSerializer;

        public BlobFormatReaderFactory(boolean blobAsDescriptor, RowType projectedRowType) {
            this.blobAsDescriptor = blobAsDescriptor;
            this.fieldCount = projectedRowType.getFieldCount();
            this.blobIndex = findBlobFieldIndex(projectedRowType);
            Preconditions.checkState(
                    this.blobIndex >= 0,
                    "Read type of a blob format does not contain any blob field.");
            DataType blobFieldType = projectedRowType.getTypeAt(this.blobIndex);
            this.elementSerializer = BlobElementSerializerFactory.create(blobFieldType);
        }

        @Override
        public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
            FileIO fileIO = context.fileIO();
            Path filePath = context.filePath();
            SeekableInputStream in = null;
            boolean elementReaderOwnsInputStream = false;
            try {
                in = fileIO.newInputStream(filePath);
                BlobFileMeta fileMeta =
                        new BlobFileMeta(in, context.fileSize(), context.selection());
                BlobElementSerializer.Reader elementReader =
                        elementSerializer.createReader(fileIO, filePath, in, blobAsDescriptor);
                elementReaderOwnsInputStream = elementReader.requiresInputStream();
                return new BlobFormatReader(
                        filePath, fileMeta, fieldCount, blobIndex, elementReader);
            } finally {
                if (!elementReaderOwnsInputStream) {
                    IOUtils.closeQuietly(in);
                }
            }
        }

        private static int findBlobFieldIndex(RowType rowType) {
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (BlobElementSerializerFactory.supports(rowType.getTypeAt(i))) {
                    return i;
                }
            }
            return -1;
        }
    }
}
