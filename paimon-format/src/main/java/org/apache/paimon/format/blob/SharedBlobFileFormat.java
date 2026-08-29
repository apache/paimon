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
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** File format in which logical rows may share one physical BLOB payload. */
public class SharedBlobFileFormat extends FileFormat {

    private final boolean blobAsDescriptor;
    private final int copyBufferSize;
    private boolean writeNullOnMissingFile;
    private boolean writeNullOnFetchFailure;
    private BlobFetchMetricReporter blobFetchMetricReporter = BlobFetchMetricReporter.NOOP;

    public SharedBlobFileFormat(boolean blobAsDescriptor, int copyBufferSize) {
        super(SharedBlobFileFormatFactory.IDENTIFIER);
        this.blobAsDescriptor = blobAsDescriptor;
        this.copyBufferSize = copyBufferSize;
    }

    public void setWriteNullOnMissingFile(boolean writeNullOnMissingFile) {
        this.writeNullOnMissingFile = writeNullOnMissingFile;
    }

    public void setWriteNullOnFetchFailure(boolean writeNullOnFetchFailure) {
        this.writeNullOnFetchFailure = writeNullOnFetchFailure;
    }

    public void setBlobFetchMetricReporter(BlobFetchMetricReporter blobFetchMetricReporter) {
        this.blobFetchMetricReporter = blobFetchMetricReporter;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        return new SharedBlobFormatReaderFactory(blobAsDescriptor, projectedRowType);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        validateDataFields(type);
        return new SharedBlobFormatWriterFactory(type);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        checkArgument(
                rowType.getFieldCount() == 1
                        && rowType.getTypeAt(0).getTypeRoot() == DataTypeRoot.BLOB,
                "SharedBlobFileFormat only supports one scalar BLOB field.");
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new EmptyStatsExtractor());
    }

    private class SharedBlobFormatWriterFactory implements FormatWriterFactory {

        private final RowType type;

        private SharedBlobFormatWriterFactory(RowType type) {
            this.type = type;
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression) {
            return new SharedBlobFormatWriter(
                    out,
                    type,
                    writeNullOnMissingFile,
                    writeNullOnFetchFailure,
                    blobFetchMetricReporter,
                    copyBufferSize);
        }
    }

    private static class SharedBlobFormatReaderFactory implements FormatReaderFactory {

        private final boolean blobAsDescriptor;
        private final int fieldCount;
        private final int blobIndex;

        private SharedBlobFormatReaderFactory(boolean blobAsDescriptor, RowType projectedRowType) {
            this.blobAsDescriptor = blobAsDescriptor;
            this.fieldCount = projectedRowType.getFieldCount();
            this.blobIndex = findBlobFieldIndex(projectedRowType);
            Preconditions.checkState(
                    blobIndex >= 0,
                    "Read type of a shared blob format does not contain a scalar BLOB field.");
        }

        @Override
        public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
            FileIO fileIO = context.fileIO();
            Path filePath = context.filePath();
            SeekableInputStream in = fileIO.newInputStream(filePath);
            SharedBlobFileMeta fileMeta;
            try {
                fileMeta = new SharedBlobFileMeta(in, context.fileSize(), context.selection());
            } catch (Exception e) {
                IOUtils.closeQuietly(in);
                throw e;
            }
            return new SharedBlobFormatReader(
                    fileIO, filePath, fileMeta, in, fieldCount, blobIndex, blobAsDescriptor);
        }

        private static int findBlobFieldIndex(RowType rowType) {
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (rowType.getTypeAt(i).getTypeRoot() == DataTypeRoot.BLOB) {
                    return i;
                }
            }
            return -1;
        }
    }
}
