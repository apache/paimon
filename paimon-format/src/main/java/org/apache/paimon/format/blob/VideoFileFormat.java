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
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** File format that packs complete encoded videos and maps logical rows to frame ordinals. */
public class VideoFileFormat extends FileFormat {

    private final int copyBufferSize;
    private boolean writeNullOnMissingFile;
    private boolean writeNullOnFetchFailure;
    private BlobFetchMetricReporter blobFetchMetricReporter = BlobFetchMetricReporter.NOOP;

    public VideoFileFormat(int copyBufferSize) {
        super(VideoFileFormatFactory.IDENTIFIER);
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
        return new VideoFormatReaderFactory(projectedRowType);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        validateDataFields(type);
        return new VideoFormatWriterFactory(type);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        checkArgument(
                rowType.getFieldCount() == 1
                        && rowType.getTypeAt(0).getTypeRoot() == DataTypeRoot.BLOB,
                "VideoFileFormat only supports one scalar BLOB field.");
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new EmptyStatsExtractor());
    }

    private class VideoFormatWriterFactory implements FormatWriterFactory {

        private final RowType type;

        private VideoFormatWriterFactory(RowType type) {
            this.type = type;
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression) {
            return new VideoFormatWriter(
                    out,
                    type,
                    writeNullOnMissingFile,
                    writeNullOnFetchFailure,
                    blobFetchMetricReporter,
                    copyBufferSize);
        }
    }

    private static class VideoFormatReaderFactory implements FormatReaderFactory {

        private final int fieldCount;
        private final int blobIndex;

        private VideoFormatReaderFactory(RowType projectedRowType) {
            this.fieldCount = projectedRowType.getFieldCount();
            this.blobIndex = findBlobFieldIndex(projectedRowType);
            Preconditions.checkState(
                    blobIndex >= 0,
                    "Read type of a video format does not contain a scalar BLOB field.");
        }

        @Override
        public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
            FileIO fileIO = context.fileIO();
            Path filePath = context.filePath();
            Map<Path, Object> metadataCache = context.metadataCache();
            VideoFileMeta baseMeta =
                    metadataCache == null ? null : (VideoFileMeta) metadataCache.get(filePath);
            if (baseMeta == null) {
                SeekableInputStream in = fileIO.newInputStream(filePath);
                try {
                    baseMeta = new VideoFileMeta(in, context.fileSize(), null);
                    if (metadataCache != null) {
                        metadataCache.put(filePath, baseMeta);
                    }
                } finally {
                    IOUtils.closeQuietly(in);
                }
            }
            VideoFileMeta fileMeta = baseMeta.select(context.selection());
            return new VideoFormatReader(fileIO, filePath, fileMeta, fieldCount, blobIndex);
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
