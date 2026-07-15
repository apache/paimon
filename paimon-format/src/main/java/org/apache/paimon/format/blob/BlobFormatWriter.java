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
import org.apache.paimon.format.FileAwareFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.LongArrayList;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;

/** {@link FormatWriter} for blob file. */
public class BlobFormatWriter implements FileAwareFormatWriter {

    public static final byte VERSION = 1;
    public static final int MAGIC_NUMBER = 1481511375;
    public static final byte[] MAGIC_NUMBER_BYTES = intToLittleEndian(MAGIC_NUMBER);
    public static final long NULL_LENGTH = -1L;
    public static final long PLACE_HOLDER_LENGTH = -2L;
    public static final int DEFAULT_COPY_BUFFER_SIZE = 4 * 1024;

    private final PositionOutputStream out;
    private final boolean deleteFileUponAbort;
    private final BlobElementSerializer.Writer elementWriter;
    private final LongArrayList lengths;

    public BlobFormatWriter(
            PositionOutputStream out, @Nullable BlobConsumer writeConsumer, RowType type) {
        this(out, writeConsumer, type, false, false);
    }

    public BlobFormatWriter(
            PositionOutputStream out,
            @Nullable BlobConsumer writeConsumer,
            RowType type,
            boolean writeNullOnMissingFile) {
        this(out, writeConsumer, type, writeNullOnMissingFile, false);
    }

    public BlobFormatWriter(
            PositionOutputStream out,
            @Nullable BlobConsumer writeConsumer,
            RowType type,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure) {
        this(
                out,
                writeConsumer,
                type,
                writeNullOnMissingFile,
                writeNullOnFetchFailure,
                BlobFetchMetricReporter.NOOP);
    }

    public BlobFormatWriter(
            PositionOutputStream out,
            @Nullable BlobConsumer writeConsumer,
            RowType type,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter) {
        this(
                out,
                writeConsumer,
                createElementWriter(
                        out,
                        writeConsumer,
                        type,
                        writeNullOnMissingFile,
                        writeNullOnFetchFailure,
                        blobFetchMetricReporter,
                        DEFAULT_COPY_BUFFER_SIZE));
    }

    public BlobFormatWriter(
            PositionOutputStream out,
            @Nullable BlobConsumer writeConsumer,
            RowType type,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter,
            int copyBufferSize) {
        this(
                out,
                writeConsumer,
                createElementWriter(
                        out,
                        writeConsumer,
                        type,
                        writeNullOnMissingFile,
                        writeNullOnFetchFailure,
                        blobFetchMetricReporter,
                        copyBufferSize));
    }

    BlobFormatWriter(
            PositionOutputStream out,
            @Nullable BlobConsumer writeConsumer,
            BlobElementSerializer.Writer elementWriter) {
        this.out = out;
        this.deleteFileUponAbort = writeConsumer == null;
        this.elementWriter = elementWriter;
        this.lengths = new LongArrayList(16);
    }

    @Override
    public void setFile(Path file) {
        elementWriter.setFile(file);
    }

    @Override
    public boolean deleteFileUponAbort() {
        return deleteFileUponAbort;
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        checkArgument(element.getFieldCount() == 1, "BlobFormatWriter only support one field.");
        lengths.add(elementWriter.write(element));
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        // check target size every record
        // Each blob is very large, so the cost of check is not high
        return out.getPos() >= targetSize;
    }

    @Override
    public void close() throws IOException {
        byte[] indexBytes = DeltaVarintCompressor.compress(lengths.toArray());
        out.write(indexBytes);
        out.write(intToLittleEndian(indexBytes.length));
        out.write(VERSION);
    }

    private static BlobElementSerializer.Writer createElementWriter(
            PositionOutputStream out,
            @Nullable BlobConsumer writeConsumer,
            RowType type,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter,
            int copyBufferSize) {
        checkArgument(type.getFieldCount() == 1, "BlobFormatWriter only support one field.");
        return BlobElementSerializerFactory.create(type.getTypeAt(0))
                .createWriter(
                        out,
                        type.getFieldNames().get(0),
                        writeConsumer,
                        writeNullOnMissingFile,
                        writeNullOnFetchFailure,
                        blobFetchMetricReporter,
                        copyBufferSize);
    }
}
