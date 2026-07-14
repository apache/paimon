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

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobArrayPlaceholder;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileAwareFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.rest.HttpClientUtils;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.LongArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.zip.CRC32;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;
import static org.apache.paimon.utils.StreamUtils.longToLittleEndian;

/** {@link FormatWriter} for blob file. */
public class BlobFormatWriter implements FileAwareFormatWriter {

    private static final Logger LOG = LoggerFactory.getLogger(BlobFormatWriter.class);

    public static final byte VERSION = 1;
    public static final int MAGIC_NUMBER = 1481511375;
    public static final byte[] MAGIC_NUMBER_BYTES = intToLittleEndian(MAGIC_NUMBER);
    public static final byte ARRAY_VERSION = 1;
    public static final int ARRAY_MAGIC_NUMBER = 1094861634;
    public static final byte[] ARRAY_MAGIC_NUMBER_BYTES = intToLittleEndian(ARRAY_MAGIC_NUMBER);
    public static final long NULL_LENGTH = -1L;
    public static final long PLACE_HOLDER_LENGTH = -2L;
    public static final long ARRAY_NULL_ELEMENT_LENGTH = -1L;

    private final PositionOutputStream out;
    @Nullable private final BlobConsumer writeConsumer;
    private final BlobFetchMetricReporter blobFetchMetricReporter;
    private final String blobFieldName;
    private final boolean writeNullOnMissingFile;
    private final boolean writeNullOnFetchFailure;
    private final BlobElementWriter elementWriter;
    private final CRC32 crc32;
    private final byte[] tmpBuffer;
    private final LongArrayList lengths;

    private String pathString;

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
        this.out = out;
        this.writeConsumer = writeConsumer;
        this.blobFetchMetricReporter = blobFetchMetricReporter;
        this.writeNullOnMissingFile = writeNullOnMissingFile;
        this.writeNullOnFetchFailure = writeNullOnFetchFailure;
        checkArgument(type.getFieldCount() == 1, "BlobFormatWriter only support one field.");
        this.blobFieldName = type.getFieldNames().get(0);
        this.elementWriter =
                type.getTypeAt(0).getTypeRoot() == DataTypeRoot.ARRAY
                        ? new ArrayBlobElementWriter()
                        : new RawBlobElementWriter();
        this.crc32 = new CRC32();
        this.tmpBuffer = new byte[4096];
        this.lengths = new LongArrayList(16);
    }

    @Override
    public void setFile(Path file) {
        this.pathString = file.toString();
    }

    @Override
    public boolean deleteFileUponAbort() {
        return writeConsumer == null;
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        checkArgument(element.getFieldCount() == 1, "BlobFormatWriter only support one field.");
        elementWriter.addElement(element);
    }

    private interface BlobElementWriter {
        void addElement(InternalRow element) throws IOException;
    }

    private class RawBlobElementWriter implements BlobElementWriter {

        @Override
        public void addElement(InternalRow element) throws IOException {
            if (element.isNullAt(0)) {
                recordPreCheckedMissingFileNull(element);
                writeNullElement();
                return;
            }

            Blob blob;
            try {
                blob = element.getBlob(0);
            } catch (RuntimeException e) {
                if (shouldWriteNullOnFetchFailure(e)) {
                    logWriteNullOnFetchFailure(e, null);
                    blobFetchMetricReporter.recordFetchFailureNullWritten(e);
                    writeNullElement();
                    return;
                }
                blobFetchMetricReporter.recordFetchFailure(e);
                throw e;
            }
            if (blob == BlobPlaceholder.INSTANCE) {
                lengths.add(PLACE_HOLDER_LENGTH);
                return;
            }

            addBlob(blob);
        }
    }

    private class ArrayBlobElementWriter implements BlobElementWriter {

        @Override
        public void addElement(InternalRow element) throws IOException {
            if (element.isNullAt(0)) {
                writeNullElement();
                return;
            }

            InternalArray array = element.getArray(0);
            if (array == BlobArrayPlaceholder.INSTANCE) {
                lengths.add(PLACE_HOLDER_LENGTH);
                return;
            }

            addBlobArray(array);
        }
    }

    private void addBlob(Blob blob) throws IOException {
        SeekableInputStream in = openBlobInputStream(blob);
        if (in == null) {
            writeNullElement();
            return;
        }
        long previousPos = out.getPos();
        crc32.reset();
        write(MAGIC_NUMBER_BYTES);

        BlobDescriptor descriptor = writeBlobData(in);

        long binLength = out.getPos() - previousPos + 12;
        lengths.add(binLength);
        byte[] lenBytes = longToLittleEndian(binLength);
        write(lenBytes);
        int crcValue = (int) crc32.getValue();
        out.write(intToLittleEndian(crcValue));

        if (writeConsumer != null) {
            boolean flush = writeConsumer.accept(blobFieldName, descriptor);
            if (flush) {
                out.flush();
            }
        }
        blobFetchMetricReporter.recordSuccess(descriptor.length());
    }

    private void addBlobArray(InternalArray array) throws IOException {
        long previousPos = out.getPos();
        crc32.reset();

        write(MAGIC_NUMBER_BYTES);

        write(ARRAY_MAGIC_NUMBER_BYTES);
        write(new byte[] {ARRAY_VERSION});
        write(intToLittleEndian(array.size()));

        long[] elementLengths = new long[array.size()];
        boolean flush = false;
        for (int i = 0; i < array.size(); i++) {
            if (array.isNullAt(i)) {
                elementLengths[i] = ARRAY_NULL_ELEMENT_LENGTH;
                continue;
            }

            Blob blob = getArrayBlob(array, i);
            if (blob == null) {
                elementLengths[i] = ARRAY_NULL_ELEMENT_LENGTH;
                continue;
            }
            SeekableInputStream in = openBlobInputStream(blob);
            if (in == null) {
                elementLengths[i] = ARRAY_NULL_ELEMENT_LENGTH;
                continue;
            }
            BlobDescriptor descriptor = writeBlobData(in);
            elementLengths[i] = descriptor.length();
            if (writeConsumer != null) {
                flush |= writeConsumer.accept(blobFieldName, descriptor);
            }
            blobFetchMetricReporter.recordSuccess(descriptor.length());
        }

        byte[] elementIndexBytes = DeltaVarintCompressor.compress(elementLengths);
        write(elementIndexBytes);
        write(intToLittleEndian(elementIndexBytes.length));

        long binLength = out.getPos() - previousPos + 12;
        lengths.add(binLength);
        write(longToLittleEndian(binLength));
        int crcValue = (int) crc32.getValue();
        out.write(intToLittleEndian(crcValue));

        if (flush) {
            out.flush();
        }
    }

    @Nullable
    private Blob getArrayBlob(InternalArray array, int pos) {
        try {
            return array.getBlob(pos);
        } catch (RuntimeException e) {
            if (shouldWriteNullOnFetchFailure(e)) {
                logWriteNullOnFetchFailure(e, null);
                blobFetchMetricReporter.recordFetchFailureNullWritten(e);
                return null;
            }
            blobFetchMetricReporter.recordFetchFailure(e);
            throw e;
        }
    }

    @Nullable
    private SeekableInputStream openBlobInputStream(Blob blob) throws IOException {
        try {
            return blob.newInputStream();
        } catch (IOException | RuntimeException e) {
            if (writeNullOnMissingFile && HttpClientUtils.isNotFoundError(e)) {
                LOG.warn(
                        "Failed to open blob from {} (HTTP 404), writing NULL for BLOB field {}.",
                        blobUri(blob),
                        blobFieldName,
                        e);
                blobFetchMetricReporter.recordMissingFileNullWritten(true);
                return null;
            }
            if (shouldWriteNullOnFetchFailure(e)) {
                logWriteNullOnFetchFailure(e, blob);
                blobFetchMetricReporter.recordFetchFailureNullWritten(e);
                return null;
            }
            blobFetchMetricReporter.recordFetchFailure(e);
            throw e;
        }
    }

    private BlobDescriptor writeBlobData(SeekableInputStream in) throws IOException {
        long blobPos = out.getPos();
        try (SeekableInputStream stream = in) {
            int bytesRead = stream.read(tmpBuffer);
            while (bytesRead >= 0) {
                write(tmpBuffer, bytesRead);
                bytesRead = stream.read(tmpBuffer);
            }
        } catch (IOException | RuntimeException e) {
            blobFetchMetricReporter.recordFetchFailure(e);
            throw e;
        }

        return new BlobDescriptor(pathString, blobPos, out.getPos() - blobPos);
    }

    private boolean shouldWriteNullOnFetchFailure(Throwable e) {
        return writeNullOnFetchFailure && !HttpClientUtils.isNotFoundError(e);
    }

    private void logWriteNullOnFetchFailure(Throwable e, @Nullable Blob blob) {
        Integer statusCode = HttpClientUtils.getHttpStatusCode(e);
        if (statusCode != null) {
            LOG.warn(
                    "Failed to open blob from {} (HTTP {}), writing NULL for BLOB field {}.",
                    blobUri(blob),
                    statusCode,
                    blobFieldName,
                    e);
        } else if (HttpClientUtils.isInvalidUriException(e)) {
            LOG.warn(
                    "Invalid blob URI {} while opening blob, writing NULL for BLOB field {}.",
                    blobUri(blob),
                    blobFieldName,
                    e);
        } else {
            LOG.warn(
                    "Failed to open blob from {} due to fetch failure, writing NULL for BLOB field {}.",
                    blobUri(blob),
                    blobFieldName,
                    e);
        }
    }

    private void recordPreCheckedMissingFileNull(InternalRow element) {
        if (!writeNullOnMissingFile) {
            return;
        }
        Blob blob = element.getBlob(0);
        if (blob instanceof BlobRef) {
            BlobDescriptor descriptor = ((BlobRef) blob).toDescriptor();
            blobFetchMetricReporter.recordMissingFileNullWritten(isHttpUri(descriptor.uri()));
        }
    }

    private void writeNullElement() throws IOException {
        lengths.add(NULL_LENGTH);
        if (writeConsumer != null) {
            writeConsumer.accept(blobFieldName, null);
        }
    }

    private static String blobUri(@Nullable Blob blob) {
        if (blob instanceof BlobRef) {
            return ((BlobRef) blob).toDescriptor().uri();
        }
        return "unknown";
    }

    private static boolean isHttpUri(String uri) {
        return uri.regionMatches(true, 0, "http://", 0, "http://".length())
                || uri.regionMatches(true, 0, "https://", 0, "https://".length());
    }

    private void write(byte[] bytes) throws IOException {
        write(bytes, bytes.length);
    }

    private void write(byte[] bytes, int length) throws IOException {
        crc32.update(bytes, 0, length);
        out.write(bytes, 0, length);
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        // check target size every record
        // Each blob is very large, so the cost of check is not high
        return out.getPos() >= targetSize;
    }

    @Override
    public void close() throws IOException {
        // index
        byte[] indexBytes = DeltaVarintCompressor.compress(lengths.toArray());
        out.write(indexBytes);
        // header
        out.write(intToLittleEndian(indexBytes.length));
        out.write(VERSION);
    }
}
