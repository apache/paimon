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
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.rest.HttpClientUtils;
import org.apache.paimon.utils.SensitiveConfigUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.zip.CRC32;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;
import static org.apache.paimon.utils.StreamUtils.longToLittleEndian;

/** Common writer functionality shared by blob element formats. */
abstract class AbstractBlobElementWriter implements BlobElementSerializer.Writer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBlobElementWriter.class);

    private final PositionOutputStream out;
    private final String blobFieldName;
    private final @Nullable BlobConsumer writeConsumer;
    private final boolean writeNullOnMissingFile;
    private final boolean writeNullOnFetchFailure;
    private final BlobFetchMetricReporter blobFetchMetricReporter;
    private final CRC32 crc32;
    private final byte[] copyBuffer;

    private String pathString;

    AbstractBlobElementWriter(
            PositionOutputStream out,
            String blobFieldName,
            @Nullable BlobConsumer writeConsumer,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter,
            int copyBufferSize) {
        checkArgument(
                copyBufferSize > 0,
                "BLOB copy buffer size must be positive, but was %s.",
                copyBufferSize);
        this.out = out;
        this.blobFieldName = blobFieldName;
        this.writeConsumer = writeConsumer;
        this.writeNullOnMissingFile = writeNullOnMissingFile;
        this.writeNullOnFetchFailure = writeNullOnFetchFailure;
        this.blobFetchMetricReporter = blobFetchMetricReporter;
        this.crc32 = new CRC32();
        this.copyBuffer = new byte[copyBufferSize];
    }

    @Override
    public final void setFile(Path file) {
        this.pathString = file.toString();
    }

    protected final long startRecord() throws IOException {
        long position = out.getPos();
        crc32.reset();
        write(BlobFormatWriter.MAGIC_NUMBER_BYTES);
        return position;
    }

    protected final long finishRecord(long recordPosition) throws IOException {
        long recordLength = out.getPos() - recordPosition + 12;
        write(longToLittleEndian(recordLength));
        out.write(intToLittleEndian((int) crc32.getValue()));
        return recordLength;
    }

    protected final long writeNullElement() throws IOException {
        if (writeConsumer != null) {
            writeConsumer.accept(blobFieldName, null);
        }
        return BlobFormatWriter.NULL_LENGTH;
    }

    protected final long writePlaceholderElement() {
        return BlobFormatWriter.PLACE_HOLDER_LENGTH;
    }

    protected final BlobFetchResult getBlob(BlobGetter getter) {
        try {
            return new BlobFetchResult(getter.get(), false);
        } catch (RuntimeException e) {
            if (shouldWriteNullOnFetchFailure(e)) {
                logWriteNullOnFetchFailure(e, null);
                blobFetchMetricReporter.recordFetchFailureNullWritten(e);
                return new BlobFetchResult(null, true);
            }
            blobFetchMetricReporter.recordFetchFailure(e);
            throw e;
        }
    }

    protected final @Nullable SeekableInputStream openBlobInputStream(Blob blob)
            throws IOException {
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

    protected final BlobDescriptor writeBlobData(SeekableInputStream in) throws IOException {
        long blobPosition = out.getPos();
        try (SeekableInputStream stream = in) {
            int bytesRead = stream.read(copyBuffer);
            while (bytesRead >= 0) {
                write(copyBuffer, bytesRead);
                bytesRead = stream.read(copyBuffer);
            }
        } catch (IOException | RuntimeException e) {
            blobFetchMetricReporter.recordFetchFailure(e);
            throw e;
        }
        return new BlobDescriptor(pathString, blobPosition, out.getPos() - blobPosition);
    }

    protected final boolean accept(BlobDescriptor descriptor) throws IOException {
        return writeConsumer != null && writeConsumer.accept(blobFieldName, descriptor);
    }

    protected final void flush() throws IOException {
        out.flush();
    }

    protected final void recordSuccess(long length) {
        blobFetchMetricReporter.recordSuccess(length);
    }

    protected final void recordPreCheckedMissingFileNull(InternalRow row) {
        if (!writeNullOnMissingFile) {
            return;
        }
        Blob blob = row.getBlob(0);
        if (blob instanceof BlobRef) {
            BlobDescriptor descriptor = ((BlobRef) blob).toDescriptor();
            blobFetchMetricReporter.recordMissingFileNullWritten(isHttpUri(descriptor.uri()));
        }
    }

    protected final void write(byte[] bytes) throws IOException {
        write(bytes, bytes.length);
    }

    private void write(byte[] bytes, int length) throws IOException {
        crc32.update(bytes, 0, length);
        out.write(bytes, 0, length);
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

    private static String blobUri(@Nullable Blob blob) {
        if (blob instanceof BlobRef) {
            // Sanitize: a signed URL carries token/signature in its query.
            return SensitiveConfigUtils.sanitizeUri(((BlobRef) blob).toDescriptor().uri());
        }
        return "unknown";
    }

    private static boolean isHttpUri(String uri) {
        return uri.regionMatches(true, 0, "http://", 0, "http://".length())
                || uri.regionMatches(true, 0, "https://", 0, "https://".length());
    }

    /** Lazily gets a Blob from a row or array. */
    interface BlobGetter {
        Blob get();
    }

    /** Result of getting a Blob, preserving whether a null came from fetch fallback. */
    static final class BlobFetchResult {

        private final @Nullable Blob blob;
        private final boolean fetchFailure;

        private BlobFetchResult(@Nullable Blob blob, boolean fetchFailure) {
            this.blob = blob;
            this.fetchFailure = fetchFailure;
        }

        @Nullable
        Blob blob() {
            return blob;
        }

        boolean fetchFailure() {
            return fetchFailure;
        }
    }
}
