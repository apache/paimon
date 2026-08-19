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
import org.apache.paimon.data.ReusingBlobRefStreamProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.rest.HttpClientUtils;
import org.apache.paimon.utils.SensitiveConfigUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
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
    private final ReusingBlobRefStreamProvider reuseSource;
    private final BlobStagingFactory stagingFactory;

    private String pathString;

    AbstractBlobElementWriter(
            PositionOutputStream out,
            String blobFieldName,
            @Nullable BlobConsumer writeConsumer,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter,
            int copyBufferSize,
            BlobStagingFactory stagingFactory) {
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
        this.reuseSource = new ReusingBlobRefStreamProvider();
        this.stagingFactory = stagingFactory;
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

    /**
     * Prepares the byte source for a blob, or returns {@code null} to write it as NULL (missing
     * file / fetch failure). An exact {@link BlobRef} with known length reuses one source stream
     * (bounded to the descriptor); other blobs open their own stream and read until EOF.
     */
    protected final @Nullable BlobCopySource prepareBlobSource(Blob blob) throws IOException {
        // Exact class only: subclasses may override newInputStream() and must not be bypassed.
        if (blob != null && blob.getClass() == BlobRef.class) {
            BlobRef ref = (BlobRef) blob;
            long length = ref.toDescriptor().length();
            if (length >= 0) {
                // Position/release the previous source first; its cleanup error is not this blob's
                // fetch failure and must not be turned into a NULL write.
                reuseSource.prepareFor(ref);
                SeekableInputStream source = openStream(ref, () -> reuseSource.openBounded(ref));
                if (source == null) {
                    return null;
                }
                return new BlobCopySource(blob, source, length, true, null);
            }
        }

        SeekableInputStream source = openStream(blob, blob::newInputStream);
        if (source == null) {
            return null;
        }
        return new BlobCopySource(blob, source, -1L, false, null);
    }

    @Nullable
    private SeekableInputStream openStream(Blob blob, StreamOpener opener) throws IOException {
        try {
            return opener.open();
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

    /**
     * Fully stages {@code source} when fetch failures may be converted to NULL. The returned source
     * contains only bytes which were fetched successfully, so copying it can never expose a partial
     * source payload to the final BLOB output.
     */
    protected final @Nullable BlobCopySource prepareBlobForWrite(BlobCopySource source)
            throws IOException {
        if (!writeNullOnFetchFailure) {
            return source;
        }

        final BlobStaging staging;
        try {
            staging = stagingFactory.create();
        } catch (RuntimeException | Error | IOException e) {
            closeOrSuppress(source.reused() ? reuseSource : source, e);
            throw e;
        }
        Throwable sourceReadFailure = null;
        Throwable fatalFailure = null;
        try {
            sourceReadFailure = copyToStaging(source, staging);
            if (sourceReadFailure == null) {
                staging.finish();
            }
        } catch (RuntimeException | Error | IOException e) {
            // Staging creation/write/finish failures are local failures, not source fetch failures.
            fatalFailure = e;
        }

        try {
            if (source.reused() && (sourceReadFailure != null || fatalFailure != null)) {
                // A failed staged read leaves the shared source at an unknown position. Close it
                // with error propagation so the next BLOB reopens and cleanup failures stay fatal.
                reuseSource.close();
            } else {
                source.close();
            }
        } catch (RuntimeException | Error | IOException e) {
            // A source cleanup failure stays fatal even if the body read also failed.
            if (fatalFailure == null) {
                fatalFailure = e;
            } else {
                fatalFailure.addSuppressed(e);
            }
        }

        if (fatalFailure != null) {
            if (sourceReadFailure != null && sourceReadFailure != fatalFailure) {
                fatalFailure.addSuppressed(sourceReadFailure);
            }
            closeOrSuppress(staging, fatalFailure);
            throwFailure(fatalFailure);
        }

        if (sourceReadFailure != null) {
            Throwable cleanupFailure = closeAndGetFailure(staging);
            if (cleanupFailure != null) {
                cleanupFailure.addSuppressed(sourceReadFailure);
                throwFailure(cleanupFailure);
            }
            return handleSourceReadFailure(sourceReadFailure, source.blob());
        }

        try {
            return new BlobCopySource(
                    source.blob(), staging.openInputStream(), staging.length(), false, staging);
        } catch (RuntimeException | Error | IOException e) {
            closeOrSuppress(staging, e);
            throw e;
        }
    }

    /** Returns the source-read failure, or {@code null} after the complete payload was staged. */
    @Nullable
    private Throwable copyToStaging(BlobCopySource source, BlobStaging staging) throws IOException {
        long remaining = source.length();
        while (remaining != 0) {
            checkNotInterrupted();
            int toRead =
                    remaining < 0
                            ? copyBuffer.length
                            : (int) Math.min(copyBuffer.length, remaining);
            final int bytesRead;
            try {
                bytesRead = source.stream().read(copyBuffer, 0, toRead);
            } catch (IOException | RuntimeException e) {
                return e;
            }

            if (bytesRead < 0) {
                if (remaining < 0) {
                    return null;
                }
                return unexpectedEof(source.length(), remaining);
            }
            if (bytesRead == 0) {
                return new IOException(
                        "Source returned 0 bytes while staging BLOB payload for field "
                                + blobFieldName);
            }

            checkNotInterrupted();
            // Keep this outside the source-read catch: a local staging failure must stay fatal.
            staging.write(copyBuffer, 0, bytesRead);
            if (remaining > 0) {
                remaining -= bytesRead;
            }
        }
        return null;
    }

    @Nullable
    private BlobCopySource handleSourceReadFailure(Throwable failure, Blob blob)
            throws IOException {
        if (writeNullOnMissingFile && HttpClientUtils.isNotFoundError(failure)) {
            LOG.warn(
                    "Failed to read blob from {} (HTTP 404), writing NULL for BLOB field {}.",
                    blobUri(blob),
                    blobFieldName,
                    failure);
            blobFetchMetricReporter.recordMissingFileNullWritten(true);
            return null;
        }
        if (shouldWriteNullOnFetchFailure(failure)) {
            logWriteNullOnFetchFailure(failure, blob);
            // Record the fallback only after source and staging cleanup completed successfully.
            blobFetchMetricReporter.recordFetchFailureNullWritten(failure);
            return null;
        }
        blobFetchMetricReporter.recordFetchFailure(failure);
        throwFailure(failure);
        return null;
    }

    protected final BlobDescriptor writeBlobData(BlobCopySource source) throws IOException {
        long blobPosition = out.getPos();
        try {
            if (source.length() >= 0) {
                copyExactly(source.stream(), source.length());
            } else {
                copyUntilEof(source.stream());
            }
        } catch (IOException | RuntimeException e) {
            if (!source.staged()) {
                blobFetchMetricReporter.recordFetchFailure(e);
                if (source.reused()) {
                    // Source is at an unknown position now; drop it so the next blob reopens.
                    reuseSource.discardQuietly();
                }
            }
            throw e;
        }
        return new BlobDescriptor(pathString, blobPosition, out.getPos() - blobPosition);
    }

    private void copyUntilEof(InputStream stream) throws IOException {
        checkNotInterrupted();
        int bytesRead = stream.read(copyBuffer);
        while (bytesRead >= 0) {
            checkNotInterrupted();
            write(copyBuffer, bytesRead);
            checkNotInterrupted();
            bytesRead = stream.read(copyBuffer);
        }
    }

    /** Copies exactly {@code length} bytes from {@code stream}, throwing on premature EOF. */
    private void copyExactly(InputStream stream, long length) throws IOException {
        long remaining = length;
        while (remaining > 0) {
            checkNotInterrupted();
            int toRead = (int) Math.min(copyBuffer.length, remaining);
            int bytesRead = stream.read(copyBuffer, 0, toRead);
            if (bytesRead < 0) {
                throw unexpectedEof(length, remaining);
            }
            if (bytesRead == 0) {
                throw new IOException(
                        "Source returned 0 bytes while copying BLOB payload for field "
                                + blobFieldName);
            }
            checkNotInterrupted();
            write(copyBuffer, bytesRead);
            remaining -= bytesRead;
        }
    }

    private EOFException unexpectedEof(long length, long remaining) {
        return new EOFException(
                String.format(
                        "Unexpected EOF while copying BLOB payload for field %s: expected %d "
                                + "bytes but source ended %d bytes early.",
                        blobFieldName, length, remaining));
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
        return writeNullOnFetchFailure
                && !isTaskCancellation(e)
                && !HttpClientUtils.isNotFoundError(e);
    }

    private static boolean isTaskCancellation(Throwable failure) {
        if (Thread.currentThread().isInterrupted()) {
            return true;
        }

        Throwable current = failure;
        while (current != null) {
            if (current instanceof InterruptedException
                    || current instanceof ClosedByInterruptException) {
                Thread.currentThread().interrupt();
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void checkNotInterrupted() throws InterruptedIOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException(
                    "Interrupted while copying BLOB payload for field " + blobFieldName);
        }
    }

    private void logWriteNullOnFetchFailure(Throwable e, @Nullable Blob blob) {
        Integer statusCode = HttpClientUtils.getHttpStatusCode(e);
        if (statusCode != null) {
            LOG.warn(
                    "Failed to fetch blob from {} (HTTP {}), writing NULL for BLOB field {}.",
                    blobUri(blob),
                    statusCode,
                    blobFieldName,
                    e);
        } else if (HttpClientUtils.isInvalidUriException(e)) {
            LOG.warn(
                    "Invalid blob URI {} while fetching blob, writing NULL for BLOB field {}.",
                    blobUri(blob),
                    blobFieldName,
                    e);
        } else {
            LOG.warn(
                    "Failed to fetch blob from {} due to fetch failure, writing NULL for BLOB field {}.",
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

    @Override
    public final void close() throws IOException {
        reuseSource.close();
    }

    @FunctionalInterface
    private interface StreamOpener {
        SeekableInputStream open() throws IOException;
    }

    private static void closeOrSuppress(Closeable closeable, Throwable primary) {
        Throwable closeFailure = closeAndGetFailure(closeable);
        if (closeFailure != null) {
            primary.addSuppressed(closeFailure);
        }
    }

    @Nullable
    private static Throwable closeAndGetFailure(Closeable closeable) {
        try {
            closeable.close();
            return null;
        } catch (RuntimeException | Error | IOException e) {
            return e;
        }
    }

    private static void throwFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException(failure);
    }

    /** The byte source of a single blob payload to be copied into the blob file. */
    protected static final class BlobCopySource implements Closeable {

        private final Blob blob;
        private final InputStream stream;
        private final long length;
        private final boolean reused;
        private final @Nullable BlobStaging staging;

        private BlobCopySource(
                Blob blob,
                InputStream stream,
                long length,
                boolean reused,
                @Nullable BlobStaging staging) {
            this.blob = blob;
            this.stream = stream;
            this.length = length;
            this.reused = reused;
            this.staging = staging;
        }

        private Blob blob() {
            return blob;
        }

        private boolean reused() {
            return reused;
        }

        private boolean staged() {
            return staging != null;
        }

        private InputStream stream() {
            return stream;
        }

        private long length() {
            return length;
        }

        @Override
        public void close() throws IOException {
            if (reused) {
                return;
            }

            Throwable failure = closeAndGetFailure(stream);
            if (staging != null) {
                Throwable stagingFailure = closeAndGetFailure(staging);
                if (failure == null) {
                    failure = stagingFailure;
                } else if (stagingFailure != null) {
                    failure.addSuppressed(stagingFailure);
                }
            }
            if (failure != null) {
                throwFailure(failure);
            }
        }
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
