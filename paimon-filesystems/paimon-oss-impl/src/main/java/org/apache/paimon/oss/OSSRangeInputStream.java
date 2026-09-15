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

package org.apache.paimon.oss;

import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadUtils;
import org.apache.paimon.fs.VectoredReadable;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import org.apache.hadoop.fs.FileSystem;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.ExceptionUtils.firstOrSuppressed;
import static org.apache.paimon.utils.ExceptionUtils.rethrowIOException;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Lazy, bounded OSS range reads for files whose length is already recorded in metadata. */
class OSSRangeInputStream extends SeekableInputStream implements VectoredReadable {

    private static final int BUFFER_SIZE = 64 * 1024;

    private final OSSClient client;
    private final String bucket;
    private final String key;
    private final long fileSize;
    @Nullable private final FileSystem.Statistics statistics;
    private final Set<InputStream> activeRequests = new HashSet<>();

    private volatile boolean closed;
    private long position;
    private byte[] buffer;
    private long bufferStart;
    private int bufferLength;

    OSSRangeInputStream(
            OSSClient client,
            String bucket,
            String key,
            long fileSize,
            @Nullable FileSystem.Statistics statistics) {
        checkArgument(fileSize >= 0, "File size must be non-negative.");
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.fileSize = fileSize;
        this.statistics = statistics;
    }

    @Override
    public synchronized void seek(long desired) throws IOException {
        ensureOpen();
        if (desired < 0 || desired > fileSize) {
            throw new EOFException("Seek outside file: " + desired);
        }
        position = desired;
    }

    @Override
    public synchronized long getPos() throws IOException {
        ensureOpen();
        return position;
    }

    @Override
    public synchronized int read() throws IOException {
        ensureOpen();
        if (position == fileSize) {
            return -1;
        }
        if (position < bufferStart || position >= bufferStart + bufferLength) {
            fillBuffer();
        }
        int value = buffer[(int) (position++ - bufferStart)] & 0xff;
        if (statistics != null) {
            statistics.incrementBytesRead(1);
        }
        return value;
    }

    @Override
    public synchronized int read(byte[] bytes, int offset, int length) throws IOException {
        checkBounds(bytes, offset, length);
        ensureOpen();
        if (length == 0) {
            return 0;
        }
        if (position == fileSize) {
            return -1;
        }
        final int count;
        if (position >= bufferStart && position < bufferStart + bufferLength) {
            count = (int) Math.min(length, bufferStart + bufferLength - position);
            System.arraycopy(buffer, (int) (position - bufferStart), bytes, offset, count);
        } else if (length >= BUFFER_SIZE) {
            count = readRange(position, bytes, offset, length);
        } else {
            fillBuffer();
            count = Math.min(length, bufferLength);
            System.arraycopy(buffer, 0, bytes, offset, count);
        }
        position += count;
        if (statistics != null) {
            statistics.incrementBytesRead(count);
        }
        return count;
    }

    private void fillBuffer() throws IOException {
        if (buffer == null) {
            buffer = new byte[BUFFER_SIZE];
        }
        bufferStart = position;
        bufferLength = 0;
        bufferLength =
                readRange(position, buffer, 0, (int) Math.min(BUFFER_SIZE, fileSize - position));
    }

    @Override
    public int pread(long start, byte[] bytes, int offset, int length) throws IOException {
        int count = readRange(start, bytes, offset, length);
        if (count > 0 && statistics != null) {
            statistics.incrementBytesRead(count);
        }
        return count;
    }

    private int readRange(long start, byte[] bytes, int offset, int length) throws IOException {
        checkBounds(bytes, offset, length);
        ensureOpen();
        checkArgument(start >= 0, "Read position must be non-negative.");
        if (length == 0) {
            return 0;
        }
        if (start >= fileSize) {
            return -1;
        }
        int count = (int) Math.min(length, fileSize - start);
        GetObjectRequest request = new GetObjectRequest(bucket, key);
        request.setRange(start, start + count - 1);
        final InputStream input;
        try {
            input = client.getObject(request).getObjectContent();
            if (statistics != null) {
                statistics.incrementReadOps(1);
            }
        } catch (OSSException e) {
            if ("NoSuchKey".equals(e.getErrorCode())) {
                FileNotFoundException missing = new FileNotFoundException(key);
                missing.initCause(e);
                throw missing;
            }
            throw new IOException("Failed to open OSS range for " + key, e);
        } catch (RuntimeException e) {
            throw new IOException("Failed to open OSS range for " + key, e);
        }
        synchronized (activeRequests) {
            if (closed) {
                input.close();
                throw new IOException("Stream is closed.");
            }
            activeRequests.add(input);
        }
        try (InputStream in = input) {
            int read = 0;
            while (read < count) {
                int n = in.read(bytes, offset + read, count - read);
                if (n < 0) {
                    throw new EOFException("OSS range ended before the recorded file length.");
                }
                if (n == 0) {
                    int value = in.read();
                    if (value < 0) {
                        throw new EOFException("OSS range ended before the recorded file length.");
                    }
                    bytes[offset + read++] = (byte) value;
                } else {
                    read += n;
                }
            }
            return count;
        } finally {
            synchronized (activeRequests) {
                activeRequests.remove(input);
            }
        }
    }

    @Override
    public void readVectored(List<? extends FileRange> ranges) throws IOException {
        ensureOpen();
        for (FileRange range : ranges) {
            if (range.getLength() > 0
                    && (range.getOffset() < 0
                            || range.getOffset() >= fileSize
                            || range.getLength() > fileSize - range.getOffset())) {
                throw new EOFException("Range exceeds the recorded file length.");
            }
        }
        // Even a single combined range must use its exact bounds, not the sequential buffer.
        VectoredReadUtils.readVectored(
                this,
                ranges,
                VectoredReadUtils.ReadOptions.from(this).withSequentialReadFallback(false));
    }

    @Override
    public void close() throws IOException {
        final List<InputStream> requests;
        synchronized (activeRequests) {
            if (closed) {
                return;
            }
            closed = true;
            requests = new ArrayList<>(activeRequests);
            activeRequests.clear();
        }
        Throwable failure = null;
        for (InputStream input : requests) {
            try {
                input.close();
            } catch (Throwable e) {
                failure = firstOrSuppressed(e, failure);
            }
        }
        if (failure != null) {
            rethrowIOException(failure);
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed.");
        }
    }

    private static void checkBounds(byte[] bytes, int offset, int length) {
        if (offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IndexOutOfBoundsException();
        }
    }
}
