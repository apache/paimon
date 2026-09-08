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

package org.apache.paimon.format.parquet;

import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadUtils;
import org.apache.paimon.fs.VectoredReadable;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A bounded, per-open-file tail buffer. Usually the footer and page indexes share one range GET;
 * larger metadata falls through to the original reader without changing the Parquet format.
 */
final class ParquetTailInputStream extends SeekableInputStream implements VectoredReadable {

    private static final int TAIL_SIZE = 128 * 1024;

    private final SeekableInputStream delegate;
    private final VectoredReadable positioned;
    private final long fileSize;
    private final long tailStart;
    private volatile byte[] tail;
    private volatile boolean closed;
    private long position;

    ParquetTailInputStream(SeekableInputStream delegate, long fileSize) {
        this.delegate = delegate;
        this.positioned = (VectoredReadable) delegate;
        this.fileSize = fileSize;
        this.tailStart = Math.max(0, fileSize - TAIL_SIZE);
    }

    @Override
    public void seek(long desired) throws IOException {
        ensureOpen();
        if (desired < 0 || desired > fileSize) {
            throw new EOFException("Seek outside Parquet file: " + desired);
        }
        position = desired;
    }

    @Override
    public long getPos() throws IOException {
        ensureOpen();
        return position;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        if (position == fileSize) {
            return -1;
        }
        if (position >= tailStart) {
            return tail()[(int) (position++ - tailStart)] & 0xff;
        }
        delegate.seek(position);
        int value = delegate.read();
        if (value >= 0) {
            position++;
        }
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        if (offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IndexOutOfBoundsException();
        }
        ensureOpen();
        if (length == 0) {
            return 0;
        }
        if (position == fileSize) {
            return -1;
        }
        final int count;
        if (position >= tailStart) {
            count = (int) Math.min(length, fileSize - position);
            System.arraycopy(tail(), (int) (position - tailStart), bytes, offset, count);
        } else {
            delegate.seek(position);
            count = delegate.read(bytes, offset, (int) Math.min(length, tailStart - position));
        }
        if (count > 0) {
            position += count;
        }
        return count;
    }

    private synchronized byte[] tail() throws IOException {
        ensureOpen();
        if (tail == null) {
            byte[] bytes = new byte[(int) (fileSize - tailStart)];
            positioned.preadFully(tailStart, bytes, 0, bytes.length);
            ensureOpen();
            tail = bytes;
        }
        return tail;
    }

    @Override
    public int pread(long start, byte[] bytes, int offset, int length) throws IOException {
        if (start < 0 || offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IndexOutOfBoundsException();
        }
        ensureOpen();
        if (length == 0) {
            return 0;
        }
        if (start >= fileSize) {
            return -1;
        }
        if (start >= tailStart) {
            int count = (int) Math.min(length, fileSize - start);
            System.arraycopy(tail(), (int) (start - tailStart), bytes, offset, count);
            return count;
        }
        return positioned.pread(start, bytes, offset, length);
    }

    @Override
    public void readVectored(List<? extends FileRange> ranges) throws IOException {
        ensureOpen();
        if (ranges.isEmpty()) {
            return;
        }
        List<FileRange> uncached = new ArrayList<>();
        for (FileRange range : VectoredReadUtils.validateAndSortRanges(ranges)) {
            if (range.getLength() == 0) {
                range.getData().complete(VectoredReadUtils.getOrCreateBuffer(range));
                continue;
            }
            if (range.getOffset() >= fileSize || range.getLength() > fileSize - range.getOffset()) {
                throw new EOFException("Range exceeds the Parquet file length.");
            }
            if (range.getOffset() >= tailStart
                    && range.getOffset() <= fileSize
                    && range.getLength() <= fileSize - range.getOffset()) {
                int start = (int) (range.getOffset() - tailStart);
                byte[] buffer = VectoredReadUtils.getOrCreateBuffer(range);
                System.arraycopy(tail(), start, buffer, 0, range.getLength());
                range.getData().complete(buffer);
            } else {
                uncached.add(range);
            }
        }
        // Preserve native vectored implementations for all uncached data ranges.
        if (!uncached.isEmpty()) {
            positioned.readVectored(uncached);
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            delegate.close();
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed.");
        }
    }
}
