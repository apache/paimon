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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.mosaic.InputFile;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Adapts Paimon's {@link FileIO} to Mosaic's {@link InputFile}; each concurrent read borrows its
 * own {@link SeekableInputStream} from a pool because streams serialize concurrent reads.
 */
public class MosaicInputFileAdapter implements InputFile, Closeable {

    private final FileIO fileIO;
    private final Path path;

    private final ArrayDeque<SeekableInputStream> idleStreams = new ArrayDeque<>();
    private final List<SeekableInputStream> allStreams = new ArrayList<>();
    private boolean closed;

    // I/O counters (diagnostics; see MosaicRecordsReader#close).
    private final AtomicLong readCount = new AtomicLong();
    private final AtomicLong smallReadCount = new AtomicLong();
    private final AtomicLong mediumReadCount = new AtomicLong(); // [4 KiB, 256 KiB)
    private final AtomicLong largeReadCount = new AtomicLong(); // [256 KiB, 1 MiB)
    private final AtomicLong hugeReadCount = new AtomicLong(); // >= 1 MiB
    private final AtomicLong readBytes = new AtomicLong();
    private final AtomicLong readNanos = new AtomicLong();
    private final AtomicLong maxReadNanos = new AtomicLong();

    public MosaicInputFileAdapter(FileIO fileIO, Path path) throws IOException {
        this.fileIO = fileIO;
        this.path = path;
        // Open eagerly so that a missing file fails here rather than in a native callback.
        release(fileIO.newInputStream(path));
    }

    public String ioStats() {
        return "reads="
                + readCount.get()
                + "|small_reads="
                + smallReadCount.get()
                + "|medium_reads="
                + mediumReadCount.get()
                + "|large_reads="
                + largeReadCount.get()
                + "|huge_reads="
                + hugeReadCount.get()
                + "|bytes="
                + readBytes.get()
                + "|read_ms="
                + readNanos.get() / 1_000_000
                + "|max_read_ms="
                + maxReadNanos.get() / 1_000_000
                + "|streams="
                + streamCount();
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        long start = System.nanoTime();
        SeekableInputStream in = borrow();
        try {
            doReadFully(in, position, buffer, offset, length);
        } finally {
            release(in);
            long nanos = System.nanoTime() - start;
            readCount.incrementAndGet();
            if (length < 4096) {
                smallReadCount.incrementAndGet();
            } else if (length < 256 * 1024) {
                mediumReadCount.incrementAndGet();
            } else if (length < 1024 * 1024) {
                largeReadCount.incrementAndGet();
            } else {
                hugeReadCount.incrementAndGet();
            }
            readBytes.addAndGet(length);
            readNanos.addAndGet(nanos);
            maxReadNanos.accumulateAndGet(nanos, Math::max);
        }
    }

    private void doReadFully(
            SeekableInputStream in, long position, byte[] buffer, int offset, int length)
            throws IOException {
        if (in instanceof VectoredReadable) {
            ((VectoredReadable) in).preadFully(position, buffer, offset, length);
            return;
        }
        // The stream is borrowed exclusively, so seek + read needs no extra locking.
        in.seek(position);
        int remaining = length;
        int off = offset;
        while (remaining > 0) {
            int read = in.read(buffer, off, remaining);
            if (read < 0) {
                throw new EOFException(
                        "Reached end of file while reading " + path + " at position " + position);
            }
            off += read;
            remaining -= read;
        }
    }

    private SeekableInputStream borrow() throws IOException {
        synchronized (this) {
            if (closed) {
                throw new IOException("Input file " + path + " is closed");
            }
            SeekableInputStream idle = idleStreams.poll();
            if (idle != null) {
                return idle;
            }
        }
        SeekableInputStream opened = fileIO.newInputStream(path);
        synchronized (this) {
            if (closed) {
                opened.close();
                throw new IOException("Input file " + path + " is closed");
            }
            allStreams.add(opened);
        }
        return opened;
    }

    private void release(SeekableInputStream in) throws IOException {
        synchronized (this) {
            if (!closed) {
                if (!allStreams.contains(in)) {
                    allStreams.add(in);
                }
                idleStreams.push(in);
                return;
            }
        }
        in.close();
    }

    private synchronized int streamCount() {
        return allStreams.size();
    }

    @Override
    public void close() throws IOException {
        List<SeekableInputStream> toClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            toClose = new ArrayList<>(allStreams);
            allStreams.clear();
            idleStreams.clear();
        }
        IOException failure = null;
        for (SeekableInputStream in : toClose) {
            try {
                in.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
