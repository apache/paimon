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
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Adapter that exposes a Paimon {@link SeekableInputStream} as a Mosaic {@link InputFile}.
 *
 * <p>Each read borrows one of at most {@code maxStreams} input streams, so concurrent reads do not
 * serialize on a single stream; a read that finds every stream busy waits for one.
 */
public class MosaicInputFileAdapter implements InputFile, Closeable {

    private final FileIO fileIO;
    private final Path path;
    private final int maxStreams;

    private final ArrayDeque<SeekableInputStream> idleStreams = new ArrayDeque<>();
    private final List<SeekableInputStream> allStreams = new ArrayList<>();
    private int openingStreams;
    private boolean closed;

    public MosaicInputFileAdapter(FileIO fileIO, Path path) throws IOException {
        this(fileIO, path, 1);
    }

    public MosaicInputFileAdapter(FileIO fileIO, Path path, int maxStreams) throws IOException {
        this.fileIO = fileIO;
        this.path = path;
        this.maxStreams = Math.max(1, maxStreams);
        // Open eagerly so that a missing file fails here rather than in a native callback.
        SeekableInputStream first = fileIO.newInputStream(path);
        allStreams.add(first);
        idleStreams.push(first);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        SeekableInputStream in = borrow();
        try {
            doReadFully(in, position, buffer, offset, length);
        } finally {
            release(in);
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
            while (true) {
                if (closed) {
                    throw new IOException("Input file " + path + " is closed");
                }
                SeekableInputStream idle = idleStreams.poll();
                if (idle != null) {
                    return idle;
                }
                if (allStreams.size() + openingStreams < maxStreams) {
                    openingStreams++;
                    break;
                }
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException(
                            "Interrupted while waiting for an input stream of " + path);
                }
            }
        }
        SeekableInputStream opened = null;
        try {
            opened = fileIO.newInputStream(path);
        } finally {
            synchronized (this) {
                openingStreams--;
                if (opened != null && !closed) {
                    allStreams.add(opened);
                } else {
                    notifyAll();
                }
            }
        }
        if (closed) {
            opened.close();
            throw new IOException("Input file " + path + " is closed");
        }
        return opened;
    }

    private void release(SeekableInputStream in) throws IOException {
        synchronized (this) {
            if (!closed) {
                idleStreams.push(in);
                notifyAll();
                return;
            }
        }
        in.close();
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
            notifyAll();
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
