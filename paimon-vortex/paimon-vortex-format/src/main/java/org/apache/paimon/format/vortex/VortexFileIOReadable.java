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

package org.apache.paimon.format.vortex;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;

import dev.vortex.io.NativeReadable;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Lets the native Vortex reader read a file through a Paimon {@link FileIO}.
 *
 * <p>Native worker threads read concurrently. A stream that supports positional reads is shared by
 * all of them. Other streams are pooled, so each concurrent read seeks its own stream.
 */
final class VortexFileIOReadable implements NativeReadable {

    private static final int COPY_CHUNK_SIZE = 1024 * 1024;

    private final FileIO fileIO;
    private final Path path;
    private final long length;
    private final ConcurrentLinkedQueue<SeekableInputStream> idleStreams =
            new ConcurrentLinkedQueue<>();
    private final List<SeekableInputStream> allStreams = new ArrayList<>();
    private boolean closed;

    VortexFileIOReadable(FileIO fileIO, Path path, long length) {
        this.fileIO = fileIO;
        this.path = path;
        this.length = length;
    }

    @Override
    public String name() {
        return path.toString();
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public void readFully(long position, ByteBuffer buffer) throws IOException {
        if (position < 0 || position + buffer.remaining() > length) {
            throw new EOFException(
                    String.format(
                            "Read of %d bytes at %d is past the end of %s (%d bytes)",
                            buffer.remaining(), position, path, length));
        }

        SeekableInputStream stream = borrow();
        try {
            byte[] chunk = new byte[Math.min(buffer.remaining(), COPY_CHUNK_SIZE)];
            long offset = position;
            while (buffer.hasRemaining()) {
                int n = Math.min(buffer.remaining(), chunk.length);
                read(stream, offset, chunk, n);
                buffer.put(chunk, 0, n);
                offset += n;
            }
        } finally {
            idleStreams.offer(stream);
        }
    }

    @Override
    public void close() throws IOException {
        List<SeekableInputStream> streams;
        synchronized (this) {
            closed = true;
            streams = new ArrayList<>(allStreams);
            allStreams.clear();
        }
        idleStreams.clear();

        IOException failure = null;
        for (SeekableInputStream stream : streams) {
            try {
                stream.close();
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

    private SeekableInputStream borrow() throws IOException {
        SeekableInputStream stream = idleStreams.poll();
        if (stream != null) {
            return stream;
        }

        stream = fileIO.newInputStream(path);
        synchronized (this) {
            if (!closed) {
                allStreams.add(stream);
                return stream;
            }
        }
        stream.close();
        throw new IOException("Readable for " + path + " is closed");
    }

    private static void read(SeekableInputStream stream, long position, byte[] chunk, int n)
            throws IOException {
        if (stream instanceof VectoredReadable) {
            ((VectoredReadable) stream).preadFully(position, chunk, 0, n);
            return;
        }

        stream.seek(position);
        int read = 0;
        while (read < n) {
            int r = stream.read(chunk, read, n - read);
            if (r < 0) {
                throw new EOFException("Unexpected end of stream while reading " + n + " bytes");
            }
            read += r;
        }
    }
}
