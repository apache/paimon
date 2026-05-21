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
import org.apache.paimon.mosaic.InputFile;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Semaphore;

/**
 * Adapts Paimon's {@link FileIO} to Mosaic's {@link InputFile} interface.
 *
 * <p>Maintains a pool of up to 8 {@link SeekableInputStream} instances. When all 8 are in use,
 * callers block until one is returned. Thread-safe: Mosaic may invoke {@link #readFully}
 * concurrently from multiple threads.
 */
public class MosaicInputFileAdapter implements InputFile, Closeable {

    private static final int MAX_POOL_SIZE = 8;

    private final FileIO fileIO;
    private final Path path;
    private final Semaphore semaphore;
    private final Deque<SeekableInputStream> pool;
    private boolean closed;

    public MosaicInputFileAdapter(FileIO fileIO, Path path) {
        this.fileIO = fileIO;
        this.path = path;
        this.semaphore = new Semaphore(MAX_POOL_SIZE);
        this.pool = new ArrayDeque<>(MAX_POOL_SIZE);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        SeekableInputStream in = acquire();
        try {
            in.seek(position);
            int remaining = length;
            int off = offset;
            while (remaining > 0) {
                int read = in.read(buffer, off, remaining);
                if (read < 0) {
                    throw new EOFException(
                            "Reached end of file while reading "
                                    + path
                                    + " at position "
                                    + position);
                }
                off += read;
                remaining -= read;
            }
            release(in);
        } catch (Throwable t) {
            closeQuietly(in);
            semaphore.release();
            throw t;
        }
    }

    private SeekableInputStream acquire() throws IOException {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for stream", e);
        }
        synchronized (pool) {
            if (closed) {
                semaphore.release();
                throw new IOException("MosaicInputFileAdapter is closed");
            }
            SeekableInputStream in = pool.pollFirst();
            if (in != null) {
                return in;
            }
        }
        return fileIO.newInputStream(path);
    }

    private void release(SeekableInputStream in) {
        synchronized (pool) {
            if (!closed) {
                pool.addLast(in);
                semaphore.release();
                return;
            }
        }
        closeQuietly(in);
        semaphore.release();
    }

    @Override
    public void close() throws IOException {
        Deque<SeekableInputStream> toClose;
        synchronized (pool) {
            closed = true;
            toClose = new ArrayDeque<>(pool);
            pool.clear();
        }
        IOException firstException = null;
        for (SeekableInputStream in : toClose) {
            try {
                in.close();
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    private static void closeQuietly(SeekableInputStream in) {
        try {
            in.close();
        } catch (IOException ignored) {
        }
    }
}
