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

package org.apache.paimon.fs.cache;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.utils.IOUtils;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link SeekableInputStream} that caches reads at block granularity on local disk.
 *
 * <p>{@link #pread} and {@link #preadFully} are thread-safe, as {@link VectoredReadable} requires,
 * and open exactly one remote stream between them. {@link #close} is safe to call concurrently with
 * them and never leaks that stream, but a read already in flight may fail with an
 * implementation-specific {@link IOException} from the remote. The positional methods {@link
 * #seek}, {@link #read} and {@link #getPos} are not thread-safe, exactly as for any other stream.
 */
public class CachingSeekableInputStream extends SeekableInputStream implements VectoredReadable {

    private final FileIO fileIO;
    private final Path path;
    private final LocalCacheManager cache;
    private final String cacheKey;
    private final AtomicReference<SeekableInputStream> remoteStream = new AtomicReference<>();
    // private, so no caller can stall a lazy init by locking the stream itself
    private final Object lock = new Object();
    private long pos;
    private volatile long fileSize;
    private volatile boolean closed;

    public CachingSeekableInputStream(FileIO fileIO, Path path, LocalCacheManager cache) {
        this(fileIO, path, cache, path.toString(), -1);
    }

    CachingSeekableInputStream(
            FileIO fileIO, Path path, LocalCacheManager cache, String cacheKey, long fileSize) {
        this.fileIO = fileIO;
        this.path = path;
        this.cache = cache;
        this.cacheKey = cacheKey;
        this.fileSize = fileSize;
        this.pos = 0;
    }

    private long fileSize() throws IOException {
        // guarded like the remote stream below, and for the same reason: a vectored fan-out reaches
        // this first, and an unguarded lazy init would issue one getFileStatus per thread
        long current = fileSize;
        if (current >= 0) {
            return current;
        }
        synchronized (lock) {
            current = fileSize;
            if (current < 0) {
                current = cache.getFileSize(cacheKey);
                if (current < 0) {
                    current = fileIO.getFileStatus(path).getLen();
                    cache.putFileSize(cacheKey, current);
                }
                fileSize = current;
            }
        }
        return current;
    }

    @Override
    public void seek(long desired) throws IOException {
        this.pos = Math.max(0, desired);
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public int read() throws IOException {
        checkNotClosed();
        if (pos >= fileSize()) {
            return -1;
        }
        int blockSize = cache.blockSize();
        int blockIndex = (int) (pos / blockSize);
        byte[] blockData = readBlock(blockIndex);
        int offsetInBlock = (int) (pos - (long) blockIndex * blockSize);
        pos++;
        return blockData[offsetInBlock] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkNotClosed();
        if (len == 0) {
            return 0;
        }
        if (pos >= fileSize()) {
            return -1;
        }

        int blockSize = cache.blockSize();
        long end = Math.min(pos + len, fileSize());
        int totalRead = 0;

        while (pos < end) {
            int blockIndex = (int) (pos / blockSize);
            byte[] blockData = readBlock(blockIndex);

            long blockStart = (long) blockIndex * blockSize;
            int startInBlock = (int) (pos - blockStart);
            int endInBlock = (int) Math.min(end - blockStart, blockData.length);
            int bytesToCopy = endInBlock - startInBlock;

            System.arraycopy(blockData, startInBlock, b, off + totalRead, bytesToCopy);
            totalRead += bytesToCopy;
            pos += bytesToCopy;
        }

        return totalRead;
    }

    @Override
    public int pread(long position, byte[] buffer, int offset, int length) throws IOException {
        checkNotClosed();
        if (length == 0) {
            return 0;
        }
        long end = Math.min(position + length, fileSize());
        if (position >= end) {
            return -1;
        }

        int blockSize = cache.blockSize();
        int totalRead = 0;

        while (position < end) {
            int blockIndex = (int) (position / blockSize);
            byte[] blockData = readBlock(blockIndex);

            long blockStart = (long) blockIndex * blockSize;
            int startInBlock = (int) (position - blockStart);
            int endInBlock = (int) Math.min(end - blockStart, blockData.length);
            int bytesToCopy = endInBlock - startInBlock;

            System.arraycopy(blockData, startInBlock, buffer, offset + totalRead, bytesToCopy);
            totalRead += bytesToCopy;
            position += bytesToCopy;
        }

        return totalRead;
    }

    private byte[] readBlock(int blockIndex) throws IOException {
        byte[] cached = cache.getBlock(cacheKey, blockIndex);
        if (cached != null) {
            return cached;
        }

        int blockSize = cache.blockSize();
        long offset = (long) blockIndex * blockSize;
        int readSize = (int) Math.min(blockSize, fileSize() - offset);

        byte[] data = readRemote(offset, readSize);

        cache.putBlock(cacheKey, blockIndex, data);
        return data;
    }

    private byte[] readRemote(long offset, int size) throws IOException {
        SeekableInputStream stream = getRemoteStream();
        if (stream instanceof VectoredReadable) {
            byte[] buf = new byte[size];
            ((VectoredReadable) stream).preadFully(offset, buf, 0, size);
            return buf;
        }
        synchronized (stream) {
            stream.seek(offset);
            // must not tolerate a short read: the block is handed to putBlock, so a zero-padded
            // tail would be cached and returned as file content for every later read
            byte[] buf = new byte[size];
            IOUtils.readFully(stream, buf, 0, size);
            return buf;
        }
    }

    private SeekableInputStream getRemoteStream() throws IOException {
        // reached concurrently: readVectored fans preadFully out over an IO thread pool, so an
        // unguarded lazy init would open one stream per thread and leak all but the last
        SeekableInputStream current = remoteStream.get();
        if (current != null) {
            checkNotClosed();
            return current;
        }

        boolean opened = false;
        synchronized (lock) {
            current = remoteStream.get();
            if (current == null) {
                // close() may have won while this call was queued for the lock
                checkNotClosed();
                current = fileIO.newInputStream(path);
                remoteStream.set(current);
                opened = true;
            }
        }

        // close() does not wait for the open above, so it may have run right through it. Only the
        // thread that opened the stream hands it back: close() sets the flag before detaching and
        // this reads it after publishing, so one of the two always sees the other.
        if (opened && closed) {
            IOUtils.closeQuietly(remoteStream.getAndSet(null));
        }
        checkNotClosed();
        return current;
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed: " + path);
        }
    }

    @Override
    public void close() throws IOException {
        // takes no lock, so it never waits behind an in-flight remote open
        closed = true;
        SeekableInputStream current = remoteStream.getAndSet(null);
        if (current != null) {
            current.close();
        }
    }
}
