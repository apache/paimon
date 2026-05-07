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

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link SeekableInputStream} that caches reads at block granularity on local disk. */
public class CachingSeekableInputStream extends SeekableInputStream {

    private final FileIO fileIO;
    private final Path path;
    private final BlockDiskCache cache;
    private long pos;
    private long fileSize = -1;
    @Nullable private SeekableInputStream remoteStream;

    public CachingSeekableInputStream(FileIO fileIO, Path path, BlockDiskCache cache) {
        this.fileIO = fileIO;
        this.path = path;
        this.cache = cache;
        this.pos = 0;
    }

    private long fileSize() throws IOException {
        if (fileSize == -1) {
            fileSize = fileIO.getFileStatus(path).getLen();
        }
        return fileSize;
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

    private byte[] readBlock(int blockIndex) throws IOException {
        byte[] cached = cache.getBlock(path.toString(), blockIndex);
        if (cached != null) {
            return cached;
        }

        int blockSize = cache.blockSize();
        long offset = (long) blockIndex * blockSize;
        int readSize = (int) Math.min(blockSize, fileSize() - offset);

        SeekableInputStream stream = getRemoteStream();
        stream.seek(offset);
        byte[] data = readFully(stream, readSize);

        cache.putBlock(path.toString(), blockIndex, data);
        return data;
    }

    private SeekableInputStream getRemoteStream() throws IOException {
        if (remoteStream == null) {
            remoteStream = fileIO.newInputStream(path);
        }
        return remoteStream;
    }

    private static byte[] readFully(SeekableInputStream in, int size) throws IOException {
        byte[] buf = new byte[size];
        int remaining = size;
        int off = 0;
        while (remaining > 0) {
            int n = in.read(buf, off, remaining);
            if (n < 0) {
                break;
            }
            off += n;
            remaining -= n;
        }
        return buf;
    }

    @Override
    public void close() throws IOException {
        if (remoteStream != null) {
            remoteStream.close();
            remoteStream = null;
        }
    }
}
