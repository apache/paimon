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

package org.apache.paimon.vfs.hadoop;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * VFSInputStream wrap over paimon SeekableInputStream to support hadoop FSDataInputStream. TODO:
 * SeekableInputStream interface is too simple to fully support all FSDataInputStream operations: 1.
 * ByteBufferReadable and ByteBufferPositionedReadable should be implemented for full support.
 */
public class VFSInputStream extends FSInputStream {
    private SeekableInputStream in;
    private byte[] oneByteBuf = new byte[1];
    private FileSystem.Statistics statistics;

    public VFSInputStream(SeekableInputStream in, FileSystem.Statistics statistics) {
        this.in = in;
        this.statistics = statistics;
    }

    @Override
    public void seek(long pos) throws IOException {
        in.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
        return in.getPos();
    }

    @Override
    public boolean seekToNewSource(long var1) throws IOException {
        return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        if (in instanceof VectoredReadable) {
            int byteRead = ((VectoredReadable) in).pread(position, buffer, offset, length);
            if (statistics != null && byteRead >= 0) {
                statistics.incrementBytesRead(byteRead);
            }
            return byteRead;
        }
        return super.read(position, buffer, offset, length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int byteRead = in.read(b, off, len);
        if (statistics != null && byteRead >= 0) {
            statistics.incrementBytesRead(byteRead);
        }
        return byteRead;
    }

    @Override
    public int read() throws IOException {
        int n;
        while ((n = read(oneByteBuf, 0, 1)) == 0) {
            /* no op */
        }
        if (statistics != null && n >= 0) {
            statistics.incrementBytesRead(n);
        }
        return (n == -1) ? -1 : oneByteBuf[0] & 0xff;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
