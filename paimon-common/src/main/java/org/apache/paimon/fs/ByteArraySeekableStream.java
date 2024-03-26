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

package org.apache.paimon.fs;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

/** Wrap byte buf to a seekable input stream. */
public class ByteArraySeekableStream extends SeekableInputStream {

    private final ByteArrayStream byteArrayStream;

    public ByteArraySeekableStream(byte[] buf) {
        this.byteArrayStream = new ByteArrayStream(buf);
    }

    @Override
    public void seek(long desired) throws IOException {
        byteArrayStream.seek((int) desired);
    }

    @Override
    public long getPos() throws IOException {
        return byteArrayStream.getPos();
    }

    @Override
    public int read() throws IOException {
        return byteArrayStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return byteArrayStream.read(b, off, len);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return byteArrayStream.read(b);
    }

    @Override
    public long skip(long n) throws IOException {
        return byteArrayStream.skip(n);
    }

    @Override
    public int available() throws IOException {
        return byteArrayStream.available();
    }

    @Override
    public synchronized void mark(int readlimit) {
        byteArrayStream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        byteArrayStream.reset();
    }

    @Override
    public boolean markSupported() {
        return byteArrayStream.markSupported();
    }

    @Override
    public void close() throws IOException {
        byteArrayStream.close();
    }

    private static class ByteArrayStream extends ByteArrayInputStream {
        public ByteArrayStream(byte[] buf) {
            super(buf);
        }

        public void seek(int position) throws IOException {
            if (position >= count) {
                throw new EOFException("Can't seek position: " + position + ", length is " + count);
            }
            pos = position;
        }

        public long getPos() {
            return pos;
        }
    }
}
