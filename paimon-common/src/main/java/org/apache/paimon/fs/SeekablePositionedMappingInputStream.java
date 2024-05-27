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

import java.io.EOFException;
import java.io.IOException;

/** Positioned mapping seekable input stream. */
public class SeekablePositionedMappingInputStream extends SeekableInputStream {

    private final SeekableInputStream seekableInputStream;
    private final int start;
    private final int length;

    public SeekablePositionedMappingInputStream(byte[] bytes) {
        this.seekableInputStream = new ByteArraySeekableStream(bytes);
        this.start = 0;
        this.length = bytes.length;
    }

    public SeekablePositionedMappingInputStream(
            SeekableInputStream seekableInputStream, int start, int length) throws IOException {
        this.seekableInputStream = seekableInputStream;
        this.start = start;
        this.length = length;

        seek(0);
    }

    @Override
    public void seek(long desired) throws IOException {
        if (desired >= length) {
            throw new EOFException();
        }
        seekableInputStream.seek(desired + start);
    }

    @Override
    public long getPos() throws IOException {
        return seekableInputStream.getPos() - start;
    }

    @Override
    public int read() throws IOException {
        if (available() <= 0) {
            throw new EOFException();
        }
        return seekableInputStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len > available()) {
            throw new EOFException();
        }
        return seekableInputStream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        if (available() < n) {
            throw new EOFException();
        }
        return seekableInputStream.skip(n);
    }

    @Override
    public int available() throws IOException {
        return length + start - (int) seekableInputStream.getPos();
    }

    public byte[] readAllBytes() {
        byte[] b = new byte[length];
        try {
            seekableInputStream.seek(start);
            int n = 0;
            int len = b.length;
            // read fully until b is full else throw.
            while (n < len) {
                int count = seekableInputStream.read(b, n, len - n);
                if (count < 0) {
                    throw new EOFException();
                }
                n += count;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return b;
    }

    public int length() {
        return length;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
