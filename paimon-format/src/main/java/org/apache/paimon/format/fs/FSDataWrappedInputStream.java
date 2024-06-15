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

package org.apache.paimon.format.fs;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;

/** A {@link InputStream} to wrap {@link SeekableInputStream} for Paimon's input streams. */
public class FSDataWrappedInputStream extends InputStream implements Seekable, PositionedReadable {

    private final SeekableInputStream seekableInputStream;

    public FSDataWrappedInputStream(SeekableInputStream seekableInputStream) {
        this.seekableInputStream = seekableInputStream;
    }

    public SeekableInputStream wrapped() {
        return seekableInputStream;
    }

    @Override
    public int read() throws IOException {
        return seekableInputStream.read();
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        seekableInputStream.seek(position);
        return seekableInputStream.read(buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        seekableInputStream.seek(position);
        IOUtils.readFully(seekableInputStream, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void seek(long pos) throws IOException {
        seekableInputStream.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
        return seekableInputStream.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
        return false;
    }

    @Override
    public void close() throws IOException {
        seekableInputStream.close();
    }
}
