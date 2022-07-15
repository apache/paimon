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

package org.apache.flink.table.store.format.fs;

import org.apache.flink.core.fs.FSDataInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;

/** A {@link InputStream} to wrap {@link FSDataInputStream} for Flink's input streams. */
public class FlinkInputStream extends InputStream implements Seekable, PositionedReadable {

    private final org.apache.flink.core.fs.FSDataInputStream fsDataInputStream;

    public FlinkInputStream(FSDataInputStream fsDataInputStream) {
        this.fsDataInputStream = fsDataInputStream;
    }

    @Override
    public int read() throws IOException {
        return fsDataInputStream.read();
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        fsDataInputStream.seek(position);
        return fsDataInputStream.read(buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        fsDataInputStream.seek(position);
        IOUtils.read(fsDataInputStream, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        fsDataInputStream.seek(position);
        IOUtils.read(fsDataInputStream, buffer);
    }

    @Override
    public void seek(long pos) throws IOException {
        fsDataInputStream.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
        return fsDataInputStream.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
        return false;
    }
}
