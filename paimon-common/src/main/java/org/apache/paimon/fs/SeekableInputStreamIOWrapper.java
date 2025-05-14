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

import org.apache.paimon.fs.metrics.IOMetrics;

import java.io.IOException;

/** Wrap a {@link SeekableInputStream}. */
public class SeekableInputStreamIOWrapper extends SeekableInputStream implements VectoredReadable {

    protected final SeekableInputStream in;
    private IOMetrics metrics;

    public SeekableInputStreamIOWrapper(SeekableInputStream in, IOMetrics metrics) {
        this.in = in;
        this.metrics = metrics;
    }

    @Override
    public void seek(long desired) throws IOException {
        in.seek(desired);
    }

    @Override
    public long getPos() throws IOException {
        return in.getPos();
    }

    @Override
    public int read() throws IOException {
        int bytesRead = 0;
        try {
            bytesRead = in.read();
        } finally {
            if (metrics != null && bytesRead != -1) {
                metrics.recordReadEvent(bytesRead);
            }
        }
        return bytesRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int bytesRead = 0;
        try {
            bytesRead = in.read(b, off, len);
        } finally {
            if (metrics != null && bytesRead != -1) {
                metrics.recordReadEvent(bytesRead);
            }
        }
        return bytesRead;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public int pread(long position, byte[] buffer, int offset, int length) throws IOException {
        int bytesRead = 0;
        try {
            if (in instanceof VectoredReadable) {
                bytesRead = ((VectoredReadable) in).pread(position, buffer, offset, length);
            } else {
                long originalPos = in.getPos();
                in.seek(position);
                bytesRead = in.read(buffer, offset, length);
                in.seek(originalPos);
            }
        } finally {
            if (metrics != null && bytesRead != -1) {
                metrics.recordReadEvent(bytesRead);
            }
        }
        return bytesRead;
    }
}
