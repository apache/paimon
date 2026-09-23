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

import org.apache.paimon.utils.IOUtils;

import java.io.IOException;

/**
 * A {@link SeekableInputStream} wrapping another {@link SeekableInputStream} with offset and
 * length.
 */
public class OffsetSeekableInputStream extends SeekableInputStream {

    private final SeekableInputStream wrapped;
    private final long offset;
    private final long length;

    public OffsetSeekableInputStream(SeekableInputStream wrapped, long offset, long length)
            throws IOException {
        this.wrapped = wrapped;
        this.offset = offset;
        this.length = length;
        if (offset != 0) {
            try {
                wrapped.seek(offset);
            } catch (IOException | RuntimeException | Error e) {
                // Constructor failed: close the wrapped stream so it doesn't leak.
                IOUtils.closeQuietly(wrapped);
                throw e;
            }
        }
    }

    @Override
    public void seek(long desired) throws IOException {
        wrapped.seek(offset + desired);
    }

    @Override
    public long getPos() throws IOException {
        return wrapped.getPos() - offset;
    }

    @Override
    public int read() throws IOException {
        if (length != -1) {
            if (getPos() >= length) {
                return -1;
            }
        }
        return wrapped.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        if (length != -1) {
            long remaining = length - getPos();
            if (remaining <= 0) {
                return -1;
            }
            len = (int) Math.min(len, remaining);
        }
        return wrapped.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }
}
