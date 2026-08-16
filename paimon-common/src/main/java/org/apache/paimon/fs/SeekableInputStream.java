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

import org.apache.paimon.annotation.Public;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@code SeekableInputStream} provides seek methods.
 *
 * @since 0.4.0
 */
@Public
public abstract class SeekableInputStream extends InputStream {

    /**
     * Seek to the given offset from the start of the file. The next read() will be from that
     * location. Can't seek past the end of the stream.
     *
     * @param desired the desired offset
     * @throws IOException Thrown if an error occurred while seeking inside the input stream.
     */
    public abstract void seek(long desired) throws IOException;

    /**
     * Gets the current position in the input stream.
     *
     * @return current position in the input stream
     * @throws IOException Thrown if an I/O error occurred in the underlying stream implementation
     *     while accessing the stream's position.
     */
    public abstract long getPos() throws IOException;

    /**
     * Reads up to <code>len</code> bytes of data from the input stream into an array of bytes. An
     * attempt is made to read as many as <code>len</code> bytes, but a smaller number may be read.
     * The number of bytes actually read is returned as an integer.
     */
    public abstract int read(byte[] b, int off, int len) throws IOException;

    /**
     * Closes this input stream and releases any system resources associated with the stream.
     *
     * <p>The <code>close</code> method of <code>InputStream</code> does nothing.
     *
     * @exception IOException if an I/O error occurs.
     */
    public abstract void close() throws IOException;

    /**
     * Wrap a normal {@link InputStream} to a {@link SeekableInputStream}. The returned {@link
     * SeekableInputStream} does not support seek operations.
     */
    public static SeekableInputStream wrap(InputStream inputStream) {
        final InputStream in = inputStream;
        return new SeekableInputStream() {
            @Override
            public void seek(long desired) {
                throw new UnsupportedOperationException("Seek not supported");
            }

            @Override
            public long getPos() {
                throw new UnsupportedOperationException("getPos not supported");
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return in.read(b, off, len);
            }

            @Override
            public void close() throws IOException {
                in.close();
            }

            @Override
            public int read() throws IOException {
                return in.read();
            }
        };
    }
}
