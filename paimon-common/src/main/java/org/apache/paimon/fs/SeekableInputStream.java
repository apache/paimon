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
     * Reads up to <code>len</code> bytes of data from the input stream at the given <code>position
     * </code> into an array of bytes. An attempt is made to read as many as <code>len</code> bytes,
     * but a smaller number may be read. The number of bytes actually read is returned as an
     * integer. The method will not change the current offset of this stream, and should be
     * thread-safe. default implementation: use method <code>read</code>.
     *
     * @return
     * @throws IOException
     */
    public int pread(long position, byte[] b, int off, int len) throws IOException {
        synchronized (this) {
            long currentPos = getPos();
            seek(position);
            int actualReadBytes = read(b, off, len);
            seek(currentPos);
            return actualReadBytes;
        }
    }

    /**
     * Closes this input stream and releases any system resources associated with the stream.
     *
     * <p>The <code>close</code> method of <code>InputStream</code> does nothing.
     *
     * @exception IOException if an I/O error occurs.
     */
    public abstract void close() throws IOException;
}
