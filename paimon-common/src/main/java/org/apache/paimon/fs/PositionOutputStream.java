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
import java.io.OutputStream;

/**
 * {@code PositionOutputStream} provides position methods.
 *
 * @since 0.4.0
 */
@Public
public abstract class PositionOutputStream extends OutputStream {

    /**
     * Gets the position of the stream (non-negative), defined as the number of bytes from the
     * beginning of the file to the current writing position. The position corresponds to the
     * zero-based index of the next byte that will be written.
     *
     * <p>This method must report accurately report the current position of the stream. Various
     * components of the high-availability and recovery logic rely on the accurate
     *
     * @return The current position in the stream, defined as the number of bytes from the beginning
     *     of the file to the current writing position.
     * @throws IOException Thrown if an I/O error occurs while obtaining the position from the
     *     stream implementation.
     */
    public abstract long getPos() throws IOException;

    /**
     * Writes <code>b.length</code> bytes from the specified byte array to this output stream. The
     * general contract for <code>write(b)</code> is that it should have exactly the same effect as
     * the call <code>write(b, 0, b.length)</code>.
     */
    public abstract void write(byte[] b) throws IOException;

    /**
     * Writes <code>len</code> bytes from the specified byte array starting at offset <code>off
     * </code> to this output stream. The general contract for <code>write(b, off, len)</code> is
     * that some of the bytes in the array <code>b</code> are written to the output stream in order;
     * element <code>b[off]</code> is the first byte written and <code>b[off+len-1]</code> is the
     * last byte written by this operation.
     */
    public abstract void write(byte[] b, int off, int len) throws IOException;

    /**
     * Flushes the stream, writing any data currently buffered in stream implementation to the
     * proper output stream. After this method has been called, the stream implementation must not
     * hold onto any buffered data any more.
     *
     * <p>Implementation note: This overrides the method defined in {@link OutputStream} as abstract
     * to force implementations of the {@code PositionOutputStream} to implement this method
     * directly.
     *
     * @throws IOException Thrown if an I/O error occurs while flushing the stream.
     */
    public abstract void flush() throws IOException;

    /**
     * Closes the output stream. After this method returns, the implementation must guarantee that
     * all data written to the stream is persistent/visible.
     *
     * <p>The above implies that the method must block until persistence can be guaranteed. For
     * example for distributed replicated file systems, the method must block until the replication
     * quorum has been reached. If the calling thread is interrupted in the process, it must fail
     * with an {@code IOException} to indicate that persistence cannot be guaranteed.
     *
     * <p>If this method throws an exception, the data in the stream cannot be assumed to be
     * persistent.
     *
     * <p>Implementation note: This overrides the method defined in {@link OutputStream} as abstract
     * to force implementations of the {@code PositionOutputStream} to implement this method
     * directly.
     *
     * @throws IOException Thrown, if an error occurred while closing the stream or guaranteeing
     *     that the data is persistent.
     */
    public abstract void close() throws IOException;
}
