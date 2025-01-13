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

package org.apache.paimon.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static java.util.Arrays.asList;

/** An utility class for I/O related functionality. */
public final class IOUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

    /** The block size for byte operations in byte. */
    public static final int BLOCKSIZE = 4096;

    // ------------------------------------------------------------------------
    //  Byte copy operations
    // ------------------------------------------------------------------------

    /**
     * Copies from one stream to another.
     *
     * @param in InputStream to read from
     * @param out OutputStream to write to
     * @param buffSize the size of the buffer
     * @param close whether or not close the InputStream and OutputStream at the end. The streams
     *     are closed in the finally clause.
     * @throws IOException thrown if an error occurred while writing to the output stream
     */
    public static void copyBytes(
            final InputStream in, final OutputStream out, final int buffSize, final boolean close)
            throws IOException {

        @SuppressWarnings("resource")
        final PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        final byte[] buf = new byte[buffSize];
        try {
            int bytesRead = in.read(buf);
            while (bytesRead >= 0) {
                out.write(buf, 0, bytesRead);
                if ((ps != null) && ps.checkError()) {
                    throw new IOException("Unable to write to output stream.");
                }
                bytesRead = in.read(buf);
            }
        } finally {
            if (close) {
                out.close();
                in.close();
            }
        }
    }

    /**
     * Copies from one stream to another. <strong>closes the input and output streams at the
     * end</strong>.
     *
     * @param in InputStream to read from
     * @param out OutputStream to write to
     * @throws IOException thrown if an I/O error occurs while copying
     */
    public static void copyBytes(final InputStream in, final OutputStream out) throws IOException {
        copyBytes(in, out, BLOCKSIZE, true);
    }

    // ------------------------------------------------------------------------
    //  Stream input skipping
    // ------------------------------------------------------------------------

    /** Reads all into a bytes. */
    public static byte[] readFully(InputStream in, boolean close) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        copyBytes(in, output, BLOCKSIZE, close);
        return output.toByteArray();
    }

    /**
     * Reads len bytes in a loop.
     *
     * @param in The InputStream to read from
     * @param buf The buffer to fill
     * @throws IOException if it could not read requested number of bytes for any reason (including
     *     EOF)
     */
    public static void readFully(final InputStream in, final byte[] buf) throws IOException {
        readFully(in, buf, 0, buf.length);
    }

    /**
     * Reads len bytes in a loop.
     *
     * @param in The InputStream to read from
     * @param buf The buffer to fill
     * @param off offset from the buffer
     * @param len the length of bytes to read
     * @throws IOException if it could not read requested number of bytes for any reason (including
     *     EOF)
     */
    public static void readFully(final InputStream in, final byte[] buf, int off, final int len)
            throws IOException {
        int toRead = len;
        while (toRead > 0) {
            final int ret = in.read(buf, off, toRead);
            if (ret < 0) {
                throw new IOException("Premature EOF from inputStream");
            }
            toRead -= ret;
            off += ret;
        }
    }

    public static String readUTF8Fully(final InputStream in) throws IOException {
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            builder.append(line);
        }
        return builder.toString();
    }

    // ------------------------------------------------------------------------
    //  Silent I/O cleanup / closing
    // ------------------------------------------------------------------------

    /** @see #closeAll(Iterable) */
    public static void closeAll(AutoCloseable... closeables) throws Exception {
        closeAll(asList(closeables));
    }

    /**
     * Closes all {@link AutoCloseable} objects in the parameter, suppressing exceptions. Exception
     * will be emitted after calling close() on every object.
     *
     * @param closeables iterable with closeables to close.
     * @throws Exception collected exceptions that occurred during closing
     */
    public static void closeAll(Iterable<? extends AutoCloseable> closeables) throws Exception {
        closeAll(closeables, Exception.class);
    }

    /**
     * Closes all {@link AutoCloseable} objects in the parameter, suppressing exceptions. Exception
     * will be emitted after calling close() on every object.
     *
     * @param closeables iterable with closeables to close.
     * @param suppressedException class of exceptions which should be suppressed during the closing.
     * @throws Exception collected exceptions that occurred during closing
     */
    public static <T extends Throwable> void closeAll(
            Iterable<? extends AutoCloseable> closeables, Class<T> suppressedException)
            throws Exception {
        if (null != closeables) {

            Exception collectedExceptions = null;

            for (AutoCloseable closeable : closeables) {
                try {
                    if (null != closeable) {
                        closeable.close();
                    }
                } catch (Throwable e) {
                    if (!suppressedException.isAssignableFrom(e.getClass())) {
                        throw e;
                    }

                    Exception ex = e instanceof Exception ? (Exception) e : new Exception(e);
                    collectedExceptions = ExceptionUtils.firstOrSuppressed(ex, collectedExceptions);
                }
            }

            if (null != collectedExceptions) {
                throw collectedExceptions;
            }
        }
    }

    /**
     * Closes all {@link AutoCloseable} objects in the parameter quietly.
     *
     * <p><b>Important:</b> This method is expected to never throw an exception.
     */
    public static void closeAllQuietly(Iterable<? extends AutoCloseable> closeables) {
        closeables.forEach(IOUtils::closeQuietly);
    }

    /**
     * Closes the given AutoCloseable.
     *
     * <p><b>Important:</b> This method is expected to never throw an exception.
     */
    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Throwable e) {
            LOG.debug("Exception occurs when closing " + closeable, e);
        }
    }
}
