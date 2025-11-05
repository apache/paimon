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

package org.apache.paimon.format.text;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/** Base class for text-based file readers that provides common functionality. */
public abstract class BaseTextFileReader implements FileRecordReader<InternalRow> {

    private static final int MAX_LINE_LENGTH = Integer.MAX_VALUE;

    private final Path filePath;
    private final InputStream decompressedStream;
    private final TextRecordIterator reader;

    protected final RowType rowType;
    protected final BufferedReader bufferedReader;
    protected final byte[] recordDelimiterBytes;

    protected boolean readerClosed = false;

    protected BaseTextFileReader(
            FileIO fileIO, Path filePath, RowType rowType, String recordDelimiter)
            throws IOException {
        this.filePath = filePath;
        this.rowType = rowType;
        this.recordDelimiterBytes =
                recordDelimiter != null && !"\n".equals(recordDelimiter)
                        ? recordDelimiter.getBytes(StandardCharsets.UTF_8)
                        : null;
        this.decompressedStream =
                HadoopCompressionUtils.createDecompressedInputStream(
                        fileIO.newInputStream(filePath), filePath);
        this.bufferedReader =
                new BufferedReader(
                        new InputStreamReader(this.decompressedStream, StandardCharsets.UTF_8));
        this.reader = new TextRecordIterator();
    }

    /**
     * Parses a single line of text into an InternalRow. Subclasses must implement this method to
     * handle their specific format.
     */
    @Nullable
    protected abstract InternalRow parseLine(String line) throws IOException;

    /**
     * Performs any additional setup before reading records. Subclasses can override this method if
     * they need to perform setup operations like skipping headers.
     */
    protected void setupReading() throws IOException {
        // Default implementation does nothing
    }

    @Override
    @Nullable
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        if (readerClosed) {
            return null;
        }

        // Perform any setup needed before reading
        setupReading();

        if (reader.end) {
            return null;
        }
        return reader;
    }

    @Override
    public void close() throws IOException {
        if (!readerClosed) {
            // Close the buffered reader first
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            // Explicitly close the decompressed stream to prevent resource leaks
            if (decompressedStream != null) {
                decompressedStream.close();
            }
            readerClosed = true;
        }
    }

    /** Record iterator for text-based file readers. */
    private class TextRecordIterator implements FileRecordIterator<InternalRow> {

        protected long currentPosition = 0;
        protected boolean end = false;

        @Override
        public InternalRow next() throws IOException {
            while (true) {
                if (readerClosed) {
                    return null;
                }
                String nextLine = readLine();
                if (nextLine == null) {
                    end = true;
                    return null;
                }

                currentPosition++;
                InternalRow row = parseLine(nextLine);
                if (row != null) {
                    return row;
                }
            }
        }

        @Override
        public void releaseBatch() {
            // Default implementation does nothing
        }

        @Override
        public Path filePath() {
            return filePath;
        }

        @Override
        public long returnedPosition() {
            return Math.max(0, currentPosition - 1);
        }
    }

    /**
     * Reads a single line from the input stream, using either the default line delimiter or a
     * custom delimiter.
     *
     * <p>This method supports multi-character custom delimiters by using a simple pattern matching
     * algorithm. For standard delimiters (null or empty), it delegates to BufferedReader's
     * readLine() for optimal performance.
     *
     * <p>The algorithm maintains a partial match index and accumulates bytes until:
     *
     * <ul>
     *   <li>A complete delimiter is found (returns line without delimiter)
     *   <li>End of stream is reached (returns accumulated data or null if empty)
     *   <li>Maximum line length is exceeded (throws IOException)
     * </ul>
     *
     * @return the next line as a string (without delimiter), or null if end of stream
     * @throws IOException if an I/O error occurs or line exceeds maximum length
     */
    protected String readLine() throws IOException {
        // Fast path: use BufferedReader for standard delimiters
        if (recordDelimiterBytes == null || recordDelimiterBytes.length == 0) {
            return bufferedReader.readLine();
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        int matchIndex = 0;

        while (true) {
            int b = decompressedStream.read();
            if (b == -1) {
                // End of stream: flush any partially matched delimiter bytes to output
                if (matchIndex > 0) {
                    out.write(recordDelimiterBytes, 0, matchIndex);
                }
                // Return null if nothing was read, otherwise return the accumulated line
                return out.size() == 0 ? null : out.toString(StandardCharsets.UTF_8.name());
            }

            // Guard against extremely long lines that could cause memory issues
            if (MAX_LINE_LENGTH - matchIndex < out.size()) {
                throw new IOException("Line exceeds maximum length: " + MAX_LINE_LENGTH);
            }

            byte current = (byte) b;
            if (current == recordDelimiterBytes[matchIndex]) {
                // Current byte matches the next expected delimiter byte
                matchIndex++;
                if (matchIndex == recordDelimiterBytes.length) {
                    // Complete delimiter found, return the line without the delimiter
                    return out.toString(StandardCharsets.UTF_8.name());
                }
            } else if (matchIndex > 0) {
                // Mismatch: handle partial matches
                out.write(recordDelimiterBytes, 0, matchIndex);
                if (current == recordDelimiterBytes[0]) {
                    matchIndex = 1;
                } else {
                    out.write(current);
                    matchIndex = 0;
                }
            } else {
                // just add the current byte to output
                out.write(current);
            }
        }
    }
}
