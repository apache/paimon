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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/** Base class for text-based file readers that provides common functionality. */
public abstract class BaseTextFileReader implements FileRecordReader<InternalRow> {

    private final Path filePath;
    private final InputStream decompressedStream;
    private final TextRecordIterator reader;

    protected final RowType rowType;
    protected final BufferedReader bufferedReader;
    protected final byte[] recordDelimiterBytes;

    protected boolean readerClosed = false;

    protected BaseTextFileReader(FileIO fileIO, Path filePath, RowType rowType) throws IOException {
        this(fileIO, filePath, rowType, null);
    }

    protected BaseTextFileReader(
            FileIO fileIO, Path filePath, RowType rowType, String recordDelimiter)
            throws IOException {
        this.filePath = filePath;
        this.rowType = rowType;
        this.recordDelimiterBytes =
                recordDelimiter != null ? recordDelimiter.getBytes(StandardCharsets.UTF_8) : null;
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
     * Reads a line from the buffered reader using custom record delimiter if configured. Following
     * Hadoop's textinputformat.record.delimiter approach. If no custom delimiter is set, uses the
     * default BufferedReader.readLine().
     */
    protected String readLine() throws IOException {
        if (recordDelimiterBytes == null) {
            // Use default readLine for standard delimiters
            return bufferedReader.readLine();
        }

        // Custom delimiter handling following Hadoop's LineReader approach
        StringBuilder line = new StringBuilder();
        int matchIndex = 0;
        int c;

        while ((c = bufferedReader.read()) != -1) {
            byte currentByte = (byte) c;

            if (currentByte == recordDelimiterBytes[matchIndex]) {
                matchIndex++;
                if (matchIndex == recordDelimiterBytes.length) {
                    // Found complete delimiter, return line without delimiter
                    return line.toString();
                }
            } else {
                // No match, append any previously matched delimiter bytes
                if (matchIndex > 0) {
                    line.append(
                            new String(
                                    recordDelimiterBytes, 0, matchIndex, StandardCharsets.UTF_8));
                    matchIndex = 0;
                }
                line.append((char) currentByte);
            }
        }

        // End of stream - append any remaining matched bytes
        if (matchIndex > 0) {
            line.append(new String(recordDelimiterBytes, 0, matchIndex, StandardCharsets.UTF_8));
        }

        // Return null if nothing was read
        if (line.length() == 0) {
            return null;
        }

        return line.toString();
    }
}
