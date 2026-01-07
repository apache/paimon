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

import java.io.IOException;
import java.io.InputStream;

import static org.apache.paimon.format.text.HadoopCompressionUtils.createDecompressedInputStream;

/** Base class for text-based file readers that provides common functionality. */
public abstract class AbstractTextFileReader implements FileRecordReader<InternalRow> {

    private final Path filePath;
    private final TextRecordIterator reader;

    protected final RowType rowType;
    protected final long offset;
    protected final TextLineReader lineReader;

    protected boolean readerClosed = false;

    protected AbstractTextFileReader(
            FileIO fileIO,
            Path filePath,
            RowType rowType,
            String delimiter,
            long offset,
            @Nullable Long length)
            throws IOException {
        this.filePath = filePath;
        this.rowType = rowType;
        this.offset = offset;
        InputStream decompressedStream =
                createDecompressedInputStream(fileIO.newInputStream(filePath), filePath);
        this.lineReader = TextLineReader.create(decompressedStream, delimiter, offset, length);
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
            if (lineReader != null) {
                lineReader.close();
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
            if (offset > 0) {
                throw new UnsupportedOperationException(
                        "Cannot return position with reading offset.");
            }
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
        return lineReader.readLine();
    }
}
