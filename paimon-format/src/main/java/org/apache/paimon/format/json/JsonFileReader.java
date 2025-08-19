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

package org.apache.paimon.format.json;

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

/** High-performance JSON file reader implementation with optimized buffering. */
public class JsonFileReader implements FileRecordReader<InternalRow> {

    private static final int DEFAULT_BUFFER_SIZE = 8192; // 8KB buffer for better I/O performance

    private final FileIO fileIO;
    private final Path filePath;
    private final JsonOptions options;
    private final RowType rowType;

    private InputStream inputStream;
    private boolean readerClosed = false;
    private JsonRecordIterator reader;

    public JsonFileReader(FileIO fileIO, Path filePath, RowType rowType, JsonOptions options)
            throws IOException {
        this.fileIO = fileIO;
        this.filePath = filePath;
        this.options = options;
        this.rowType = rowType;
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
        if (reader != null) {
            reader.close();
            readerClosed = true;
        }
    }

    @Override
    @Nullable
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        if (readerClosed) {
            return null;
        }

        if (inputStream == null) {
            inputStream = fileIO.newInputStream(filePath);
        }
        if (reader == null) {
            this.reader = new JsonRecordIterator();
        }

        if (!reader.hasNext) {
            return null;
        }
        return reader;
    }

    private class JsonRecordIterator implements FileRecordIterator<InternalRow> {

        private final BufferedReader bufferedReader;
        private boolean hasNext = true;
        private long currentPosition = 0;

        public JsonRecordIterator() throws IOException {
            // Use optimized buffer size for better I/O performance
            this.bufferedReader =
                    new BufferedReader(
                            new InputStreamReader(inputStream, StandardCharsets.UTF_8),
                            DEFAULT_BUFFER_SIZE);
        }

        @Override
        public InternalRow next() throws IOException {
            while (hasNext) {
                String line = bufferedReader.readLine();
                currentPosition++;

                if (line == null) {
                    hasNext = false;
                    return null;
                }

                // Skip empty lines for better performance
                if (line.trim().isEmpty()) {
                    continue;
                }

                try {
                    return JsonSerde.convertJsonStringToRow(line, rowType, options);
                } catch (Exception e) {
                    if (!options.ignoreParseErrors()) {
                        throw new IOException(
                                "Failed to parse JSON line " + currentPosition + ": " + line, e);
                    }
                    // Continue to next line if ignoring parse errors
                }
            }
            return null;
        }

        @Override
        public void releaseBatch() {
            // Nothing to release for JSON reader
        }

        public void close() throws IOException {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }

        @Override
        public Path filePath() {
            return filePath;
        }

        @Override
        public long returnedPosition() {
            return Math.max(0, currentPosition - 1); // Return position of last returned row
        }
    }
}
