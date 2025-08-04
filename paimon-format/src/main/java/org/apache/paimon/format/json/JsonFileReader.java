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
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/** A {@link FileRecordReader} implementation for JSON format. */
public class JsonFileReader implements FileRecordReader<InternalRow> {

    private final FileIO fileIO;
    private final Path filePath;
    private final long fileSize;
    private final RowType rowType;
    private final Options options;

    private final ObjectMapper objectMapper;
    private final JsonToRowConverter converter;

    private InputStream inputStream;
    private boolean readerClosed = false;
    private long currentRowPosition = 0;
    private JsonRecordIterator reader;

    public JsonFileReader(
            FileIO fileIO, Path filePath, long fileSize, RowType rowType, Options options)
            throws IOException {
        this.fileIO = fileIO;
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.rowType = rowType;
        this.options = options;

        this.objectMapper = new ObjectMapper();
        this.converter = new JsonToRowConverter(rowType, options);
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
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
        private final ObjectMapper objectMapper;
        private final JsonToRowConverter converter;
        private final boolean ignoreParseErrors;
        private boolean hasNext = true;

        public JsonRecordIterator() throws IOException {
            this.bufferedReader =
                    new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            this.objectMapper = new ObjectMapper();
            this.converter = new JsonToRowConverter(rowType, options);
            this.ignoreParseErrors = options.get(JsonFileFormat.JSON_IGNORE_PARSE_ERRORS);
        }

        @Override
        public InternalRow next() throws IOException {
            while (hasNext) {
                String line = bufferedReader.readLine();
                if (line == null) {
                    hasNext = false;
                    return null;
                }

                try {
                    JsonNode jsonNode = objectMapper.readTree(line);
                    return converter.convert(jsonNode);
                } catch (Exception e) {
                    if (ignoreParseErrors) {
                        // Skip this line and continue with next line
                        continue;
                    } else {
                        throw new IOException("Failed to parse JSON line: " + line, e);
                    }
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
            return currentRowPosition;
        }
    }
}
