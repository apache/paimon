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
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/** High-performance JSON file reader implementation with optimized buffering. */
public class JsonFileReader implements FileRecordReader<InternalRow> {

    private final Path filePath;
    private final JsonOptions options;
    private final RowType rowType;
    private final BufferedReader bufferedReader;
    private boolean readerClosed = false;
    private JsonRecordIterator reader;

    public JsonFileReader(FileIO fileIO, Path filePath, RowType rowType, JsonOptions options)
            throws IOException {
        this.filePath = filePath;
        this.options = options;
        this.rowType = rowType;
        reader = new JsonRecordIterator();
        this.bufferedReader =
                new BufferedReader(
                        new InputStreamReader(
                                fileIO.newInputStream(filePath), StandardCharsets.UTF_8));
    }

    @Override
    @Nullable
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        if (readerClosed) {
            return null;
        }
        if (reader.end) {
            return null;
        }
        return reader;
    }

    @Override
    public void close() throws IOException {
        if (!readerClosed && bufferedReader != null) {
            bufferedReader.close();
            readerClosed = true;
        }
    }

    private class JsonRecordIterator implements FileRecordIterator<InternalRow> {

        private long currentPosition = 0;
        boolean end = false;

        @Override
        public InternalRow next() throws IOException {
            if (readerClosed) {
                return null;
            }
            String nextLine = bufferedReader.readLine();
            if (nextLine == null) {
                end = true;
                return null;
            }

            currentPosition++;
            return JsonSerde.convertJsonStringToRow(nextLine, rowType, options);
        }

        @Override
        public void releaseBatch() {
            // Nothing to release for JSON reader
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
}
