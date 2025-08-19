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

package org.apache.paimon.format.csv;

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** CSV file reader implementation. */
public class CsvFileReader implements FileRecordReader<InternalRow> {

    private static final CsvMapper CSV_MAPPER = new CsvMapper();
    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

    // Performance optimization: Cache frequently used cast executors
    private static final Map<String, CastExecutor<?, ?>> CAST_EXECUTOR_CACHE =
            new ConcurrentHashMap<>(32);

    private final RowType rowType;
    private final CsvOptions options;
    private final Path filePath;
    private final CsvSchema schema;

    private BufferedReader bufferedReader;
    private boolean headerSkipped = false;
    private boolean readerClosed = false;
    private CsvRecordIterator reader;

    public CsvFileReader(FormatReaderFactory.Context context, RowType rowType, CsvOptions options)
            throws IOException {
        this.rowType = rowType;
        this.filePath = context.filePath();
        this.options = options;
        this.schema =
                CsvSchema.emptySchema()
                        .withQuoteChar(options.quoteCharacter().charAt(0))
                        .withColumnSeparator(options.fieldDelimiter().charAt(0))
                        .withEscapeChar(options.escapeCharacter().charAt(0));
        if (!options.includeHeader()) {
            this.schema.withoutHeader();
        }
        FileIO fileIO = context.fileIO();
        SeekableInputStream inputStream = fileIO.newInputStream(context.filePath());
        reader = new CsvRecordIterator();
        InputStreamReader inputStreamReader =
                new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        this.bufferedReader = new BufferedReader(inputStreamReader);
    }

    @Override
    @Nullable
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        if (readerClosed) {
            return null;
        }

        // Skip header if needed
        if (options.includeHeader() && !headerSkipped) {
            bufferedReader.readLine();
            headerSkipped = true;
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

    private class CsvRecordIterator implements FileRecordIterator<InternalRow> {
        private boolean batchRead = false;
        private long currentPosition = 0;
        private String nextLine = null;
        boolean end = false;

        @Override
        @Nullable
        public InternalRow next() throws IOException {
            if (batchRead || readerClosed) {
                return null;
            }
            nextLine = bufferedReader.readLine();
            if (nextLine == null) {
                batchRead = true;
                end = true;
                return null;
            }

            currentPosition++;
            return parseCsvLine(nextLine, schema);
        }

        @Override
        public void releaseBatch() {
            // No resources to release for CSV
        }

        @Override
        public long returnedPosition() {
            return currentPosition - 1; // Return position of last returned row
        }

        @Override
        public Path filePath() {
            return filePath;
        }
    }

    protected static String[] parseCsvLineToArray(String line, CsvSchema schema)
            throws IOException {
        if (line == null || line.isEmpty()) {
            return new String[] {};
        }
        return CSV_MAPPER.readerFor(String[].class).with(schema).readValue(line);
    }

    private InternalRow parseCsvLine(String line, CsvSchema schema) throws IOException {
        String[] fields = parseCsvLineToArray(line, schema);
        int fieldCount = Math.min(fields.length, rowType.getFieldCount());
        Object[] values = new Object[fieldCount]; // Pre-allocated array

        for (int i = 0; i < fieldCount; i++) {
            String field = fields[i];

            // Fast path for null values
            if (field == null || field.equals(options.nullLiteral()) || field.isEmpty()) {
                values[i] = null;
                continue;
            }

            // Optimized field parsing with cached cast executors
            values[i] = parseFieldOptimized(field.trim(), rowType.getTypeAt(i));
        }

        return GenericRow.of(values);
    }

    /** Optimized field parsing with caching and fast paths for common types. */
    private Object parseFieldOptimized(String field, DataType dataType) {
        if (field == null || field.equals(options.nullLiteral())) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case TINYINT:
                return Byte.parseByte(field);
            case SMALLINT:
                return Short.parseShort(field);
            case INTEGER:
                return Integer.parseInt(field);
            case BIGINT:
                return Long.parseLong(field);
            case FLOAT:
                return Float.parseFloat(field);
            case DOUBLE:
                return Double.parseDouble(field);
            case BOOLEAN:
                return Boolean.parseBoolean(field);
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString(field);
            default:
                return useCachedCastExecutor(field, dataType);
        }
    }

    private Object useCachedCastExecutor(String field, DataType dataType) {
        String cacheKey = dataType.toString();
        @SuppressWarnings("unchecked")
        CastExecutor<BinaryString, Object> cast =
                (CastExecutor<BinaryString, Object>)
                        CAST_EXECUTOR_CACHE.computeIfAbsent(
                                cacheKey, k -> CastExecutors.resolve(DataTypes.STRING(), dataType));

        if (cast != null) {
            return cast.cast(BinaryString.fromString(field));
        }
        return BinaryString.fromString(field);
    }
}
