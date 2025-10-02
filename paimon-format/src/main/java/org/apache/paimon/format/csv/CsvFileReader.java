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
import org.apache.paimon.format.text.BaseTextFileReader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** CSV file reader implementation. */
public class CsvFileReader extends BaseTextFileReader {

    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
    private static final CsvMapper CSV_MAPPER = new CsvMapper();
    private static final InternalRow DROP_ROW = new GenericRow(1);

    // Performance optimization: Cache frequently used cast executors
    private static final Map<String, CastExecutor<?, ?>> CAST_EXECUTOR_CACHE =
            new ConcurrentHashMap<>(32);

    private final CsvOptions formatOptions;
    private final CsvSchema schema;
    private final RowType dataSchemaRowType;
    private final RowType projectedRowType;
    private final int[] projectionMapping;
    private boolean headerSkipped = false;

    public CsvFileReader(
            FileIO fileIO,
            Path filePath,
            RowType rowReadType,
            RowType projectedRowType,
            CsvOptions options)
            throws IOException {
        super(fileIO, filePath, projectedRowType);
        this.dataSchemaRowType = rowReadType;
        this.projectedRowType = projectedRowType;
        this.formatOptions = options;
        this.projectionMapping = createProjectionMapping(rowReadType, projectedRowType);
        this.schema =
                CsvSchema.emptySchema()
                        .withQuoteChar(formatOptions.quoteCharacter().charAt(0))
                        .withColumnSeparator(formatOptions.fieldDelimiter().charAt(0))
                        .withEscapeChar(formatOptions.escapeCharacter().charAt(0));
        if (!formatOptions.includeHeader()) {
            this.schema.withoutHeader();
        }
    }

    @Override
    protected BaseTextRecordIterator createRecordIterator() {
        return new CsvRecordIterator();
    }

    @Override
    protected InternalRow parseLine(String line) throws IOException {
        return parseCsvLine(line, schema);
    }

    @Override
    protected void setupReading() throws IOException {
        // Skip header if needed
        if (formatOptions.includeHeader() && !headerSkipped) {
            bufferedReader.readLine();
            headerSkipped = true;
        }
    }

    private class CsvRecordIterator extends BaseTextRecordIterator {
        @Override
        public InternalRow next() throws IOException {
            while (true) {
                if (readerClosed) {
                    return null;
                }
                String nextLine = bufferedReader.readLine();
                if (nextLine == null) {
                    end = true;
                    return null;
                }

                currentPosition++;
                InternalRow row = parseLine(nextLine);
                if (row != DROP_ROW) {
                    return row;
                }
            }
        }
    }

    protected static String[] parseCsvLineToArray(String line, CsvSchema schema)
            throws IOException {
        if (line == null || line.isEmpty()) {
            return new String[] {};
        }
        return CSV_MAPPER.readerFor(String[].class).with(schema).readValue(line);
    }

    /**
     * Creates a mapping array from read schema to projected schema. Returns indices of projected
     * columns in the read schema.
     */
    private static int[] createProjectionMapping(RowType rowReadType, RowType projectedRowType) {
        int[] mapping = new int[projectedRowType.getFieldCount()];
        for (int i = 0; i < projectedRowType.getFieldCount(); i++) {
            String projectedFieldName = projectedRowType.getFieldNames().get(i);
            int readIndex = rowReadType.getFieldNames().indexOf(projectedFieldName);
            if (readIndex == -1) {
                throw new IllegalArgumentException(
                        String.format(
                                "Projected field '%s' not found in read schema",
                                projectedFieldName));
            }
            mapping[i] = readIndex;
        }
        return mapping;
    }

    private InternalRow parseCsvLine(String line, CsvSchema schema) throws IOException {
        String[] fields = parseCsvLineToArray(line, schema);
        int fieldCount = fields.length;

        // Directly parse only projected fields to avoid unnecessary parsing
        Object[] projectedValues = new Object[projectedRowType.getFieldCount()];
        for (int i = 0; i < projectedRowType.getFieldCount(); i++) {
            int readIndex = projectionMapping[i];
            // Check if the field exists in the CSV line
            if (readIndex < fieldCount) {
                String field = fields[readIndex];
                // Fast path for null values - check if field is null or empty first
                if (field == null || field.isEmpty() || field.equals(formatOptions.nullLiteral())) {
                    projectedValues[i] = null;
                    continue;
                }

                // Optimized field parsing with cached cast executors
                try {
                    projectedValues[i] =
                            parseFieldOptimized(
                                    field.trim(), dataSchemaRowType.getTypeAt(readIndex));
                } catch (Exception e) {
                    switch (formatOptions.mode()) {
                        case PERMISSIVE:
                            projectedValues[i] = null;
                            break;
                        case DROPMALFORMED:
                            return DROP_ROW;
                        case FAILFAST:
                            throw e;
                    }
                }
            } else {
                projectedValues[i] = null; // Field not present in the CSV line
            }
        }

        return GenericRow.of(projectedValues);
    }

    /** Optimized field parsing with caching and fast paths for common types. */
    private Object parseFieldOptimized(String field, DataType dataType) {
        if (field == null || field.equals(formatOptions.nullLiteral())) {
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
            case BINARY:
            case VARBINARY:
                return BASE64_DECODER.decode(field);
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
