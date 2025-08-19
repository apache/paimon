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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.CloseShieldOutputStream;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** CSV format writer implementation. */
public class CsvFormatWriter implements FormatWriter {

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    // Performance optimization: Cache frequently used cast executors
    private static final Map<String, CastExecutor<?, ?>> CAST_EXECUTOR_CACHE =
            new ConcurrentHashMap<>(32);

    private final RowType rowType;
    private final String fieldDelimiter;
    private final String lineDelimiter;
    private final String quoteCharacter;
    private final String escapeCharacter;
    private final String nullLiteral;
    private final boolean includeHeader;

    private final BufferedWriter writer;
    private final PositionOutputStream outputStream;
    private boolean headerWritten = false;

    private final StringBuilder stringBuilder;

    public CsvFormatWriter(PositionOutputStream out, RowType rowType, Options options)
            throws IOException {
        this.rowType = rowType;
        this.fieldDelimiter = options.get(CsvFileFormat.FIELD_DELIMITER);
        this.lineDelimiter = options.get(CsvFileFormat.CSV_LINE_DELIMITER);
        this.quoteCharacter = options.get(CsvFileFormat.CSV_QUOTE_CHARACTER);
        this.escapeCharacter = options.get(CsvFileFormat.CSV_ESCAPE_CHARACTER);
        this.nullLiteral = options.get(CsvFileFormat.CSV_NULL_LITERAL);
        this.includeHeader = options.get(CsvFileFormat.CSV_INCLUDE_HEADER);
        this.outputStream = out;
        CloseShieldOutputStream shieldOutputStream = new CloseShieldOutputStream(out);
        OutputStreamWriter outputStreamWriter =
                new OutputStreamWriter(shieldOutputStream, StandardCharsets.UTF_8);
        this.writer = new BufferedWriter(outputStreamWriter);
        this.stringBuilder = new StringBuilder();
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        // Write header if needed
        if (includeHeader && !headerWritten) {
            writeHeader();
            headerWritten = true;
        }

        // Reuse StringBuilder for better performance
        stringBuilder.setLength(0); // Reset without reallocating

        int fieldCount = rowType.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                stringBuilder.append(fieldDelimiter);
            }

            Object value =
                    InternalRow.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(element);
            String fieldValue = escapeField(castToStringOptimized(value, rowType.getTypeAt(i)));
            stringBuilder.append(fieldValue);
        }
        stringBuilder.append(lineDelimiter);

        writer.write(stringBuilder.toString());
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        if (outputStream != null && suggestedCheck) {
            return outputStream.getPos() >= targetSize;
        }
        return false;
    }

    private void writeHeader() throws IOException {
        stringBuilder.setLength(0); // Reuse StringBuilder

        int fieldCount = rowType.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                stringBuilder.append(fieldDelimiter);
            }
            stringBuilder.append(escapeField(rowType.getFieldNames().get(i)));
        }
        stringBuilder.append(lineDelimiter);
        writer.write(stringBuilder.toString());
    }

    private String escapeField(String field) {
        if (field == null) {
            return nullLiteral;
        }

        // Optimized escaping with early exit checks
        boolean needsQuoting =
                field.indexOf(fieldDelimiter.charAt(0)) >= 0
                        || field.indexOf(lineDelimiter.charAt(0)) >= 0
                        || field.indexOf(quoteCharacter.charAt(0)) >= 0;

        if (!needsQuoting) {
            return field;
        }

        // Only escape if needed
        String escaped = field.replace(quoteCharacter, escapeCharacter + quoteCharacter);
        return quoteCharacter + escaped + quoteCharacter;
    }

    /** Optimized string casting with caching and fast paths for common types. */
    private String castToStringOptimized(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
                return value.toString();
            case CHAR:
            case VARCHAR:
                return value.toString();
            default:
                return useCachedStringCastExecutor(value, dataType);
        }
    }

    private String useCachedStringCastExecutor(Object value, DataType dataType) {
        String cacheKey = dataType.toString() + "_toString";
        @SuppressWarnings("unchecked")
        CastExecutor<Object, ?> cast =
                (CastExecutor<Object, ?>)
                        CAST_EXECUTOR_CACHE.computeIfAbsent(
                                cacheKey, k -> CastExecutors.resolveToString(dataType));

        if (cast != null) {
            Object result = cast.cast(value);
            return result != null ? result.toString() : null;
        }
        return value.toString();
    }
}
