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
import org.apache.paimon.format.text.TextFileWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** CSV format writer implementation. */
public class CsvFormatWriter extends TextFileWriter {

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    // Performance optimization: Cache frequently used cast executors
    private static final Map<String, CastExecutor<?, ?>> CAST_EXECUTOR_CACHE =
            new ConcurrentHashMap<>(32);

    private final CsvOptions csvOptions;
    private boolean headerWritten = false;
    private final StringBuilder stringBuilder;

    public CsvFormatWriter(
            PositionOutputStream out, RowType rowType, CsvOptions options, String compression)
            throws IOException {
        super(out, rowType, compression);
        this.csvOptions = options;
        this.stringBuilder = new StringBuilder();
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        // Write header if needed
        if (csvOptions.includeHeader() && !headerWritten) {
            writeHeader();
            headerWritten = true;
        }

        // Reuse StringBuilder for better performance
        stringBuilder.setLength(0); // Reset without reallocating

        int fieldCount = rowType.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                stringBuilder.append(csvOptions.fieldDelimiter());
            }

            Object value =
                    InternalRow.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(element);
            String fieldValue = escapeField(castToStringOptimized(value, rowType.getTypeAt(i)));
            stringBuilder.append(fieldValue);
        }
        stringBuilder.append(csvOptions.lineDelimiter());

        writer.write(stringBuilder.toString());
    }

    private void writeHeader() throws IOException {
        stringBuilder.setLength(0); // Reuse StringBuilder

        int fieldCount = rowType.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                stringBuilder.append(csvOptions.fieldDelimiter());
            }
            stringBuilder.append(escapeField(rowType.getFieldNames().get(i)));
        }
        stringBuilder.append(csvOptions.lineDelimiter());
        writer.write(stringBuilder.toString());
    }

    private String escapeField(String field) {
        if (field == null) {
            return csvOptions.nullLiteral();
        }

        // Optimized escaping with early exit checks
        boolean needsQuoting =
                field.indexOf(csvOptions.fieldDelimiter().charAt(0)) >= 0
                        || field.indexOf(csvOptions.lineDelimiter().charAt(0)) >= 0
                        || field.indexOf(csvOptions.quoteCharacter().charAt(0)) >= 0;

        if (!needsQuoting) {
            return field;
        }

        // Only escape if needed
        String escaped =
                field.replace(
                        csvOptions.quoteCharacter(),
                        csvOptions.escapeCharacter() + csvOptions.quoteCharacter());
        return csvOptions.quoteCharacter() + escaped + csvOptions.quoteCharacter();
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
            case CHAR:
            case VARCHAR:
                return value.toString();
            case BINARY:
            case VARBINARY:
                return BASE64_ENCODER.encodeToString((byte[]) value);
            default:
                return useCachedStringCastExecutor(value, dataType);
        }
    }

    private String useCachedStringCastExecutor(Object value, DataType dataType) {
        String cacheKey = dataType.toString();
        @SuppressWarnings("unchecked")
        CastExecutor<Object, ?> cast =
                (CastExecutor<Object, ?>)
                        CAST_EXECUTOR_CACHE.computeIfAbsent(
                                cacheKey, k -> CastExecutors.resolveToString(dataType));
        Object result = cast.cast(value);
        return result != null ? result.toString() : null;
    }
}
