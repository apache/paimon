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
import org.apache.paimon.format.text.AbstractTextFileWriter;
import org.apache.paimon.format.text.TextLineReader;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** CSV format writer implementation. */
public class CsvFormatWriter extends AbstractTextFileWriter {

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    // Performance optimization: Cache frequently used cast executors
    private static final Map<String, CastExecutor<?, ?>> CAST_EXECUTOR_CACHE =
            new ConcurrentHashMap<>(32);

    private final CsvOptions csvOptions;
    private boolean headerWritten = false;
    private final StringBuilder stringBuilder;
    private final String[] fieldNames;
    // CR and LF only split a line when StandardLineReader is in use, which TextLineReader picks
    // solely from the delimiter; under a custom delimiter they are ordinary bytes.
    private final boolean lineBreakSplitsLine;

    public CsvFormatWriter(
            PositionOutputStream out, RowType rowType, CsvOptions options, String compression)
            throws IOException {
        super(out, rowType, compression);
        this.csvOptions = options;
        this.stringBuilder = new StringBuilder();
        this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
        this.lineBreakSplitsLine = TextLineReader.isDefaultDelimiter(options.lineDelimiter());
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
            String fieldValue =
                    escapeField(castToStringOptimized(value, rowType.getTypeAt(i)), fieldNames[i]);
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
            stringBuilder.append(escapeField(fieldNames[i], fieldNames[i]));
        }
        stringBuilder.append(csvOptions.lineDelimiter());
        writer.write(stringBuilder.toString());
    }

    private String escapeField(String field, String fieldName) {
        if (field == null) {
            return csvOptions.nullLiteral();
        }

        String quote = csvOptions.quoteCharacter();
        String escape = csvOptions.escapeCharacter();
        String lineDelimiter = csvOptions.lineDelimiter();

        // A value carrying the row separator cannot be read back. The line readers match it
        // without tracking quotes, and a split boundary may fall inside the value, so quoting
        // cannot rescue it without giving up splittability. Refuse the value rather than write a
        // file that reads back as extra rows. CR and LF only count when they are the separator:
        // under a custom delimiter CustomLineReader treats them as ordinary bytes, which is the
        // documented way to carry a line break inside a value.
        if (field.contains(lineDelimiter)
                || (lineBreakSplitsLine
                        && (field.indexOf('\r') >= 0 || field.indexOf('\n') >= 0))) {
            throw new IllegalArgumentException(
                    String.format(
                            "Column '%s' contains the row separator, which the CSV format cannot "
                                    + "represent: '%s'",
                            fieldName, truncate(field)));
        }

        // Optimized escaping with early exit checks. A value that merely starts a delimiter match
        // still has to be quoted: CustomLineReader is leftmost-match, so the delimiter appended
        // after the row would complete a match begun by the value's own trailing bytes.
        boolean needsQuoting =
                field.equals(csvOptions.nullLiteral())
                        || field.contains(csvOptions.fieldDelimiter())
                        || field.indexOf(lineDelimiter.charAt(0)) >= 0
                        || field.contains(quote)
                        || field.contains(escape);

        if (!needsQuoting) {
            return field;
        }

        // Only escape if needed. The escape character goes first: CsvParser drops an escape
        // character that is not followed by a quote or another escape, and escaping the quotes
        // first would double the escape characters inserted for them.
        String escaped = field.replace(escape, escape + escape);
        return quote + escaped.replace(quote, escape + quote) + quote;
    }

    /** Keeps an unbounded STRING value from turning into an unbounded exception message. */
    private static String truncate(String field) {
        return field.length() <= 64 ? field : field.substring(0, 64) + "...";
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
