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
import org.apache.paimon.format.csv.CsvOptions.Mode;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StringUtils.isNullOrWhitespaceOnly;

/** Csv parser for CSV format. */
public class CsvParser {

    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
    private static final Map<String, CastExecutor<?, ?>> CAST_EXECUTOR_CACHE =
            new ConcurrentHashMap<>();

    private final RowType dataSchemaRowType;
    private final int[] projectMapping;
    private final GenericRow emptyRow;
    private final char separatorChar;
    private final char quoteChar;
    private final char escapeChar;
    private final String nullLiteral;
    private final Mode mode;
    private final StringBuilder buffer;
    private final String[] rowValues;

    public CsvParser(RowType dataSchemaRowType, int[] projectMapping, CsvOptions options) {
        this.dataSchemaRowType = dataSchemaRowType;
        this.projectMapping = projectMapping;
        this.emptyRow = new GenericRow(projectMapping.length);
        this.nullLiteral = options.nullLiteral();
        this.mode = options.mode();
        this.buffer = new StringBuilder(1024);
        int columnCount = Arrays.stream(projectMapping).max().orElse(-1) + 1;
        this.rowValues = new String[columnCount];

        this.separatorChar = options.fieldDelimiter().charAt(0);
        this.quoteChar = options.quoteCharacter().charAt(0);
        this.escapeChar = options.escapeCharacter().charAt(0);

        checkArgument(separatorChar != '\0', "Separator cannot be the null character (ASCII 0)");
        checkArgument(
                separatorChar != quoteChar, "Separator and quote character cannot be the same");
        checkArgument(
                separatorChar != escapeChar, "Separator and escape character cannot be the same");

        // Quote and escape character can be the same when both are the null character (quoting and
        // escaping are disabled)
        if (quoteChar != '\0' || escapeChar != '\0') {
            checkArgument(quoteChar != escapeChar, "Quote and escape character cannot be the same");
        }
    }

    @Nullable
    public GenericRow parse(String line) {
        Arrays.fill(rowValues, null);
        buffer.setLength(0);

        // empty line results in all null values
        if (isNullOrWhitespaceOnly(line) || projectMapping.length == 0) {
            return emptyRow;
        }

        int columnIndex = 0;
        boolean inQuotes = false;
        boolean inField = false;

        int position = 0;
        while (position < line.length() && columnIndex < rowValues.length) {
            char c = line.charAt(position);
            if (c == escapeChar) {
                // if the next character is special, process it here as to not trigger the special
                // handling
                if (inQuotes || inField) {
                    int nextCharacter = peekNextCharacter(line, position);
                    if (nextCharacter == quoteChar || nextCharacter == escapeChar) {
                        buffer.append(line.charAt(position + 1));
                        position++;
                    }
                }
            } else if (c == quoteChar) {
                // a quote character can be escaped with another quote character
                if ((inQuotes || inField) && peekNextCharacter(line, position) == quoteChar) {
                    buffer.append(line.charAt(position + 1));
                    position++;
                } else {
                    // the tricky case of an embedded quote in the middle: a,bc"d"ef,g
                    // Embedded quote is not for first 3 characters of the line, and is not allowed
                    // immediately before a separator
                    if (position > 2
                            && line.charAt(position - 1) != separatorChar
                            && line.length() > (position + 1)
                            && line.charAt(position + 1) != separatorChar) {
                        // if field starts begins whitespace, skip the whitespace and quote
                        if (buffer.length() != 0 && isAllWhitespace(buffer)) {
                            buffer.setLength(0);
                        } else {
                            // otherwise write the quote as a literal value
                            buffer.append(c);
                        }
                    }
                    inQuotes = !inQuotes;
                }
                inField = !inField;
            } else if (c == separatorChar && !inQuotes) {
                // end of a value
                rowValues[columnIndex] = buffer.toString();
                columnIndex++;
                buffer.setLength(0);
                inField = false;
            } else {
                buffer.append(c);
                inField = true;
            }
            position++;
        }

        // if last field is an unterminated field, ignore the value
        if (columnIndex < rowValues.length && !inQuotes) {
            rowValues[columnIndex] = buffer.toString();
        }
        buffer.setLength(0);

        GenericRow row = new GenericRow(projectMapping.length);
        for (int i = 0; i < projectMapping.length; i++) {
            int ordinal = projectMapping[i];
            DataType type = dataSchemaRowType.getTypeAt(ordinal);
            Object field = null;
            try {
                field = parseField(rowValues[ordinal], type);
            } catch (Exception e) {
                switch (mode) {
                    case PERMISSIVE:
                        break;
                    case DROPMALFORMED:
                        return null;
                    case FAILFAST:
                        throw e;
                }
            }
            row.setField(i, field);
        }
        return row;
    }

    private static int peekNextCharacter(String line, int position) {
        return line.length() > position + 1 ? line.charAt(position + 1) : -1;
    }

    private static boolean isAllWhitespace(CharSequence sequence) {
        for (int i = 0; i < sequence.length(); i++) {
            if (!Character.isWhitespace(sequence.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private Object parseField(String field, DataType dataType) {
        if (field == null || field.equals(nullLiteral)) {
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
                return parseByCastExecutor(field, dataType);
        }
    }

    private Object parseByCastExecutor(String field, DataType dataType) {
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
