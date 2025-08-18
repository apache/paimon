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

/** CSV format writer implementation. */
public class CsvFormatWriter implements FormatWriter {

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
        this.writer =
                new BufferedWriter(
                        new OutputStreamWriter(
                                new CloseShieldOutputStream(out), StandardCharsets.UTF_8));
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        // Write header if needed
        if (includeHeader && !headerWritten) {
            writeHeader();
            headerWritten = true;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) {
                sb.append(fieldDelimiter);
            }

            Object value =
                    InternalRow.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(element);
            String fieldValue = escapeField(castToString(value, rowType.getTypeAt(i)));
            sb.append(fieldValue);
        }
        sb.append(lineDelimiter);

        writer.write(sb.toString());
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
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) {
                sb.append(fieldDelimiter);
            }
            sb.append(escapeField(rowType.getFieldNames().get(i)));
        }
        sb.append(lineDelimiter);
        writer.write(sb.toString());
    }

    private String escapeField(String field) {
        if (field == null) {
            return nullLiteral;
        }

        // Simple escaping - wrap in quotes if contains delimiter
        if (field.contains(fieldDelimiter)
                || field.contains(lineDelimiter)
                || field.contains(quoteCharacter)) {
            String escaped = field.replace(quoteCharacter, escapeCharacter + quoteCharacter);
            return quoteCharacter + escaped + quoteCharacter;
        }

        return field;
    }

    public static String castToString(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case BINARY:
            case VARBINARY:
                return Base64.getEncoder().encodeToString((byte[]) value);
            default:
                CastExecutor cast = CastExecutors.resolveToString(dataType);
                return cast.cast(value).toString();
        }
    }
}
