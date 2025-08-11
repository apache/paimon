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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.CloseShieldOutputStream;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.json.RowToJsonConverter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.TimeZone;

import static org.apache.paimon.utils.DateTimeUtils.timestampWithLocalZoneToTimestamp;

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

    private final RowToJsonConverter converter;
    private final ObjectMapper objectMapper;
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter FORMATTER_0 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter FORMATTER_1 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
    private static final DateTimeFormatter FORMATTER_2 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS");
    private static final DateTimeFormatter FORMATTER_3 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter FORMATTER_6 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter FORMATTER_9 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

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
        this.converter = new RowToJsonConverter(rowType, options);
        this.objectMapper = new ObjectMapper();
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
            String fieldValue = formatField(value, rowType.getTypeAt(i));
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

    private String formatField(Object value, DataType dataType) throws JsonProcessingException {
        if (value == null) {
            return nullLiteral;
        }
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return escapeField(value.toString());
            case DECIMAL:
                return escapeField(((Decimal) value).toBigDecimal().toString());
            case CHAR:
            case VARCHAR:
                return escapeField(((BinaryString) value).toString());
            case BINARY:
            case VARBINARY:
                return Base64.getEncoder().encodeToString((byte[]) value);
            case DATE:
                LocalDate date = LocalDate.ofEpochDay((int) value);
                return date.format(DATE_FORMATTER);
            case TIME_WITHOUT_TIME_ZONE:
                LocalTime time = LocalTime.ofNanoOfDay((int) value * 1_000_000L);
                return time.format(TIME_FORMATTER);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) dataType;
                Timestamp timestamp = (Timestamp) value;
                return formatTimeStampWithPrecision(
                        timestamp.toLocalDateTime(), timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) dataType;
                timestamp = (Timestamp) value;
                timestamp = timestampWithLocalZoneToTimestamp(timestamp, TimeZone.getDefault());
                return formatTimeStampWithPrecision(
                        timestamp.toLocalDateTime(), localZonedTimestampType.getPrecision());
            case ARRAY:
            case MAP:
            case ROW:
                return objectMapper.writeValueAsString(converter.convertValue(value, dataType));
            default:
                return value.toString();
        }
    }

    private String formatTimeStampWithPrecision(LocalDateTime dateTime, int precision) {
        switch (precision) {
            case 0:
                return dateTime.format(FORMATTER_0);
            case 1:
                return dateTime.format(FORMATTER_1);
            case 2:
                return dateTime.format(FORMATTER_2);
            case 3:
                return dateTime.format(FORMATTER_3);
            case 6:
                return dateTime.format(FORMATTER_6);
            case 9:
                return dateTime.format(FORMATTER_9);
            default:
                return dateTime.format(FORMATTER_0);
        }
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
}
