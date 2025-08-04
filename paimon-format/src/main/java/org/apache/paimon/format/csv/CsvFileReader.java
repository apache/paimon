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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/** CSV file reader implementation. */
public class CsvFileReader implements FileRecordReader<InternalRow> {

    private final RowType rowType;
    private final String fieldDelimiter;
    private final String quoteCharacter;
    private final String escapeCharacter;
    private final String nullLiteral;
    private final boolean includeHeader;
    private final Path filePath;

    private BufferedReader bufferedReader;
    private boolean headerSkipped = false;
    private String nextLine = null;
    private boolean readerClosed = false;
    private CsvRecordIterator reader;

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public CsvFileReader(FormatReaderFactory.Context context, RowType rowType, Options options)
            throws IOException {
        this.rowType = rowType;
        this.fieldDelimiter = options.getString("csv.field-delimiter", ",");
        this.quoteCharacter = options.getString("csv.quote-character", "\"");
        this.escapeCharacter = options.getString("csv.escape-character", "\\");
        this.nullLiteral = options.getString("csv.null-literal", "");
        this.includeHeader = options.getBoolean("csv.include-header", false);
        this.filePath = context.filePath();

        FileIO fileIO = context.fileIO();
        SeekableInputStream inputStream = fileIO.newInputStream(context.filePath());
        reader = new CsvRecordIterator();
        this.bufferedReader =
                new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    }

    @Override
    @Nullable
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        if (readerClosed) {
            return null;
        }

        // Skip header if needed
        if (includeHeader && !headerSkipped) {
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
            return parseCsvLine(nextLine);
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

    private InternalRow parseCsvLine(String line) {
        String[] fields = splitCsvLine(line);
        Object[] values = new Object[rowType.getFieldCount()];

        for (int i = 0; i < Math.min(fields.length, rowType.getFieldCount()); i++) {
            String field = fields[i].trim();
            if (field.equals(nullLiteral)) {
                values[i] = null;
            } else {
                values[i] = parseField(field, rowType.getTypeAt(i));
            }
        }

        return GenericRow.of(values);
    }

    private String[] splitCsvLine(String line) {
        // Simple CSV parsing - could be enhanced for proper quote handling
        return line.split(fieldDelimiter, -1);
    }

    private Object parseField(String field, DataType dataType) {
        if (field == null || field.equals(nullLiteral)) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        try {
            switch (typeRoot) {
                case BOOLEAN:
                    return Boolean.parseBoolean(field);
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
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) dataType;
                    return Decimal.fromBigDecimal(
                            new BigDecimal(field),
                            decimalType.getPrecision(),
                            decimalType.getScale());
                case CHAR:
                case VARCHAR:
                    return BinaryString.fromString(field);
                case BINARY:
                case VARBINARY:
                    return field.getBytes(StandardCharsets.UTF_8);
                case DATE:
                    LocalDate date = LocalDate.parse(field, DATE_FORMATTER);
                    return (int) date.toEpochDay();
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    LocalDateTime dateTime;
                    try {
                        dateTime = LocalDateTime.parse(field, TIMESTAMP_FORMATTER);
                    } catch (DateTimeParseException e) {
                        // Try without time part
                        LocalDate date2 = LocalDate.parse(field, DATE_FORMATTER);
                        dateTime = date2.atStartOfDay();
                    }
                    return Timestamp.fromLocalDateTime(dateTime);
                default:
                    // For complex types, treat as string
                    return field;
            }
        } catch (Exception e) {
            // Return as string if parsing fails
            return BinaryString.fromString(field);
        }
    }
}
