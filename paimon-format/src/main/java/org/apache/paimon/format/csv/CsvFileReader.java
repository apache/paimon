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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.apache.paimon.utils.TypeUtils.castFromString;

/** CSV file reader implementation. */
public class CsvFileReader implements FileRecordReader<InternalRow> {

    private final RowType rowType;
    private final String fieldDelimiter;
    private final String nullLiteral;
    private final boolean includeHeader;
    private final String quoteCharacter;
    private final String escapeCharacter;
    private final Path filePath;

    private BufferedReader bufferedReader;
    private boolean headerSkipped = false;
    private boolean readerClosed = false;
    private CsvRecordIterator reader;

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public CsvFileReader(FormatReaderFactory.Context context, RowType rowType, Options options)
            throws IOException {
        this.rowType = rowType;
        this.filePath = context.filePath();
        this.fieldDelimiter = options.get(CsvFileFormat.FIELD_DELIMITER);
        this.nullLiteral = options.get(CsvFileFormat.CSV_NULL_LITERAL);
        this.includeHeader = options.get(CsvFileFormat.CSV_INCLUDE_HEADER);
        this.quoteCharacter = options.get(CsvFileFormat.CSV_QUOTE_CHARACTER);
        this.escapeCharacter = options.get(CsvFileFormat.CSV_ESCAPE_CHARACTER);
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
        List<String> fields =
                CsvParser.splitCsvLine(
                        rowType.getFieldCount(),
                        line,
                        fieldDelimiter.charAt(0),
                        quoteCharacter.charAt(0));

        Object[] values = new Object[rowType.getFieldCount()];
        int fieldCount = Math.min(fields.size(), rowType.getFieldCount());

        for (int i = 0; i < fieldCount; i++) {
            String field = fields.get(i);

            // Handle null values early
            if (field == null || field.equals(nullLiteral) || field.isEmpty()) {
                values[i] = null;
                continue;
            }

            // Trim whitespace only for non-JSON fields
            String trimmedField = field.trim();
            values[i] = parseField(trimmedField, rowType.getTypeAt(i));
        }

        return GenericRow.of(values);
    }

    /**
     * Parse a single field value according to its data type.
     *
     * @param field the field value as string
     * @param dataType the target data type
     * @return parsed value or null for null/empty fields
     */
    private Object parseField(String field, DataType dataType) {
        if (field == null || field.equals(nullLiteral)) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case BINARY:
            case VARBINARY:
                // Handle base64 encoded binary data
                try {
                    return java.util.Base64.getDecoder().decode(field);
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException("Failed to decode base64 binary data: " + field, e);
                }
            default:
                // Use Paimon's built-in type casting
                return castFromString(field, dataType);
        }
    }
}
