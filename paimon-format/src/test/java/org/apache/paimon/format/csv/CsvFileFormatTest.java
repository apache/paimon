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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CsvFileFormat}. */
public class CsvFileFormatTest extends FormatReadWriteTest {

    protected CsvFileFormatTest() {
        super("csv");
    }

    @Override
    protected FileFormat fileFormat() {
        Options options = new Options();
        options.set(CoreOptions.FILE_COMPRESSION, compression());
        return new CsvFileFormatFactory().create(new FormatContext(options, 1024, 1024));
    }

    @Test
    public void testCsvParsingWithEmptyFields() throws IOException {

        // First row: ,25,"Software Engineer" (empty first field)
        String csvLine = ",25,\"Software Engineer\"\n";
        String[] fields = parse(csvLine);
        assertThat(fields).isNotNull();
        assertThat(fields[0] == null); // empty field becomes null
        assertThat(fields[1]).isEqualTo("25");
        assertThat(fields[2]).isEqualTo("Software Engineer");

        // Second row: "John Doe",,"Developer" (empty middle field)
        csvLine = "\"John Doe\",,\"Developer\"\n";
        fields = parse(csvLine);
        assertThat(fields).isNotNull();
        assertThat(fields[0]).isEqualTo("John Doe");
        assertThat(fields[1] == null); // empty field becomes null
        assertThat(fields[2]).isEqualTo("Developer");

        // Third row: "Jane Smith",30, (empty last field)
        csvLine = "\"Jane Smith\",30,\n";
        fields = parse(csvLine);
        assertThat(fields).isNotNull();
        assertThat(fields[0]).isEqualTo("Jane Smith");
        assertThat(fields[1]).isEqualTo("30");
        assertThat(fields[2] == null); // empty field becomes null
    }

    @Test
    public void testJsonArrayQuotePreservation() throws Exception {
        // Test that JSON arrays preserve quotes
        String csvLine = "name,\"[1,2,3]\",age";
        String[] fields = parse(csvLine);

        assertThat(fields).hasSize(3);
        assertThat(fields[0]).isEqualTo("name");
        assertThat(fields[1]).isEqualTo("[1,2,3]"); // Quotes should be preserved
        assertThat(fields[2]).isEqualTo("age");
    }

    @Test
    public void testJsonObjectQuotePreservation() throws Exception {
        // Test that JSON objects preserve quotes
        String csvLine = "id,{\"key\":\"value\"},status";
        String[] fields = parse(csvLine);

        assertThat(fields).hasSize(3);
        assertThat(fields[0]).isEqualTo("id");
        assertThat(fields[1]).isEqualTo("{\"key\":\"value\"}"); // Quotes should be preserved
        assertThat(fields[2]).isEqualTo("status");
    }

    @Test
    public void testComplexJsonArrayQuotePreservation() throws Exception {
        // Test complex JSON array with nested objects
        String csvLine =
                "field1,\"[{\"\"name\"\":\"\"John\"\"},{\"\"name\"\":\"\"Jane\"\"}]\",field3";
        String[] fields = parse(csvLine);

        assertThat(fields).hasSize(3);
        assertThat(fields[0]).isEqualTo("field1");
        assertThat(fields[1]).isEqualTo("[{\"name\":\"John\"},{\"name\":\"Jane\"}]");
        assertThat(fields[2]).isEqualTo("field3");
    }

    @Test
    public void testRegularQuotedFieldsRemoveQuotes() throws Exception {
        // Test that regular quoted fields (not JSON) still remove quotes
        String csvLine = "\"John,Doe\",\"25\",\"Engineer\"";
        String[] fields = parse(csvLine);

        assertThat(fields).hasSize(3);
        assertThat(fields[0]).isEqualTo("John,Doe"); // Quotes removed for regular field
        assertThat(fields[1]).isEqualTo("25"); // Quotes removed
        assertThat(fields[2]).isEqualTo("Engineer"); // Quotes removed
    }

    @Test
    public void testJsonWithWhitespace() throws Exception {
        // Test JSON with leading whitespace after quote
        String csvLine = "field1,\" [1,2,3]\",field3";
        String[] fields = parse(csvLine);

        assertThat(fields).hasSize(3);
        assertThat(fields[0]).isEqualTo("field1");
        assertThat(fields[1])
                .isEqualTo(" [1,2,3]"); // Should preserve quotes due to [ after whitespace
        assertThat(fields[2]).isEqualTo("field3");
    }

    @Test
    public void testCsvFieldDelimiterWriteRead() throws IOException {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.INT().notNull(),
                        DataTypes.STRING(),
                        DataTypes.DOUBLE().notNull());

        String[] delimiters = {",", ";", "|", "\t", "\001"};

        // Create test data once (reused for all delimiters)
        List<InternalRow> testData =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Alice"), 100.5),
                        GenericRow.of(2, BinaryString.fromString("Bob"), 200.75),
                        GenericRow.of(3, BinaryString.fromString("Charlie"), 300.25));

        for (String delimiter : delimiters) {
            Options options = new Options();
            options.set(CsvOptions.FIELD_DELIMITER, delimiter);

            List<InternalRow> result =
                    writeThenRead(
                            options,
                            rowType,
                            rowType,
                            testData,
                            "test_field_delim_" + delimiter.hashCode());

            // Verify results
            assertThat(result).hasSize(3);
            assertThat(result.get(0).getInt(0)).isEqualTo(1);
            assertThat(result.get(0).getString(1).toString()).isEqualTo("Alice");
            assertThat(result.get(0).getDouble(2)).isEqualTo(100.5);
            assertThat(result.get(1).getInt(0)).isEqualTo(2);
            assertThat(result.get(1).getString(1).toString()).isEqualTo("Bob");
            assertThat(result.get(1).getDouble(2)).isEqualTo(200.75);
            assertThat(result.get(2).getInt(0)).isEqualTo(3);
            assertThat(result.get(2).getString(1).toString()).isEqualTo("Charlie");
            assertThat(result.get(2).getDouble(2)).isEqualTo(300.25);
        }
    }

    @Test
    public void testCsvLineDelimiterWriteRead() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.STRING());

        String[] delimiters = {"\n", "\r", "\r\n"};

        // Create test data once (reused for all delimiters)
        List<InternalRow> testData =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("First")),
                        GenericRow.of(2, BinaryString.fromString("Second")),
                        GenericRow.of(3, BinaryString.fromString("Third")));

        for (String delimiter : delimiters) {
            Options options = new Options();
            options.set(CsvOptions.LINE_DELIMITER, delimiter);

            List<InternalRow> result =
                    writeThenRead(
                            options,
                            rowType,
                            rowType,
                            testData,
                            "test_line_delim_" + delimiter.hashCode());

            // Verify results
            assertThat(result).hasSize(3);
            assertThat(result.get(0).getInt(0)).isEqualTo(1);
            assertThat(result.get(0).getString(1).toString()).isEqualTo("First");
            assertThat(result.get(1).getInt(0)).isEqualTo(2);
            assertThat(result.get(1).getString(1).toString()).isEqualTo("Second");
            assertThat(result.get(2).getInt(0)).isEqualTo(3);
            assertThat(result.get(2).getString(1).toString()).isEqualTo("Third");
        }
    }

    @Test
    public void testCustomLineDelimiter() throws IOException {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.INT().notNull(),
                        DataTypes.STRING(),
                        DataTypes.DOUBLE().notNull());

        String[] customDelimiters = {"|||", "###", "<EOL>", "\t\t", "abc"};

        List<InternalRow> testData =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("ab"), 100.5),
                        GenericRow.of(2, BinaryString.fromString("Bob"), 200.75),
                        GenericRow.of(3, BinaryString.fromString("Charlie"), 300.25));

        for (String delimiter : customDelimiters) {
            Options options = new Options();
            options.set(CsvOptions.LINE_DELIMITER, delimiter);

            List<InternalRow> result =
                    writeThenRead(
                            options,
                            rowType,
                            rowType,
                            testData,
                            "test_custom_line_delim_" + delimiter.hashCode());

            // Verify results
            assertThat(result).hasSize(3);
            assertThat(result.get(0).getInt(0)).isEqualTo(1);
            assertThat(result.get(0).getString(1).toString()).isEqualTo("ab");
            assertThat(result.get(0).getDouble(2)).isEqualTo(100.5);
            assertThat(result.get(1).getInt(0)).isEqualTo(2);
            assertThat(result.get(1).getString(1).toString()).isEqualTo("Bob");
            assertThat(result.get(1).getDouble(2)).isEqualTo(200.75);
            assertThat(result.get(2).getInt(0)).isEqualTo(3);
            assertThat(result.get(2).getString(1).toString()).isEqualTo("Charlie");
            assertThat(result.get(2).getDouble(2)).isEqualTo(300.25);
        }
    }

    @Test
    public void testCsvQuoteCharacterWriteRead() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.STRING());

        String[] quoteChars = {"\"", "'", "`"};

        // Create test data with values that need quoting (contain spaces/commas)
        List<InternalRow> testData =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Hello, World")),
                        GenericRow.of(2, BinaryString.fromString("Test Value")),
                        GenericRow.of(3, BinaryString.fromString("Another, Test")));

        for (String quoteChar : quoteChars) {
            Options options = new Options();
            options.set(CsvOptions.QUOTE_CHARACTER, quoteChar);

            List<InternalRow> result =
                    writeThenRead(
                            options,
                            rowType,
                            rowType,
                            testData,
                            "test_quote_char_" + quoteChar.hashCode());

            // Verify results
            assertThat(result).hasSize(3);
            assertThat(result.get(0).getInt(0)).isEqualTo(1);
            assertThat(result.get(0).getString(1).toString()).isEqualTo("Hello, World");
            assertThat(result.get(1).getInt(0)).isEqualTo(2);
            assertThat(result.get(1).getString(1).toString()).isEqualTo("Test Value");
            assertThat(result.get(2).getInt(0)).isEqualTo(3);
            assertThat(result.get(2).getString(1).toString()).isEqualTo("Another, Test");
        }
    }

    @Test
    public void testCsvEscapeCharacterWriteRead() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.STRING());

        String[] escapeChars = {"\\", "/"};

        // Create test data with values that might need escaping
        List<InternalRow> testData =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Value\"With\"Quotes")),
                        GenericRow.of(2, BinaryString.fromString("Normal Value")),
                        GenericRow.of(3, BinaryString.fromString("Special\\Characters")));

        for (String escapeChar : escapeChars) {
            Options options = new Options();
            options.set(CsvOptions.ESCAPE_CHARACTER, escapeChar);

            List<InternalRow> result =
                    writeThenRead(
                            options,
                            rowType,
                            rowType,
                            testData,
                            "test_escape_char_" + escapeChar.hashCode());

            // Verify results
            assertThat(result).hasSize(3);
            assertThat(result.get(0).getInt(0)).isEqualTo(1);
            assertThat(result.get(1).getInt(0)).isEqualTo(2);
            assertThat(result.get(1).getString(1).toString()).isEqualTo("Normal Value");
            assertThat(result.get(2).getInt(0)).isEqualTo(3);
        }
    }

    @Test
    public void testCsvIncludeHeaderWriteRead() throws IOException {
        RowType rowType =
                DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.STRING(), DataTypes.BOOLEAN());

        boolean[] includeHeaderOptions = {false, true};

        // Create test data
        List<InternalRow> testData =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Alice"), true),
                        GenericRow.of(2, BinaryString.fromString("Bob"), false),
                        GenericRow.of(3, BinaryString.fromString("Charlie"), true));

        for (boolean includeHeader : includeHeaderOptions) {
            Options options = new Options();
            options.set(CsvOptions.INCLUDE_HEADER, includeHeader);

            List<InternalRow> result =
                    writeThenRead(
                            options,
                            rowType,
                            rowType,
                            testData,
                            "test_include_header_" + includeHeader);

            // Verify results
            assertThat(result).hasSize(3);
            assertThat(result.get(0).getInt(0)).isEqualTo(1);
            assertThat(result.get(0).getString(1).toString()).isEqualTo("Alice");
            assertThat(result.get(0).getBoolean(2)).isEqualTo(true);
            assertThat(result.get(1).getInt(0)).isEqualTo(2);
            assertThat(result.get(1).getString(1).toString()).isEqualTo("Bob");
            assertThat(result.get(1).getBoolean(2)).isEqualTo(false);
            assertThat(result.get(2).getInt(0)).isEqualTo(3);
            assertThat(result.get(2).getString(1).toString()).isEqualTo("Charlie");
            assertThat(result.get(2).getBoolean(2)).isEqualTo(true);
        }
    }

    @Test
    public void testCsvNullLiteralWriteRead() throws IOException {
        RowType rowType =
                DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.STRING(), DataTypes.INT());

        String[] nullLiterals = {"", "NULL", "null"};

        // Create test data with null values
        List<InternalRow> testData =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Alice"), null),
                        GenericRow.of(2, null, 100),
                        GenericRow.of(3, BinaryString.fromString("Charlie"), 300));

        for (String nullLiteral : nullLiterals) {
            Options options = new Options();
            options.set(CsvOptions.NULL_LITERAL, nullLiteral);

            List<InternalRow> result =
                    writeThenRead(
                            options,
                            rowType,
                            rowType,
                            testData,
                            "test_null_literal_" + nullLiteral.hashCode());

            // Verify results
            assertThat(result).hasSize(3);
            assertThat(result.get(0).getInt(0)).isEqualTo(1);
            assertThat(result.get(0).getString(1).toString()).isEqualTo("Alice");
            assertThat(result.get(0).isNullAt(2)).isTrue();
            assertThat(result.get(1).getInt(0)).isEqualTo(2);
            assertThat(result.get(1).isNullAt(1)).isTrue();
            assertThat(result.get(1).getInt(2)).isEqualTo(100);
            assertThat(result.get(2).getInt(0)).isEqualTo(3);
            assertThat(result.get(2).getString(1).toString()).isEqualTo("Charlie");
            assertThat(result.get(2).getInt(2)).isEqualTo(300);
        }
    }

    @Test
    public void testCsvOptionsCombinationWriteRead() throws IOException {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.INT().notNull(),
                        DataTypes.STRING(),
                        DataTypes.DOUBLE(),
                        DataTypes.BOOLEAN());

        // Test multiple CSV options together
        Options options = new Options();
        options.set(CsvOptions.FIELD_DELIMITER, ";");
        options.set(CsvOptions.LINE_DELIMITER, "\r\n");
        options.set(CsvOptions.QUOTE_CHARACTER, "'");
        options.set(CsvOptions.ESCAPE_CHARACTER, "/");
        options.set(CsvOptions.INCLUDE_HEADER, true);
        options.set(CsvOptions.NULL_LITERAL, "NULL");

        // Create test data with various scenarios
        List<InternalRow> testData =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Alice; Test"), 100.5, true),
                        GenericRow.of(2, null, 200.75, false),
                        GenericRow.of(3, BinaryString.fromString("Charlie's Data"), null, true),
                        GenericRow.of(4, BinaryString.fromString("Normal"), 400.0, null));

        List<InternalRow> result =
                writeThenRead(options, rowType, rowType, testData, "test_csv_combination");

        // Verify results
        assertThat(result).hasSize(4);

        // Verify first row
        assertThat(result.get(0).getInt(0)).isEqualTo(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("Alice; Test");
        assertThat(result.get(0).getDouble(2)).isEqualTo(100.5);
        assertThat(result.get(0).getBoolean(3)).isEqualTo(true);

        // Verify second row (with null string)
        assertThat(result.get(1).getInt(0)).isEqualTo(2);
        assertThat(result.get(1).isNullAt(1)).isTrue();
        assertThat(result.get(1).getDouble(2)).isEqualTo(200.75);
        assertThat(result.get(1).getBoolean(3)).isEqualTo(false);

        // Verify third row (with null double)
        assertThat(result.get(2).getInt(0)).isEqualTo(3);
        assertThat(result.get(2).getString(1).toString()).isEqualTo("Charlie's Data");
        assertThat(result.get(2).isNullAt(2)).isTrue();
        assertThat(result.get(2).getBoolean(3)).isEqualTo(true);

        // Verify fourth row (with null boolean)
        assertThat(result.get(3).getInt(0)).isEqualTo(4);
        assertThat(result.get(3).getString(1).toString()).isEqualTo("Normal");
        assertThat(result.get(3).getDouble(2)).isEqualTo(400.0);
        assertThat(result.get(3).isNullAt(3)).isTrue();
    }

    @Test
    public void testCsvModeWriteRead() throws IOException {
        RowType rowType =
                DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.STRING(), DataTypes.DOUBLE());

        // Test PERMISSIVE mode
        Options permissiveOptions = new Options();
        permissiveOptions.set(CsvOptions.MODE, CsvOptions.Mode.PERMISSIVE);
        FileFormat format =
                new CsvFileFormatFactory().create(new FormatContext(permissiveOptions, 1024, 1024));
        Path testFile = new Path(parent, "test_mode_" + UUID.randomUUID() + ".csv");

        fileIO.writeFile(testFile, "1,Alice,aaaa,100.23\n2,Bob,200.75", false);
        List<InternalRow> permissiveResult = read(format, rowType, rowType, testFile);
        assertThat(permissiveResult).hasSize(2);
        assertThat(permissiveResult.get(0).getInt(0)).isEqualTo(1);
        assertThat(permissiveResult.get(0).getString(1).toString()).isEqualTo("Alice");
        assertThat(permissiveResult.get(0).isNullAt(2)).isTrue();
        assertThat(permissiveResult.get(1).getInt(0)).isEqualTo(2);
        assertThat(permissiveResult.get(1).getString(1).toString()).isEqualTo("Bob");
        assertThat(permissiveResult.get(1).getDouble(2)).isEqualTo(200.75);

        // Test DROPMALFORMED mode
        Options dropMalformedOptions = new Options();
        dropMalformedOptions.set(CsvOptions.MODE, CsvOptions.Mode.DROPMALFORMED);
        format =
                new CsvFileFormatFactory()
                        .create(new FormatContext(dropMalformedOptions, 1024, 1024));
        List<InternalRow> dropMalformedResult = read(format, rowType, rowType, testFile);
        assertThat(dropMalformedResult).hasSize(1);
        assertThat(dropMalformedResult.get(0).getInt(0)).isEqualTo(2);
        assertThat(dropMalformedResult.get(0).getString(1).toString()).isEqualTo("Bob");
        assertThat(dropMalformedResult.get(0).getDouble(2)).isEqualTo(200.75);

        // Test FAILFAST mode
        Options failFastOptions = new Options();
        failFastOptions.set(CsvOptions.MODE, CsvOptions.Mode.FAILFAST);
        assertThatThrownBy(
                        () -> {
                            read(
                                    new CsvFileFormatFactory()
                                            .create(new FormatContext(failFastOptions, 1024, 1024)),
                                    rowType,
                                    rowType,
                                    testFile);
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testSpecialCases() throws IOException {
        RowType rowType =
                DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.STRING(), DataTypes.DOUBLE());

        FileFormat format =
                new CsvFileFormatFactory().create(new FormatContext(new Options(), 1024, 1024));
        Path testFile = new Path(parent, "test_mode_" + UUID.randomUUID() + ".csv");

        fileIO.writeFile(
                testFile,
                "1,Alice,aaaa,100.23\n"
                        + "2,\"Bob\",200.75\n"
                        + "3,\"Json\"v,300.64\n"
                        + "4,Jack\"o\"n,400.81",
                false);
        List<InternalRow> permissiveResult = read(format, rowType, rowType, testFile);
        assertThat(permissiveResult).hasSize(4);
        assertThat(permissiveResult.get(0).getInt(0)).isEqualTo(1);
        assertThat(permissiveResult.get(0).getString(1).toString()).isEqualTo("Alice");
        assertThat(permissiveResult.get(0).isNullAt(2)).isTrue();
        assertThat(permissiveResult.get(1).getInt(0)).isEqualTo(2);
        assertThat(permissiveResult.get(1).getString(1).toString()).isEqualTo("Bob");
        assertThat(permissiveResult.get(1).getDouble(2)).isEqualTo(200.75);
        assertThat(permissiveResult.get(2).getInt(0)).isEqualTo(3);
        assertThat(permissiveResult.get(2).getString(1).toString()).isEqualTo("Json\"v");
        assertThat(permissiveResult.get(2).getDouble(2)).isEqualTo(300.64);
        assertThat(permissiveResult.get(3).getInt(0)).isEqualTo(4);
        assertThat(permissiveResult.get(3).getString(1).toString()).isEqualTo("Jack\"o\"n");
        assertThat(permissiveResult.get(3).getDouble(2)).isEqualTo(400.81);
    }

    private List<InternalRow> read(
            FileFormat format, RowType fullRowType, RowType readRowType, Path testFile)
            throws IOException {
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(fullRowType, readRowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, testFile, fileIO.getFileSize(testFile)))) {

            InternalRowSerializer serializer = new InternalRowSerializer(readRowType);
            List<InternalRow> result = new ArrayList<>();
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));
            return result;
        }
    }

    @Override
    protected RowType rowTypeForFullTypesTest() {
        RowType.Builder builder =
                RowType.builder()
                        .field("id", DataTypes.INT().notNull())
                        .field("name", DataTypes.STRING()) /* optional by default */
                        .field("salary", DataTypes.DOUBLE().notNull())
                        .field("boolean", DataTypes.BOOLEAN().nullable())
                        .field("tinyint", DataTypes.TINYINT())
                        .field("smallint", DataTypes.SMALLINT())
                        .field("bigint", DataTypes.BIGINT())
                        .field("bytes", DataTypes.BYTES())
                        .field("timestamp", DataTypes.TIMESTAMP())
                        .field("timestamp_3", DataTypes.TIMESTAMP(3))
                        .field("timestamp_ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
                        .field("timestamp_ltz_3", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                        .field("date", DataTypes.DATE())
                        .field("decimal", DataTypes.DECIMAL(2, 2))
                        .field("decimal2", DataTypes.DECIMAL(38, 2))
                        .field("decimal3", DataTypes.DECIMAL(10, 1));

        RowType rowType = builder.build();

        if (ThreadLocalRandom.current().nextBoolean()) {
            rowType = (RowType) rowType.notNull();
        }

        return rowType;
    }

    @Override
    protected GenericRow expectedRowForFullTypesTest() {
        List<Object> values =
                Arrays.asList(
                        1,
                        fromString("name"),
                        5.26D,
                        true,
                        (byte) 3,
                        (short) 6,
                        12304L,
                        new byte[] {1, 5, 2},
                        Timestamp.fromMicros(123123123),
                        Timestamp.fromEpochMillis(123123123),
                        Timestamp.fromMicros(123123123),
                        Timestamp.fromEpochMillis(123123123),
                        2456,
                        Decimal.fromBigDecimal(new BigDecimal("0.22"), 2, 2),
                        Decimal.fromBigDecimal(new BigDecimal("12312455.22"), 38, 2),
                        Decimal.fromBigDecimal(new BigDecimal("12455.1"), 10, 1));
        return GenericRow.of(values.toArray());
    }

    @Override
    public boolean supportNestedReadPruning() {
        return false;
    }

    @Override
    public String compression() {
        return HadoopCompressionType.NONE.value();
    }

    @Override
    public boolean supportDataFileWithoutExtension() {
        return true;
    }

    @Test
    public void testProjectionPushdown() throws IOException {
        RowType fullRowType =
                RowType.builder()
                        .field("id", DataTypes.INT().notNull())
                        .field("name", DataTypes.STRING())
                        .field("score", DataTypes.DOUBLE())
                        .field("active", DataTypes.BOOLEAN())
                        .build();

        RowType projectedRowType =
                RowType.builder()
                        .field("score", DataTypes.DOUBLE())
                        .field("name", DataTypes.STRING())
                        .build();

        List<InternalRow> testData =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Alice"), null, true),
                        GenericRow.of(2, null, 87.2, false),
                        GenericRow.of(3, BinaryString.fromString("Charlie"), 92.8, null));

        List<InternalRow> result =
                writeThenRead(
                        new Options(), fullRowType, projectedRowType, testData, "test_projection");

        assertThat(result).hasSize(3);
        assertThat(result.get(0).isNullAt(0)).isTrue(); // score is null
        assertThat(result.get(0).getString(1).toString()).isEqualTo("Alice");

        assertThat(result.get(1).getDouble(0)).isEqualTo(87.2);
        assertThat(result.get(1).isNullAt(1)).isTrue(); // name is null

        assertThat(result.get(2).getDouble(0)).isEqualTo(92.8);
        assertThat(result.get(2).getString(1).toString()).isEqualTo("Charlie");
    }

    private String[] parse(String csvLine) throws IOException {
        CsvSchema schema =
                CsvSchema.emptySchema()
                        .withQuoteChar('\"')
                        .withColumnSeparator(',')
                        .withoutHeader()
                        .withNullValue("null");
        return new CsvMapper().readerFor(String[].class).with(schema).readValue(csvLine);
    }

    /**
     * Performs a complete write-read test with the given options and test data. Returns the data
     * that was read back for further verification.
     */
    private List<InternalRow> writeThenRead(
            Options options,
            RowType fullRowType,
            RowType rowType,
            List<InternalRow> testData,
            String testPrefix)
            throws IOException {
        FileFormat format =
                new CsvFileFormatFactory().create(new FormatContext(options, 1024, 1024));
        Path testFile = new Path(parent, testPrefix + "_" + UUID.randomUUID() + ".csv");

        FormatWriterFactory writerFactory = format.createWriterFactory(fullRowType);
        try (PositionOutputStream out = fileIO.newOutputStream(testFile, false);
                FormatWriter writer = writerFactory.create(out, "none")) {
            for (InternalRow row : testData) {
                writer.addElement(row);
            }
        }
        return read(format, fullRowType, rowType, testFile);
    }
}
