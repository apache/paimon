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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CsvFileFormat}. */
public class CsvFileFormatTest extends FormatReadWriteTest {

    @TempDir File tempDir;

    protected CsvFileFormatTest() {
        super("csv");
    }

    @Override
    protected FileFormat fileFormat() {
        return new CsvFileFormatFactory().create(new FormatContext(new Options(), 1024, 1024));
    }

    @Override
    public boolean supportNestedReadPruning() {
        return false;
    }

    @Override
    public boolean supportDataFileWithoutExtension() {
        return true;
    }

    @Test
    public void testCsvParsingWithEmptyFields() throws IOException {
        // Create test CSV content with empty fields
        String csvContent =
                "\",25,\"Software Engineer\"\n"
                        + "\"John Doe\",,\"Developer\"\n"
                        + "\"Jane Smith\",30,\"\n";

        // Write to temporary file
        File csvFile = new File(tempDir, "test_empty.csv");
        try (FileWriter writer = new FileWriter(csvFile)) {
            writer.write(csvContent);
        }

        // Create row type
        RowType rowType =
                RowType.of(
                        DataTypes.STRING(), // name
                        DataTypes.INT(), // age
                        DataTypes.STRING() // job
                        );

        // Create options
        Options options = new Options();
        options.set(CsvFileFormat.FIELD_DELIMITER, ",");
        options.set(CsvFileFormat.CSV_QUOTE_CHARACTER, "\"");
        options.set(CsvFileFormat.CSV_NULL_LITERAL, "null");
        options.set(CsvFileFormat.CSV_INCLUDE_HEADER, false);

        // Create reader using FormatReaderContext
        FileIO fileIO = new LocalFileIO();
        Path filePath = new Path(csvFile.getAbsolutePath());
        FormatReaderContext context = new FormatReaderContext(fileIO, filePath, csvFile.length());

        CsvFileReader reader = new CsvFileReader(context, rowType, options);

        // Read and verify data
        FileRecordIterator<InternalRow> iterator = reader.readBatch();
        assertThat(iterator).isNotNull();

        // First row: "",25,"Software Engineer"
        InternalRow row1 = iterator.next();
        assertThat(row1).isNotNull();
        assertThat(row1.isNullAt(0)).isTrue(); // empty string becomes null
        assertThat(row1.getInt(1)).isEqualTo(25);
        assertThat(row1.getString(2).toString()).isEqualTo("Software Engineer");

        // Second row: "John Doe",,"Developer"
        InternalRow row2 = iterator.next();
        assertThat(row2).isNotNull();
        assertThat(row2.getString(0).toString()).isEqualTo("John Doe");
        assertThat(row2.isNullAt(1)).isTrue(); // empty field becomes null
        assertThat(row2.getString(2).toString()).isEqualTo("Developer");

        // Third row: "Jane Smith",30,""
        InternalRow row3 = iterator.next();
        assertThat(row3).isNotNull();
        assertThat(row3.getString(0).toString()).isEqualTo("Jane Smith");
        assertThat(row3.getInt(1)).isEqualTo(30);
        assertThat(row3.isNullAt(2)).isTrue(); // empty string becomes null

        // No more rows
        assertThat(iterator.next()).isNull();

        // Release resources
        iterator.releaseBatch();
        reader.close();
    }

    @Test
    public void testJsonArrayQuotePreservation() throws Exception {
        // Test that JSON arrays preserve quotes
        String csvLine = "name,\"[1,2,3]\",age";
        CsvSchema schema =
                CsvSchema.emptySchema()
                        .withQuoteChar('\"')
                        .withColumnSeparator(',')
                        .withoutHeader();
        String[] fields = CsvFileReader.parseCsvLineToArray(csvLine, schema);

        assertThat(fields).hasSize(3);
        assertThat(fields[0]).isEqualTo("name");
        assertThat(fields[1]).isEqualTo("[1,2,3]"); // Quotes should be preserved
        assertThat(fields[2]).isEqualTo("age");
    }

    @Test
    public void testJsonObjectQuotePreservation() throws Exception {
        // Test that JSON objects preserve quotes
        String csvLine = "id,{\"key\":\"value\"},status";
        CsvSchema schema =
                CsvSchema.emptySchema().withQuoteChar('"').withColumnSeparator(',').withoutHeader();
        String[] fields = CsvFileReader.parseCsvLineToArray(csvLine, schema);

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
        CsvSchema schema =
                CsvSchema.emptySchema()
                        .withQuoteChar('\"')
                        .withColumnSeparator(',')
                        .withEscapeChar('\\')
                        .withoutHeader();
        String[] fields = CsvFileReader.parseCsvLineToArray(csvLine, schema);

        assertThat(fields).hasSize(3);
        assertThat(fields[0]).isEqualTo("field1");
        assertThat(fields[1]).isEqualTo("[{\"name\":\"John\"},{\"name\":\"Jane\"}]");
        assertThat(fields[2]).isEqualTo("field3");
    }

    @Test
    public void testRegularQuotedFieldsRemoveQuotes() throws Exception {
        // Test that regular quoted fields (not JSON) still remove quotes
        String csvLine = "\"John,Doe\",\"25\",\"Engineer\"";
        CsvSchema schema =
                CsvSchema.emptySchema()
                        .withQuoteChar('\"')
                        .withColumnSeparator(',')
                        .withoutHeader();
        String[] fields = CsvFileReader.parseCsvLineToArray(csvLine, schema);

        assertThat(fields).hasSize(3);
        assertThat(fields[0]).isEqualTo("John,Doe"); // Quotes removed for regular field
        assertThat(fields[1]).isEqualTo("25"); // Quotes removed
        assertThat(fields[2]).isEqualTo("Engineer"); // Quotes removed
    }

    @Test
    public void testJsonWithWhitespace() throws Exception {
        // Test JSON with leading whitespace after quote
        String csvLine = "field1,\" [1,2,3]\",field3";
        CsvSchema schema =
                CsvSchema.emptySchema().withQuoteChar('"').withColumnSeparator(',').withoutHeader();
        String[] fields = CsvFileReader.parseCsvLineToArray(csvLine, schema);

        assertThat(fields).hasSize(3);
        assertThat(fields[0]).isEqualTo("field1");
        assertThat(fields[1])
                .isEqualTo(" [1,2,3]"); // Should preserve quotes due to [ after whitespace
        assertThat(fields[2]).isEqualTo("field3");
    }
}
