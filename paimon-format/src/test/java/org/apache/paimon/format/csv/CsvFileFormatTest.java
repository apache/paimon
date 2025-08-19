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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CsvOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CsvFileFormat}. */
public class CsvFileFormatTest extends FormatReadWriteTest {

    protected CsvFileFormatTest() {
        super("csv");
    }

    @Override
    protected FileFormat fileFormat() {
        return new CsvFileFormatFactory().create(new FormatContext(new Options(), 1024, 1024));
    }

    @Test
    public void testWhenUseHiveDefaultDelimiter() throws IOException {
        Options options = new Options();
        options.set(CsvOptions.FIELD_DELIMITER, "\001");
        FileFormat format =
                new CsvFileFormatFactory().create(new FormatContext(new Options(), 1024, 1024));
        testSimpleTypesUtil(
                format, new Path(new Path(parent.toUri()), UUID.randomUUID() + "." + formatType));
        testFullTypesUtil(
                format, new Path(new Path(parent.toUri()), UUID.randomUUID() + "." + formatType));
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
    public boolean supportDataFileWithoutExtension() {
        return true;
    }

    private String[] parse(String csvLine) throws IOException {
        CsvSchema schema =
                CsvSchema.emptySchema()
                        .withQuoteChar('\"')
                        .withColumnSeparator(',')
                        .withoutHeader()
                        .withNullValue("null");
        return CsvFileReader.parseCsvLineToArray(csvLine, schema);
    }
}
