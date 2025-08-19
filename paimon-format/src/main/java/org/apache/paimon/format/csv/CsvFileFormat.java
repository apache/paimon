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
import org.apache.paimon.format.CloseShieldPositionOutputStream;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** CSV {@link FileFormat}. */
public class CsvFileFormat extends FileFormat {

    public static final String CSV_IDENTIFIER = "csv";

    protected static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("field-delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("The field delimiter for CSV or TXT format");

    protected static final ConfigOption<String> CSV_LINE_DELIMITER =
            ConfigOptions.key("csv.line-delimiter")
                    .stringType()
                    .defaultValue("\n")
                    .withDescription("The line delimiter for CSV format");

    protected static final ConfigOption<String> CSV_QUOTE_CHARACTER =
            ConfigOptions.key("csv.quote-character")
                    .stringType()
                    .defaultValue("\"")
                    .withDescription("The quote character for CSV format");

    protected static final ConfigOption<String> CSV_ESCAPE_CHARACTER =
            ConfigOptions.key("csv.escape-character")
                    .stringType()
                    .defaultValue("\\")
                    .withDescription("The escape character for CSV format");

    protected static final ConfigOption<Boolean> CSV_INCLUDE_HEADER =
            ConfigOptions.key("csv.include-header")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to include header in CSV files");

    protected static final ConfigOption<String> CSV_NULL_LITERAL =
            ConfigOptions.key("csv.null-literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription("The literal for null values in CSV format");

    private final Options options;

    public CsvFileFormat(FormatContext context) {
        this(context, CSV_IDENTIFIER);
    }

    public CsvFileFormat(FormatContext context, String identifier) {
        super(identifier);
        this.options = context.options();
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType projectedRowType, @Nullable List<Predicate> filters) {
        return new CsvReaderFactory(projectedRowType, options);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new CsvWriterFactory(type, options);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        List<DataType> fieldTypes = rowType.getFieldTypes();
        for (DataType dataType : fieldTypes) {
            validateDataType(dataType);
        }
    }

    private void validateDataType(DataType dataType) {
        // CSV format supports primitive types and string representation of complex types
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // These are directly supported
                break;
            case ARRAY:
            case MAP:
            case ROW:
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type for CSV format: " + dataType);
        }
    }

    /** A {@link FormatWriterFactory} to write {@link InternalRow} to CSV. */
    private static class CsvWriterFactory implements FormatWriterFactory, SupportsDirectWrite {

        private final RowType rowType;
        private final Options options;

        public CsvWriterFactory(RowType rowType, Options options) {
            this.rowType = rowType;
            this.options = options;
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression)
                throws IOException {
            // Wrap the output stream to prevent premature closing/flushing
            PositionOutputStream protectedOut = new CloseShieldPositionOutputStream(out);
            return new CsvFormatWriter(protectedOut, rowType, options);
        }

        @Override
        public FormatWriter create(FileIO fileIO, Path path, String compression)
                throws IOException {
            // Direct file I/O management - this bypasses SingleFileWriter's stream management
            PositionOutputStream out = fileIO.newOutputStream(path, false);
            return new CsvFormatWriter(out, rowType, options);
        }
    }
}
