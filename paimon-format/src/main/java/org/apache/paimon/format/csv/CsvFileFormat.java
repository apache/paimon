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
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.CloseShieldOutputStream;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** CSV {@link FileFormat}. */
public class CsvFileFormat extends FileFormat {

    public static final String CSV_IDENTIFIER = "csv";

    private final CsvOptions options;

    public CsvFileFormat(FormatContext context) {
        super(CSV_IDENTIFIER);
        this.options = new CsvOptions(context.options());
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        return new CsvReaderFactory(dataSchemaRowType, projectedRowType, options);
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
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case BINARY:
            case VARBINARY:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // These are directly supported
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type for CSV format: " + dataType);
        }
    }

    /** CSV {@link FormatReaderFactory} implementation. */
    private static class CsvReaderFactory implements FormatReaderFactory {

        private final RowType dataSchemaRowType;
        private final RowType projectedRowType;
        private final CsvOptions options;

        public CsvReaderFactory(
                RowType dataSchemaRowType, RowType projectedRowType, CsvOptions options) {
            this.dataSchemaRowType = dataSchemaRowType;
            this.projectedRowType = projectedRowType;
            this.options = options;
        }

        @Override
        public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
            return new CsvFileReader(
                    context.fileIO(),
                    context.filePath(),
                    dataSchemaRowType,
                    projectedRowType,
                    options,
                    0,
                    null);
        }

        @Override
        public FileRecordReader<InternalRow> createReader(Context context, long offset, long length)
                throws IOException {
            return new CsvFileReader(
                    context.fileIO(),
                    context.filePath(),
                    dataSchemaRowType,
                    projectedRowType,
                    options,
                    offset,
                    length);
        }
    }

    /** A {@link FormatWriterFactory} to write {@link InternalRow} to CSV. */
    private static class CsvWriterFactory implements FormatWriterFactory {

        private final RowType rowType;
        private final CsvOptions options;

        public CsvWriterFactory(RowType rowType, CsvOptions options) {
            this.rowType = rowType;
            this.options = options;
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression)
                throws IOException {
            return new CsvFormatWriter(
                    new CloseShieldOutputStream(out), rowType, options, compression);
        }
    }
}
