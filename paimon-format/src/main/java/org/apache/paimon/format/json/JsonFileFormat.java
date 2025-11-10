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

package org.apache.paimon.format.json;

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

/** JSON {@link FileFormat}. */
public class JsonFileFormat extends FileFormat {

    public static final String IDENTIFIER = "json";

    private final JsonOptions options;

    public JsonFileFormat(FormatContext context) {
        super(IDENTIFIER);
        this.options = new JsonOptions(context.options());
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        return new JsonReaderFactory(projectedRowType, options);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new JsonWriterFactory(type, options);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        List<DataType> fieldTypes = rowType.getFieldTypes();
        for (DataType dataType : fieldTypes) {
            validateDataType(dataType);
        }
    }

    private void validateDataType(DataType dataType) {
        // JSON format supports all data types since they can be represented as JSON values
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
            case ARRAY:
            case MAP:
            case ROW:
                // All types are supported in JSON
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type for JSON format: " + dataType);
        }
    }

    /** Factory to create {@link JsonFileReader}. */
    private static class JsonReaderFactory implements FormatReaderFactory {

        private final RowType projectedRowType;
        private final JsonOptions options;

        public JsonReaderFactory(RowType projectedRowType, JsonOptions options) {
            this.projectedRowType = projectedRowType;
            this.options = options;
        }

        @Override
        public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
            return new JsonFileReader(
                    context.fileIO(), context.filePath(), projectedRowType, options, 0, null);
        }

        @Override
        public FileRecordReader<InternalRow> createReader(Context context, long offset, long length)
                throws IOException {
            return new JsonFileReader(
                    context.fileIO(),
                    context.filePath(),
                    projectedRowType,
                    options,
                    offset,
                    length);
        }
    }

    /** A {@link FormatWriterFactory} to write {@link InternalRow} to JSON. */
    private static class JsonWriterFactory implements FormatWriterFactory {

        private final RowType rowType;
        private final JsonOptions options;

        public JsonWriterFactory(RowType rowType, JsonOptions options) {
            this.rowType = rowType;
            this.options = options;
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression)
                throws IOException {
            return new JsonFormatWriter(
                    new CloseShieldOutputStream(out), rowType, options, compression);
        }
    }
}
