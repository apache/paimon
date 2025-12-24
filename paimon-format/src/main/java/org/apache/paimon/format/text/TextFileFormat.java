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

package org.apache.paimon.format.text;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.CloseShieldOutputStream;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** TEXT {@link FileFormat}. */
public class TextFileFormat extends FileFormat {

    public static final String TEXT_IDENTIFIER = "text";

    private final TextOptions options;

    public TextFileFormat(FileFormatFactory.FormatContext context) {
        super(TEXT_IDENTIFIER);
        this.options = new TextOptions(context.options());
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        // For text format, since it only has one string column, the projectedRowType will typically
        // be a RowType with a single string field or empty (for example, when count *).
        return new TextReaderFactory(projectedRowType, options);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new TextWriterFactory(type, options);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        if (rowType.getFieldCount() != 1
                && !rowType.getFieldTypes().get(0).equals(DataTypes.STRING())) {
            throw new IllegalArgumentException("Text format only supports a single string column");
        }
    }

    /** TEXT {@link FormatReaderFactory} implementation. */
    private static class TextReaderFactory implements FormatReaderFactory {

        private final RowType projectedRowType;
        private final TextOptions options;

        public TextReaderFactory(RowType projectedRowType, TextOptions options) {
            this.projectedRowType = projectedRowType;
            this.options = options;
        }

        @Override
        public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
            return new TextFileReader(
                    context.fileIO(), context.filePath(), projectedRowType, options, 0, null);
        }

        @Override
        public FileRecordReader<InternalRow> createReader(Context context, long offset, long length)
                throws IOException {
            return new TextFileReader(
                    context.fileIO(),
                    context.filePath(),
                    projectedRowType,
                    options,
                    offset,
                    length);
        }
    }

    /** A {@link FormatWriterFactory} to write {@link InternalRow} to TEXT. */
    private static class TextWriterFactory implements FormatWriterFactory {

        private final RowType rowType;
        private final TextOptions options;

        public TextWriterFactory(RowType rowType, TextOptions options) {
            this.rowType = rowType;
            this.options = options;
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression)
                throws IOException {
            return new TextFormatWriter(
                    new CloseShieldOutputStream(out), rowType, options, compression);
        }
    }
}
