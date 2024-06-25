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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for avro file format. */
public class AvroFileFormatTest {

    @TempDir java.nio.file.Path tempPath;

    private static AvroFileFormat fileFormat;

    @BeforeAll
    public static void before() {
        fileFormat = new AvroFileFormat(new FormatContext(new Options(), 1024));
    }

    @Test
    public void testSupportedDataTypes() {
        ArrayList<DataField> dataFields = new ArrayList<>();
        int index = 0;
        dataFields.add(new DataField(index++, "boolean_type", DataTypes.BOOLEAN()));
        dataFields.add(new DataField(index++, "tinyint_type", DataTypes.TINYINT()));
        dataFields.add(new DataField(index++, "smallint_type", DataTypes.SMALLINT()));
        dataFields.add(new DataField(index++, "int_type", DataTypes.INT()));
        dataFields.add(new DataField(index++, "bigint_type", DataTypes.BIGINT()));
        dataFields.add(new DataField(index++, "float_type", DataTypes.FLOAT()));
        dataFields.add(new DataField(index++, "double_type", DataTypes.DOUBLE()));
        dataFields.add(new DataField(index++, "char_type", DataTypes.CHAR(10)));
        dataFields.add(new DataField(index++, "varchar_type", DataTypes.VARCHAR(20)));
        dataFields.add(new DataField(index++, "binary_type", DataTypes.BINARY(20)));
        dataFields.add(new DataField(index++, "varbinary_type", DataTypes.VARBINARY(20)));
        dataFields.add(new DataField(index++, "timestamp_type", DataTypes.TIMESTAMP(3)));
        dataFields.add(new DataField(index++, "date_type", DataTypes.DATE()));
        dataFields.add(new DataField(index++, "decimal_type", DataTypes.DECIMAL(10, 3)));
        dataFields.add(
                new DataField(
                        index++,
                        "local_timestamp_type",
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)));

        RowType rowType = new RowType(dataFields);
        fileFormat.validateDataFields(rowType);
    }

    @Test
    public void testSupportedComplexDataTypes() {
        ArrayList<DataField> dataFields = new ArrayList<>();
        int index = 0;
        dataFields.add(
                new DataField(
                        index++,
                        "map_type",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        dataFields.add(new DataField(index++, "array_type", DataTypes.ARRAY(DataTypes.STRING())));
        dataFields.add(
                new DataField(
                        index++,
                        "row_type",
                        DataTypes.ROW(DataTypes.STRING(), DataTypes.BIGINT())));

        RowType rowType = new RowType(dataFields);
        fileFormat.validateDataFields(rowType);
    }

    @Test
    void testReadRowPosition() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull());
        FileFormat format = new AvroFileFormat(new FormatContext(new Options(), 1024));

        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());

        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
            for (int i = 0; i < 1000000; i++) {
                writer.addElement(GenericRow.of(i));
            }
            writer.flush();
            writer.finish();
        }

        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file))); ) {
            reader.forEachRemainingWithPosition(
                    (rowPosition, row) -> assertThat(row.getInt(0) == rowPosition).isTrue());
        }
    }
}
