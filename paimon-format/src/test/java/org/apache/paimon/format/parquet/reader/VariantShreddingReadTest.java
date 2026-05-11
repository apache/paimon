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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.VariantMetadataUtils;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.format.parquet.ParquetFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for variant shredding read. */
public class VariantShreddingReadTest {

    @TempDir java.nio.file.Path tempPath;

    protected FileIO fileIO;
    protected Path file;
    protected Path parent;

    @BeforeEach
    public void beforeEach() {
        this.fileIO = LocalFileIO.create();
        this.parent = new Path(tempPath.toUri());
        this.file = new Path(new Path(tempPath.toUri()), UUID.randomUUID() + ".parquet");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "null",
                "{\"type\":\"ROW\",\"fields\":[{\"name\":\"v\",\"type\":{\"type\":\"ROW\",\"fields\":[{\"name\":\"age\",\"type\":\"INT\"},{\"name\":\"city\",\"type\":\"STRING\"}]}}]}",
                "{\"type\":\"ROW\",\"fields\":[{\"name\":\"v\",\"type\":{\"type\":\"ROW\",\"fields\":[{\"name\":\"weather\",\"type\":\"INT\"}]}}]}",
                "{\"type\":\"ROW\",\"fields\":[{\"name\":\"v\",\"type\":{\"type\":\"ROW\",\"fields\":[{\"name\":\"other\",\"type\":\"STRING\"}]}}]}"
            })
    public void testReadShreddedVariant(String shreddingSchema) throws Exception {
        Options options = new Options();
        if (!shreddingSchema.equals("null")) {
            options.set("parquet.variant.shreddingSchema", shreddingSchema);
        }
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(
                factory,
                GenericRow.of(GenericVariant.fromJson("{\"age\":35,\"city\":\"Chicago\"}")),
                GenericRow.of(GenericVariant.fromJson("{\"age\":25,\"other\":\"Hello\"}")));

        // read without pruning
        List<InternalRow> result1 = readRows(format, writeType);
        assertThat(result1.get(0).getVariant(0).toJson())
                .isEqualTo("{\"age\":35,\"city\":\"Chicago\"}");
        assertThat(result1.get(1).getVariant(0).toJson())
                .isEqualTo("{\"age\":25,\"other\":\"Hello\"}");

        // read with typed col only
        RowType variantRowType2 =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.age")
                        .build();
        RowType readType2 = DataTypes.ROW(DataTypes.FIELD(0, "v", variantRowType2));
        List<InternalRow> result2 = readRows(format, readType2);
        assertThat(result2.get(0).equals(GenericRow.of(GenericRow.of(35)))).isTrue();
        assertThat(result2.get(1).equals(GenericRow.of(GenericRow.of(25)))).isTrue();

        // read with typed col and untyped col
        RowType variantRowType3 =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.age")
                        .field(DataTypes.STRING(), "$.other")
                        .build();
        RowType readType3 = DataTypes.ROW(DataTypes.FIELD(0, "v", variantRowType3));
        List<InternalRow> result3 = readRows(format, readType3);
        assertThat(result3.get(0).equals(GenericRow.of(GenericRow.of(35, null)))).isTrue();
        assertThat(
                        result3.get(1)
                                .equals(
                                        GenericRow.of(
                                                GenericRow.of(
                                                        25, BinaryString.fromString("Hello")))))
                .isTrue();

        // read unexisted col
        RowType variantRowType4 =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.unexist_col")
                        .build();
        RowType readType4 = DataTypes.ROW(DataTypes.FIELD(0, "v", variantRowType4));
        List<InternalRow> result4 = readRows(format, readType4);
        assertThat(result4.get(0).getRow(0, 1).isNullAt(0)).isTrue();
        assertThat(result4.get(1).getRow(0, 1).isNullAt(0)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "null",
                "{\"type\":\"ROW\",\"fields\":[{\"name\":\"data\",\"type\":{\"type\":\"ROW\",\"fields\":[{\"name\":\"v\",\"type\":{\"type\":\"ROW\",\"fields\":[{\"name\":\"score\",\"type\":\"INT\"}]}}]}}]}"
            })
    public void testReadNestedVariantInStruct(String shreddingSchema) throws Exception {
        Options options = new Options();
        if (!shreddingSchema.equals("null")) {
            options.set("parquet.variant.shreddingSchema", shreddingSchema);
        }
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        RowType writeType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "data",
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "name", DataTypes.STRING()),
                                        DataTypes.FIELD(1, "v", DataTypes.VARIANT()))));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(
                factory,
                GenericRow.of(
                        GenericRow.of(
                                BinaryString.fromString("Alice"),
                                GenericVariant.fromJson("{\"score\":95,\"grade\":\"A\"}"))),
                GenericRow.of(
                        GenericRow.of(
                                BinaryString.fromString("Bob"),
                                GenericVariant.fromJson("{\"score\":88,\"grade\":\"B\"}"))));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.get(0).getRow(0, 2).getString(0).toString()).isEqualTo("Alice");
        assertThat(result.get(0).getRow(0, 2).getVariant(1).toJson())
                .isEqualTo("{\"grade\":\"A\",\"score\":95}");
        assertThat(result.get(1).getRow(0, 2).getString(0).toString()).isEqualTo("Bob");
        assertThat(result.get(1).getRow(0, 2).getVariant(1).toJson())
                .isEqualTo("{\"grade\":\"B\",\"score\":88}");

        // read with typed col only
        RowType variantRowType2 =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.score")
                        .build();
        RowType readType2 =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "data",
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "name", DataTypes.STRING()),
                                        DataTypes.FIELD(1, "v", variantRowType2))));
        List<InternalRow> result2 = readRows(format, readType2);
        assertThat(result2.get(0).getRow(0, 2).getString(0).toString()).isEqualTo("Alice");
        assertThat(result2.get(0).getRow(0, 2).getRow(1, 1).getInt(0)).isEqualTo(95);
        assertThat(result2.get(1).getRow(0, 2).getString(0).toString()).isEqualTo("Bob");
        assertThat(result2.get(1).getRow(0, 2).getRow(1, 1).getInt(0)).isEqualTo(88);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "null",
                "{\"type\":\"ROW\",\"fields\":[{\"name\":\"arr\",\"type\":{\"type\":\"ARRAY\",\"element\":{\"type\":\"ROW\",\"fields\":[{\"name\":\"x\",\"type\":\"INT\"}]}}}]}"
            })
    public void testReadNestedVariantInArray(String shreddingSchema) throws Exception {
        Options options = new Options();
        if (!shreddingSchema.equals("null")) {
            options.set("parquet.variant.shreddingSchema", shreddingSchema);
        }
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        RowType writeType =
                DataTypes.ROW(DataTypes.FIELD(0, "arr", DataTypes.ARRAY(DataTypes.VARIANT())));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(
                factory,
                GenericRow.of(
                        new GenericArray(
                                new Object[] {
                                    GenericVariant.fromJson("{\"x\":1,\"y\":2}"),
                                    GenericVariant.fromJson("{\"x\":3,\"y\":4}")
                                })),
                GenericRow.of(
                        new GenericArray(
                                new Object[] {GenericVariant.fromJson("{\"x\":5,\"y\":6}")})));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.get(0).getArray(0).size()).isEqualTo(2);
        assertThat(result.get(0).getArray(0).getVariant(0).toJson()).isEqualTo("{\"x\":1,\"y\":2}");
        assertThat(result.get(0).getArray(0).getVariant(1).toJson()).isEqualTo("{\"x\":3,\"y\":4}");
        assertThat(result.get(1).getArray(0).size()).isEqualTo(1);
        assertThat(result.get(1).getArray(0).getVariant(0).toJson()).isEqualTo("{\"x\":5,\"y\":6}");

        // read with typed col only
        RowType variantRowType2 =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.x")
                        .build();
        RowType readType2 =
                DataTypes.ROW(DataTypes.FIELD(0, "arr", DataTypes.ARRAY(variantRowType2)));
        List<InternalRow> result2 = readRows(format, readType2);
        assertThat(result2.get(0).getArray(0).size()).isEqualTo(2);
        assertThat(result2.get(0).getArray(0).getRow(0, 1).getInt(0)).isEqualTo(1);
        assertThat(result2.get(0).getArray(0).getRow(1, 1).getInt(0)).isEqualTo(3);
        assertThat(result2.get(1).getArray(0).size()).isEqualTo(1);
        assertThat(result2.get(1).getArray(0).getRow(0, 1).getInt(0)).isEqualTo(5);
    }

    protected List<InternalRow> readRows(ParquetFileFormat format, RowType rowType)
            throws IOException {
        List<InternalRow> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            InternalRowSerializer serializer = new InternalRowSerializer(rowType);
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        }
        return result;
    }

    protected void writeRows(FormatWriterFactory factory, InternalRow... rows) throws IOException {
        FormatWriter writer;
        PositionOutputStream out = null;
        if (factory instanceof SupportsDirectWrite) {
            writer = ((SupportsDirectWrite) factory).create(fileIO, file, "zstd");
        } else {
            out = fileIO.newOutputStream(file, false);
            writer = factory.create(out, "zstd");
        }
        for (InternalRow row : rows) {
            writer.addElement(row);
        }
        writer.close();
        if (out != null) {
            out.close();
        }
    }
}
