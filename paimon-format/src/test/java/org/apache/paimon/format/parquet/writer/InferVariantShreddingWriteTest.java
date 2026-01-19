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

package org.apache.paimon.format.parquet.writer;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
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
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.format.parquet.VariantUtils;
import org.apache.paimon.format.variant.InferVariantShreddingWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.data.variant.PaimonShreddingUtils.variantShreddingSchema;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link InferVariantShreddingWriter}. */
public class InferVariantShreddingWriteTest {

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

    public Options defaultOptions() {
        Options options = new Options();
        options.set(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA.key(), "true");
        return options;
    }

    @Test
    public void testInferSchemaWithSimpleObject() throws Exception {
        ParquetFileFormat format = createFormat();
        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(
                factory,
                GenericRow.of(GenericVariant.fromJson("{\"age\":30,\"name\":\"Alice\"}")),
                GenericRow.of(GenericVariant.fromJson("{\"age\":25,\"name\":\"Bob\"}")),
                GenericRow.of(GenericVariant.fromJson("{\"age\":35,\"name\":\"Charlie\"}")));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.get(0).getVariant(0).toJson())
                .isEqualTo("{\"age\":30,\"name\":\"Alice\"}");
        assertThat(result.get(1).getVariant(0).toJson()).isEqualTo("{\"age\":25,\"name\":\"Bob\"}");
        assertThat(result.get(2).getVariant(0).toJson())
                .isEqualTo("{\"age\":35,\"name\":\"Charlie\"}");

        RowType expectShreddedType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"age", "name"});
        verifyShreddingSchema(expectShreddedType);

        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.age")
                        .build();
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "v", variantRowType));
        List<InternalRow> result2 = readRows(format, readType);
        assertThat(result2.get(0)).isEqualTo(GenericRow.of(GenericRow.of(30)));
        assertThat(result2.get(1)).isEqualTo(GenericRow.of(GenericRow.of(25)));
        assertThat(result2.get(2)).isEqualTo(GenericRow.of(GenericRow.of(35)));
    }

    @Test
    public void testInferSchemaWithArray() throws Exception {
        ParquetFileFormat format = createFormat();
        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(
                factory,
                GenericRow.of(GenericVariant.fromJson("{\"numbers\":[1,2,3]}")),
                GenericRow.of(GenericVariant.fromJson("{\"numbers\":[4,5,6]}")));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.get(0).getVariant(0).toJson()).isEqualTo("{\"numbers\":[1,2,3]}");
        assertThat(result.get(1).getVariant(0).toJson()).isEqualTo("{\"numbers\":[4,5,6]}");

        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.ARRAY(DataTypes.BIGINT()), "$.numbers")
                        .build();
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "v", variantRowType));
        List<InternalRow> result2 = readRows(format, readType);
        // Verify the nested row structure and array content
        InternalRow row1 = result2.get(0).getRow(0, 1);
        assertThat(row1.getArray(0).size()).isEqualTo(3);
        assertThat(row1.getArray(0).getLong(0)).isEqualTo(1L);
        assertThat(row1.getArray(0).getLong(1)).isEqualTo(2L);
        assertThat(row1.getArray(0).getLong(2)).isEqualTo(3L);

        InternalRow row2 = result2.get(1).getRow(0, 1);
        assertThat(row2.getArray(0).size()).isEqualTo(3);
        assertThat(row2.getArray(0).getLong(0)).isEqualTo(4L);
        assertThat(row2.getArray(0).getLong(1)).isEqualTo(5L);
        assertThat(row2.getArray(0).getLong(2)).isEqualTo(6L);

        RowType expectShreddedType =
                RowType.of(
                        new DataType[] {DataTypes.ARRAY(DataTypes.BIGINT())},
                        new String[] {"numbers"});
        verifyShreddingSchema(expectShreddedType);
    }

    @Test
    public void testInferSchemaWithMixedTypes() throws Exception {
        ParquetFileFormat format = createFormat();
        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(
                factory,
                GenericRow.of(
                        GenericVariant.fromJson(
                                "{\"str\":\"hello\",\"num\":42,\"bool\":true,\"dec\":3.14}")),
                GenericRow.of(
                        GenericVariant.fromJson(
                                "{\"str\":\"world\",\"num\":100,\"bool\":false,\"dec\":2.71}")));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.get(0).getVariant(0).toJson())
                .isEqualTo("{\"bool\":true,\"dec\":3.14,\"num\":42,\"str\":\"hello\"}");
        assertThat(result.get(1).getVariant(0).toJson())
                .isEqualTo("{\"bool\":false,\"dec\":2.71,\"num\":100,\"str\":\"world\"}");

        RowType expectShreddedType =
                RowType.of(
                        new DataType[] {
                            DataTypes.BOOLEAN(),
                            DataTypes.DECIMAL(18, 2),
                            DataTypes.BIGINT(),
                            DataTypes.STRING()
                        },
                        new String[] {"bool", "dec", "num", "str"});
        verifyShreddingSchema(expectShreddedType);
    }

    @Test
    public void testInferSchemaWithNullValues() throws Exception {
        ParquetFileFormat format = createFormat();
        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(
                factory,
                GenericRow.of(GenericVariant.fromJson("{\"a\":1,\"b\":null}")),
                GenericRow.of(GenericVariant.fromJson("{\"a\":2,\"b\":3}")));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.get(0).getVariant(0).toJson()).isEqualTo("{\"a\":1,\"b\":null}");
        assertThat(result.get(1).getVariant(0).toJson()).isEqualTo("{\"a\":2,\"b\":3}");

        RowType expectShreddedType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.BIGINT()},
                        new String[] {"a", "b"});
        verifyShreddingSchema(expectShreddedType);
    }

    @Test
    public void testInferSchemaWithConflictingTypes() throws Exception {
        ParquetFileFormat format = createFormat();
        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(
                factory,
                GenericRow.of(GenericVariant.fromJson("{\"field\":\"text\"}")),
                GenericRow.of(GenericVariant.fromJson("{\"field\":123}")),
                GenericRow.of(GenericVariant.fromJson("{\"field\":true}")));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.get(0).getVariant(0).toJson()).isEqualTo("{\"field\":\"text\"}");
        assertThat(result.get(1).getVariant(0).toJson()).isEqualTo("{\"field\":123}");
        assertThat(result.get(2).getVariant(0).toJson()).isEqualTo("{\"field\":true}");

        // When types conflict, the field should be inferred as VARIANT type
        RowType expectShreddedType =
                RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"field"});
        verifyShreddingSchema(expectShreddedType);

        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.VARIANT(), "$.field")
                        .build();
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "v", variantRowType));
        List<InternalRow> result2 = readRows(format, readType);
        assertThat(result2.get(0).getRow(0, 1).getVariant(0).toJson()).isEqualTo("\"text\"");
        assertThat(result2.get(1).getRow(0, 1).getVariant(0).toJson()).isEqualTo("123");
        assertThat(result2.get(2).getRow(0, 1).getVariant(0).toJson()).isEqualTo("true");
    }

    @Test
    public void testInferSchemaWithDeepNesting() throws Exception {
        ParquetFileFormat format = createFormat();
        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        String deepJson = "{\"level1\":{\"level2\":{\"level3\":{\"value\":42}}}}";
        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(factory, GenericRow.of(GenericVariant.fromJson(deepJson)));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.get(0).getVariant(0).toJson()).isEqualTo(deepJson);

        // Deep nesting: level1 -> level2 -> level3 -> value
        RowType level3Type =
                RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"value"});
        RowType level2Type = RowType.of(new DataType[] {level3Type}, new String[] {"level3"});
        RowType level1Type = RowType.of(new DataType[] {level2Type}, new String[] {"level2"});
        RowType expectShreddedType =
                RowType.of(new DataType[] {level1Type}, new String[] {"level1"});
        verifyShreddingSchema(expectShreddedType);
    }

    @Test
    public void testMultipleVariantFields() throws Exception {
        ParquetFileFormat format = createFormat();
        RowType writeType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "v1", DataTypes.VARIANT()),
                        DataTypes.FIELD(1, "v2", DataTypes.VARIANT()),
                        DataTypes.FIELD(2, "id", DataTypes.INT()));

        GenericVariant variant1 = GenericVariant.fromJson("{\"name\":\"Alice\"}");
        GenericVariant variant2 = GenericVariant.fromJson("{\"age\":30}");

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(
                factory,
                GenericRow.of(variant1, variant2, 1),
                GenericRow.of(variant1, variant2, 2));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.get(0).getVariant(0).toJson()).isEqualTo("{\"name\":\"Alice\"}");
        assertThat(result.get(0).getVariant(1).toJson()).isEqualTo("{\"age\":30}");
        assertThat(result.get(0).getInt(2)).isEqualTo(1);
        assertThat(result.get(1).getVariant(0).toJson()).isEqualTo("{\"name\":\"Alice\"}");
        assertThat(result.get(1).getVariant(1).toJson()).isEqualTo("{\"age\":30}");
        assertThat(result.get(1).getInt(2)).isEqualTo(2);

        // v1 has "name" field
        RowType expectShreddedType1 =
                RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"name"});
        // v2 has "age" field
        RowType expectShreddedType2 =
                RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"age"});
        verifyShreddingSchema(expectShreddedType1, expectShreddedType2);
    }

    @Test
    public void testInferSchemaWithAllPrimitiveTypes() throws Exception {
        ParquetFileFormat format = createFormat();
        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        String json =
                "{\"string\":\"test\",\"long\":123456789,\"double\":3.14159,\"boolean\":true,\"null\":null}";
        // Fields are sorted alphabetically in variant
        String expectedJson =
                "{\"boolean\":true,\"double\":3.14159,\"long\":123456789,\"null\":null,\"string\":\"test\"}";
        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(factory, GenericRow.of(GenericVariant.fromJson(json)));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.get(0).getVariant(0).toJson()).isEqualTo(expectedJson);

        RowType expectShreddedType =
                RowType.of(
                        new DataType[] {
                            DataTypes.BOOLEAN(),
                            DataTypes.DECIMAL(18, 5),
                            DataTypes.BIGINT(),
                            DataTypes.VARIANT(),
                            DataTypes.STRING()
                        },
                        new String[] {"boolean", "double", "long", "null", "string"});
        verifyShreddingSchema(expectShreddedType);
    }

    @Test
    public void testAllNullRecords() throws Exception {
        ParquetFileFormat format = createFormat();
        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        GenericRow[] rows = new GenericRow[10];
        for (int i = 0; i < 10; i++) {
            rows[i] = GenericRow.of((GenericVariant) null);
        }
        writeRows(factory, rows);

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.size()).isEqualTo(10);
        for (InternalRow row : result) {
            assertThat(row.isNullAt(0)).isTrue();
        }
    }

    @Test
    public void testMixedNullAndValidRecords() throws Exception {
        ParquetFileFormat format = createFormat();
        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        List<GenericRow> rows = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            if (i % 3 == 0) {
                rows.add(GenericRow.of((GenericVariant) null));
            } else {
                rows.add(
                        GenericRow.of(
                                GenericVariant.fromJson(
                                        String.format("{\"id\":%d,\"value\":\"data%d\"}", i, i))));
            }
        }

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(factory, rows.toArray(new InternalRow[0]));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.size()).isEqualTo(20);
        for (int i = 0; i < 20; i++) {
            if (i % 3 == 0) {
                assertThat(result.get(i).isNullAt(0)).isTrue();
            } else {
                assertThat(result.get(i).getVariant(0).toJson())
                        .isEqualTo(String.format("{\"id\":%d,\"value\":\"data%d\"}", i, i));
            }
        }

        RowType expectShreddedType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"id", "value"});
        verifyShreddingSchema(expectShreddedType);

        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.BIGINT(), "$.id")
                        .build();
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "v", variantRowType));
        List<InternalRow> result2 = readRows(format, readType);
        assertThat(result2.size()).isEqualTo(20);
        for (int i = 0; i < 20; i++) {
            if (i % 3 == 0) {
                assertThat(result2.get(i).isNullAt(0)).isTrue();
            } else {
                assertThat(result2.get(i)).isEqualTo(GenericRow.of(GenericRow.of((long) i)));
            }
        }
    }

    @Test
    public void testMaxInferBufferRowBoundary() throws Exception {
        // Test with buffer size = 2
        // First 2 rows have integer values, 3rd row has string value
        // Schema will be inferred from first 2 rows (buffer size), so field becomes BIGINT
        Options customOptions = defaultOptions();
        customOptions.set(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW, 2);
        ParquetFileFormat format = createFormat(customOptions);

        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        GenericRow[] rows = new GenericRow[3];
        rows[0] = GenericRow.of(GenericVariant.fromJson("{\"value\":100}"));
        rows[1] = GenericRow.of(GenericVariant.fromJson("{\"value\":200}"));
        rows[2] = GenericRow.of(GenericVariant.fromJson("{\"value\":\"text\"}"));
        writeRows(factory, rows);

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.size()).isEqualTo(3);
        assertThat(result.get(0).getVariant(0).toJson()).isEqualTo("{\"value\":100}");
        assertThat(result.get(1).getVariant(0).toJson()).isEqualTo("{\"value\":200}");
        assertThat(result.get(2).getVariant(0).toJson()).isEqualTo("{\"value\":\"text\"}");

        // Schema should be inferred as BIGINT (based on first 2 rows in buffer)
        RowType expectShreddedType =
                RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"value"});
        verifyShreddingSchema(expectShreddedType);
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.STRING(), "$.value")
                        .build();
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "v", variantRowType));
        List<InternalRow> result2 = readRows(format, readType);
        assertThat(result2.size()).isEqualTo(3);
        assertThat(result2.get(0))
                .isEqualTo(GenericRow.of(GenericRow.of(BinaryString.fromString("100"))));
        assertThat(result2.get(1))
                .isEqualTo(GenericRow.of(GenericRow.of(BinaryString.fromString("200"))));
        assertThat(result2.get(2))
                .isEqualTo(GenericRow.of(GenericRow.of(BinaryString.fromString("text"))));
    }

    @Test
    public void testMaxInferBufferRowExactMatch() throws Exception {
        // Test with buffer size = 5, write exactly 5 rows
        Options customOptions = defaultOptions();
        customOptions.set(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW, 5);
        ParquetFileFormat format = createFormat(customOptions);

        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        GenericRow[] rows = new GenericRow[5];
        for (int i = 0; i < 5; i++) {
            rows[i] =
                    GenericRow.of(
                            GenericVariant.fromJson(
                                    "{\"id\":" + i + ",\"name\":\"user" + i + "\"}"));
        }
        writeRows(factory, rows);

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.size()).isEqualTo(5);

        RowType expectShreddedType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"id", "name"});
        verifyShreddingSchema(expectShreddedType);
    }

    @Test
    public void testMaxInferBufferRowBelowThreshold() throws Exception {
        // Test with buffer size = 10, write only 3 rows
        // Schema inferred at close time
        Options customOptions = defaultOptions();
        customOptions.set(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW, 10);
        ParquetFileFormat format = createFormat(customOptions);

        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        writeRows(
                factory,
                GenericRow.of(GenericVariant.fromJson("{\"id\":1}")),
                GenericRow.of(GenericVariant.fromJson("{\"id\":2}")),
                GenericRow.of(GenericVariant.fromJson("{\"id\":3}")));

        List<InternalRow> result = readRows(format, writeType);
        assertThat(result.size()).isEqualTo(3);

        RowType expectShreddedType =
                RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"id"});
        verifyShreddingSchema(expectShreddedType);
    }

    protected ParquetFileFormat createFormat() {
        return createFormat(defaultOptions());
    }

    protected ParquetFileFormat createFormat(Options options) {
        return new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
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

    protected void verifyShreddingSchema(RowType... expectShreddedTypes) throws IOException {
        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(fileIO, file, fileIO.getFileSize(file))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            for (int i = 0; i < expectShreddedTypes.length; i++) {
                assertThat(VariantUtils.variantFileType(schema.getType(i)))
                        .isEqualTo(variantShreddingSchema(expectShreddedTypes[i]));
            }
        }
    }
}
