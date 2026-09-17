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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.BufferedBinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DirectBinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for avro file format. */
public class AvroFileFormatTest {

    @TempDir java.nio.file.Path tempPath;

    private static AvroFileFormat fileFormat;

    @BeforeAll
    public static void before() {
        fileFormat = new AvroFileFormat(new FormatContext(new Options(), 1024, 1024));
    }

    @ParameterizedTest
    @CsvSource({
        ", 64000, null",
        ", 64000, deflate",
        "1 kb, 1024, null",
        "1 kb, 1024, deflate",
        "4 kb, 4096, null",
        "4 kb, 4096, deflate",
        "128 kb, 131072, null",
        "128 kb, 131072, deflate"
    })
    void testFileBlockSize(String blockSize, int expectedBlockSize, String compression)
            throws IOException {
        Options options = new Options();
        if (blockSize != null) {
            options.setString("file.block-size", blockSize);
        }
        FileFormat format = FileFormat.fromIdentifier("avro", options);
        assertFileBlockSize(format, expectedBlockSize, compression);
    }

    @ParameterizedTest
    @ValueSource(strings = {"avro", "parquet", "orc"})
    void testManifestIgnoresDataFileBlockSize(String identifier) throws IOException {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, identifier);
        options.setString("file.block-size", "1 kb");

        FileFormat manifestFormat = FileFormat.manifestFormat(new CoreOptions(options));
        assertFileBlockSize(manifestFormat, 64000, "null");

        assertThat(options.get(CoreOptions.FILE_BLOCK_SIZE).getBytes()).isEqualTo(1024);
        assertFileBlockSize(FileFormat.fromIdentifier("avro", options), 1024, "null");
    }

    @Test
    void testManifestIgnoresLargeDataFileBlockSize() throws IOException {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, "parquet");
        options.setString("file.block-size", "256 mb");

        assertFileBlockSize(FileFormat.manifestFormat(new CoreOptions(options)), 64000, "null");
    }

    private void assertFileBlockSize(FileFormat format, int expectedBlockSize, String compression)
            throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull()).notNull();
        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
        int numRecords = 300_000;

        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer =
                        format.createWriterFactory(rowType).create(out, compression)) {
            // Each record is encoded as one byte, so record counts also give uncompressed sizes.
            for (int i = 0; i < numRecords; i++) {
                writer.addElement(GenericRow.of(0));
            }
        }

        long records = 0;
        try (AvroBlockReader reader = new AvroBlockReader(fileIO.newInputStream(file))) {
            while (reader.hasNextBlock()) {
                AvroRawBlock block = reader.nextBorrowedRawBlock();
                long expectedRecords = Math.min(expectedBlockSize, numRecords - records);
                assertThat(block.recordCount()).isEqualTo(expectedRecords);
                assertThat(block.decompress(null).remaining()).isEqualTo((int) expectedRecords);
                records += block.recordCount();
            }
        }
        assertThat(records).isEqualTo(numRecords);
    }

    @ParameterizedTest
    @ValueSource(longs = {2147483648L, 4294968320L, Long.MAX_VALUE})
    void testFileBlockSizeOverflow(long blockSize) throws IOException {
        Options options = new Options();
        options.setString("file.block-size", Long.toString(blockSize));
        FileFormat format = FileFormat.fromIdentifier("avro", options);
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull()).notNull();
        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());

        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            assertThatThrownBy(
                            () -> {
                                try (FormatWriter writer =
                                        format.createWriterFactory(rowType).create(out, "null")) {
                                    writer.addElement(GenericRow.of(0));
                                }
                            })
                    .isInstanceOf(ArithmeticException.class)
                    .hasMessage("integer overflow");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "deflate", "snappy", "zstd"})
    void testCloseFlushesBufferedRecords(String compression) throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull()).notNull();
        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());

        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer =
                        fileFormat.createWriterFactory(rowType).create(out, compression)) {
            assertThat(writer)
                    .extracting("writer")
                    .extracting("bufOut")
                    .isInstanceOf(BufferedBinaryEncoder.class);
            // Leave a partial record-encoder buffer as well as a partial Avro block.
            writer.addElement(GenericRow.of(42));
        }

        List<Integer> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                fileFormat
                        .createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            reader.forEachRemaining(row -> result.add(row.getInt(0)));
        }
        assertThat(result).containsExactly(42);
    }

    @ParameterizedTest
    @ValueSource(strings = {"avro", "parquet", "orc"})
    void testManifestWriterUsesDirectEncoder(String dataFileFormat) throws IOException {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, dataFileFormat);
        FileFormat manifestFormat = FileFormat.manifestFormat(new CoreOptions(options));
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull()).notNull();
        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());

        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer =
                        manifestFormat.createWriterFactory(rowType).create(out, "null")) {
            assertThat(writer)
                    .extracting("writer")
                    .extracting("bufOut")
                    .isInstanceOf(DirectBinaryEncoder.class);
            writer.addElement(GenericRow.of(42));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "deflate", "snappy", "zstd"})
    void testRecordsAroundRawBlockCopy(String compression) throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull()).notNull();
        LocalFileIO fileIO = LocalFileIO.create();
        Path source = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
        Path target = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());

        try (PositionOutputStream out = fileIO.newOutputStream(source, false);
                FormatWriter writer =
                        fileFormat.createWriterFactory(rowType).create(out, "deflate")) {
            writer.addElement(GenericRow.of(2));
            writer.addElement(GenericRow.of(3));
        }

        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(encoded, null);
        encoder.writeInt(1);
        encoder.flush();

        try (AvroBlockReader reader = new AvroBlockReader(fileIO.newInputStream(source));
                PositionOutputStream out = fileIO.newOutputStream(target, false);
                AvroBlockWriter writer = fileFormat.createBlockWriter(out, rowType, compression)) {
            assertThat(writer)
                    .extracting("writer")
                    .extracting("bufOut")
                    .isInstanceOf(DirectBinaryEncoder.class);
            writer.addElement(GenericRow.of(0));
            ByteBuffer record = ByteBuffer.wrap(encoded.toByteArray());
            writer.addEncoded(record);
            assertThat(record.position()).isZero();
            assertThat(record.remaining()).isEqualTo(encoded.size());
            // Copying a raw block must first flush pending rows and encoded records.
            writer.addEncodedBlock(reader.nextBorrowedRawBlock());
            writer.addElement(GenericRow.of(4));
        }

        List<Integer> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                fileFormat
                        .createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, target, fileIO.getFileSize(target), null, null))) {
            reader.forEachRemaining(row -> result.add(row.getInt(0)));
        }
        assertThat(result).containsExactly(0, 1, 2, 3, 4);
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
        FileFormat format = new AvroFileFormat(new FormatContext(new Options(), 1024, 1024));

        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());

        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
            for (int i = 0; i < 1000000; i++) {
                writer.addElement(GenericRow.of(i));
            }
            writer.close();
        }

        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            reader.forEachRemainingWithPosition(
                    (rowPosition, row) -> assertThat(row.getInt(0) == rowPosition).isTrue());
        }
    }

    @Test
    void testReadBlocksFromEmptyFile() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull()).notNull();
        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());

        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            fileFormat.createWriterFactory(rowType).create(out, "zstd").close();
        }

        try (AvroBlockReader reader = new AvroBlockReader(fileIO.newInputStream(file))) {
            assertThat(reader.hasNextBlock()).isFalse();
            assertThatThrownBy(reader::nextBorrowedRawBlock)
                    .isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void testReadBorrowedRawBlocks() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull()).notNull();
        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
        int numRecords = 100_000;

        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = fileFormat.createWriterFactory(rowType).create(out, "zstd");
            for (int i = 0; i < numRecords; i++) {
                writer.addElement(GenericRow.of(i));
            }
            writer.close();
        }

        long records = 0;
        int blocks = 0;
        AvroRawBlock previous = null;
        try (AvroBlockReader reader = new AvroBlockReader(fileIO.newInputStream(file))) {
            while (reader.hasNextBlock()) {
                AvroRawBlock block = reader.nextBorrowedRawBlock();
                if (previous != null) {
                    assertThat(block).isSameAs(previous);
                }
                previous = block;
                records += block.recordCount();
                blocks++;
            }
            assertThatThrownBy(reader::nextBorrowedRawBlock)
                    .isInstanceOf(NoSuchElementException.class);
        }

        assertThat(blocks).isGreaterThan(1);
        assertThat(records).isEqualTo(numRecords);
    }

    @Test
    void testRawBlockCompatibilityUsesBinaryLayoutAndFieldIdentity() {
        Schema expected =
                SchemaBuilder.record("Expected")
                        .fields()
                        .requiredInt("id")
                        .name("nested")
                        .type(
                                SchemaBuilder.record("ExpectedNested")
                                        .fields()
                                        .requiredLong("value")
                                        .endRecord())
                        .noDefault()
                        .endRecord();
        Schema renamedRecords =
                SchemaBuilder.record("Actual")
                        .fields()
                        .requiredInt("id")
                        .name("nested")
                        .type(
                                SchemaBuilder.record("ActualNested")
                                        .fields()
                                        .requiredLong("value")
                                        .endRecord())
                        .noDefault()
                        .endRecord();
        Schema renamedField =
                SchemaBuilder.record("Actual")
                        .fields()
                        .requiredInt("other_id")
                        .name("nested")
                        .type(renamedRecords.getField("nested").schema())
                        .noDefault()
                        .endRecord();
        Schema missingField = SchemaBuilder.record("Actual").fields().requiredInt("id").endRecord();

        assertThat(AvroBlockReader.hasSameBinaryLayout(expected, renamedRecords)).isTrue();
        assertThat(AvroBlockReader.hasSameBinaryLayout(expected, renamedField)).isFalse();
        assertThat(AvroBlockReader.hasSameBinaryLayout(expected, missingField)).isFalse();
    }

    @Test
    void testRowReaderProjectsIntoReusedRow() throws IOException {
        Schema writerSchema =
                SchemaBuilder.record("record")
                        .fields()
                        .requiredInt("first")
                        .requiredInt("second")
                        .endRecord();
        RowType projectedType =
                new RowType(
                        false,
                        Arrays.asList(
                                new DataField(0, "second", DataTypes.INT().notNull()),
                                new DataField(1, "missing", DataTypes.INT())));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        encoder.writeInt(10);
        encoder.writeInt(20);
        encoder.flush();

        AvroRowDatumReader reader = new AvroRowDatumReader(projectedType);
        reader.setSchema(writerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(output.toByteArray(), null);
        GenericRow reuse = GenericRow.of(100, 200);
        InternalRow result = reader.read(reuse, decoder);

        assertThat(result).isSameAs(reuse);
        assertThat(result.getInt(0)).isEqualTo(20);
        assertThat(result.isNullAt(1)).isTrue();
        assertThat(decoder.isEnd()).isTrue();
    }

    @Test
    void testReadNumericTypeWidening() throws IOException {
        Schema writerSchema =
                SchemaBuilder.record("record")
                        .fields()
                        .requiredInt("int_to_bigint")
                        .requiredInt("int_to_double")
                        .requiredFloat("float_to_double")
                        .endRecord();
        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());

        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                DataFileWriter<GenericRecord> writer =
                        new DataFileWriter<>(new GenericDatumWriter<>(writerSchema))) {
            writer.create(writerSchema, out);
            GenericRecord record = new GenericData.Record(writerSchema);
            record.put("int_to_bigint", 42);
            record.put("int_to_double", 21);
            record.put("float_to_double", 10.5f);
            writer.append(record);
        }

        RowType tableType =
                RowType.builder()
                        .field("int_to_bigint", DataTypes.BIGINT().notNull())
                        .field("int_to_double", DataTypes.DOUBLE().notNull())
                        .field("float_to_double", DataTypes.DOUBLE().notNull())
                        .build();
        List<Object> values = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                fileFormat
                        .createReaderFactory(tableType, tableType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            reader.forEachRemaining(
                    row -> {
                        values.add(row.getLong(0));
                        values.add(row.getDouble(1));
                        values.add(row.getDouble(2));
                    });
        }

        assertThat(values).containsExactly(42L, 21.0d, 10.5d);
    }

    @Test
    void testReadNullableUnionWithNullSecond() throws IOException {
        Schema nullableInt =
                Schema.createUnion(
                        Arrays.asList(
                                Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL)));
        Schema recordSchema =
                SchemaBuilder.record("record")
                        .fields()
                        .name("skipped")
                        .type(nullableInt)
                        .noDefault()
                        .name("value")
                        .type(nullableInt)
                        .noDefault()
                        .endRecord();
        Schema writerSchema =
                Schema.createUnion(Arrays.asList(recordSchema, Schema.create(Schema.Type.NULL)));
        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());

        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                DataFileWriter<GenericRecord> writer =
                        new DataFileWriter<>(new GenericDatumWriter<>(writerSchema))) {
            writer.create(writerSchema, out);
            GenericRecord value = new GenericData.Record(recordSchema);
            value.put("skipped", 100);
            value.put("value", 42);
            writer.append(value);
            GenericRecord nullValue = new GenericData.Record(recordSchema);
            nullValue.put("skipped", null);
            nullValue.put("value", null);
            writer.append(nullValue);
        }

        RowType tableType =
                RowType.builder()
                        .field("skipped", DataTypes.INT())
                        .field("value", DataTypes.INT())
                        .build();
        RowType projectedType = RowType.builder().field("value", DataTypes.INT()).build();
        List<Integer> values = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                fileFormat
                        .createReaderFactory(tableType, projectedType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            reader.forEachRemaining(row -> values.add(row.isNullAt(0) ? null : row.getInt(0)));
        }

        assertThat(values).containsExactly(42, null);
    }

    @Test
    void testNonNullFieldTypePreservesNullableRecordDecoding() throws IOException {
        Schema fileSchema = SchemaBuilder.record("file").fields().requiredInt("id").endRecord();
        Schema nullSchema = Schema.create(Schema.Type.NULL);
        for (Schema fieldSchema :
                Arrays.asList(
                        fileSchema,
                        Schema.createUnion(Arrays.asList(nullSchema, fileSchema)),
                        Schema.createUnion(Arrays.asList(fileSchema, nullSchema)))) {
            Schema writerSchema =
                    SchemaBuilder.record("manifest")
                            .fields()
                            .name("_FILE")
                            .type(fieldSchema)
                            .noDefault()
                            .endRecord();
            AvroRecordDecoder decoder = new AvroRecordDecoder(writerSchema);
            AvroRecordDecoder.FieldType rawType = decoder.fieldType(0);
            assertThat(decoder.nonNullFieldType(0)).isEqualTo(AvroRecordDecoder.FieldType.RECORD);
            assertThat(decoder.fieldType(0)).isEqualTo(rawType);

            GenericRecord file = new GenericData.Record(fileSchema);
            file.put("id", 42);
            GenericRecord manifest = new GenericData.Record(writerSchema);
            manifest.put("_FILE", file);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            new GenericDatumWriter<GenericRecord>(writerSchema).write(manifest, encoder);
            encoder.flush();
            byte[] bytes = output.toByteArray();
            AvroRecordDecoder.FieldDecoder field =
                    decoder.createFieldDecoder(
                            0, RowType.builder().field("id", DataTypes.INT().notNull()).build());
            decoder.reset(bytes, 0, bytes.length);
            assertThat(decoder.readRecordStart()).isTrue();
            assertThat(((InternalRow) field.read(decoder, null)).getInt(0)).isEqualTo(42);
            assertThat(decoder.isEnd()).isTrue();

            decoder.reset(bytes, 0, bytes.length);
            assertThat(decoder.readRecordStart()).isTrue();
            field.skip(decoder);
            assertThat(decoder.isEnd()).isTrue();
        }
    }

    @Test
    void testNonNullFieldTypeRejectsNonNullableUnions() {
        Schema record = SchemaBuilder.record("file").fields().requiredInt("id").endRecord();
        Schema integer = Schema.create(Schema.Type.INT);
        for (Schema union :
                Arrays.asList(
                        Schema.createUnion(Arrays.asList(record, integer)),
                        Schema.createUnion(
                                Arrays.asList(Schema.create(Schema.Type.NULL), record, integer)))) {
            Schema writerSchema =
                    SchemaBuilder.record("manifest")
                            .fields()
                            .name("_FILE")
                            .type(union)
                            .noDefault()
                            .endRecord();
            AvroRecordDecoder decoder = new AvroRecordDecoder(writerSchema);
            assertThat(decoder.fieldType(0)).isEqualTo(AvroRecordDecoder.FieldType.UNION);
            assertThatThrownBy(() -> decoder.nonNullFieldType(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Only nullable Avro unions are supported");
        }
    }

    @Test
    void testReadsLargeZstdBlock() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("payload", DataTypes.VARBINARY(500_000).notNull())
                        .field("id", DataTypes.INT().notNull())
                        .build();
        AvroFileFormat format = new AvroFileFormat(new FormatContext(new Options(), 1024, 1024));
        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
        byte[] payload = new byte[400_000];
        Arrays.fill(payload, (byte) 7);

        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
            writer.addElement(GenericRow.of(payload, 42));
            writer.close();
        }

        try (AvroBlockReader blockReader = new AvroBlockReader(fileIO.newInputStream(file))) {
            AvroRawBlock block = blockReader.nextBorrowedRawBlock();
            assertThat(block.recordCount()).isEqualTo(1);
            ByteBuffer decoded = block.decompress(null);
            assertThat(decoded.remaining()).isGreaterThan(payload.length);
            assertThat(decoded.get(10)).isEqualTo((byte) 7);
            assertThat(block.decompress(ByteBuffer.allocate(1))).isEqualTo(decoded);
        }
    }

    @Test
    void testGetRealIOException() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull());
        FileFormat format = new AvroFileFormat(new FormatContext(new Options(), 16, 16));

        LocalFileIO localFileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
        try (PositionOutputStream out = localFileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
            ThreadLocalRandom random = ThreadLocalRandom.current();
            // magic number tested by hand
            for (int i = 0; i < 100000; i++) {
                writer.addElement(GenericRow.of(random.nextInt()));
            }
            writer.close();
        }

        FileIO failingFileIO =
                new LocalFileIO() {

                    @Override
                    public SeekableInputStream newInputStream(Path path) throws IOException {
                        return new FailingInputStream(toFile(path));
                    }

                    class FailingInputStream extends LocalFileIO.LocalSeekableInputStream {

                        private int cnt;

                        public FailingInputStream(File file) throws FileNotFoundException {
                            super(file);
                            cnt = 0;
                        }

                        @Override
                        public int read() throws IOException {
                            checkException();
                            return super.read();
                        }

                        @Override
                        public int read(byte[] b, int off, int len) throws IOException {
                            checkException();
                            return super.read(b, off, len);
                        }

                        private void checkException() throws IOException {
                            cnt++;
                            // magic number tested by hand
                            if (cnt == 200) {
                                throw new IOException("Artificial exception");
                            }
                        }
                    }
                };
        RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(
                                        failingFileIO,
                                        file,
                                        failingFileIO.getFileSize(file),
                                        null,
                                        null));
        assertThatThrownBy(() -> reader.forEachRemaining(row -> {}))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Artificial exception");
    }

    @Test
    void testCompression() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull());
        AvroFileFormat format = new AvroFileFormat(new FormatContext(new Options(), 1024, 1024));
        LocalFileIO localFileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
        try (PositionOutputStream out = localFileIO.newOutputStream(file, false)) {
            assertThatThrownBy(() -> format.createWriterFactory(rowType).create(out, "unsupported"))
                    .hasMessageContaining("Unrecognized codec: unsupported");
        }
    }
}
