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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SupportsReaderArrowSchema;
import org.apache.paimon.format.SupportsWriterMetadata;
import org.apache.paimon.format.parquet.writer.ParquetBuilder;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.OutputFile;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/** A parquet {@link FormatReadWriteTest}. */
public class ParquetFormatReadWriteTest extends FormatReadWriteTest {

    protected ParquetFormatReadWriteTest() {
        super("parquet");
    }

    @Override
    protected FileFormat fileFormat() {
        return new ParquetFileFormat(
                new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
    }

    @Test
    public void testWriteMetadata() throws Exception {
        ParquetFileFormat format =
                new ParquetFileFormat(
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "name", DataTypes.STRING()));

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        Map<String, String> fieldMetadata = new HashMap<>();
        fieldMetadata.put("paimon.test.field-key", "field-value");
        fieldMetadata.put("paimon.test.field-version", "1");
        Schema arrowSchema =
                new Schema(
                        Arrays.asList(
                                new Field(
                                        "id",
                                        new FieldType(
                                                true,
                                                Types.MinorType.INT.getType(),
                                                null,
                                                Collections.singletonMap("PARQUET:field_id", "0")),
                                        null),
                                new Field(
                                        "name",
                                        new FieldType(
                                                true,
                                                Types.MinorType.VARCHAR.getType(),
                                                null,
                                                fieldMetadata),
                                        null)));
        byte[] arrowSchemaBytes = arrowSchema.serializeAsMessage();
        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put("paimon.test.key", "paimon-test-value".getBytes(StandardCharsets.UTF_8));
        metadata.put(FormatMetadataUtils.ARROW_SCHEMA_METADATA_KEY, arrowSchemaBytes);
        ((SupportsWriterMetadata) writer).addMetadata(metadata);
        writer.addElement(GenericRow.of(1, BinaryString.fromString("one")));
        writer.close();
        Assertions.assertThatThrownBy(() -> ((SupportsWriterMetadata) writer).addMetadata(metadata))
                .isInstanceOf(IllegalStateException.class);
        out.close();

        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        fileIO, file, fileIO.getFileSize(file), new Options())) {
            Map<String, String> fileMetadata =
                    reader.getFooter().getFileMetaData().getKeyValueMetaData();
            Map<String, byte[]> decodedMetadata = FormatMetadataUtils.decodeMetadata(fileMetadata);
            Assertions.assertThat(
                            new String(
                                    decodedMetadata.get("paimon.test.key"), StandardCharsets.UTF_8))
                    .isEqualTo("paimon-test-value");
        }

        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        RowType emptyRowType = new RowType(Collections.emptyList());
        try (FileRecordReader<InternalRow> reader =
                format.createReaderFactory(emptyRowType, emptyRowType, Collections.emptyList())
                        .createReader(context)) {
            Optional<Schema> readArrowSchema =
                    ((SupportsReaderArrowSchema) reader).readArrowSchema();
            Assertions.assertThat(readArrowSchema).hasValue(arrowSchema);
            Assertions.assertThat(FormatMetadataUtils.readFieldMetadata(readArrowSchema.get()))
                    .containsEntry("name", fieldMetadata);
        }
    }

    @Test
    public void testUnsupportedMetadataBuilderFailsExplicitly() throws Exception {
        ParquetBuilder<InternalRow> unsupportedMetadataBuilder =
                new ParquetBuilder<InternalRow>() {
                    @Override
                    public ParquetWriter<InternalRow> createWriter(
                            OutputFile out, String compression) {
                        throw new AssertionError("Two-argument createWriter should not be called.");
                    }
                };
        ParquetWriterFactory factory = new ParquetWriterFactory(unsupportedMetadataBuilder);
        PositionOutputStream out = fileIO.newOutputStream(file, false);
        try {
            Assertions.assertThatThrownBy(() -> factory.create(out, "zstd"))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("does not support writer metadata");
        } finally {
            out.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEnableBloomFilter(boolean enabled) throws Exception {
        Options options = new Options();
        options.set("parquet.bloom.filter.enabled", String.valueOf(enabled));
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.BIGINT());

        if (ThreadLocalRandom.current().nextBoolean()) {
            rowType = rowType.notNull();
        }

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.addElement(GenericRow.of(1, 1L));
        writer.addElement(GenericRow.of(2, 2L));
        writer.addElement(GenericRow.of(3, null));
        writer.close();
        out.close();

        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        fileIO, file, fileIO.getFileSize(file), new Options())) {
            ParquetMetadata parquetMetadata = reader.getFooter();
            List<BlockMetaData> blockMetaDataList = parquetMetadata.getBlocks();
            for (BlockMetaData blockMetaData : blockMetaDataList) {
                List<ColumnChunkMetaData> columnChunkMetaDataList = blockMetaData.getColumns();
                for (ColumnChunkMetaData columnChunkMetaData : columnChunkMetaDataList) {
                    BloomFilter filter = reader.readBloomFilter(columnChunkMetaData);
                    Assertions.assertThat(enabled == (filter != null)).isTrue();
                }
            }
        }
    }

    @Test
    public void testColumnCompressionCodec() throws Exception {
        Options options = new Options();
        options.set("parquet.compression#name", "none");
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "name", DataTypes.STRING()));

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.addElement(GenericRow.of(1, BinaryString.fromString("one")));
        writer.addElement(GenericRow.of(2, BinaryString.fromString("two")));
        writer.addElement(GenericRow.of(3, BinaryString.fromString("three")));
        writer.close();
        out.close();

        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        fileIO, file, fileIO.getFileSize(file), new Options())) {
            Map<String, CompressionCodecName> codecs = new HashMap<>();
            for (BlockMetaData blockMetaData : reader.getFooter().getBlocks()) {
                for (ColumnChunkMetaData columnChunkMetaData : blockMetaData.getColumns()) {
                    codecs.put(
                            columnChunkMetaData.getPath().toDotString(),
                            columnChunkMetaData.getCodec());
                }
            }

            Assertions.assertThat(codecs)
                    .containsEntry("id", CompressionCodecName.ZSTD)
                    .containsEntry("name", CompressionCodecName.UNCOMPRESSED);
        }
    }
}
