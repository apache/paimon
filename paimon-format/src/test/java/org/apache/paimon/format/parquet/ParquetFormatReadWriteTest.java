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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SupportsFieldMetadata;
import org.apache.paimon.format.SupportsWriterMetadata;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    public void testArrayBlobDescriptors() throws Exception {
        testArrayBlobDescriptorRoundTrip();
    }

    @Test
    public void testGeospatialWkbRoundTrip() throws Exception {
        byte[] pointWkb =
                new byte[] {
                    1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xf0, 0x3f, 0, 0, 0, 0, 0, 0, 0, 0x40
                };
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "geom", DataTypes.GEOMETRY()),
                        DataTypes.FIELD(1, "geog", DataTypes.GEOGRAPHY()),
                        DataTypes.FIELD(2, "geometries", DataTypes.ARRAY(DataTypes.GEOMETRY())),
                        DataTypes.FIELD(
                                3,
                                "geospatial_map",
                                DataTypes.MAP(DataTypes.GEOMETRY(), DataTypes.GEOGRAPHY())),
                        DataTypes.FIELD(
                                4,
                                "nested",
                                DataTypes.ROW(
                                        DataTypes.FIELD(5, "nested_geom", DataTypes.GEOMETRY()))));
        Map<byte[], byte[]> map = new LinkedHashMap<>();
        map.put(pointWkb, pointWkb);

        write(
                fileFormat().createWriterFactory(rowType),
                file,
                GenericRow.of(
                        pointWkb,
                        pointWkb,
                        new GenericArray(new Object[] {pointWkb, null}),
                        GenericMap.fromBinaryKeyMap(map),
                        GenericRow.of(pointWkb)));

        try (RecordReader<InternalRow> reader =
                fileFormat()
                        .createReaderFactory(rowType, rowType, java.util.Collections.emptyList())
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            InternalRow row = new InternalRowSerializer(rowType).copy(reader.readBatch().next());
            Assertions.assertThat(row.getBinary(0)).isEqualTo(pointWkb);
            Assertions.assertThat(row.getBinary(1)).isEqualTo(pointWkb);
            InternalArray geometries = row.getArray(2);
            Assertions.assertThat(geometries.getBinary(0)).isEqualTo(pointWkb);
            Assertions.assertThat(geometries.isNullAt(1)).isTrue();
            InternalMap geospatialMap = row.getMap(3);
            Assertions.assertThat(geospatialMap.keyArray().getBinary(0)).isEqualTo(pointWkb);
            Assertions.assertThat(geospatialMap.valueArray().getBinary(0)).isEqualTo(pointWkb);
            Assertions.assertThat(row.getRow(4, 1).getBinary(0)).isEqualTo(pointWkb);
        }

        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        fileIO, file, fileIO.getFileSize(file), new Options())) {
            Map<String, ColumnChunkMetaData> columns = new HashMap<>();
            for (ColumnChunkMetaData column : reader.getFooter().getBlocks().get(0).getColumns()) {
                columns.put(column.getPath().toDotString(), column);
            }
            Assertions.assertThat(columns)
                    .containsKeys(
                            "geom",
                            "geog",
                            "geometries.list.element",
                            "geospatial_map.key_value.key",
                            "geospatial_map.key_value.value",
                            "nested.nested_geom");
            for (ColumnChunkMetaData column : columns.values()) {
                Assertions.assertThat(column.getStatistics().hasNonNullValue())
                        .as(column.getPath().toDotString())
                        .isFalse();
                Assertions.assertThat(column.getStatistics().isNumNullsSet())
                        .as(column.getPath().toDotString())
                        .isTrue();
            }
            Assertions.assertThat(
                            columns.get("geometries.list.element").getStatistics().getNumNulls())
                    .isEqualTo(1);
            Assertions.assertThat(columns.get("geom").getGeospatialStatistics()).isNotNull();
            Assertions.assertThat(columns.get("geometries.list.element").getGeospatialStatistics())
                    .isNotNull();
            Assertions.assertThat(
                            columns.get("geospatial_map.key_value.key").getGeospatialStatistics())
                    .isNotNull();
            Assertions.assertThat(columns.get("nested.nested_geom").getGeospatialStatistics())
                    .isNotNull();
        }
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
        Map<String, Map<String, String>> fieldMetadataByName = new HashMap<>();
        fieldMetadataByName.put("name", fieldMetadata);
        byte[] arrowSchemaBytes =
                FormatMetadataUtils.buildArrowSchemaMetadata(
                        rowType, fieldMetadataByName, FormatMetadataUtils.PARQUET_FIELD_ID_KEY);
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
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), null, null);
        Map<String, Map<String, String>> readFieldMetadata =
                ((SupportsFieldMetadata) format).readFieldMetadata(context);
        Assertions.assertThat(readFieldMetadata).containsKey("id").containsKey("name");
        Assertions.assertThat(readFieldMetadata.get("id"))
                .containsEntry(FormatMetadataUtils.PARQUET_FIELD_ID_KEY, "0");
        Assertions.assertThat(readFieldMetadata.get("name")).containsAllEntriesOf(fieldMetadata);
        Assertions.assertThat(readFieldMetadata.get("name"))
                .containsEntry(FormatMetadataUtils.PARQUET_FIELD_ID_KEY, "1");
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

    @Test
    public void testWriteByteStreamSplit() throws Exception {
        Options options = new Options();
        options.set("parquet.enable.dictionary", "false");
        options.set("parquet.enable.bytestreamsplit", "true");
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "float_value", DataTypes.FLOAT()),
                        DataTypes.FIELD(1, "double_value", DataTypes.DOUBLE()));

        write(
                format.createWriterFactory(rowType),
                file,
                GenericRow.of(1.25f, 2.5d),
                GenericRow.of(3.75f, 5.0d));

        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        fileIO, file, fileIO.getFileSize(file), new Options())) {
            for (ColumnChunkMetaData column : reader.getFooter().getBlocks().get(0).getColumns()) {
                Assertions.assertThat(column.getEncodings()).contains(Encoding.BYTE_STREAM_SPLIT);
            }
        }

        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, java.util.Collections.emptyList())
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            InternalRowSerializer serializer = new InternalRowSerializer(rowType);
            List<InternalRow> rows = new ArrayList<>();
            reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
            Assertions.assertThat(rows)
                    .containsExactly(GenericRow.of(1.25f, 2.5d), GenericRow.of(3.75f, 5.0d));
        }
    }
}
