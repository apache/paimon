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
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.data.serializer.InternalMapSerializer.convertToJavaMap;
import static org.assertj.core.api.Assertions.assertThat;

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
    public void testMapShreddingRoundTrip() throws Exception {
        Options options = new Options();
        options.set("map.shredding.columns", "labels");
        options.set("map.shredding.maxKeys", "2");
        options.set("map.shredding.maxInferBufferRow", "10");
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "labels",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING())));

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.addElement(GenericRow.of(labels("job", "api", "namespace", "prod", "region", "us")));
        writer.close();
        out.close();

        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        fileIO, file, fileIO.getFileSize(file), new Options())) {
            assertThat(reader.getFooter().getFileMetaData().getSchema().containsField("labels"))
                    .isTrue();
            assertThat(
                            reader.getFooter()
                                    .getFileMetaData()
                                    .getSchema()
                                    .containsField("dynamic_column_labels_value_0"))
                    .isTrue();
            assertThat(
                            reader.getFooter()
                                    .getFileMetaData()
                                    .getKeyValueMetaData()
                                    .get("parquet.meta.dynamic.column.map.keys.of.labels")
                                    .split(","))
                    .containsExactlyInAnyOrder("job", "namespace");
        }

        InternalMap logicalMap = readOne(format, rowType).getMap(0);
        assertThat(toStringMap(logicalMap))
                .containsExactlyInAnyOrderEntriesOf(
                        stringMap("job", "api", "namespace", "prod", "region", "us"));
    }

    @Test
    public void testMapShreddingNonStringValueRoundTrip() throws Exception {
        Options options = new Options();
        options.set("map.shredding.columns", "headers");
        options.set("map.shredding.maxKeys", "2");
        options.set("map.shredding.maxInferBufferRow", "10");
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "headers",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT())));

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.addElement(GenericRow.of(intLabels("status", 200, "retry", 3, "region", 1)));
        writer.close();
        out.close();

        InternalMap logicalMap = readOne(format, rowType).getMap(0);
        assertThat(toIntMap(logicalMap))
                .containsExactlyInAnyOrderEntriesOf(intMap("status", 200, "retry", 3, "region", 1));
    }

    @Test
    public void testMapShreddingVariantValueRoundTrip() throws Exception {
        Options options = new Options();
        options.set("map.shredding.columns", "params");
        options.set("map.shredding.maxKeys", "2");
        options.set("map.shredding.maxInferBufferRow", "10");
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "params",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.VARIANT())));

        GenericVariant user = GenericVariant.fromJson("{\"id\":1,\"name\":\"alice\"}");
        GenericVariant request = GenericVariant.fromJson("{\"id\":\"r1\"}");
        GenericVariant extra = GenericVariant.fromJson("{\"debug\":true}");

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.addElement(
                GenericRow.of(variantLabels("user", user, "request", request, "extra", extra)));
        writer.close();
        out.close();

        InternalMap logicalMap = readOne(format, rowType).getMap(0);
        assertThat(toVariantMap(logicalMap))
                .containsExactlyInAnyOrderEntriesOf(
                        variantMap("user", user, "request", request, "extra", extra));
    }

    @Test
    public void testNestedMapShreddingRoundTrip() throws Exception {
        Options options = new Options();
        options.set("map.shredding.columns", "payload.labels");
        options.set("map.shredding.maxKeys", "2");
        options.set("map.shredding.maxInferBufferRow", "10");
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        RowType payloadType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "labels",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING())));
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "payload", payloadType));

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.addElement(
                GenericRow.of(
                        GenericRow.of(labels("job", "api", "namespace", "prod", "region", "us"))));
        writer.close();
        out.close();

        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        fileIO, file, fileIO.getFileSize(file), new Options())) {
            assertThat(
                            reader.getFooter()
                                    .getFileMetaData()
                                    .getSchema()
                                    .getType("payload")
                                    .asGroupType()
                                    .containsField("dynamic_column_payload_labels_value_0"))
                    .isTrue();
            assertThat(
                            reader.getFooter()
                                    .getFileMetaData()
                                    .getKeyValueMetaData()
                                    .get("parquet.meta.dynamic.column.map.keys.of.payload.labels")
                                    .split(","))
                    .containsExactlyInAnyOrder("job", "namespace");
        }

        InternalMap logicalMap = readOne(format, rowType).getRow(0, 1).getMap(0);
        assertThat(toStringMap(logicalMap))
                .containsExactlyInAnyOrderEntriesOf(
                        stringMap("job", "api", "namespace", "prod", "region", "us"));
    }

    private InternalRow readOne(FileFormat format, RowType rowType) throws Exception {
        RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)));
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        assertThat(result).hasSize(1);
        return result.get(0);
    }

    private static GenericMap labels(String... kvs) {
        Map<BinaryString, BinaryString> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(BinaryString.fromString(kvs[i]), BinaryString.fromString(kvs[i + 1]));
        }
        return new GenericMap(map);
    }

    private static GenericMap intLabels(Object... kvs) {
        Map<BinaryString, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(BinaryString.fromString((String) kvs[i]), (Integer) kvs[i + 1]);
        }
        return new GenericMap(map);
    }

    private static GenericMap variantLabels(Object... kvs) {
        Map<BinaryString, GenericVariant> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(BinaryString.fromString((String) kvs[i]), (GenericVariant) kvs[i + 1]);
        }
        return new GenericMap(map);
    }

    private static Map<String, String> toStringMap(InternalMap map) {
        Map<Object, Object> raw = convertToJavaMap(map, DataTypes.STRING(), DataTypes.STRING());
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : raw.entrySet()) {
            result.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return result;
    }

    private static Map<String, Integer> toIntMap(InternalMap map) {
        Map<Object, Object> raw = convertToJavaMap(map, DataTypes.STRING(), DataTypes.INT());
        Map<String, Integer> result = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : raw.entrySet()) {
            result.put(entry.getKey().toString(), (Integer) entry.getValue());
        }
        return result;
    }

    private static Map<String, Variant> toVariantMap(InternalMap map) {
        Map<Object, Object> raw = convertToJavaMap(map, DataTypes.STRING(), DataTypes.VARIANT());
        Map<String, Variant> result = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : raw.entrySet()) {
            result.put(entry.getKey().toString(), (Variant) entry.getValue());
        }
        return result;
    }

    private static Map<String, String> stringMap(String... kvs) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(kvs[i], kvs[i + 1]);
        }
        return map;
    }

    private static Map<String, Integer> intMap(Object... kvs) {
        Map<String, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put((String) kvs[i], (Integer) kvs[i + 1]);
        }
        return map;
    }

    private static Map<String, Variant> variantMap(Object... kvs) {
        Map<String, Variant> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put((String) kvs[i], (Variant) kvs[i + 1]);
        }
        return map;
    }
}
