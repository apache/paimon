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
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.VariantAccessInfo;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

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
                ParquetUtil.getParquetReader(fileIO, file, fileIO.getFileSize(file))) {
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
    public void testReadShreddedVariant() throws Exception {
        Options options = new Options();
        options.set(
                "parquet.variant.shreddingSchema",
                "{\"type\":\"ROW\",\"fields\":[{\"name\":\"v\",\"type\":{\"type\":\"ROW\",\"fields\":[{\"name\":\"age\",\"type\":\"INT\"},{\"name\":\"city\",\"type\":\"STRING\"}]}}]}");
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        write(
                factory,
                file,
                GenericRow.of(GenericVariant.fromJson("{\"age\":35,\"city\":\"Chicago\"}")),
                GenericRow.of(GenericVariant.fromJson("{\"age\":25,\"other\":\"Hello\"}")));

        // read without pruning
        List<InternalRow> result1 = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(writeType, writeType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            InternalRowSerializer serializer = new InternalRowSerializer(writeType);
            reader.forEachRemaining(row -> result1.add(serializer.copy(row)));
        }
        assertThat(result1.get(0).getVariant(0).toJson())
                .isEqualTo("{\"age\":35,\"city\":\"Chicago\"}");
        assertThat(result1.get(1).getVariant(0).toJson())
                .isEqualTo("{\"age\":25,\"other\":\"Hello\"}");

        // read with typed col only
        List<VariantAccessInfo.VariantField> variantFields2 = new ArrayList<>();
        variantFields2.add(
                new VariantAccessInfo.VariantField(
                        new DataField(0, "age", DataTypes.INT()), "$.age"));
        VariantAccessInfo[] variantAccess2 = {new VariantAccessInfo("v", variantFields2)};
        RowType readStructType2 =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0, "v", DataTypes.ROW(DataTypes.FIELD(0, "age", DataTypes.INT()))));
        List<InternalRow> result2 = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(writeType, writeType, new ArrayList<>(), variantAccess2)
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            InternalRowSerializer serializer = new InternalRowSerializer(readStructType2);
            reader.forEachRemaining(row -> result2.add(serializer.copy(row)));
        }
        assertThat(result2.get(0).equals(GenericRow.of(GenericRow.of(35)))).isTrue();
        assertThat(result2.get(1).equals(GenericRow.of(GenericRow.of(25)))).isTrue();

        // read with typed col and untyped col
        List<VariantAccessInfo.VariantField> variantFields3 = new ArrayList<>();
        variantFields3.add(
                new VariantAccessInfo.VariantField(
                        new DataField(0, "age", DataTypes.INT()), "$.age"));
        variantFields3.add(
                new VariantAccessInfo.VariantField(
                        new DataField(1, "other", DataTypes.STRING()), "$.other"));
        VariantAccessInfo[] variantAccess3 = {new VariantAccessInfo("v", variantFields3)};
        RowType readStructType3 =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "v",
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "age", DataTypes.INT()),
                                        DataTypes.FIELD(1, "other", DataTypes.STRING()))));
        List<InternalRow> result3 = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(writeType, writeType, new ArrayList<>(), variantAccess3)
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            InternalRowSerializer serializer = new InternalRowSerializer(readStructType3);
            reader.forEachRemaining(row -> result3.add(serializer.copy(row)));
        }
        assertThat(result3.get(0).equals(GenericRow.of(GenericRow.of(35, null)))).isTrue();
        assertThat(
                        result3.get(1)
                                .equals(
                                        GenericRow.of(
                                                GenericRow.of(
                                                        25, BinaryString.fromString("Hello")))))
                .isTrue();
    }
}
