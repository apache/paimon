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
import org.apache.paimon.data.Decimal;
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
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.NestedFieldTransform;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataType;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
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

    // -----------------------------------------------------------------------------------------
    // end-to-end: a nested predicate must not silently drop rows
    // -----------------------------------------------------------------------------------------

    private RowType nestedPayloadType() {
        return RowType.of(
                new DataType[] {
                    DataTypes.BIGINT(),
                    RowType.of(
                            new DataType[] {
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.TIMESTAMP(3),
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                                DataTypes.BIGINT()
                            },
                            new String[] {"amount", "ts", "ltz", "qty"})
                },
                new String[] {"pk", "payload"});
    }

    /**
     * Reads with {@code predicate} pushed down and returns the primary keys that survived. Parquet
     * filtering is row-group granular, so a matching row may come back alongside non-matching ones
     * — what must never happen is the matching row disappearing.
     */
    private List<Long> readPks(RowType rowType, Predicate predicate) throws IOException {
        List<Predicate> filters = new ArrayList<>();
        filters.add(predicate);
        List<Long> pks = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                fileFormat()
                        .createReaderFactory(rowType, rowType, filters)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    pks.add(row.getLong(0));
                }
                batch.releaseBatch();
            }
        }
        return pks;
    }

    private void writeTwoPayloadRows(RowType rowType) throws IOException {
        Decimal match = Decimal.fromBigDecimal(new BigDecimal("12.34"), 10, 2);
        Decimal other = Decimal.fromBigDecimal(new BigDecimal("99.99"), 10, 2);
        org.apache.paimon.data.Timestamp early =
                org.apache.paimon.data.Timestamp.fromEpochMillis(1704067200000L);
        org.apache.paimon.data.Timestamp late =
                org.apache.paimon.data.Timestamp.fromEpochMillis(1704067200000L + 60_000L);
        write(
                fileFormat().createWriterFactory(rowType),
                file,
                GenericRow.of(1L, GenericRow.of(match, early, early, 7L)),
                GenericRow.of(2L, GenericRow.of(other, late, late, 8L)));
    }

    private NestedFieldTransform payloadLeaf(RowType rowType, String leaf) {
        RowType payload = (RowType) rowType.getTypeAt(1);
        return new NestedFieldTransform(
                new FieldRef(1, "payload", payload), Collections.singletonList(leaf));
    }

    /** Control: a nested BIGINT predicate already carried its full path and kept its row. */
    @Test
    public void testNestedBigIntPredicateKeepsMatchingRows() throws IOException {
        RowType rowType = nestedPayloadType();
        writeTwoPayloadRows(rowType);
        Predicate onQty = new PredicateBuilder(rowType).equal(payloadLeaf(rowType, "qty"), 7L);
        Assertions.assertThat(readPks(rowType, onQty))
                .as("control: the row whose payload.qty equals 7 must survive the filter")
                .contains(1L);
    }

    /**
     * Reading with a predicate on a nested DECIMAL must still return the matching row. The column
     * handed to parquet-mr used to carry only the leaf name, so parquet-mr saw a missing top-level
     * column, treated it as all-null and pruned the row group holding the match.
     */
    @Test
    public void testNestedDecimalPredicateKeepsMatchingRows() throws IOException {
        RowType rowType = nestedPayloadType();
        writeTwoPayloadRows(rowType);
        Decimal match = Decimal.fromBigDecimal(new BigDecimal("12.34"), 10, 2);
        Predicate onAmount =
                new PredicateBuilder(rowType).equal(payloadLeaf(rowType, "amount"), match);
        Assertions.assertThat(readPks(rowType, onAmount))
                .as("the row whose payload.amount equals the literal must survive the filter")
                .contains(1L);
    }

    /** Same as {@link #testNestedDecimalPredicateKeepsMatchingRows()} for LOCAL ZONED TIMESTAMP. */
    @Test
    public void testNestedLocalZonedTimestampPredicateKeepsMatchingRows() throws IOException {
        RowType rowType = nestedPayloadType();
        writeTwoPayloadRows(rowType);
        org.apache.paimon.data.Timestamp early =
                org.apache.paimon.data.Timestamp.fromEpochMillis(1704067200000L);
        Predicate onLtz = new PredicateBuilder(rowType).equal(payloadLeaf(rowType, "ltz"), early);
        Assertions.assertThat(readPks(rowType, onLtz))
                .as("the row whose payload.ltz equals the literal must survive the filter")
                .contains(1L);
    }

    /** Same as {@link #testNestedDecimalPredicateKeepsMatchingRows()} for TIMESTAMP. */
    @Test
    public void testNestedTimestampPredicateKeepsMatchingRows() throws IOException {
        RowType rowType = nestedPayloadType();
        writeTwoPayloadRows(rowType);
        org.apache.paimon.data.Timestamp early =
                org.apache.paimon.data.Timestamp.fromEpochMillis(1704067200000L);
        Predicate onTs = new PredicateBuilder(rowType).equal(payloadLeaf(rowType, "ts"), early);
        Assertions.assertThat(readPks(rowType, onTs))
                .as("the row whose payload.ts equals the literal must survive the filter")
                .contains(1L);
    }

    /**
     * A nested component whose own name contains a dot cannot be addressed by a dot-joined path.
     * Resolution fails, the filter is built against a path the file does not hold, and every row
     * group is pruned — so the matching row disappears.
     */
    @Test
    public void testNestedComponentContainingADotKeepsMatchingRows() throws IOException {
        RowType inner = RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"a.b"});
        RowType rowType =
                RowType.of(new DataType[] {DataTypes.BIGINT(), inner}, new String[] {"pk", "s"});

        write(
                fileFormat().createWriterFactory(rowType),
                file,
                GenericRow.of(1L, GenericRow.of(7L)),
                GenericRow.of(2L, GenericRow.of(8L)));

        Predicate onDotted =
                new PredicateBuilder(rowType)
                        .equal(
                                new NestedFieldTransform(
                                        new FieldRef(1, "s", inner),
                                        Collections.singletonList("a.b")),
                                7L);

        Assertions.assertThat(readPks(rowType, onDotted))
                .as("the row whose s.`a.b` equals 7 must survive the filter")
                .contains(1L);
    }

    /** The dot may sit in the top-level column's own name; the matching row must still survive. */
    @Test
    public void testTopLevelNameContainingADotKeepsMatchingRows() throws IOException {
        RowType inner = RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"city"});
        RowType rowType =
                RowType.of(new DataType[] {DataTypes.BIGINT(), inner}, new String[] {"pk", "a.b"});

        write(
                fileFormat().createWriterFactory(rowType),
                file,
                GenericRow.of(1L, GenericRow.of(7L)),
                GenericRow.of(2L, GenericRow.of(8L)));

        Predicate onNested =
                new PredicateBuilder(rowType)
                        .equal(
                                new NestedFieldTransform(
                                        new FieldRef(1, "a.b", inner),
                                        Collections.singletonList("city")),
                                7L);

        Assertions.assertThat(readPks(rowType, onNested))
                .as("the row whose `a.b`.city equals 7 must survive the filter")
                .contains(1L);
    }

    /**
     * A nested path that joins cleanly (no component contains a dot) can still collide with an
     * unrelated top-level column whose own literal name equals that joined path. The schema below
     * has both a top-level column named {@code "s.a"} (declared INT, so its physical column is
     * INT32) and a nested {@code s.a} (row {@code s} with BIGINT field {@code a}, physical INT64).
     * A predicate on the nested field is re-dispatched as {@code FieldRef("s.a")}, and {@link
     * org.apache.parquet.filter2.predicate.ParquetFilters}'s column lookup checks an exact
     * top-level name before walking the split components - so it resolves the predicate's physical
     * type against the unrelated top-level column instead of {@code s -> a}.
     *
     * <p>parquet-mr itself re-splits any dot-joined column name it is given, so the {@link
     * org.apache.parquet.filter2.predicate.FilterPredicate} still ends up addressing the real,
     * two-segment {@code s -> a} column chunk - but tagged with the wrong (top-level) column's
     * physical type. parquet-mr's {@code SchemaCompatibilityValidator} catches that mismatch at
     * read time and throws {@link IllegalArgumentException}, so today this does not silently drop
     * the row - it fails the read outright for a query that has nothing wrong with it.
     */
    @Test
    public void testNestedPathCollidingWithADottedTopLevelNameKeepsMatchingRows()
            throws IOException {
        RowType inner = RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"a"});
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.INT(), inner},
                        new String[] {"pk", "s.a", "s"});

        write(
                fileFormat().createWriterFactory(rowType),
                file,
                // top-level `s.a` (INT32) = 999 (does not match), nested s.a (INT64) = 7 (matches)
                GenericRow.of(1L, 999, GenericRow.of(7L)));

        Predicate onNestedSA =
                new PredicateBuilder(rowType)
                        .equal(
                                new NestedFieldTransform(
                                        new FieldRef(2, "s", inner),
                                        Collections.singletonList("a")),
                                7L);

        Assertions.assertThat(readPks(rowType, onNestedSA))
                .as(
                        "the row whose nested s.a equals 7 must survive the filter, "
                                + "even though the unrelated top-level `s.a` column does not")
                .contains(1L);
    }
}
