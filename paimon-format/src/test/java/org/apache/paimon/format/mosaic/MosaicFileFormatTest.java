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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the Mosaic file format. */
public class MosaicFileFormatTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testBasicRoundTrip() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("value", DataTypes.DOUBLE())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(GenericRow.of(i, BinaryString.fromString("name_" + i), i * 1.5));
        }

        Path path = new Path(tempDir.toString(), "basic.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getString(1).toString()).isEqualTo("name_" + i);
            assertThat(result.get(i).getDouble(2)).isEqualTo(i * 1.5);
        }
    }

    @Test
    public void testProjectionPushdown() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.STRING())
                        .field("c", DataTypes.BIGINT())
                        .field("d", DataTypes.DOUBLE())
                        .field("e", DataTypes.FLOAT())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            data.add(
                    GenericRow.of(
                            i,
                            BinaryString.fromString("val_" + i),
                            (long) i * 100,
                            i * 2.5,
                            (float) i * 0.1f));
        }

        Path path = new Path(tempDir.toString(), "proj.mosaic");
        write(rowType, data, path);

        // Project only columns a and c
        RowType projectedType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("c", DataTypes.BIGINT())
                        .build();

        List<InternalRow> result = read(rowType, projectedType, path);

        assertThat(result).hasSize(50);
        for (int i = 0; i < 50; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getLong(1)).isEqualTo((long) i * 100);
        }
    }

    @Test
    public void testProjectionSkipsVariableLengthColumns() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_str1", DataTypes.STRING())
                        .field("f_bytes", DataTypes.BYTES())
                        .field("f_str2", DataTypes.STRING())
                        .field("f_decimal_large", DataTypes.DECIMAL(30, 5))
                        .field("f_target", DataTypes.BIGINT())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(
                    GenericRow.of(
                            i,
                            BinaryString.fromString("variable_length_string_" + i),
                            ("binary_data_" + i).getBytes(),
                            BinaryString.fromString("another_string_value_" + i),
                            Decimal.fromBigDecimal(
                                    new BigDecimal("123456789012345678901234.12345"), 30, 5),
                            (long) i * 1000));
        }

        Path path = new Path(tempDir.toString(), "skip_varlen.mosaic");
        write(rowType, data, path);

        // Project only f_int and f_target, forcing reader to skip variable-length columns in
        // between
        RowType projectedType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_target", DataTypes.BIGINT())
                        .build();

        List<InternalRow> result = read(rowType, projectedType, path);

        assertThat(result).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getLong(1)).isEqualTo((long) i * 1000);
        }
    }

    @Test
    public void testNullValues() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING().nullable())
                        .field("value", DataTypes.DOUBLE().nullable())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        data.add(GenericRow.of(1, BinaryString.fromString("hello"), 1.0));
        data.add(GenericRow.of(2, null, 2.0));
        data.add(GenericRow.of(3, BinaryString.fromString("world"), null));
        data.add(GenericRow.of(4, null, null));

        Path path = new Path(tempDir.toString(), "nulls.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(4);

        assertThat(result.get(0).getInt(0)).isEqualTo(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("hello");
        assertThat(result.get(0).getDouble(2)).isEqualTo(1.0);

        assertThat(result.get(1).getInt(0)).isEqualTo(2);
        assertThat(result.get(1).isNullAt(1)).isTrue();
        assertThat(result.get(1).getDouble(2)).isEqualTo(2.0);

        assertThat(result.get(2).getInt(0)).isEqualTo(3);
        assertThat(result.get(2).getString(1).toString()).isEqualTo("world");
        assertThat(result.get(2).isNullAt(2)).isTrue();

        assertThat(result.get(3).getInt(0)).isEqualTo(4);
        assertThat(result.get(3).isNullAt(1)).isTrue();
        assertThat(result.get(3).isNullAt(2)).isTrue();
    }

    @Test
    public void testAllPrimitiveTypes() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("f_boolean", DataTypes.BOOLEAN())
                        .field("f_tinyint", DataTypes.TINYINT())
                        .field("f_smallint", DataTypes.SMALLINT())
                        .field("f_int", DataTypes.INT())
                        .field("f_bigint", DataTypes.BIGINT())
                        .field("f_float", DataTypes.FLOAT())
                        .field("f_double", DataTypes.DOUBLE())
                        .field("f_string", DataTypes.STRING())
                        .field("f_bytes", DataTypes.BYTES())
                        .field("f_decimal_compact", DataTypes.DECIMAL(10, 2))
                        .field("f_decimal_large", DataTypes.DECIMAL(30, 5))
                        .field("f_date", DataTypes.DATE())
                        .field("f_timestamp", DataTypes.TIMESTAMP(3))
                        .field("f_timestamp_high", DataTypes.TIMESTAMP(9))
                        .build();

        List<InternalRow> data = new ArrayList<>();
        data.add(
                GenericRow.of(
                        true,
                        (byte) 42,
                        (short) 1234,
                        999999,
                        123456789012345L,
                        3.14f,
                        2.718281828,
                        BinaryString.fromString("hello world"),
                        new byte[] {1, 2, 3, 4, 5},
                        Decimal.fromBigDecimal(new BigDecimal("12345.67"), 10, 2),
                        Decimal.fromBigDecimal(
                                new BigDecimal("123456789012345678901234.12345"), 30, 5),
                        19000, // days since epoch
                        Timestamp.fromEpochMillis(1700000000000L),
                        Timestamp.fromEpochMillis(1700000000000L, 123456)));

        Path path = new Path(tempDir.toString(), "all_types.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(1);
        InternalRow row = result.get(0);
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getByte(1)).isEqualTo((byte) 42);
        assertThat(row.getShort(2)).isEqualTo((short) 1234);
        assertThat(row.getInt(3)).isEqualTo(999999);
        assertThat(row.getLong(4)).isEqualTo(123456789012345L);
        assertThat(row.getFloat(5)).isEqualTo(3.14f);
        assertThat(row.getDouble(6)).isEqualTo(2.718281828);
        assertThat(row.getString(7).toString()).isEqualTo("hello world");
        assertThat(row.getBinary(8)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(row.getDecimal(9, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("12345.67"));
        assertThat(row.getDecimal(10, 30, 5).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("123456789012345678901234.12345"));
        assertThat(row.getInt(11)).isEqualTo(19000);
        assertThat(row.getTimestamp(12, 3).getMillisecond()).isEqualTo(1700000000000L);
        assertThat(row.getTimestamp(13, 9).getMillisecond()).isEqualTo(1700000000000L);
        assertThat(row.getTimestamp(13, 9).getNanoOfMillisecond()).isEqualTo(123456);
    }

    @Test
    public void testWideTable() throws IOException {
        int columnCount = 10000;
        int rowCount = 10;

        RowType rowType = buildWideRowType(columnCount);
        List<InternalRow> data = new ArrayList<>();
        for (int r = 0; r < rowCount; r++) {
            Object[] fields = new Object[columnCount];
            for (int c = 0; c < columnCount; c++) {
                fields[c] = r * columnCount + c;
            }
            data.add(GenericRow.of(fields));
        }

        Path path = new Path(tempDir.toString(), "wide.mosaic");
        LocalFileIO fileIO = new LocalFileIO();
        write(rowType, data, path);
        long mosaicSize = fileIO.getFileSize(path);

        // Compare with ORC
        Path orcPath = new Path(tempDir.toString(), "wide.orc");
        OrcFileFormat orc =
                new OrcFileFormat(
                        new FormatContext(
                                new Options(), 1024, 1024, MemorySize.ofMebiBytes(128), 9, null));
        FormatWriterFactory orcWriterFactory = orc.createWriterFactory(rowType);
        PositionOutputStream orcOut = fileIO.newOutputStream(orcPath, false);
        FormatWriter orcWriter = orcWriterFactory.create(orcOut, "zstd");
        for (InternalRow row : data) {
            orcWriter.addElement(row);
        }
        orcWriter.close();
        orcOut.close();
        long orcSize = fileIO.getFileSize(orcPath);

        System.out.println("=== Wide Table: Mosaic vs ORC ===");
        System.out.printf("Mosaic: %,d bytes (%.1f KB)%n", mosaicSize, mosaicSize / 1024.0);
        System.out.printf("ORC:    %,d bytes (%.1f KB)%n", orcSize, orcSize / 1024.0);
        System.out.printf("Ratio:  ORC is %.1fx larger%n", (double) orcSize / mosaicSize);

        assertThat(mosaicSize).isLessThan(orcSize);

        // Verify correctness
        List<InternalRow> result = read(rowType, rowType, path);
        assertThat(result).hasSize(rowCount);
        for (int r = 0; r < rowCount; r++) {
            for (int c = 0; c < columnCount; c++) {
                assertThat(result.get(r).getInt(c)).isEqualTo(r * columnCount + c);
            }
        }
    }

    @Test
    public void testWideTableProjection() throws IOException {
        int columnCount = 10000;
        int rowCount = 100;

        RowType rowType = buildWideRowType(columnCount);
        List<InternalRow> data = new ArrayList<>();
        for (int r = 0; r < rowCount; r++) {
            Object[] fields = new Object[columnCount];
            for (int c = 0; c < columnCount; c++) {
                fields[c] = r * columnCount + c;
            }
            data.add(GenericRow.of(fields));
        }

        Path path = new Path(tempDir.toString(), "wide_proj.mosaic");
        write(rowType, data, path);

        // Project 10 columns
        int[] projectedIndices = {0, 100, 500, 1000, 2000, 5000, 7000, 8000, 9000, 9999};
        RowType projectedType = rowType.project(projectedIndices);

        List<InternalRow> result = read(rowType, projectedType, path);

        assertThat(result).hasSize(rowCount);
        for (int r = 0; r < rowCount; r++) {
            for (int i = 0; i < projectedIndices.length; i++) {
                int c = projectedIndices[i];
                assertThat(result.get(r).getInt(i)).isEqualTo(r * columnCount + c);
            }
        }
    }

    @Test
    public void testEmptyTable() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build();

        Path path = new Path(tempDir.toString(), "empty.mosaic");
        write(rowType, new ArrayList<>(), path);
        List<InternalRow> result = read(rowType, rowType, path);
        assertThat(result).isEmpty();
    }

    @Test
    public void testSingleColumn() throws IOException {
        RowType rowType = RowType.builder().field("id", DataTypes.INT()).build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(GenericRow.of(i));
        }

        Path path = new Path(tempDir.toString(), "single.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(10);
        for (int i = 0; i < 10; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
        }
    }

    // ==================== Helpers ====================

    private void write(RowType rowType, List<InternalRow> data, Path path) throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        MosaicFileFormat format = createFormat();
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = writerFactory.create(out, "zstd");
        for (InternalRow row : data) {
            writer.addElement(row);
        }
        writer.close();
        out.close();
    }

    private List<InternalRow> read(RowType dataType, RowType projectedType, Path path)
            throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        MosaicFileFormat format = createFormat();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(dataType, projectedType, null);
        RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    int fieldCount = projectedType.getFieldCount();
                    Object[] fields = new Object[fieldCount];
                    for (int i = 0; i < fieldCount; i++) {
                        if (row.isNullAt(i)) {
                            fields[i] = null;
                        } else {
                            fields[i] =
                                    InternalRow.createFieldGetter(projectedType.getTypeAt(i), i)
                                            .getFieldOrNull(row);
                        }
                    }
                    result.add(GenericRow.of(fields));
                });
        reader.close();
        return result;
    }

    private MosaicFileFormat createFormat() {
        return new MosaicFileFormat(
                new FormatContext(new Options(), 1024, 1024, MemorySize.ofMebiBytes(128), 3, null));
    }

    private RowType buildWideRowType(int columnCount) {
        RowType.Builder builder = RowType.builder();
        for (int i = 0; i < columnCount; i++) {
            builder.field(
                    String.format(
                            "this_is_a_very_long_column_name_for_testing_compression_ratio_column_index_%05d",
                            i),
                    DataTypes.INT());
        }
        return builder.build();
    }
}
