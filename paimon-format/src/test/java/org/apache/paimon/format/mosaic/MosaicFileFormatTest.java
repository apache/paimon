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
import org.apache.paimon.reader.FileRecordReader;
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

    @Test
    public void testMultiRowGroupStringStability() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build();

        // Use tiny writeBatchMemory to force multiple row groups
        MosaicFileFormat format =
                new MosaicFileFormat(
                        new FormatContext(
                                new Options(), 1024, 1024, MemorySize.ofBytes(1), 3, null));

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(GenericRow.of(i, BinaryString.fromString("string_value_" + i)));
        }

        Path path = new Path(tempDir.toString(), "multi_rg_string.mosaic");
        LocalFileIO fileIO = new LocalFileIO();
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = writerFactory.create(out, "zstd");
        for (InternalRow row : data) {
            writer.addElement(row);
        }
        writer.close();
        out.close();

        // Project only the string column
        RowType projectedType = RowType.builder().field("name", DataTypes.STRING()).build();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, projectedType, null);
        FileRecordReader<InternalRow> reader =
                (FileRecordReader<InternalRow>)
                        readerFactory.createReader(
                                new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        // Read batches one by one; retain string values from earlier batches
        List<BinaryString> allStrings = new ArrayList<>();
        RecordReader.RecordIterator<InternalRow> batch;
        while ((batch = reader.readBatch()) != null) {
            InternalRow row;
            while ((row = batch.next()) != null) {
                allStrings.add(row.getString(0));
            }
            batch.releaseBatch();
        }
        reader.close();

        // Verify all retained strings are still correct
        assertThat(allStrings).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(allStrings.get(i).toString()).isEqualTo("string_value_" + i);
        }
    }

    // ==================== Columnar Encoding Tests ====================

    @Test
    public void testConstEncoding() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("const_int", DataTypes.INT())
                        .field("const_long", DataTypes.BIGINT())
                        .field("const_double", DataTypes.DOUBLE())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            data.add(GenericRow.of(i, 42, 999L, 3.14));
        }

        Path path = new Path(tempDir.toString(), "const_enc.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(200);
        for (int i = 0; i < 200; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getInt(1)).isEqualTo(42);
            assertThat(result.get(i).getLong(2)).isEqualTo(999L);
            assertThat(result.get(i).getDouble(3)).isEqualTo(3.14);
        }
    }

    @Test
    public void testConstEncodingWithNulls() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("const_nullable", DataTypes.INT().nullable())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(GenericRow.of(i, i % 3 == 0 ? null : 42));
        }

        Path path = new Path(tempDir.toString(), "const_null.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            if (i % 3 == 0) {
                assertThat(result.get(i).isNullAt(1)).isTrue();
            } else {
                assertThat(result.get(i).getInt(1)).isEqualTo(42);
            }
        }
    }

    @Test
    public void testBooleanConstEncoding() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("flag_true", DataTypes.BOOLEAN())
                        .field("flag_false", DataTypes.BOOLEAN())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(GenericRow.of(i, true, false));
        }

        Path path = new Path(tempDir.toString(), "bool_const.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getBoolean(1)).isTrue();
            assertThat(result.get(i).getBoolean(2)).isFalse();
        }
    }

    @Test
    public void testBooleanDictEncoding() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("flag", DataTypes.BOOLEAN())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(GenericRow.of(i, i % 2 == 0));
        }

        Path path = new Path(tempDir.toString(), "bool_dict.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getBoolean(1)).isEqualTo(i % 2 == 0);
        }
    }

    @Test
    public void testDictEncoding() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("status", DataTypes.INT())
                        .field("category", DataTypes.BIGINT())
                        .field("level", DataTypes.SMALLINT())
                        .build();

        int[] statuses = {1, 2, 3, 4, 5};
        long[] categories = {100L, 200L, 300L};
        short[] levels = {10, 20};

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            data.add(GenericRow.of(i, statuses[i % 5], categories[i % 3], levels[i % 2]));
        }

        Path path = new Path(tempDir.toString(), "dict_enc.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(200);
        for (int i = 0; i < 200; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getInt(1)).isEqualTo(statuses[i % 5]);
            assertThat(result.get(i).getLong(2)).isEqualTo(categories[i % 3]);
            assertThat(result.get(i).getShort(3)).isEqualTo(levels[i % 2]);
        }
    }

    @Test
    public void testDictEncodingWithNulls() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("flag", DataTypes.TINYINT().nullable())
                        .build();

        byte[] flags = {1, 2, 3};
        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(GenericRow.of(i, i % 4 == 0 ? null : flags[i % 3]));
        }

        Path path = new Path(tempDir.toString(), "dict_null.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            if (i % 4 == 0) {
                assertThat(result.get(i).isNullAt(1)).isTrue();
            } else {
                assertThat(result.get(i).getByte(1)).isEqualTo(flags[i % 3]);
            }
        }
    }

    @Test
    public void testDictEncodingBoundary() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("dict_255", DataTypes.INT())
                        .field("plain_256", DataTypes.INT())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 512; i++) {
            data.add(GenericRow.of(i % 255, i % 256));
        }

        Path path = new Path(tempDir.toString(), "dict_boundary.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(512);
        for (int i = 0; i < 512; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i % 255);
            assertThat(result.get(i).getInt(1)).isEqualTo(i % 256);
        }
    }

    @Test
    public void testFloatDictEncoding() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("f_float", DataTypes.FLOAT())
                        .field("f_double", DataTypes.DOUBLE())
                        .build();

        float[] floats = {1.5f, 2.5f, 3.5f};
        double[] doubles = {10.1, 20.2};

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(GenericRow.of(i, floats[i % 3], doubles[i % 2]));
        }

        Path path = new Path(tempDir.toString(), "float_dict.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getFloat(1)).isEqualTo(floats[i % 3]);
            assertThat(result.get(i).getDouble(2)).isEqualTo(doubles[i % 2]);
        }
    }

    @Test
    public void testAllNullEncoding() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("always_null_str", DataTypes.STRING().nullable())
                        .field("always_null_dbl", DataTypes.DOUBLE().nullable())
                        .field("always_null_int", DataTypes.INT().nullable())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            data.add(GenericRow.of(i, null, null, null));
        }

        Path path = new Path(tempDir.toString(), "all_null_enc.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(50);
        for (int i = 0; i < 50; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).isNullAt(1)).isTrue();
            assertThat(result.get(i).isNullAt(2)).isTrue();
            assertThat(result.get(i).isNullAt(3)).isTrue();
        }
    }

    @Test
    public void testMixedEncodings() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("plain_col", DataTypes.INT())
                        .field("const_col", DataTypes.BIGINT())
                        .field("dict_col", DataTypes.SMALLINT())
                        .field("all_null_col", DataTypes.DOUBLE().nullable())
                        .field("plain_str", DataTypes.STRING())
                        .build();

        short[] dictValues = {10, 20, 30, 40, 50};
        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            data.add(
                    GenericRow.of(
                            i, 999L, dictValues[i % 5], null, BinaryString.fromString("str_" + i)));
        }

        Path path = new Path(tempDir.toString(), "mixed_enc.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(1000);
        for (int i = 0; i < 1000; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getLong(1)).isEqualTo(999L);
            assertThat(result.get(i).getShort(2)).isEqualTo(dictValues[i % 5]);
            assertThat(result.get(i).isNullAt(3)).isTrue();
            assertThat(result.get(i).getString(4).toString()).isEqualTo("str_" + i);
        }
    }

    @Test
    public void testMixedEncodingsWithProjection() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("plain_col", DataTypes.INT())
                        .field("const_col", DataTypes.BIGINT())
                        .field("dict_col", DataTypes.SMALLINT())
                        .field("all_null_col", DataTypes.DOUBLE().nullable())
                        .field("plain_str", DataTypes.STRING())
                        .build();

        short[] dictValues = {10, 20, 30};
        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(
                    GenericRow.of(
                            i, 42L, dictValues[i % 3], null, BinaryString.fromString("s" + i)));
        }

        Path path = new Path(tempDir.toString(), "mixed_proj.mosaic");
        write(rowType, data, path);

        RowType projectedType =
                RowType.builder()
                        .field("dict_col", DataTypes.SMALLINT())
                        .field("const_col", DataTypes.BIGINT())
                        .build();

        List<InternalRow> result = read(rowType, projectedType, path);
        assertThat(result).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(result.get(i).getShort(0)).isEqualTo(dictValues[i % 3]);
            assertThat(result.get(i).getLong(1)).isEqualTo(42L);
        }
    }

    // ==================== Schema Prefix Compression Tests ====================

    @Test
    public void testSchemaPrefixCompression() throws IOException {
        int numCols = 100;
        RowType.Builder builder = RowType.builder();
        for (int i = 0; i < numCols; i++) {
            builder.field(
                    "com.example.sensors.signal_" + String.format("%03d", i),
                    DataTypes.DOUBLE().nullable());
        }
        RowType rowType = builder.build();

        List<InternalRow> data = new ArrayList<>();
        for (int r = 0; r < 50; r++) {
            Object[] fields = new Object[numCols];
            for (int c = 0; c < numCols; c++) {
                fields[c] = (double) (r * numCols + c);
            }
            data.add(GenericRow.of(fields));
        }

        Path path = new Path(tempDir.toString(), "prefix.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(50);
        for (int r = 0; r < 50; r++) {
            for (int c = 0; c < numCols; c++) {
                assertThat(result.get(r).getDouble(c)).isEqualTo((double) (r * numCols + c));
            }
        }

        RowType projectedType =
                RowType.builder()
                        .field("com.example.sensors.signal_050", DataTypes.DOUBLE().nullable())
                        .build();
        List<InternalRow> projected = read(rowType, projectedType, path);
        assertThat(projected).hasSize(50);
        for (int r = 0; r < 50; r++) {
            assertThat(projected.get(r).getDouble(0)).isEqualTo((double) (r * numCols + 50));
        }
    }

    @Test
    public void testSchemaMixedPrefixAndNonPrefix() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("group.a.signal_1", DataTypes.DOUBLE())
                        .field("group.a.signal_2", DataTypes.DOUBLE())
                        .field("name", DataTypes.STRING())
                        .field("group.b.signal_1", DataTypes.FLOAT())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            data.add(
                    GenericRow.of(
                            i,
                            (double) i,
                            (double) (i * 2),
                            BinaryString.fromString("n" + i),
                            (float) i));
        }

        Path path = new Path(tempDir.toString(), "mixed_prefix.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(20);
        for (int i = 0; i < 20; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getDouble(1)).isEqualTo((double) i);
            assertThat(result.get(i).getDouble(2)).isEqualTo((double) (i * 2));
            assertThat(result.get(i).getString(3).toString()).isEqualTo("n" + i);
            assertThat(result.get(i).getFloat(4)).isEqualTo((float) i);
        }
    }

    @Test
    public void testSchemaSerializationRoundTrip() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("simple", DataTypes.INT())
                        .field("a.b.col1", DataTypes.DOUBLE())
                        .field("a.b.col2", DataTypes.STRING())
                        .field("x.y.z.col3", DataTypes.BIGINT())
                        .build();

        MosaicSchema original = MosaicSchema.create(rowType, 10);
        byte[] serialized = original.serialize();
        MosaicSchema restored = MosaicSchema.deserialize(serialized);

        assertThat(restored.numBuckets()).isEqualTo(10);

        RowType projAll = rowType;
        for (int b = 0; b < 10; b++) {
            int[] origMapping = original.getProjectionMapping(b, projAll);
            int[] restoredMapping = restored.getProjectionMapping(b, projAll);
            if (origMapping == null) {
                assertThat(restoredMapping).isNull();
            } else {
                assertThat(restoredMapping).isEqualTo(origMapping);
            }
        }
    }

    // ==================== ALL_NULL Column Pruning Tests ====================

    @Test
    public void testAllNullColumnPruningRoundTrip() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("null_col_1", DataTypes.DOUBLE().nullable())
                        .field("value", DataTypes.BIGINT())
                        .field("null_col_2", DataTypes.STRING().nullable())
                        .field("null_col_3", DataTypes.INT().nullable())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(GenericRow.of(i, null, (long) i * 10, null, null));
        }

        Path path = new Path(tempDir.toString(), "prune.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).isNullAt(1)).isTrue();
            assertThat(result.get(i).getLong(2)).isEqualTo((long) i * 10);
            assertThat(result.get(i).isNullAt(3)).isTrue();
            assertThat(result.get(i).isNullAt(4)).isTrue();
        }
    }

    @Test
    public void testProjectPrunedAllNullColumn() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("always_null", DataTypes.DOUBLE().nullable())
                        .field("value", DataTypes.INT())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            data.add(GenericRow.of(i, null, i * 2));
        }

        Path path = new Path(tempDir.toString(), "proj_pruned.mosaic");
        write(rowType, data, path);

        RowType projNull =
                RowType.builder().field("always_null", DataTypes.DOUBLE().nullable()).build();
        List<InternalRow> result = read(rowType, projNull, path);
        assertThat(result).hasSize(50);
        for (int i = 0; i < 50; i++) {
            assertThat(result.get(i).isNullAt(0)).isTrue();
        }

        RowType projMixed =
                RowType.builder()
                        .field("always_null", DataTypes.DOUBLE().nullable())
                        .field("value", DataTypes.INT())
                        .build();
        List<InternalRow> result2 = read(rowType, projMixed, path);
        assertThat(result2).hasSize(50);
        for (int i = 0; i < 50; i++) {
            assertThat(result2.get(i).isNullAt(0)).isTrue();
            assertThat(result2.get(i).getInt(1)).isEqualTo(i * 2);
        }
    }

    @Test
    public void testAllNullPruningWideTable() throws IOException {
        int totalCols = 500;
        int nonNullCols = 50;

        RowType.Builder builder = RowType.builder();
        for (int i = 0; i < totalCols; i++) {
            builder.field("col_" + String.format("%04d", i), DataTypes.INT().nullable());
        }
        RowType rowType = builder.build();

        List<InternalRow> data = new ArrayList<>();
        for (int r = 0; r < 100; r++) {
            Object[] fields = new Object[totalCols];
            for (int c = 0; c < nonNullCols; c++) {
                fields[c] = r * totalCols + c;
            }
            data.add(GenericRow.of(fields));
        }

        Path path = new Path(tempDir.toString(), "wide_prune.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(100);
        for (int r = 0; r < 100; r++) {
            for (int c = 0; c < nonNullCols; c++) {
                assertThat(result.get(r).getInt(c)).isEqualTo(r * totalCols + c);
            }
            for (int c = nonNullCols; c < totalCols; c++) {
                assertThat(result.get(r).isNullAt(c)).isTrue();
            }
        }

        // Verify pruning reduced schema size (compared to no pruning)
        LocalFileIO fileIO = new LocalFileIO();
        long prunedFileSize = fileIO.getFileSize(path);

        // Write same data without pruning (multi-row-group forces no pruning)
        Path noPrunePath = new Path(tempDir.toString(), "wide_no_prune.mosaic");
        MosaicFileFormat tinyFormat =
                new MosaicFileFormat(
                        new FormatContext(
                                new Options(), 1024, 1024, MemorySize.ofBytes(1), 3, null));
        FormatWriterFactory noPruneFactory = tinyFormat.createWriterFactory(rowType);
        PositionOutputStream noPruneOut = fileIO.newOutputStream(noPrunePath, false);
        FormatWriter noPruneWriter = noPruneFactory.create(noPruneOut, "zstd");
        for (InternalRow row : data) {
            noPruneWriter.addElement(row);
        }
        noPruneWriter.close();
        noPruneOut.close();
        long noPruneSize = fileIO.getFileSize(noPrunePath);

        System.out.printf(
                "Pruning test: pruned=%,d bytes, unpruned=%,d bytes, saved=%.0f%%%n",
                prunedFileSize, noPruneSize, (1.0 - (double) prunedFileSize / noPruneSize) * 100);
        assertThat(prunedFileSize).isLessThan(noPruneSize);
    }

    @Test
    public void testMultiRowGroupNoPruning() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("nullable", DataTypes.INT().nullable())
                        .build();

        MosaicFileFormat format =
                new MosaicFileFormat(
                        new FormatContext(
                                new Options(), 1024, 1024, MemorySize.ofBytes(1), 3, null));

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(GenericRow.of(i, i == 0 ? 42 : null));
        }

        Path path = new Path(tempDir.toString(), "multi_rg_no_prune.mosaic");
        LocalFileIO fileIO = new LocalFileIO();
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = writerFactory.create(out, "zstd");
        for (InternalRow row : data) {
            writer.addElement(row);
        }
        writer.close();
        out.close();

        FormatReaderFactory readerFactory = format.createReaderFactory(rowType, rowType, null);
        List<InternalRow> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)))) {
            reader.forEachRemaining(
                    row -> {
                        Object[] fields = new Object[rowType.getFieldCount()];
                        for (int i = 0; i < fields.length; i++) {
                            if (!row.isNullAt(i)) {
                                fields[i] =
                                        InternalRow.createFieldGetter(rowType.getTypeAt(i), i)
                                                .getFieldOrNull(row);
                            }
                        }
                        result.add(GenericRow.of(fields));
                    });
        }

        assertThat(result).hasSize(100);
        assertThat(result.get(0).getInt(0)).isEqualTo(0);
        assertThat(result.get(0).getInt(1)).isEqualTo(42);
        for (int i = 1; i < 100; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).isNullAt(1)).isTrue();
        }
    }

    @Test
    public void testAllColumnsAllNull() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("a", DataTypes.INT().nullable())
                        .field("b", DataTypes.STRING().nullable())
                        .field("c", DataTypes.DOUBLE().nullable())
                        .build();

        List<InternalRow> data = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            data.add(GenericRow.of(null, null, null));
        }

        Path path = new Path(tempDir.toString(), "all_cols_null.mosaic");
        write(rowType, data, path);
        List<InternalRow> result = read(rowType, rowType, path);

        assertThat(result).hasSize(30);
        for (int i = 0; i < 30; i++) {
            assertThat(result.get(i).isNullAt(0)).isTrue();
            assertThat(result.get(i).isNullAt(1)).isTrue();
            assertThat(result.get(i).isNullAt(2)).isTrue();
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
