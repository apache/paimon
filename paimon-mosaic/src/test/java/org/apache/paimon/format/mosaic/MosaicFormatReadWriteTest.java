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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Round-trip read/write tests for Mosaic format. */
class MosaicFormatReadWriteTest extends FormatReadWriteTest {

    MosaicFormatReadWriteTest() {
        super("mosaic");
    }

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Mosaic native library not available");
    }

    @Override
    protected FileFormat fileFormat() {
        return new MosaicFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
    }

    @Override
    public String compression() {
        return "zstd";
    }

    @Override
    public boolean supportNestedReadPruning() {
        return false;
    }

    @Override
    protected RowType rowTypeForFullTypesTest() {
        return RowType.builder()
                .field("f_int", DataTypes.INT().notNull())
                .field("f_string", DataTypes.STRING())
                .field("f_double", DataTypes.DOUBLE().notNull())
                .field("f_boolean", DataTypes.BOOLEAN())
                .field("f_tinyint", DataTypes.TINYINT())
                .field("f_smallint", DataTypes.SMALLINT())
                .field("f_bigint", DataTypes.BIGINT())
                .field("f_float", DataTypes.FLOAT())
                .field("f_binary", DataTypes.BYTES())
                .field("f_date", DataTypes.DATE())
                .field("f_timestamp3", DataTypes.TIMESTAMP(3))
                .field("f_timestamp6", DataTypes.TIMESTAMP(6))
                .field("f_decimal_5_2", DataTypes.DECIMAL(5, 2))
                .field("f_decimal_20_0", DataTypes.DECIMAL(20, 0))
                .field("f_array", DataTypes.ARRAY(DataTypes.INT()))
                .field("f_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                .build();
    }

    @Override
    protected GenericRow expectedRowForFullTypesTest() {
        return GenericRow.of(
                42,
                BinaryString.fromString("hello mosaic"),
                3.14d,
                true,
                (byte) 7,
                (short) 256,
                9876543210L,
                1.5f,
                new byte[] {1, 2, 3},
                18000,
                Timestamp.fromEpochMillis(1700000000000L),
                Timestamp.fromMicros(1700000000000000L),
                Decimal.fromBigDecimal(new BigDecimal("123.45"), 5, 2),
                Decimal.fromBigDecimal(new BigDecimal("12345678901234567890"), 20, 0),
                new GenericArray(new int[] {10, 20, 30}),
                new GenericMap(buildMap()));
    }

    private static Map<BinaryString, Integer> buildMap() {
        Map<BinaryString, Integer> map = new HashMap<>();
        map.put(BinaryString.fromString("k1"), 1);
        map.put(BinaryString.fromString("k2"), 2);
        return map;
    }

    @Override
    protected void validateFullTypesResult(InternalRow actual, InternalRow expected) {
        for (int i = 0; i < 16; i++) {
            if (expected.isNullAt(i)) {
                assertThat(actual.isNullAt(i)).isTrue();
            }
        }
        assertThat(actual.getInt(0)).isEqualTo(expected.getInt(0));
        assertThat(actual.getString(1)).isEqualTo(expected.getString(1));
        assertThat(actual.getDouble(2)).isEqualTo(expected.getDouble(2));
        assertThat(actual.getBoolean(3)).isEqualTo(expected.getBoolean(3));
        assertThat(actual.getByte(4)).isEqualTo(expected.getByte(4));
        assertThat(actual.getShort(5)).isEqualTo(expected.getShort(5));
        assertThat(actual.getLong(6)).isEqualTo(expected.getLong(6));
        assertThat(actual.getFloat(7)).isEqualTo(expected.getFloat(7));
        assertThat(actual.getBinary(8)).isEqualTo(expected.getBinary(8));
        assertThat(actual.getInt(9)).isEqualTo(expected.getInt(9));
        assertThat(actual.getTimestamp(10, 3)).isEqualTo(expected.getTimestamp(10, 3));
        assertThat(actual.getTimestamp(11, 6)).isEqualTo(expected.getTimestamp(11, 6));
        assertThat(actual.getDecimal(12, 5, 2)).isEqualTo(expected.getDecimal(12, 5, 2));
        assertThat(actual.getDecimal(13, 20, 0)).isEqualTo(expected.getDecimal(13, 20, 0));
        assertThat(actual.getArray(14).toIntArray()).isEqualTo(expected.getArray(14).toIntArray());
        assertThat(toJavaMap(actual.getMap(15))).isEqualTo(toJavaMap(expected.getMap(15)));
    }

    private static Map<String, Integer> toJavaMap(InternalMap m) {
        Map<String, Integer> out = new HashMap<>();
        for (int i = 0; i < m.size(); i++) {
            out.put(m.keyArray().getString(i).toString(), m.valueArray().getInt(i));
        }
        return out;
    }

    /** ARRAY/MAP with null fields, null elements and empty collections. */
    @Test
    void testArrayMapNullAndEmpty() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("f_arr", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("f_map", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        .build();

        Map<Integer, BinaryString> m0 = new HashMap<>();
        m0.put(1, BinaryString.fromString("a"));
        InternalRow populated =
                GenericRow.of(
                        new GenericArray(new BinaryString[] {BinaryString.fromString("x"), null}),
                        new GenericMap(m0));
        InternalRow nulls = GenericRow.of(null, null);
        InternalRow empties =
                GenericRow.of(
                        new GenericArray(new BinaryString[0]), new GenericMap(new HashMap<>()));

        FormatWriterFactory factory = fileFormat().createWriterFactory(rowType);
        write(factory, file, populated, nulls, empties);

        // Materialize inside the callback: reader reuses native-backed vectors per batch.
        List<String> arrSummary = new ArrayList<>();
        List<String> mapSummary = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                fileFormat()
                        .createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            reader.forEachRemaining(
                    r -> {
                        arrSummary.add(
                                r.isNullAt(0)
                                        ? "null"
                                        : r.getArray(0).size()
                                                + ":"
                                                + (r.getArray(0).size() > 1
                                                        && r.getArray(0).isNullAt(1)));
                        mapSummary.add(r.isNullAt(1) ? "null" : String.valueOf(r.getMap(1).size()));
                    });
        }

        assertThat(arrSummary).containsExactly("2:true", "null", "0:false");
        assertThat(mapSummary).containsExactly("1", "null", "0");
    }

    private static boolean isNativeAvailable() {
        try {
            Class.forName("org.apache.paimon.mosaic.NativeLib");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
