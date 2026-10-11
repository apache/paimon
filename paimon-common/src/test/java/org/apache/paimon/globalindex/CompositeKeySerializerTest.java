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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for serialized global index key ordering. */
class CompositeKeySerializerTest {

    @ParameterizedTest
    @MethodSource("typesAndSortedValues")
    void testCompositeSliceOrdering(DataType type, List<Object> values) {
        KeySerializer serializer =
                new CompositeKeySerializer(RowType.of(DataTypes.STRING(), type, DataTypes.INT()));
        List<GenericRow> keys = new ArrayList<>();
        keys.add(GenericRow.of(null, null, null));
        char[] prefix = new char[160];
        Arrays.fill(prefix, 'a');
        BinaryString leading = BinaryString.fromString(new String(prefix));
        List<Object> nullable = new ArrayList<>();
        nullable.add(null);
        nullable.addAll(values);
        for (Object value : nullable) {
            keys.add(GenericRow.of(leading, value, null));
            keys.add(GenericRow.of(leading, value, -1));
            keys.add(GenericRow.of(leading, value, 1));
        }
        keys.add(GenericRow.of(BinaryString.fromString("z"), null, null));
        assertOrdering(serializer, keys);
    }

    @ParameterizedTest
    @MethodSource("typesAndSortedValues")
    void testScalarSliceOrdering(DataType type, List<Object> values) {
        assertOrdering(KeySerializer.create(type), values);
    }

    @Test
    void testSliceComparisonDoesNotDeserializeRows() {
        KeySerializer serializer =
                new CompositeKeySerializer(RowType.of(DataTypes.STRING(), DataTypes.INT())) {
                    @Override
                    public Object deserialize(MemorySlice data) {
                        throw new AssertionError("Slice comparison must not deserialize a row.");
                    }
                };
        assertOrdering(
                serializer,
                Arrays.asList(
                        GenericRow.of(null, null),
                        GenericRow.of(BinaryString.fromString("a"), -1),
                        GenericRow.of(BinaryString.fromString("a"), 1),
                        GenericRow.of(BinaryString.fromString("b"), -1)));
    }

    private void assertOrdering(KeySerializer serializer, List<?> keys) {
        Comparator<MemorySlice> comparator = serializer.createSliceComparator();
        List<MemorySlice> left = new ArrayList<>();
        List<MemorySlice> right = new ArrayList<>();
        for (Object key : keys) {
            byte[] bytes = serializer.serialize(key);
            left.add(sliceAtOffset(bytes, 5));
            right.add(sliceAtOffset(bytes, 13));
        }
        for (int i = 0; i < keys.size(); i++) {
            for (int j = 0; j < keys.size(); j++) {
                assertThat(Integer.signum(comparator.compare(left.get(i), right.get(j))))
                        .as("ordering of %s and %s", keys.get(i), keys.get(j))
                        .isEqualTo(Integer.signum(i - j));
            }
        }
    }

    private MemorySlice sliceAtOffset(byte[] bytes, int offset) {
        byte[] padded = new byte[offset + bytes.length + 7];
        Arrays.fill(padded, (byte) 0xff);
        System.arraycopy(bytes, 0, padded, offset, bytes.length);
        return MemorySlice.wrap(padded).slice(offset, bytes.length);
    }

    private static Stream<Arguments> typesAndSortedValues() {
        List<Object> strings =
                Arrays.asList(
                        BinaryString.fromString(""),
                        BinaryString.fromString("a"),
                        BinaryString.fromString("a\u0000b"),
                        BinaryString.fromString("aa"),
                        BinaryString.fromString("b"),
                        BinaryString.fromString("中文"));
        List<Object> timestamps =
                Arrays.asList(
                        Timestamp.fromEpochMillis(-1),
                        Timestamp.fromEpochMillis(0),
                        Timestamp.fromEpochMillis(1));
        List<Object> preciseTimestamps =
                Arrays.asList(
                        Timestamp.fromEpochMillis(-1, 999999),
                        Timestamp.fromEpochMillis(0),
                        Timestamp.fromEpochMillis(0, 1),
                        Timestamp.fromEpochMillis(0, 999999),
                        Timestamp.fromEpochMillis(1));
        return Stream.of(
                Arguments.of(DataTypes.BOOLEAN(), Arrays.asList(false, true)),
                Arguments.of(
                        DataTypes.TINYINT(),
                        Arrays.asList(Byte.MIN_VALUE, (byte) 0, Byte.MAX_VALUE)),
                Arguments.of(
                        DataTypes.SMALLINT(),
                        Arrays.asList(Short.MIN_VALUE, (short) 0, Short.MAX_VALUE)),
                Arguments.of(
                        DataTypes.INT(),
                        Arrays.asList(Integer.MIN_VALUE, -1, 0, 256, Integer.MAX_VALUE)),
                Arguments.of(
                        DataTypes.BIGINT(), Arrays.asList(Long.MIN_VALUE, -1L, 0L, Long.MAX_VALUE)),
                Arguments.of(
                        DataTypes.FLOAT(),
                        Arrays.asList(
                                Float.NEGATIVE_INFINITY,
                                -1.0f,
                                -0.0f,
                                0.0f,
                                Float.POSITIVE_INFINITY,
                                Float.NaN)),
                Arguments.of(
                        DataTypes.DOUBLE(),
                        Arrays.asList(
                                Double.NEGATIVE_INFINITY,
                                -1.0d,
                                -0.0d,
                                0.0d,
                                Double.POSITIVE_INFINITY,
                                Double.NaN)),
                Arguments.of(DataTypes.CHAR(256), strings),
                Arguments.of(DataTypes.STRING(), strings),
                Arguments.of(
                        DataTypes.DECIMAL(18, 3),
                        decimals(18, 3, "-999999999999999.999", "0", "999999999999999.999")),
                Arguments.of(
                        DataTypes.DECIMAL(38, 18),
                        decimals(
                                38,
                                18,
                                "-99999999999999999999.999999999999999999",
                                "0",
                                "99999999999999999999.999999999999999999")),
                Arguments.of(DataTypes.DATE(), Arrays.asList(-1, 0, 1)),
                Arguments.of(DataTypes.TIME(), Arrays.asList(0, 1, 86399999)),
                Arguments.of(DataTypes.TIMESTAMP(3), timestamps),
                Arguments.of(DataTypes.TIMESTAMP(9), preciseTimestamps),
                Arguments.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), timestamps),
                Arguments.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9), preciseTimestamps));
    }

    private static List<Object> decimals(int precision, int scale, String... values) {
        List<Object> result = new ArrayList<>();
        for (String value : values) {
            result.add(Decimal.fromBigDecimal(new BigDecimal(value), precision, scale));
        }
        return result;
    }
}
