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

package org.apache.paimon.fileindex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;

/** Tests for {@link InternalRowToBytesVisitor}. */
public class InternalRowToBytesReaderTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testCharType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.CHAR(10).accept(InternalRowToBytesVisitor.INSTANCE);
        BinaryString binaryString = BinaryString.fromString(randomString(10));
        Assertions.assertThat(function.apply(GenericRow.of(binaryString), 0))
                .containsExactly(binaryString.toBytes());
    }

    @Test
    public void testVarCharType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.VARCHAR(10).accept(InternalRowToBytesVisitor.INSTANCE);
        BinaryString binaryString = BinaryString.fromString(randomString(10));
        Assertions.assertThat(function.apply(GenericRow.of(binaryString), 0))
                .containsExactly(binaryString.toBytes());
    }

    @Test
    public void testBooleanType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.BOOLEAN().accept(InternalRowToBytesVisitor.INSTANCE);
        Boolean b = RANDOM.nextBoolean();
        Assertions.assertThat(function.apply(GenericRow.of(b), 0))
                .containsExactly(b ? new byte[] {0x01} : new byte[] {0x00});
    }

    @Test
    public void testBinaryType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.BINARY(10).accept(InternalRowToBytesVisitor.INSTANCE);
        byte[] b = new byte[10];
        RANDOM.nextBytes(b);
        Assertions.assertThat(function.apply(GenericRow.of(b), 0)).containsExactly(b);
    }

    @Test
    public void testVarBinaryType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.VARBINARY(10).accept(InternalRowToBytesVisitor.INSTANCE);
        byte[] b = new byte[10];
        RANDOM.nextBytes(b);
        Assertions.assertThat(function.apply(GenericRow.of(b), 0)).containsExactly(b);
    }

    @Test
    public void testDecimalType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.DECIMAL(10, 5).accept(InternalRowToBytesVisitor.INSTANCE);
        Decimal decimal = Decimal.fromBigDecimal(new BigDecimal("0.00123"), 10, 5);
        Assertions.assertThat(function.apply(GenericRow.of(decimal), 0))
                .containsExactly(decimal.toUnscaledBytes());
    }

    @Test
    public void testTinyIntType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.TINYINT().accept(InternalRowToBytesVisitor.INSTANCE);
        Byte c = (byte) RANDOM.nextInt();
        Assertions.assertThat(function.apply(GenericRow.of(c), 0)).containsExactly(c);
    }

    @Test
    public void testSmallIntType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.SMALLINT().accept(InternalRowToBytesVisitor.INSTANCE);
        short c = (short) RANDOM.nextInt();
        Assertions.assertThat(function.apply(GenericRow.of(c), 0))
                .containsExactly((byte) (c & 0xff), (byte) (c >> 8 & 0xff));
    }

    @Test
    public void testIntType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.INT().accept(InternalRowToBytesVisitor.INSTANCE);
        int c = RANDOM.nextInt();
        Assertions.assertThat(function.apply(GenericRow.of(c), 0))
                .containsExactly((ObjectToBytesVisitor.intToBytes(c)));
    }

    @Test
    public void testBigIntType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.BIGINT().accept(InternalRowToBytesVisitor.INSTANCE);
        long c = RANDOM.nextLong();
        Assertions.assertThat(function.apply(GenericRow.of(c), 0))
                .containsExactly((ObjectToBytesVisitor.longToBytes(c)));
    }

    @Test
    public void testFloatType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.FLOAT().accept(InternalRowToBytesVisitor.INSTANCE);
        float c = RANDOM.nextFloat();
        Assertions.assertThat(function.apply(GenericRow.of(c), 0))
                .containsExactly((ObjectToBytesVisitor.intToBytes(Float.floatToIntBits(c))));
    }

    @Test
    public void testDoubleType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.DOUBLE().accept(InternalRowToBytesVisitor.INSTANCE);
        double c = RANDOM.nextDouble();
        Assertions.assertThat(function.apply(GenericRow.of(c), 0))
                .containsExactly((ObjectToBytesVisitor.longToBytes(Double.doubleToLongBits(c))));
    }

    @Test
    public void testDateType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.DATE().accept(InternalRowToBytesVisitor.INSTANCE);
        int c = RANDOM.nextInt();
        Assertions.assertThat(function.apply(GenericRow.of(c), 0))
                .containsExactly((ObjectToBytesVisitor.intToBytes(c)));
    }

    @Test
    public void testTimestampType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.TIMESTAMP().accept(InternalRowToBytesVisitor.INSTANCE);
        Timestamp c = Timestamp.fromEpochMillis(System.currentTimeMillis());
        Assertions.assertThat(function.apply(GenericRow.of(c), 0))
                .containsExactly((ObjectToBytesVisitor.longToBytes(c.getMillisecond())));
    }

    @Test
    public void testLocalZonedTimestampType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()
                        .accept(InternalRowToBytesVisitor.INSTANCE);
        Timestamp c = Timestamp.fromEpochMillis(System.currentTimeMillis());
        Assertions.assertThat(function.apply(GenericRow.of(c), 0))
                .containsExactly((ObjectToBytesVisitor.longToBytes(c.getMillisecond())));
    }

    @Test
    public void testArrayType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.ARRAY(DataTypes.INT()).accept(InternalRowToBytesVisitor.INSTANCE);
        int[] ins = new int[] {0, 1, 3, 4};
        GenericArray genericArray = new GenericArray(ins);
        byte[] b = new byte[16];
        for (int i = 0; i < 4; i++) {
            System.arraycopy(ObjectToBytesVisitor.intToBytes(ins[i]), 0, b, 4 * i, 4);
        }
        Assertions.assertThat(function.apply(GenericRow.of(genericArray), 0)).containsExactly(b);
    }

    @Test
    public void testMapType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.MAP(DataTypes.INT(), DataTypes.INT())
                        .accept(InternalRowToBytesVisitor.INSTANCE);
        Map<Integer, Integer> map = new HashMap<>();

        map.put(1, 1);
        map.put(2, 2);
        GenericMap genericMap = new GenericMap(map);
        byte[] b = new byte[16];
        System.arraycopy(ObjectToBytesVisitor.intToBytes(1), 0, b, 0, 4);
        System.arraycopy(ObjectToBytesVisitor.intToBytes(1), 0, b, 4, 4);
        System.arraycopy(ObjectToBytesVisitor.intToBytes(2), 0, b, 8, 4);
        System.arraycopy(ObjectToBytesVisitor.intToBytes(2), 0, b, 12, 4);
        Assertions.assertThat(function.apply(GenericRow.of(genericMap), 0)).containsExactly(b);
    }

    @Test
    public void testMultisetType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.MULTISET(DataTypes.INT()).accept(InternalRowToBytesVisitor.INSTANCE);
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 1);
        map.put(2, 2);
        GenericMap genericMap = new GenericMap(map);
        byte[] b = new byte[8];
        System.arraycopy(ObjectToBytesVisitor.intToBytes(1), 0, b, 0, 4);
        System.arraycopy(ObjectToBytesVisitor.intToBytes(2), 0, b, 4, 4);
        Assertions.assertThat(function.apply(GenericRow.of(genericMap), 0)).containsExactly(b);
    }

    @Test
    public void testRowType() {
        BiFunction<DataGetters, Integer, byte[]> function =
                DataTypes.ROW(DataTypes.INT()).accept(InternalRowToBytesVisitor.INSTANCE);
        Assertions.assertThat(function.apply(GenericRow.of(GenericRow.of(1)), 0))
                .containsExactly(ObjectToBytesVisitor.intToBytes(1));
    }

    public static String randomString(int length) {
        byte[] buffer = new byte[length];

        for (int i = 0; i < length; i += 1) {
            buffer[i] = (byte) ('a' + RANDOM.nextInt(26));
        }

        return new String(buffer);
    }
}
