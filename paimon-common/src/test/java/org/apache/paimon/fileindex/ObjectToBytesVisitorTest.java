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
import java.util.function.Function;

/** Tests for {@link ObjectToBytesVisitor}. */
public class ObjectToBytesVisitorTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testCharType() {
        Function<Object, byte[]> function =
                DataTypes.CHAR(10).accept(ObjectToBytesVisitor.INSTANCE);
        BinaryString binaryString = BinaryString.fromString(randomString(10));
        Assertions.assertThat(function.apply(binaryString)).containsExactly(binaryString.toBytes());
    }

    @Test
    public void testVarCharType() {
        Function<Object, byte[]> function =
                DataTypes.VARCHAR(10).accept(ObjectToBytesVisitor.INSTANCE);
        BinaryString binaryString = BinaryString.fromString(randomString(10));
        Assertions.assertThat(function.apply(binaryString)).containsExactly(binaryString.toBytes());
    }

    @Test
    public void testBooleanType() {
        Function<Object, byte[]> function =
                DataTypes.BOOLEAN().accept(ObjectToBytesVisitor.INSTANCE);
        Boolean b = RANDOM.nextBoolean();
        Assertions.assertThat(function.apply(b))
                .containsExactly(b ? new byte[] {0x01} : new byte[] {0x00});
    }

    @Test
    public void testBinaryType() {
        Function<Object, byte[]> function =
                DataTypes.BINARY(10).accept(ObjectToBytesVisitor.INSTANCE);
        byte[] b = new byte[10];
        RANDOM.nextBytes(b);
        Assertions.assertThat(function.apply(b)).containsExactly(b);
    }

    @Test
    public void testVarBinaryType() {
        Function<Object, byte[]> function =
                DataTypes.VARBINARY(10).accept(ObjectToBytesVisitor.INSTANCE);
        byte[] b = new byte[10];
        RANDOM.nextBytes(b);
        Assertions.assertThat(function.apply(b)).containsExactly(b);
    }

    @Test
    public void testDecimalType() {
        Function<Object, byte[]> function =
                DataTypes.DECIMAL(10, 5).accept(ObjectToBytesVisitor.INSTANCE);
        Decimal decimal = Decimal.fromBigDecimal(new BigDecimal("0.00123"), 10, 5);
        Assertions.assertThat(function.apply(decimal)).containsExactly(decimal.toUnscaledBytes());
    }

    @Test
    public void testTinyIntType() {
        Function<Object, byte[]> function =
                DataTypes.TINYINT().accept(ObjectToBytesVisitor.INSTANCE);
        Byte c = (byte) RANDOM.nextInt();
        Assertions.assertThat(function.apply(c)).containsExactly(c);
    }

    @Test
    public void testSmallIntType() {
        Function<Object, byte[]> function =
                DataTypes.SMALLINT().accept(ObjectToBytesVisitor.INSTANCE);
        short c = (short) RANDOM.nextInt();
        Assertions.assertThat(function.apply(c))
                .containsExactly((byte) (c & 0xff), (byte) (c >> 8 & 0xff));
    }

    @Test
    public void testIntType() {
        Function<Object, byte[]> function = DataTypes.INT().accept(ObjectToBytesVisitor.INSTANCE);
        int c = RANDOM.nextInt();
        Assertions.assertThat(function.apply(c))
                .containsExactly((ObjectToBytesVisitor.intToBytes(c)));
    }

    @Test
    public void testBigIntType() {
        Function<Object, byte[]> function =
                DataTypes.BIGINT().accept(ObjectToBytesVisitor.INSTANCE);
        long c = RANDOM.nextLong();
        Assertions.assertThat(function.apply(c))
                .containsExactly((ObjectToBytesVisitor.longToBytes(c)));
    }

    @Test
    public void testFloatType() {
        Function<Object, byte[]> function = DataTypes.FLOAT().accept(ObjectToBytesVisitor.INSTANCE);
        float c = RANDOM.nextFloat();
        Assertions.assertThat(function.apply(c))
                .containsExactly((ObjectToBytesVisitor.intToBytes(Float.floatToIntBits(c))));
    }

    @Test
    public void testDoubleType() {
        Function<Object, byte[]> function =
                DataTypes.DOUBLE().accept(ObjectToBytesVisitor.INSTANCE);
        double c = RANDOM.nextDouble();
        Assertions.assertThat(function.apply(c))
                .containsExactly((ObjectToBytesVisitor.longToBytes(Double.doubleToLongBits(c))));
    }

    @Test
    public void testDateType() {
        Function<Object, byte[]> function = DataTypes.DATE().accept(ObjectToBytesVisitor.INSTANCE);
        int c = RANDOM.nextInt();
        Assertions.assertThat(function.apply(c))
                .containsExactly((ObjectToBytesVisitor.intToBytes(c)));
    }

    @Test
    public void testTimestampType() {
        Function<Object, byte[]> function =
                DataTypes.TIMESTAMP().accept(ObjectToBytesVisitor.INSTANCE);
        Timestamp c = Timestamp.fromEpochMillis(System.currentTimeMillis());
        Assertions.assertThat(function.apply(c))
                .containsExactly((ObjectToBytesVisitor.longToBytes(c.getMillisecond())));
    }

    @Test
    public void testLocalZonedTimestampType() {
        Function<Object, byte[]> function =
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().accept(ObjectToBytesVisitor.INSTANCE);
        Timestamp c = Timestamp.fromEpochMillis(System.currentTimeMillis());
        Assertions.assertThat(function.apply(c))
                .containsExactly((ObjectToBytesVisitor.longToBytes(c.getMillisecond())));
    }

    @Test
    public void testArrayType() {
        Function<Object, byte[]> function =
                DataTypes.ARRAY(DataTypes.INT()).accept(ObjectToBytesVisitor.INSTANCE);
        int[] ins = new int[] {0, 1, 3, 4};
        GenericArray genericArray = new GenericArray(ins);
        byte[] b = new byte[16];
        for (int i = 0; i < 4; i++) {
            System.arraycopy(ObjectToBytesVisitor.intToBytes(ins[i]), 0, b, 4 * i, 4);
        }
        Assertions.assertThat(function.apply(genericArray)).containsExactly(b);
    }

    @Test
    public void testMapType() {
        Function<Object, byte[]> function =
                DataTypes.MAP(DataTypes.INT(), DataTypes.INT())
                        .accept(ObjectToBytesVisitor.INSTANCE);
        Map<Integer, Integer> map = new HashMap<>();

        map.put(1, 1);
        map.put(2, 2);
        GenericMap genericMap = new GenericMap(map);
        byte[] b = new byte[16];
        System.arraycopy(ObjectToBytesVisitor.intToBytes(1), 0, b, 0, 4);
        System.arraycopy(ObjectToBytesVisitor.intToBytes(1), 0, b, 4, 4);
        System.arraycopy(ObjectToBytesVisitor.intToBytes(2), 0, b, 8, 4);
        System.arraycopy(ObjectToBytesVisitor.intToBytes(2), 0, b, 12, 4);
        Assertions.assertThat(function.apply(genericMap)).containsExactly(b);
    }

    @Test
    public void testMultisetType() {
        Function<Object, byte[]> function =
                DataTypes.MULTISET(DataTypes.INT()).accept(ObjectToBytesVisitor.INSTANCE);
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 1);
        map.put(2, 2);
        GenericMap genericMap = new GenericMap(map);
        byte[] b = new byte[8];
        System.arraycopy(ObjectToBytesVisitor.intToBytes(1), 0, b, 0, 4);
        System.arraycopy(ObjectToBytesVisitor.intToBytes(2), 0, b, 4, 4);
        Assertions.assertThat(function.apply(genericMap)).containsExactly(b);
    }

    @Test
    public void testRowType() {
        Function<Object, byte[]> function =
                DataTypes.ROW(DataTypes.INT()).accept(ObjectToBytesVisitor.INSTANCE);
        Assertions.assertThat(function.apply(GenericRow.of(1)))
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
