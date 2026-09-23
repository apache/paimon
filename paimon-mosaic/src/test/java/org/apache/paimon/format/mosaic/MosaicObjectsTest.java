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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MosaicObjects}. */
class MosaicObjectsTest {

    @Test
    void testNullBytes() {
        assertThat(MosaicObjects.convertStatsValue(null, DataTypes.INT())).isNull();
    }

    @Test
    void testEmptyBytes() {
        assertThat(MosaicObjects.convertStatsValue(new byte[0], DataTypes.INT())).isNull();
    }

    @Test
    void testBoolean() {
        assertThat(MosaicObjects.convertStatsValue(new byte[] {1}, DataTypes.BOOLEAN()))
                .isEqualTo(true);
        assertThat(MosaicObjects.convertStatsValue(new byte[] {0}, DataTypes.BOOLEAN()))
                .isEqualTo(false);
    }

    @Test
    void testTinyInt() {
        assertThat(MosaicObjects.convertStatsValue(new byte[] {42}, DataTypes.TINYINT()))
                .isEqualTo((byte) 42);
        assertThat(MosaicObjects.convertStatsValue(new byte[] {(byte) -1}, DataTypes.TINYINT()))
                .isEqualTo((byte) -1);
    }

    @Test
    void testSmallInt() {
        byte[] bytes = ByteBuffer.allocate(2).putShort((short) 1234).array();
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.SMALLINT()))
                .isEqualTo((short) 1234);
    }

    @Test
    void testInt() {
        byte[] bytes = ByteBuffer.allocate(4).putInt(123456).array();
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.INT())).isEqualTo(123456);
    }

    @Test
    void testIntNegative() {
        byte[] bytes = ByteBuffer.allocate(4).putInt(-999).array();
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.INT())).isEqualTo(-999);
    }

    @Test
    void testBigInt() {
        byte[] bytes = ByteBuffer.allocate(8).putLong(9876543210L).array();
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.BIGINT()))
                .isEqualTo(9876543210L);
    }

    @Test
    void testFloat() {
        byte[] bytes = ByteBuffer.allocate(4).putFloat(3.14f).array();
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.FLOAT())).isEqualTo(3.14f);
    }

    @Test
    void testDouble() {
        byte[] bytes = ByteBuffer.allocate(8).putDouble(2.718281828).array();
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.DOUBLE()))
                .isEqualTo(2.718281828);
    }

    @Test
    void testVarChar() {
        byte[] bytes = "hello".getBytes();
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.STRING()))
                .isEqualTo(BinaryString.fromString("hello"));
    }

    @Test
    void testBinary() {
        byte[] bytes = new byte[] {1, 2, 3, 4, 5};
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.BYTES())).isEqualTo(bytes);
    }

    @Test
    void testDate() {
        byte[] bytes = ByteBuffer.allocate(4).putInt(18000).array();
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.DATE())).isEqualTo(18000);
    }

    @Test
    void testTimestampMillis() {
        long millis = 1700000000000L;
        byte[] bytes = ByteBuffer.allocate(8).putLong(millis).array();
        Object result = MosaicObjects.convertStatsValue(bytes, DataTypes.TIMESTAMP(3));
        assertThat(result).isEqualTo(Timestamp.fromEpochMillis(millis));
    }

    @Test
    void testTimestampMicros() {
        long micros = 1700000000000000L;
        byte[] bytes = ByteBuffer.allocate(8).putLong(micros).array();
        Object result = MosaicObjects.convertStatsValue(bytes, DataTypes.TIMESTAMP(6));
        assertThat(result).isEqualTo(Timestamp.fromMicros(micros));
    }

    @Test
    void testDecimal() {
        // 1000 in big-endian two's complement = 0x03E8
        byte[] beBytes = new byte[] {0x03, (byte) 0xE8};
        Object result = MosaicObjects.convertStatsValue(beBytes, DataTypes.DECIMAL(10, 2));
        assertThat(result).isInstanceOf(Decimal.class);
        Decimal decimal = (Decimal) result;
        assertThat(decimal.toBigDecimal().intValue()).isEqualTo(10);
    }

    @Test
    void testTimestampNanos() {
        long millis = 1700000000123L;
        int nanosOfMilli = 456789;
        byte[] bytes = ByteBuffer.allocate(12).putLong(millis).putInt(nanosOfMilli).array();
        Object result = MosaicObjects.convertStatsValue(bytes, DataTypes.TIMESTAMP(9));
        assertThat(result).isEqualTo(Timestamp.fromEpochMillis(millis, nanosOfMilli));
    }

    @Test
    void testTimestampNanosPrecision7() {
        long millis = 1700000000000L;
        int nanosOfMilli = 100000;
        byte[] bytes = ByteBuffer.allocate(12).putLong(millis).putInt(nanosOfMilli).array();
        Object result = MosaicObjects.convertStatsValue(bytes, DataTypes.TIMESTAMP(7));
        assertThat(result).isEqualTo(Timestamp.fromEpochMillis(millis, nanosOfMilli));
    }

    @Test
    void testTimestampWithLocalTimeZoneMillis() {
        long millis = 1700000000000L;
        byte[] bytes = ByteBuffer.allocate(8).putLong(millis).array();
        Object result =
                MosaicObjects.convertStatsValue(bytes, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
        assertThat(result).isEqualTo(Timestamp.fromEpochMillis(millis));
    }

    @Test
    void testTimestampWithLocalTimeZoneMicros() {
        long micros = 1700000000000000L;
        byte[] bytes = ByteBuffer.allocate(8).putLong(micros).array();
        Object result =
                MosaicObjects.convertStatsValue(bytes, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6));
        assertThat(result).isEqualTo(Timestamp.fromMicros(micros));
    }

    @Test
    void testTimestampWithLocalTimeZoneNanos() {
        long millis = 1700000000123L;
        int nanosOfMilli = 456789;
        byte[] bytes = ByteBuffer.allocate(12).putLong(millis).putInt(nanosOfMilli).array();
        Object result =
                MosaicObjects.convertStatsValue(bytes, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9));
        assertThat(result).isEqualTo(Timestamp.fromEpochMillis(millis, nanosOfMilli));
    }

    @Test
    void testEmptyStringVarChar() {
        Object result = MosaicObjects.convertStatsValue(new byte[0], DataTypes.STRING());
        assertThat(result).isEqualTo(BinaryString.fromString(""));
    }

    @Test
    void testEmptyBinary() {
        Object result = MosaicObjects.convertStatsValue(new byte[0], DataTypes.BYTES());
        assertThat(result).isEqualTo(new byte[0]);
    }

    @Test
    void testUnsupportedTypeReturnsNull() {
        byte[] bytes = new byte[] {1, 2, 3};
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.ARRAY(DataTypes.INT())))
                .isNull();
    }
}
