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
import java.nio.ByteOrder;

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
        byte[] bytes = toLE(ByteBuffer.allocate(2).putShort((short) 1234));
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.SMALLINT()))
                .isEqualTo((short) 1234);
    }

    @Test
    void testInt() {
        byte[] bytes = toLE(ByteBuffer.allocate(4).putInt(123456));
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.INT())).isEqualTo(123456);
    }

    @Test
    void testIntNegative() {
        byte[] bytes = toLE(ByteBuffer.allocate(4).putInt(-999));
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.INT())).isEqualTo(-999);
    }

    @Test
    void testBigInt() {
        byte[] bytes = toLE(ByteBuffer.allocate(8).putLong(9876543210L));
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.BIGINT()))
                .isEqualTo(9876543210L);
    }

    @Test
    void testFloat() {
        byte[] bytes = toLE(ByteBuffer.allocate(4).putFloat(3.14f));
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.FLOAT())).isEqualTo(3.14f);
    }

    @Test
    void testDouble() {
        byte[] bytes = toLE(ByteBuffer.allocate(8).putDouble(2.718281828));
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
        byte[] bytes = toLE(ByteBuffer.allocate(4).putInt(18000));
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.DATE())).isEqualTo(18000);
    }

    @Test
    void testTimestampMillis() {
        long millis = 1700000000000L;
        byte[] bytes = toLE(ByteBuffer.allocate(8).putLong(millis));
        Object result = MosaicObjects.convertStatsValue(bytes, DataTypes.TIMESTAMP(3));
        assertThat(result).isEqualTo(Timestamp.fromEpochMillis(millis));
    }

    @Test
    void testTimestampMicros() {
        long micros = 1700000000000000L;
        byte[] bytes = toLE(ByteBuffer.allocate(8).putLong(micros));
        Object result = MosaicObjects.convertStatsValue(bytes, DataTypes.TIMESTAMP(6));
        assertThat(result).isEqualTo(Timestamp.fromMicros(micros));
    }

    @Test
    void testDecimal() {
        byte[] leBytes = new byte[] {(byte) 0xE8, 0x03, 0, 0, 0, 0, 0, 0};
        Object result = MosaicObjects.convertStatsValue(leBytes, DataTypes.DECIMAL(10, 2));
        assertThat(result).isInstanceOf(Decimal.class);
        Decimal decimal = (Decimal) result;
        assertThat(decimal.toBigDecimal().intValue()).isEqualTo(10);
    }

    @Test
    void testUnsupportedTypeReturnsNull() {
        byte[] bytes = new byte[] {1, 2, 3};
        assertThat(MosaicObjects.convertStatsValue(bytes, DataTypes.ARRAY(DataTypes.INT())))
                .isNull();
    }

    private static byte[] toLE(ByteBuffer bigEndianBuf) {
        byte[] beBytes = bigEndianBuf.array();
        ByteBuffer leBuf = ByteBuffer.allocate(beBytes.length).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer beBuf = ByteBuffer.wrap(beBytes).order(ByteOrder.BIG_ENDIAN);
        if (beBytes.length == 2) {
            leBuf.putShort(beBuf.getShort());
        } else if (beBytes.length == 4) {
            leBuf.putInt(beBuf.getInt());
        } else if (beBytes.length == 8) {
            leBuf.putLong(beBuf.getLong());
        }
        return leBuf.array();
    }
}
