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

package org.apache.paimon.data;

import org.apache.paimon.data.safe.SafeBinaryArray;
import org.apache.paimon.data.serializer.InternalArraySerializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class SafeBinaryArrayTest {

    @Test
    public void test() {
        BinaryArray expected = toBinaryArray(DataTypes.BOOLEAN(), true, false, null, true);
        BinaryArray converted =
                toBinaryArray(DataTypes.BOOLEAN(), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected = toBinaryArray(DataTypes.TINYINT(), (byte) 15, (byte) 12, null, (byte) 1);
        converted = toBinaryArray(DataTypes.TINYINT(), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected = toBinaryArray(DataTypes.SMALLINT(), (short) 15, (short) 12, null, (short) 1);
        converted = toBinaryArray(DataTypes.SMALLINT(), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected = toBinaryArray(DataTypes.INT(), 15, 12, null, 1);
        converted = toBinaryArray(DataTypes.INT(), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected = toBinaryArray(DataTypes.BIGINT(), 15L, 12L, null, 1L);
        converted = toBinaryArray(DataTypes.BIGINT(), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected = toBinaryArray(DataTypes.FLOAT(), 15f, 12f, null, 1f);
        converted = toBinaryArray(DataTypes.FLOAT(), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected = toBinaryArray(DataTypes.DOUBLE(), 15d, 12d, null, 1d);
        converted = toBinaryArray(DataTypes.DOUBLE(), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected =
                toBinaryArray(
                        DataTypes.STRING(),
                        BinaryString.fromString("111"),
                        BinaryString.fromString("112231asdfasdf"),
                        null,
                        BinaryString.fromString("14611adfadsfaf"));
        converted = toBinaryArray(DataTypes.STRING(), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected =
                toBinaryArray(
                        DataTypes.DECIMAL(25, 2),
                        Decimal.fromBigDecimal(BigDecimal.valueOf(12), 25, 2),
                        Decimal.fromBigDecimal(BigDecimal.valueOf(123), 25, 2),
                        null,
                        Decimal.fromBigDecimal(BigDecimal.valueOf(1243), 25, 2));
        converted =
                toBinaryArray(DataTypes.DECIMAL(25, 2), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected =
                toBinaryArray(
                        DataTypes.DECIMAL(15, 2),
                        Decimal.fromUnscaledLong(12, 15, 2),
                        Decimal.fromUnscaledLong(123, 15, 2),
                        null,
                        Decimal.fromUnscaledLong(1243, 15, 2));
        converted =
                toBinaryArray(DataTypes.DECIMAL(15, 2), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected =
                toBinaryArray(
                        DataTypes.TIMESTAMP(3),
                        Timestamp.fromEpochMillis(1244444),
                        Timestamp.fromEpochMillis(12444244),
                        null,
                        Timestamp.fromEpochMillis(12445444));
        converted =
                toBinaryArray(DataTypes.TIMESTAMP(3), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected =
                toBinaryArray(
                        DataTypes.TIMESTAMP(6),
                        Timestamp.fromMicros(1244444),
                        Timestamp.fromMicros(12444244),
                        null,
                        Timestamp.fromMicros(12445444));
        converted =
                toBinaryArray(DataTypes.TIMESTAMP(6), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);

        expected =
                toBinaryArray(
                        DataTypes.BYTES(),
                        BinaryString.fromString("111").toBytes(),
                        BinaryString.fromString("112231asdfasdf").toBytes(),
                        null,
                        BinaryString.fromString("14611asdfadsaf").toBytes());
        converted = toBinaryArray(DataTypes.BYTES(), new SafeBinaryArray(expected.toBytes(), 0));
        assertThat(converted).isEqualTo(expected);
    }

    private BinaryArray toBinaryArray(DataType eleType, Object... values) {
        return toBinaryArray(eleType, new GenericArray(values));
    }

    private BinaryArray toBinaryArray(DataType eleType, InternalArray array) {
        return new InternalArraySerializer(eleType).toBinaryArray(array);
    }
}
