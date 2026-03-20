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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TypeUtils}. */
public class TypeUtilsTest {
    private static TimeZone originalTimeZone;

    @BeforeAll
    public static void setUp() {
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    @AfterAll
    public static void tearDown() {
        TimeZone.setDefault(originalTimeZone);
    }

    @Test
    public void testCastFromString() {
        String value =
                "[{\"key1\":null,\"key2\":\"value\"},{\"key1\":{\"nested_key1\":0},\"key2\":null}]";
        Object result =
                TypeUtils.castFromString(
                        value,
                        DataTypes.ARRAY(
                                DataTypes.ROW(
                                        new DataField(
                                                0,
                                                "key1",
                                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                                        new DataField(1, "key2", DataTypes.STRING()))));
        GenericArray expected =
                new GenericArray(
                        Arrays.asList(
                                        GenericRow.of(null, BinaryString.fromString("value")),
                                        GenericRow.of(
                                                new GenericMap(
                                                        Collections.singletonMap(
                                                                BinaryString.fromString(
                                                                        "nested_key1"),
                                                                0)),
                                                null))
                                .toArray());
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testStringCastFromString() {
        String value = "value";
        Object result = TypeUtils.castFromString(value, DataTypes.STRING());
        BinaryString expected = BinaryString.fromString("value");
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testArrayIntCastFromString() {
        String value = "[0, 1, 2]";
        Object result = TypeUtils.castFromString(value, DataTypes.ARRAY(DataTypes.INT()));
        GenericArray expected = new GenericArray(new Integer[] {0, 1, 2});
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testArrayStringCastFromString() {
        String value = "[\"0\", \"1\", \"2\"]";
        Object result = TypeUtils.castFromString(value, DataTypes.ARRAY(DataTypes.STRING()));
        GenericArray expected =
                new GenericArray(
                        Arrays.asList(
                                        BinaryString.fromString("0"),
                                        BinaryString.fromString("1"),
                                        BinaryString.fromString("2"))
                                .toArray());
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testVectorCastFromString() {
        String value = "[1.0, 2.5, 3.5]";
        Object result = TypeUtils.castFromString(value, DataTypes.VECTOR(3, DataTypes.FLOAT()));
        BinaryVector vector = (BinaryVector) result;
        assertThat(vector.toFloatArray()).isEqualTo(new float[] {1.0f, 2.5f, 3.5f});
    }

    @Test
    public void testLongCastFromString() {
        String value = "12";
        Object result = TypeUtils.castFromString(value, DataTypes.BIGINT());
        long expected = 12;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testBinaryCastFromString() {
        String value = "abc";
        Object result = TypeUtils.castFromString(value, DataTypes.BINARY(3));
        byte[] expected = "abc".getBytes(StandardCharsets.UTF_8);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testBinaryCastFromCdcValueString() {
        String value = Base64.getEncoder().encodeToString("abc".getBytes(StandardCharsets.UTF_8));
        Object result = TypeUtils.castFromCdcValueString(value, DataTypes.BINARY(3));
        byte[] expected = "abc".getBytes(StandardCharsets.UTF_8);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testBooleanTrueCastFromString() {
        String value = "true";
        Object result = TypeUtils.castFromCdcValueString(value, DataTypes.BOOLEAN());
        boolean expected = true;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testBooleanFalseCastFromString() {
        String value = "false";
        Object result = TypeUtils.castFromCdcValueString(value, DataTypes.BOOLEAN());
        boolean expected = false;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testCharCastFromString() {
        String value = "abc";
        Object result = TypeUtils.castFromCdcValueString(value, DataTypes.CHAR(3));
        BinaryString expected = BinaryString.fromString("abc");
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testDateCastFromString() {
        String value = "2017-12-12 09:30:00.0";
        Object result = TypeUtils.castFromCdcValueString(value, DataTypes.DATE());
        int expected = 17512;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testDateNumericCastFromString() {
        String value = "17512";
        Object result = TypeUtils.castFromCdcValueString(value, DataTypes.DATE());
        int expected = 17512;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testDecimalCastFromString() {
        String value = "123";
        Object result = TypeUtils.castFromCdcValueString(value, DataTypes.DECIMAL(5, 0));
        Decimal expected = Decimal.fromBigDecimal(new BigDecimal("123"), 5, 0);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testDoubleCastFromString() {
        String value = "123.456";
        Object result = TypeUtils.castFromCdcValueString(value, DataTypes.DOUBLE());
        Double expected = 123.456;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testFloatCastFromString() {
        String[] values = {"123.456", "0.00042", "1.00001", "175.26562", "0.00046", "6.1042607E-4"};
        Float[] expected = {123.456f, 0.00042f, 1.00001f, 175.26562f, 0.00046f, 0.00061042607f};
        for (int i = 0; i < values.length; i++) {
            Object result = TypeUtils.castFromCdcValueString(values[i], DataTypes.FLOAT());
            assertThat(result).isEqualTo(expected[i]);
        }
    }

    @Test
    public void testLargeFloatCastFromString() {
        String value = "123.45678";
        Object result = TypeUtils.castFromCdcValueString(value, DataTypes.FLOAT());
        Float expected = 123.45678f;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testIntCastFromString() {
        String value = "12";
        Object result = TypeUtils.castFromString(value, DataTypes.INT());
        int expected = 12;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testLocalZonedTimestampCastFromString() {
        String value = "2017-12-12 09:30:00";
        Object result = TypeUtils.castFromString(value, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        Timestamp expected =
                Timestamp.fromEpochMillis(
                        LocalDateTime.parse("2017-12-12T09:30:00")
                                        .atZone(TimeZone.getTimeZone("Asia/Tokyo").toZoneId())
                                        .toEpochSecond()
                                * 1000);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testMapStringStringCastFromString() {
        String value = "{\"a\":\"b\", \"c\":\"d\"}";
        Object result =
                TypeUtils.castFromString(
                        value, DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()));
        GenericMap expected =
                new GenericMap(
                        new HashMap<BinaryString, BinaryString>() {
                            {
                                put(BinaryString.fromString("a"), BinaryString.fromString("b"));
                                put(BinaryString.fromString("c"), BinaryString.fromString("d"));
                            }
                        });
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testMapStringIntCastFromString() {
        String value = "{\"a\":0, \"c\":1}";
        Object result =
                TypeUtils.castFromString(value, DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        GenericMap expected =
                new GenericMap(
                        new HashMap<BinaryString, Integer>() {
                            {
                                put(BinaryString.fromString("a"), 0);
                                put(BinaryString.fromString("c"), 1);
                            }
                        });
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testRowCastFromString() {
        String value = "{\"key1\":{\"nested_key1\":0},\"key2\":\"value\"}";
        Object result =
                TypeUtils.castFromString(
                        value,
                        DataTypes.ROW(
                                new DataField(
                                        0,
                                        "key1",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                                new DataField(1, "key2", DataTypes.STRING())));
        GenericRow expected =
                GenericRow.of(
                        new GenericMap(
                                Collections.singletonMap(
                                        BinaryString.fromString("nested_key1"), 0)),
                        BinaryString.fromString("value"));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testSmallIntCastFromString() {
        String value = "12";
        Object result = TypeUtils.castFromString(value, DataTypes.SMALLINT());
        short expected = 12;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testTimestampCastFromString() {
        String value = "2017-12-12 09:30:00";
        Object result = TypeUtils.castFromString(value, DataTypes.TIMESTAMP());
        Timestamp expected =
                Timestamp.fromLocalDateTime(LocalDateTime.parse("2017-12-12T09:30:00"));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testTimestampNumericCastFromString() {
        String value = "123456789000000";
        Object result = TypeUtils.castFromString(value, DataTypes.TIMESTAMP());
        Timestamp expected = Timestamp.fromMicros(123456789000000L);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testTimeCastFromString() {
        String value = "13:09:42.123456+01:00";
        Object result = TypeUtils.castFromString(value, DataTypes.TIME(3));
        int expected = 14 * 60 * 60 * 1000 + 9 * 60 * 1000 + 42 * 1000 + 123;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testTimeNumericCastFromString() {
        String value = "123456789";
        Object result = TypeUtils.castFromString(value, DataTypes.TIME());
        int expected = 123456789;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testTinyIntCastFromString() {
        String value = "6";
        Object result = TypeUtils.castFromString(value, DataTypes.TINYINT());
        byte expected = 6;
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testVarBinaryCastFromString() {
        String value = "abc";
        Object result = TypeUtils.castFromString(value, DataTypes.VARBINARY(3));
        byte[] expected = "abc".getBytes(StandardCharsets.UTF_8);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testVarCharCastFromString() {
        String value = "abc";
        Object result = TypeUtils.castFromCdcValueString(value, DataTypes.VARCHAR(3));
        BinaryString expected = BinaryString.fromString("abc");
        assertThat(result).isEqualTo(expected);
    }
}
