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

package org.apache.paimon.fileindex.bloomfilter;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Random;

/** Test for {@link FastHash}. */
public class FastHashTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testDecimalType() {
        HashConverter32 function = DataTypes.DECIMAL(10, 5).accept(FastHash.INSTANCE);
        Decimal decimal = Decimal.fromBigDecimal(new BigDecimal("0.00123"), 10, 5);
        Assertions.assertThat(function.hash(decimal))
                .isEqualTo(
                        FastHash.getLongHash(
                                Double.doubleToLongBits(decimal.toBigDecimal().doubleValue())));
    }

    @Test
    public void testTinyIntType() {
        HashConverter32 function = DataTypes.TINYINT().accept(FastHash.INSTANCE);
        byte c = (byte) RANDOM.nextInt();
        Assertions.assertThat(function.hash(c)).isEqualTo(FastHash.getLongHash(c));
    }

    @Test
    public void testSmallIntType() {
        HashConverter32 function = DataTypes.SMALLINT().accept(FastHash.INSTANCE);
        short c = (short) RANDOM.nextInt();
        Assertions.assertThat(function.hash(c)).isEqualTo(FastHash.getLongHash(c));
    }

    @Test
    public void testIntType() {
        HashConverter32 function = DataTypes.INT().accept(FastHash.INSTANCE);
        int c = RANDOM.nextInt();
        Assertions.assertThat(function.hash(c)).isEqualTo((FastHash.getLongHash(c)));
    }

    @Test
    public void testBigIntType() {
        HashConverter32 function = DataTypes.BIGINT().accept(FastHash.INSTANCE);
        long c = RANDOM.nextLong();
        Assertions.assertThat(function.hash(c)).isEqualTo((FastHash.getLongHash(c)));
    }

    @Test
    public void testFloatType() {
        HashConverter32 function = DataTypes.FLOAT().accept(FastHash.INSTANCE);
        float c = RANDOM.nextFloat();
        Assertions.assertThat(function.hash(c))
                .isEqualTo((FastHash.getLongHash(Float.floatToIntBits(c))));
    }

    @Test
    public void testDoubleType() {
        HashConverter32 function = DataTypes.DOUBLE().accept(FastHash.INSTANCE);
        double c = RANDOM.nextDouble();
        Assertions.assertThat(function.hash(c))
                .isEqualTo((FastHash.getLongHash(Double.doubleToLongBits(c))));
    }

    @Test
    public void testDateType() {
        HashConverter32 function = DataTypes.DATE().accept(FastHash.INSTANCE);
        int c = RANDOM.nextInt();
        Assertions.assertThat(function.hash(c)).isEqualTo((FastHash.getLongHash(c)));
    }

    @Test
    public void testTimestampType() {
        HashConverter32 function = DataTypes.TIMESTAMP().accept(FastHash.INSTANCE);
        Timestamp c = Timestamp.fromEpochMillis(System.currentTimeMillis());
        Assertions.assertThat(function.hash(c))
                .isEqualTo((FastHash.getLongHash(c.getMillisecond())));
    }

    @Test
    public void testLocalZonedTimestampType() {
        HashConverter32 function =
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().accept(FastHash.INSTANCE);
        Timestamp c = Timestamp.fromEpochMillis(System.currentTimeMillis());
        Assertions.assertThat(function.hash(c))
                .isEqualTo((FastHash.getLongHash(c.getMillisecond())));
    }
}
