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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Random;
import java.util.function.Function;

/** Test for {@link FastHashForNumber}. */
public class FastHashForNumberTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testDecimalType() {
        Function<Object, Long> function =
                DataTypes.DECIMAL(10, 5).accept(FastHashForNumber.INSTANCE).get();
        Decimal decimal = Decimal.fromBigDecimal(new BigDecimal("0.00123"), 10, 5);
        Assertions.assertThat(function.apply(decimal))
                .isEqualTo(FastHashForNumber.getLongHash(decimal.toUnscaledLong()));
    }

    @Test
    public void testTinyIntType() {
        Function<Object, Long> function =
                DataTypes.TINYINT().accept(FastHashForNumber.INSTANCE).get();
        byte c = (byte) RANDOM.nextInt();
        Assertions.assertThat(function.apply(c)).isEqualTo(FastHashForNumber.getLongHash(c));
    }

    @Test
    public void testSmallIntType() {
        Function<Object, Long> function =
                DataTypes.SMALLINT().accept(FastHashForNumber.INSTANCE).get();
        short c = (short) RANDOM.nextInt();
        Assertions.assertThat(function.apply(c)).isEqualTo(FastHashForNumber.getLongHash(c));
    }

    @Test
    public void testIntType() {
        Function<Object, Long> function = DataTypes.INT().accept(FastHashForNumber.INSTANCE).get();
        int c = RANDOM.nextInt();
        Assertions.assertThat(function.apply(c)).isEqualTo((FastHashForNumber.getLongHash(c)));
    }

    @Test
    public void testBigIntType() {
        Function<Object, Long> function =
                DataTypes.BIGINT().accept(FastHashForNumber.INSTANCE).get();
        long c = RANDOM.nextLong();
        Assertions.assertThat(function.apply(c)).isEqualTo((FastHashForNumber.getLongHash(c)));
    }

    @Test
    public void testFloatType() {
        Function<Object, Long> function =
                DataTypes.FLOAT().accept(FastHashForNumber.INSTANCE).get();
        float c = RANDOM.nextFloat();
        Assertions.assertThat(function.apply(c))
                .isEqualTo((FastHashForNumber.getLongHash(Float.floatToIntBits(c))));
    }

    @Test
    public void testDoubleType() {
        Function<Object, Long> function =
                DataTypes.DOUBLE().accept(FastHashForNumber.INSTANCE).get();
        double c = RANDOM.nextDouble();
        Assertions.assertThat(function.apply(c))
                .isEqualTo((FastHashForNumber.getLongHash(Double.doubleToLongBits(c))));
    }

    @Test
    public void testDateType() {
        Function<Object, Long> function = DataTypes.DATE().accept(FastHashForNumber.INSTANCE).get();
        int c = RANDOM.nextInt();
        Assertions.assertThat(function.apply(c)).isEqualTo((FastHashForNumber.getLongHash(c)));
    }

    @Test
    public void testTimestampType() {
        Function<Object, Long> function =
                DataTypes.TIMESTAMP().accept(FastHashForNumber.INSTANCE).get();
        Timestamp c = Timestamp.fromEpochMillis(System.currentTimeMillis());
        Assertions.assertThat(function.apply(c))
                .isEqualTo((FastHashForNumber.getLongHash(c.getMillisecond())));
    }

    @Test
    public void testLocalZonedTimestampType() {
        Function<Object, Long> function =
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().accept(FastHashForNumber.INSTANCE).get();
        Timestamp c = Timestamp.fromEpochMillis(System.currentTimeMillis());
        Assertions.assertThat(function.apply(c))
                .isEqualTo((FastHashForNumber.getLongHash(c.getMillisecond())));
    }
}
