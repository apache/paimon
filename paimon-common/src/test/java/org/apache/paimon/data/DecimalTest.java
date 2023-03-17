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

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.apache.paimon.utils.DecimalUtils.add;
import static org.apache.paimon.utils.DecimalUtils.castFrom;
import static org.apache.paimon.utils.DecimalUtils.castToBoolean;
import static org.apache.paimon.utils.DecimalUtils.castToDecimal;
import static org.apache.paimon.utils.DecimalUtils.castToIntegral;
import static org.apache.paimon.utils.DecimalUtils.doubleValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Decimal}. */
public class DecimalTest {

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testNormal() {
        BigDecimal bigDecimal1 = new BigDecimal("13145678.90123");
        BigDecimal bigDecimal2 = new BigDecimal("1234567890.0987654321");
        // fromUnscaledBytes
        assertThat(Decimal.fromUnscaledBytes(bigDecimal1.unscaledValue().toByteArray(), 15, 5))
                .isEqualTo(Decimal.fromBigDecimal(bigDecimal1, 15, 5));
        assertThat(Decimal.fromUnscaledBytes(bigDecimal2.unscaledValue().toByteArray(), 23, 10))
                .isEqualTo(Decimal.fromBigDecimal(bigDecimal2, 23, 10));
        // toUnscaledBytes
        assertThat(
                        Decimal.fromUnscaledBytes(bigDecimal1.unscaledValue().toByteArray(), 15, 5)
                                .toUnscaledBytes())
                .isEqualTo(bigDecimal1.unscaledValue().toByteArray());
        assertThat(
                        Decimal.fromUnscaledBytes(bigDecimal2.unscaledValue().toByteArray(), 23, 10)
                                .toUnscaledBytes())
                .isEqualTo(bigDecimal2.unscaledValue().toByteArray());

        Decimal decimal1 = Decimal.fromUnscaledLong(10, 5, 0);
        Decimal decimal2 = Decimal.fromUnscaledLong(15, 5, 0);
        assertThat(Decimal.fromBigDecimal(new BigDecimal(10), 5, 0).hashCode())
                .isEqualTo(decimal1.hashCode());
        assertThat(decimal1.copy()).isEqualTo(decimal1);
        assertThat(Decimal.fromUnscaledLong(decimal1.toUnscaledLong(), 5, 0)).isEqualTo(decimal1);
        assertThat(Decimal.fromUnscaledBytes(decimal1.toUnscaledBytes(), 5, 0)).isEqualTo(decimal1);
        assertThat(decimal1.compareTo(decimal2)).isLessThan(0);
        assertThat(doubleValue(castFrom(10.5, 5, 1))).isEqualTo(10.5);
        assertThat(add(decimal1, decimal2, 5, 0).toUnscaledLong()).isEqualTo(25);
        assertThat(castToIntegral(decimal1)).isEqualTo(10);
        assertThat(castToBoolean(decimal1)).isTrue();

        assertThat(Decimal.fromBigDecimal(new BigDecimal(Long.MAX_VALUE), 5, 0)).isNull();
        assertThat(Decimal.zero(5, 2).toBigDecimal().intValue()).isEqualTo(0);
        assertThat(Decimal.zero(20, 2).toBigDecimal().intValue()).isEqualTo(0);

        assertThat(castToDecimal(castFrom(5.0, 10, 1), 10, 2).toString()).isEqualTo("5.00");

        assertThat(castToIntegral(castFrom(5, 5, 0))).isEqualTo(5);
        assertThat(castToIntegral(castFrom("5", 5, 0))).isEqualTo(5);

        Decimal newDecimal = castFrom(castFrom(10, 5, 2), 10, 4);
        assertThat(newDecimal.precision()).isEqualTo(10);
        assertThat(newDecimal.scale()).isEqualTo(4);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testNotCompact() {
        Decimal decimal1 = Decimal.fromBigDecimal(new BigDecimal(10), 20, 0);
        Decimal decimal2 = Decimal.fromBigDecimal(new BigDecimal(15), 20, 0);
        assertThat(Decimal.fromBigDecimal(new BigDecimal(10), 20, 0).hashCode())
                .isEqualTo(decimal1.hashCode());
        assertThat(decimal1.copy()).isEqualTo(decimal1);
        assertThat(Decimal.fromBigDecimal(decimal1.toBigDecimal(), 20, 0)).isEqualTo(decimal1);
        assertThat(Decimal.fromUnscaledBytes(decimal1.toUnscaledBytes(), 20, 0))
                .isEqualTo(decimal1);
        assertThat(decimal1.compareTo(decimal2)).isLessThan(0);
        assertThat(doubleValue(castFrom(10.5, 20, 1))).isEqualTo(10.5);
        assertThat(add(decimal1, decimal2, 20, 0).toBigDecimal().longValue()).isEqualTo(25);
        assertThat(castToIntegral(decimal1)).isEqualTo(10);
        assertThat(castToBoolean(decimal1)).isTrue();

        assertThat(Decimal.fromBigDecimal(new BigDecimal(Long.MAX_VALUE), 5, 0)).isNull();
        assertThat(Decimal.zero(20, 2).toBigDecimal().intValue()).isEqualTo(0);
        assertThat(Decimal.zero(20, 2).toBigDecimal().intValue()).isEqualTo(0);

        Decimal decimal3 = Decimal.fromBigDecimal(new BigDecimal(10), 18, 0);
        Decimal decimal4 = Decimal.fromBigDecimal(new BigDecimal(15), 18, 0);
    }

    @Test
    public void testToString() {
        String val = "0.0000000000000000001";
        assertThat(castFrom(val, 39, val.length() - 2).toString()).isEqualTo(val);
        val = "123456789012345678901234567890123456789";
        assertThat(castFrom(val, 39, 0).toString()).isEqualTo(val);
    }
}
