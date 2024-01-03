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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.utils.Pair;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/** Test for {@link DecimalSerializer}. */
public abstract class DecimalSerializerTest extends SerializerTestBase<Decimal> {

    @Override
    protected DecimalSerializer createSerializer() {
        return new DecimalSerializer(getPrecision(), getScale());
    }

    @Override
    protected boolean deepEquals(Decimal t1, Decimal t2) {
        return t1.equals(t2);
    }

    protected abstract int getPrecision();

    protected abstract int getScale();

    @Override
    protected Decimal[] getTestData() {
        return new Decimal[] {
            Decimal.fromUnscaledLong(1, getPrecision(), getScale()),
            Decimal.fromUnscaledLong(2, getPrecision(), getScale()),
            Decimal.fromUnscaledLong(3, getPrecision(), getScale()),
            Decimal.fromUnscaledLong(4, getPrecision(), getScale())
        };
    }

    static final class DecimalSerializer2Test extends DecimalSerializerTest {
        @Override
        protected int getPrecision() {
            return 5;
        }

        @Override
        protected int getScale() {
            return 2;
        }
    }

    @Override
    protected List<Pair<Decimal, String>> getSerializableToStringTestData() {
        return Arrays.asList(
                Pair.of(
                        Decimal.fromBigDecimal(new BigDecimal("0.01"), getPrecision(), getScale()),
                        "0.01"),
                Pair.of(
                        Decimal.fromBigDecimal(new BigDecimal("22.02"), getPrecision(), getScale()),
                        "22.02"),
                Pair.of(
                        Decimal.fromBigDecimal(new BigDecimal("33.30"), getPrecision(), getScale()),
                        "33.30"),
                Pair.of(
                        Decimal.fromBigDecimal(
                                new BigDecimal("444.40"), getPrecision(), getScale()),
                        "444.40"));
    }

    static final class DecimalSerializer3Test extends DecimalSerializerTest {
        @Override
        protected int getPrecision() {
            return 6;
        }

        @Override
        protected int getScale() {
            return 3;
        }

        @Override
        protected List<Pair<Decimal, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            Decimal.fromBigDecimal(
                                    new BigDecimal("0.001"), getPrecision(), getScale()),
                            "0.001"),
                    Pair.of(
                            Decimal.fromBigDecimal(
                                    new BigDecimal("22.002"), getPrecision(), getScale()),
                            "22.002"),
                    Pair.of(
                            Decimal.fromBigDecimal(
                                    new BigDecimal("33.030"), getPrecision(), getScale()),
                            "33.030"),
                    Pair.of(
                            Decimal.fromBigDecimal(
                                    new BigDecimal("444.400"), getPrecision(), getScale()),
                            "444.400"));
        }
    }
}
