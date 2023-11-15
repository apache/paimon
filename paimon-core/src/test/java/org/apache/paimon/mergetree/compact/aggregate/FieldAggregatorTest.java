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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** test whether {@link FieldAggregator}' subclasses behaviors are expected. */
public class FieldAggregatorTest {

    @Test
    public void testFieldBoolAndAgg() {
        FieldBoolAndAgg fieldBoolAndAgg = new FieldBoolAndAgg(new BooleanType());
        Boolean accumulator = false;
        Boolean inputField = true;
        assertThat(fieldBoolAndAgg.agg(accumulator, inputField)).isEqualTo(false);

        accumulator = true;
        inputField = true;
        assertThat(fieldBoolAndAgg.agg(accumulator, inputField)).isEqualTo(true);
    }

    @Test
    public void testFieldBoolOrAgg() {
        FieldBoolOrAgg fieldBoolOrAgg = new FieldBoolOrAgg(new BooleanType());
        Boolean accumulator = false;
        Boolean inputField = true;
        assertThat(fieldBoolOrAgg.agg(accumulator, inputField)).isEqualTo(true);

        accumulator = false;
        inputField = false;
        assertThat(fieldBoolOrAgg.agg(accumulator, inputField)).isEqualTo(false);
    }

    @Test
    public void testFieldLastNonNullValueAgg() {
        FieldLastNonNullValueAgg fieldLastNonNullValueAgg =
                new FieldLastNonNullValueAgg(new IntType());
        Integer accumulator = null;
        Integer inputField = 1;
        assertThat(fieldLastNonNullValueAgg.agg(accumulator, inputField)).isEqualTo(1);

        accumulator = 1;
        inputField = null;
        assertThat(fieldLastNonNullValueAgg.agg(accumulator, inputField)).isEqualTo(1);
    }

    @Test
    public void testFieldLastValueAgg() {
        FieldLastValueAgg fieldLastValueAgg = new FieldLastValueAgg(new IntType());
        Integer accumulator = null;
        Integer inputField = 1;
        assertThat(fieldLastValueAgg.agg(accumulator, inputField)).isEqualTo(1);

        accumulator = 1;
        inputField = null;
        assertThat(fieldLastValueAgg.agg(accumulator, inputField)).isEqualTo(null);
    }

    @Test
    public void testFieldListaggAgg() {
        FieldListaggAgg fieldListaggAgg = new FieldListaggAgg(new VarCharType());
        BinaryString accumulator = BinaryString.fromString("user1");
        BinaryString inputField = BinaryString.fromString("user2");
        assertThat(fieldListaggAgg.agg(accumulator, inputField).toString())
                .isEqualTo("user1,user2");
    }

    @Test
    public void testFieldMaxAgg() {
        FieldMaxAgg fieldMaxAgg = new FieldMaxAgg(new IntType());
        Integer accumulator = 1;
        Integer inputField = 10;
        assertThat(fieldMaxAgg.agg(accumulator, inputField)).isEqualTo(10);
    }

    @Test
    public void testFieldMinAgg() {
        FieldMinAgg fieldMinAgg = new FieldMinAgg(new IntType());
        Integer accumulator = 1;
        Integer inputField = 10;
        assertThat(fieldMinAgg.agg(accumulator, inputField)).isEqualTo(1);
    }

    @Test
    public void testFieldSumIntAgg() {
        FieldSumAgg fieldSumAgg = new FieldSumAgg(new IntType());
        assertThat(fieldSumAgg.agg(null, 10)).isEqualTo(10);
        assertThat(fieldSumAgg.agg(1, 10)).isEqualTo(11);
        assertThat(fieldSumAgg.retract(10, 5)).isEqualTo(5);
        assertThat(fieldSumAgg.retract(null, 5)).isEqualTo(-5);
    }

    @Test
    public void testFieldSumByteAgg() {
        FieldSumAgg fieldSumAgg = new FieldSumAgg(new TinyIntType());
        assertThat(fieldSumAgg.agg(null, (byte) 10)).isEqualTo((byte) 10);
        assertThat(fieldSumAgg.agg((byte) 1, (byte) 10)).isEqualTo((byte) 11);
        assertThat(fieldSumAgg.retract((byte) 10, (byte) 5)).isEqualTo((byte) 5);
        assertThat(fieldSumAgg.retract(null, (byte) 5)).isEqualTo((byte) -5);
    }

    @Test
    public void testFieldSumShortAgg() {
        FieldSumAgg fieldSumAgg = new FieldSumAgg(new SmallIntType());
        assertThat(fieldSumAgg.agg(null, (short) 10)).isEqualTo((short) 10);
        assertThat(fieldSumAgg.agg((short) 1, (short) 10)).isEqualTo((short) 11);
        assertThat(fieldSumAgg.retract((short) 10, (short) 5)).isEqualTo((short) 5);
        assertThat(fieldSumAgg.retract(null, (short) 5)).isEqualTo((short) -5);
    }

    @Test
    public void testFieldSumLongAgg() {
        FieldSumAgg fieldSumAgg = new FieldSumAgg(new BigIntType());
        assertThat(fieldSumAgg.agg(null, 10L)).isEqualTo(10L);
        assertThat(fieldSumAgg.agg(1L, 10L)).isEqualTo(11L);
        assertThat(fieldSumAgg.retract(10L, 5L)).isEqualTo(5L);
        assertThat(fieldSumAgg.retract(null, 5L)).isEqualTo(-5L);
    }

    @Test
    public void testFieldSumFloatAgg() {
        FieldSumAgg fieldSumAgg = new FieldSumAgg(new FloatType());
        assertThat(fieldSumAgg.agg(null, (float) 10)).isEqualTo((float) 10);
        assertThat(fieldSumAgg.agg((float) 1, (float) 10)).isEqualTo((float) 11);
        assertThat(fieldSumAgg.retract((float) 10, (float) 5)).isEqualTo((float) 5);
        assertThat(fieldSumAgg.retract(null, (float) 5)).isEqualTo((float) -5);
    }

    @Test
    public void testFieldSumDoubleAgg() {
        FieldSumAgg fieldSumAgg = new FieldSumAgg(new DoubleType());
        assertThat(fieldSumAgg.agg(null, (double) 10)).isEqualTo((double) 10);
        assertThat(fieldSumAgg.agg((double) 1, (double) 10)).isEqualTo((double) 11);
        assertThat(fieldSumAgg.retract((double) 10, (double) 5)).isEqualTo((double) 5);
        assertThat(fieldSumAgg.retract(null, (double) 5)).isEqualTo((double) -5);
    }

    @Test
    public void testFieldSumDecimalAgg() {
        FieldSumAgg fieldSumAgg = new FieldSumAgg(new DecimalType());
        assertThat(fieldSumAgg.agg(null, toDecimal(10))).isEqualTo(toDecimal(10));
        assertThat(fieldSumAgg.agg(toDecimal(1), toDecimal(10))).isEqualTo(toDecimal(11));
        assertThat(fieldSumAgg.retract(toDecimal(10), toDecimal(5))).isEqualTo(toDecimal(5));
        assertThat(fieldSumAgg.retract(null, toDecimal(5))).isEqualTo(toDecimal(-5));
    }

    private static Decimal toDecimal(int i) {
        return Decimal.fromBigDecimal(
                new BigDecimal(i), DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE);
    }
}
