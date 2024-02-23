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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** test whether {@link FieldAggregator}' subclasses behaviors are expected. */
public class FieldAggregatorTest {

    @Test
    public void testFieldBoolAndAgg() {
        FieldBoolAndAgg fieldBoolAndAgg = new FieldBoolAndAgg(new BooleanType());
        assertThat(fieldBoolAndAgg.agg(false, true)).isEqualTo(false);
        assertThat(fieldBoolAndAgg.agg(true, true)).isEqualTo(true);
    }

    @Test
    public void testFieldBoolOrAgg() {
        FieldBoolOrAgg fieldBoolOrAgg = new FieldBoolOrAgg(new BooleanType());
        assertThat(fieldBoolOrAgg.agg(false, true)).isEqualTo(true);
        assertThat(fieldBoolOrAgg.agg(false, false)).isEqualTo(false);
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
    public void testFieldFirstValueAgg() {
        FieldFirstValueAgg fieldFirstValueAgg = new FieldFirstValueAgg(new IntType());
        assertThat(fieldFirstValueAgg.agg(null, 1)).isEqualTo(1);
        assertThat(fieldFirstValueAgg.agg(1, 2)).isEqualTo(1);

        fieldFirstValueAgg.reset();
        assertThat(fieldFirstValueAgg.agg(1, 3)).isEqualTo(3);
    }

    @Test
    public void testFieldFirstNonNullValueAgg() {
        FieldFirstNonNullValueAgg fieldFirstNonNullValueAgg =
                new FieldFirstNonNullValueAgg(new IntType());
        assertThat(fieldFirstNonNullValueAgg.agg(null, null)).isNull();
        assertThat(fieldFirstNonNullValueAgg.agg(null, 1)).isEqualTo(1);
        assertThat(fieldFirstNonNullValueAgg.agg(1, 2)).isEqualTo(1);

        fieldFirstNonNullValueAgg.reset();
        assertThat(fieldFirstNonNullValueAgg.agg(1, 3)).isEqualTo(3);
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
    public void testFieldCountIntAgg() {
        FieldCountAgg fieldCountAgg = new FieldCountAgg(new IntType());
        assertThat(fieldCountAgg.agg(null, 10)).isEqualTo(1);
        assertThat(fieldCountAgg.agg(1, 5)).isEqualTo(2);
        assertThat(fieldCountAgg.agg(2, 15)).isEqualTo(3);
        assertThat(fieldCountAgg.agg(3, 25)).isEqualTo(4);
    }

    @Test
    public void testFieldProductIntAgg() {
        FieldProductAgg fieldProductAgg = new FieldProductAgg(new IntType());
        assertThat(fieldProductAgg.agg(null, 10)).isEqualTo(10);
        assertThat(fieldProductAgg.agg(1, 10)).isEqualTo(10);
        assertThat(fieldProductAgg.retract(10, 5)).isEqualTo(2);
        assertThat(fieldProductAgg.retract(null, 5)).isEqualTo(5);
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
    public void testFieldProductByteAgg() {
        FieldProductAgg fieldProductAgg = new FieldProductAgg(new TinyIntType());
        assertThat(fieldProductAgg.agg(null, (byte) 10)).isEqualTo((byte) 10);
        assertThat(fieldProductAgg.agg((byte) 1, (byte) 10)).isEqualTo((byte) 10);
        assertThat(fieldProductAgg.retract((byte) 10, (byte) 5)).isEqualTo((byte) 2);
        assertThat(fieldProductAgg.retract(null, (byte) 5)).isEqualTo((byte) 5);
    }

    @Test
    public void testFieldProductShortAgg() {
        FieldProductAgg fieldProductAgg = new FieldProductAgg(new SmallIntType());
        assertThat(fieldProductAgg.agg(null, (short) 10)).isEqualTo((short) 10);
        assertThat(fieldProductAgg.agg((short) 1, (short) 10)).isEqualTo((short) 10);
        assertThat(fieldProductAgg.retract((short) 10, (short) 5)).isEqualTo((short) 2);
        assertThat(fieldProductAgg.retract(null, (short) 5)).isEqualTo((short) 5);
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
    public void testFieldProductLongAgg() {
        FieldProductAgg fieldProductAgg = new FieldProductAgg(new BigIntType());
        assertThat(fieldProductAgg.agg(null, 10L)).isEqualTo(10L);
        assertThat(fieldProductAgg.agg(1L, 10L)).isEqualTo(10L);
        assertThat(fieldProductAgg.retract(10L, 5L)).isEqualTo(2L);
        assertThat(fieldProductAgg.retract(null, 5L)).isEqualTo(5L);
    }

    @Test
    public void testFieldProductFloatAgg() {
        FieldProductAgg fieldProductAgg = new FieldProductAgg(new FloatType());
        assertThat(fieldProductAgg.agg(null, (float) 10)).isEqualTo((float) 10);
        assertThat(fieldProductAgg.agg((float) 1, (float) 10)).isEqualTo((float) 10);
        assertThat(fieldProductAgg.retract((float) 10, (float) 5)).isEqualTo((float) 2);
        assertThat(fieldProductAgg.retract(null, (float) 5)).isEqualTo((float) 5);
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
    public void testFieldProductDoubleAgg() {
        FieldProductAgg fieldProductAgg = new FieldProductAgg(new DoubleType());
        assertThat(fieldProductAgg.agg(null, (double) 10)).isEqualTo((double) 10);
        assertThat(fieldProductAgg.agg((double) 1, (double) 10)).isEqualTo((double) 10);
        assertThat(fieldProductAgg.retract((double) 10, (double) 5)).isEqualTo((double) 2);
        assertThat(fieldProductAgg.retract(null, (double) 5)).isEqualTo((double) 5);
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
    public void testFieldProductDecimalAgg() {
        FieldProductAgg fieldProductAgg = new FieldProductAgg(new DecimalType());
        assertThat(fieldProductAgg.agg(null, toDecimal(10))).isEqualTo(toDecimal(10));
        assertThat(fieldProductAgg.agg(toDecimal(1), toDecimal(10))).isEqualTo(toDecimal(10));
        assertThat(fieldProductAgg.retract(toDecimal(10), toDecimal(5))).isEqualTo(toDecimal(2));
        assertThat(fieldProductAgg.retract(null, toDecimal(5))).isEqualTo(toDecimal(5));
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

    @Test
    public void testFieldNestedUpdateAgg() {
        DataType elementRowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "k0", DataTypes.INT()),
                        DataTypes.FIELD(1, "k1", DataTypes.INT()),
                        DataTypes.FIELD(2, "v", DataTypes.STRING()));
        FieldNestedUpdateAgg agg =
                new FieldNestedUpdateAgg(
                        DataTypes.ARRAY(
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "k0", DataTypes.INT()),
                                        DataTypes.FIELD(1, "k1", DataTypes.INT()),
                                        DataTypes.FIELD(2, "v", DataTypes.STRING()))),
                        Arrays.asList("k0", "k1"));

        InternalArray accumulator;
        InternalArray.ElementGetter elementGetter =
                InternalArray.createElementGetter(elementRowType);

        InternalRow current = row(0, 0, "A");
        accumulator = (InternalArray) agg.agg(null, singletonArray(current));
        assertThat(unnest(accumulator, elementGetter))
                .containsExactlyInAnyOrderElementsOf(Collections.singletonList(current));

        current = row(0, 1, "B");
        accumulator = (InternalArray) agg.agg(accumulator, singletonArray(current));
        assertThat(unnest(accumulator, elementGetter))
                .containsExactlyInAnyOrderElementsOf(Arrays.asList(row(0, 0, "A"), row(0, 1, "B")));

        current = row(0, 1, "b");
        accumulator = (InternalArray) agg.agg(accumulator, singletonArray(current));
        assertThat(unnest(accumulator, elementGetter))
                .containsExactlyInAnyOrderElementsOf(Arrays.asList(row(0, 0, "A"), row(0, 1, "b")));

        current = row(0, 1, "b");
        accumulator = (InternalArray) agg.retract(accumulator, singletonArray(current));
        assertThat(unnest(accumulator, elementGetter))
                .containsExactlyInAnyOrderElementsOf(Collections.singletonList(row(0, 0, "A")));
    }

    @Test
    public void testFieldNestedAppendAgg() {
        DataType elementRowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "k0", DataTypes.INT()),
                        DataTypes.FIELD(1, "k1", DataTypes.INT()),
                        DataTypes.FIELD(2, "v", DataTypes.STRING()));
        FieldNestedUpdateAgg agg =
                new FieldNestedUpdateAgg(DataTypes.ARRAY(elementRowType), Collections.emptyList());

        InternalArray accumulator = null;
        InternalArray.ElementGetter elementGetter =
                InternalArray.createElementGetter(elementRowType);

        InternalRow current = row(0, 1, "B");
        accumulator = (InternalArray) agg.agg(accumulator, singletonArray(current));
        assertThat(unnest(accumulator, elementGetter))
                .containsExactlyInAnyOrderElementsOf(Collections.singletonList(row(0, 1, "B")));

        current = row(0, 1, "b");
        accumulator = (InternalArray) agg.agg(accumulator, singletonArray(current));
        assertThat(unnest(accumulator, elementGetter))
                .containsExactlyInAnyOrderElementsOf(Arrays.asList(row(0, 1, "B"), row(0, 1, "b")));

        current = row(0, 1, "b");
        accumulator = (InternalArray) agg.retract(accumulator, singletonArray(current));
        assertThat(unnest(accumulator, elementGetter))
                .containsExactlyInAnyOrderElementsOf(Collections.singletonList(row(0, 1, "B")));
    }

    private List<Object> unnest(InternalArray array, InternalArray.ElementGetter elementGetter) {
        return IntStream.range(0, array.size())
                .mapToObj(i -> elementGetter.getElementOrNull(array, i))
                .collect(Collectors.toList());
    }

    private GenericArray singletonArray(InternalRow row) {
        return new GenericArray(new InternalRow[] {row});
    }

    private InternalRow row(int k0, int k1, String v) {
        return GenericRow.of(k0, k1, BinaryString.fromString(v));
    }

    @Test
    public void testFieldCollectAggWithDistinct() {
        FieldCollectAgg agg = new FieldCollectAgg(DataTypes.ARRAY(DataTypes.INT()), true);

        InternalArray result;
        InternalArray.ElementGetter elementGetter =
                InternalArray.createElementGetter(DataTypes.INT());

        assertThat(agg.agg(null, null)).isNull();

        result = (InternalArray) agg.agg(null, new GenericArray(new int[] {1, 1, 2}));
        assertThat(unnest(result, elementGetter)).containsExactlyInAnyOrder(1, 2);

        result =
                (InternalArray)
                        agg.agg(
                                new GenericArray(new int[] {1, 1, 2}),
                                new GenericArray(new int[] {2, 3}));
        assertThat(unnest(result, elementGetter)).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    public void testFiledCollectAggWithRowType() {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        FieldCollectAgg agg = new FieldCollectAgg(DataTypes.ARRAY(rowType), true);

        InternalArray result;
        InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(rowType);

        assertThat(agg.agg(null, null)).isNull();

        Object[] input1 =
                new Object[] {
                    GenericRow.of(1, BinaryString.fromString("A")),
                    GenericRow.of(1, BinaryString.fromString("B"))
                };
        result = (InternalArray) agg.agg(null, new GenericArray(input1));
        assertThat(unnest(result, elementGetter)).containsExactlyInAnyOrder(input1);

        Object[] input2 =
                new Object[] {
                    GenericRow.of(1, BinaryString.fromString("A")),
                    GenericRow.of(2, BinaryString.fromString("A"))
                };
        result = (InternalArray) agg.agg(new GenericArray(input1), new GenericArray(input2));
        assertThat(unnest(result, elementGetter))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, BinaryString.fromString("A")),
                        GenericRow.of(1, BinaryString.fromString("B")),
                        GenericRow.of(2, BinaryString.fromString("A")));
    }

    @Test
    public void testFiledCollectAggWithArrayType() {
        ArrayType arrayType = new ArrayType(DataTypes.INT());
        FieldCollectAgg agg = new FieldCollectAgg(DataTypes.ARRAY(arrayType), true);

        InternalArray result;
        InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(arrayType);

        assertThat(agg.agg(null, null)).isNull();

        Object[] input1 =
                new Object[] {
                    new GenericArray(new Object[] {1, 1}), new GenericArray(new Object[] {1, 2})
                };
        result = (InternalArray) agg.agg(null, new GenericArray(input1));
        assertThat(unnest(result, elementGetter)).containsExactlyInAnyOrder(input1);

        Object[] input2 =
                new Object[] {
                    new GenericArray(new Object[] {1, 1}),
                    new GenericArray(new Object[] {1, 2}),
                    new GenericArray(new Object[] {2, 1})
                };
        result = (InternalArray) agg.agg(new GenericArray(input1), new GenericArray(input2));
        assertThat(unnest(result, elementGetter))
                .containsExactlyInAnyOrder(
                        new GenericArray(new Object[] {1, 1}),
                        new GenericArray(new Object[] {1, 2}),
                        new GenericArray(new Object[] {2, 1}));
    }

    @Test
    public void testFiledCollectAggWithMapType() {
        MapType mapType = new MapType(DataTypes.INT(), DataTypes.STRING());
        FieldCollectAgg agg = new FieldCollectAgg(DataTypes.ARRAY(mapType), true);

        InternalArray result;
        InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(mapType);

        assertThat(agg.agg(null, null)).isNull();

        Object[] input1 =
                new Object[] {new GenericMap(toMap(1, "A")), new GenericMap(toMap(1, "A", 2, "B"))};
        result = (InternalArray) agg.agg(null, new GenericArray(input1));
        assertThat(unnest(result, elementGetter)).containsExactlyInAnyOrder(input1);

        Object[] input2 =
                new Object[] {
                    new GenericMap(toMap(1, "A")),
                    new GenericMap(toMap(2, "B", 1, "A")),
                    new GenericMap(toMap(1, "C"))
                };
        result = (InternalArray) agg.agg(new GenericArray(input1), new GenericArray(input2));
        assertThat(unnest(result, elementGetter))
                .containsExactlyInAnyOrder(
                        new GenericMap(toMap(1, "A")),
                        new GenericMap(toMap(1, "A", 2, "B")),
                        new GenericMap(toMap(1, "C")));
    }

    @Test
    public void testFieldCollectAggWithoutDistinct() {
        FieldCollectAgg agg = new FieldCollectAgg(DataTypes.ARRAY(DataTypes.INT()), false);

        InternalArray result;
        InternalArray.ElementGetter elementGetter =
                InternalArray.createElementGetter(DataTypes.INT());

        assertThat(agg.agg(null, null)).isNull();

        result = (InternalArray) agg.agg(null, new GenericArray(new int[] {1, 1, 2}));
        assertThat(unnest(result, elementGetter)).containsExactlyInAnyOrder(1, 1, 2);

        result =
                (InternalArray)
                        agg.agg(
                                new GenericArray(new int[] {1, 1, 2}),
                                new GenericArray(new int[] {2, 3}));
        assertThat(unnest(result, elementGetter)).containsExactlyInAnyOrder(1, 1, 2, 2, 3);
    }

    @Test
    public void testFieldCollectAggRetractWithDistinct() {
        FieldCollectAgg agg;
        InternalArray.ElementGetter elementGetter;

        // primitive type
        agg = new FieldCollectAgg(DataTypes.ARRAY(DataTypes.INT()), true);
        elementGetter = InternalArray.createElementGetter(DataTypes.INT());
        InternalArray result =
                (InternalArray)
                        agg.retract(
                                new GenericArray(new int[] {1, 2, 3}),
                                new GenericArray(new int[] {1}));
        assertThat(unnest(result, elementGetter)).containsExactlyInAnyOrder(2, 3);

        // row type
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        agg = new FieldCollectAgg(DataTypes.ARRAY(rowType), true);
        elementGetter = InternalArray.createElementGetter(rowType);

        Object[] accElements =
                new Object[] {
                    GenericRow.of(1, BinaryString.fromString("A")),
                    GenericRow.of(1, BinaryString.fromString("B")),
                    GenericRow.of(2, BinaryString.fromString("B"))
                };
        Object[] retractElements =
                new Object[] {
                    GenericRow.of(1, BinaryString.fromString("A")),
                    GenericRow.of(2, BinaryString.fromString("B"))
                };
        result =
                (InternalArray)
                        agg.retract(
                                new GenericArray(accElements), new GenericArray(retractElements));
        assertThat(unnest(result, elementGetter))
                .containsExactlyInAnyOrder(GenericRow.of(1, BinaryString.fromString("B")));

        // array type
        ArrayType arrayType = new ArrayType(DataTypes.INT());
        agg = new FieldCollectAgg(DataTypes.ARRAY(arrayType), true);
        elementGetter = InternalArray.createElementGetter(arrayType);

        accElements =
                new Object[] {
                    new GenericArray(new Object[] {1, 1}),
                    new GenericArray(new Object[] {1, 2}),
                    new GenericArray(new Object[] {2, 1})
                };
        retractElements =
                new Object[] {
                    new GenericArray(new Object[] {1, 1}), new GenericArray(new Object[] {1, 2})
                };
        result =
                (InternalArray)
                        agg.retract(
                                new GenericArray(accElements), new GenericArray(retractElements));
        assertThat(unnest(result, elementGetter))
                .containsExactlyInAnyOrder(new GenericArray(new Object[] {2, 1}));

        // map type
        MapType mapType = new MapType(DataTypes.INT(), DataTypes.STRING());
        agg = new FieldCollectAgg(DataTypes.ARRAY(mapType), true);
        elementGetter = InternalArray.createElementGetter(mapType);

        accElements =
                new Object[] {
                    new GenericMap(toMap(1, "A")),
                    new GenericMap(toMap(2, "B", 1, "A")),
                    new GenericMap(toMap(1, "C"))
                };
        retractElements =
                new Object[] {new GenericMap(toMap(1, "A")), new GenericMap(toMap(1, "A", 2, "B"))};
        result =
                (InternalArray)
                        agg.retract(
                                new GenericArray(accElements), new GenericArray(retractElements));

        assertThat(unnest(result, elementGetter))
                .containsExactlyInAnyOrder(new GenericMap(toMap(1, "C")));
    }

    @Test
    public void testFieldCollectAggRetractWithoutDistinct() {
        FieldCollectAgg agg;
        InternalArray.ElementGetter elementGetter;

        // primitive type
        agg = new FieldCollectAgg(DataTypes.ARRAY(DataTypes.INT()), true);
        elementGetter = InternalArray.createElementGetter(DataTypes.INT());
        InternalArray result =
                (InternalArray)
                        agg.retract(
                                new GenericArray(new int[] {1, 1, 2, 2, 3}),
                                new GenericArray(new int[] {1, 2, 3}));
        assertThat(unnest(result, elementGetter)).containsExactlyInAnyOrder(1, 2);

        // row type
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        agg = new FieldCollectAgg(DataTypes.ARRAY(rowType), true);
        elementGetter = InternalArray.createElementGetter(rowType);

        Object[] accElements =
                new Object[] {
                    GenericRow.of(1, BinaryString.fromString("A")),
                    GenericRow.of(1, BinaryString.fromString("A")),
                    GenericRow.of(1, BinaryString.fromString("B")),
                    GenericRow.of(2, BinaryString.fromString("B"))
                };
        Object[] retractElements =
                new Object[] {
                    GenericRow.of(1, BinaryString.fromString("A")),
                    GenericRow.of(2, BinaryString.fromString("B"))
                };
        result =
                (InternalArray)
                        agg.retract(
                                new GenericArray(accElements), new GenericArray(retractElements));
        assertThat(unnest(result, elementGetter))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, BinaryString.fromString("A")),
                        GenericRow.of(1, BinaryString.fromString("B")));

        // array type
        ArrayType arrayType = new ArrayType(DataTypes.INT());
        agg = new FieldCollectAgg(DataTypes.ARRAY(arrayType), true);
        elementGetter = InternalArray.createElementGetter(arrayType);

        accElements =
                new Object[] {
                    new GenericArray(new Object[] {1, 1}),
                    new GenericArray(new Object[] {1, 1}),
                    new GenericArray(new Object[] {1, 2}),
                    new GenericArray(new Object[] {2, 1})
                };
        retractElements =
                new Object[] {
                    new GenericArray(new Object[] {1, 1}), new GenericArray(new Object[] {1, 2})
                };
        result =
                (InternalArray)
                        agg.retract(
                                new GenericArray(accElements), new GenericArray(retractElements));
        assertThat(unnest(result, elementGetter))
                .containsExactlyInAnyOrder(
                        new GenericArray(new Object[] {1, 1}),
                        new GenericArray(new Object[] {2, 1}));

        // map type
        MapType mapType = new MapType(DataTypes.INT(), DataTypes.STRING());
        agg = new FieldCollectAgg(DataTypes.ARRAY(mapType), true);
        elementGetter = InternalArray.createElementGetter(mapType);

        accElements =
                new Object[] {
                    new GenericMap(toMap(1, "A")),
                    new GenericMap(toMap(1, "A")),
                    new GenericMap(toMap(2, "B", 1, "A")),
                    new GenericMap(toMap(1, "C"))
                };
        retractElements =
                new Object[] {new GenericMap(toMap(1, "A")), new GenericMap(toMap(1, "A", 2, "B"))};
        result =
                (InternalArray)
                        agg.retract(
                                new GenericArray(accElements), new GenericArray(retractElements));

        assertThat(unnest(result, elementGetter))
                .containsExactlyInAnyOrder(
                        new GenericMap(toMap(1, "A")), new GenericMap(toMap(1, "C")));
    }

    @Test
    public void testFieldMergeMapAgg() {
        FieldMergeMapAgg agg =
                new FieldMergeMapAgg(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()));

        assertThat(agg.agg(null, null)).isNull();

        Object accumulator = agg.agg(null, new GenericMap(toMap(1, "A")));
        assertThat(toJavaMap(accumulator)).containsExactlyInAnyOrderEntriesOf(toMap(1, "A"));

        accumulator = agg.agg(accumulator, new GenericMap(toMap(1, "A", 2, "B")));
        assertThat(toJavaMap(accumulator))
                .containsExactlyInAnyOrderEntriesOf(toMap(1, "A", 2, "B"));

        accumulator = agg.agg(accumulator, new GenericMap(toMap(1, "a", 3, "c")));
        assertThat(toJavaMap(accumulator))
                .containsExactlyInAnyOrderEntriesOf(toMap(1, "a", 2, "B", 3, "c"));
    }

    @Test
    public void testFieldMergeMapAggRetract() {
        FieldMergeMapAgg agg =
                new FieldMergeMapAgg(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()));
        Object result =
                agg.retract(
                        new GenericMap(toMap(1, "A", 2, "B", 3, "C")),
                        new GenericMap(toMap(1, "A", 2, "A")));
        assertThat(toJavaMap(result)).containsExactlyInAnyOrderEntriesOf(toMap(3, "C"));
    }

    private Map<Object, Object> toMap(Object... kvs) {
        Map<Object, Object> result = new HashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            result.put(kvs[i], BinaryString.fromString((String) kvs[i + 1]));
        }
        return result;
    }

    private Map<Object, Object> toJavaMap(Object data) {
        InternalMap mapData = (InternalMap) data;
        InternalArray keyArray = mapData.keyArray();
        InternalArray valueArray = mapData.valueArray();
        Map<Object, Object> result = new HashMap<>();
        for (int i = 0; i < keyArray.size(); i++) {
            result.put(keyArray.getInt(i), valueArray.getString(i));
        }
        return result;
    }
}
