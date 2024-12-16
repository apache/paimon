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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;

import static org.apache.paimon.utils.BitSliceIndexRoaringBitmap.Operation.EQ;
import static org.apache.paimon.utils.BitSliceIndexRoaringBitmap.Operation.GT;
import static org.apache.paimon.utils.BitSliceIndexRoaringBitmap.Operation.GTE;
import static org.apache.paimon.utils.BitSliceIndexRoaringBitmap.Operation.LT;
import static org.apache.paimon.utils.BitSliceIndexRoaringBitmap.Operation.LTE;
import static org.apache.paimon.utils.BitSliceIndexRoaringBitmap.Operation.NEQ;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BitSliceIndexRoaringBitmap}. */
public class BitSliceIndexRoaringBitmapTest {

    public static final int NUM_OF_ROWS = 100000;
    public static final int VALUE_BOUND = 1000;
    public static final int VALUE_LT_MIN = 0;
    public static final int VALUE_GT_MAX = VALUE_BOUND + 100;

    private Random random;
    private List<Pair> pairs;
    private BitSliceIndexRoaringBitmap bsi;

    @BeforeEach
    public void setup() throws IOException {
        this.random = new Random();
        List<Pair> pairs = new ArrayList<>();
        long min = 0;
        long max = 0;
        for (int i = 0; i < NUM_OF_ROWS; i++) {
            if (i % 5 == 0) {
                pairs.add(new Pair(i, null));
                continue;
            }
            long next = generateNextValue();
            min = Math.min(min == 0 ? next : min, next);
            max = Math.max(max == 0 ? next : max, next);
            pairs.add(new Pair(i, next));
        }
        BitSliceIndexRoaringBitmap.Appender appender =
                new BitSliceIndexRoaringBitmap.Appender(min, max);
        for (Pair pair : pairs) {
            if (pair.value == null) {
                continue;
            }
            appender.append(pair.index, pair.value);
        }
        this.bsi = appender.build();
        this.pairs = Collections.unmodifiableList(pairs);
    }

    @Test
    public void testSerde() throws IOException {
        BitSliceIndexRoaringBitmap.Appender appender =
                new BitSliceIndexRoaringBitmap.Appender(0, 10);
        appender.append(0, 0);
        appender.append(1, 1);
        appender.append(2, 2);
        appender.append(10, 6);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        appender.serialize(new DataOutputStream(out));

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        assertThat(BitSliceIndexRoaringBitmap.map(new DataInputStream(in)))
                .isEqualTo(appender.build());
    }

    @Test
    public void testEQ() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.eq(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> Objects.equals(x.value, predicate))
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap32::new,
                                            RoaringBitmap32::add,
                                            (x, y) -> x.or(y)));
        }

        // test predicate out of the value bound
        assertThat(bsi.eq(VALUE_LT_MIN)).isEqualTo(new RoaringBitmap32());
        assertThat(bsi.eq(VALUE_GT_MAX)).isEqualTo(new RoaringBitmap32());
    }

    @Test
    public void testLT() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.lt(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value < predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap32::new,
                                            RoaringBitmap32::add,
                                            (x, y) -> x.or(y)));
        }

        // test predicate out of the value bound
        assertThat(bsi.lt(VALUE_LT_MIN)).isEqualTo(new RoaringBitmap32());
        assertThat(bsi.lt(VALUE_GT_MAX)).isEqualTo(bsi.isNotNull());
    }

    @Test
    public void testLTE() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.lte(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value <= predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap32::new,
                                            RoaringBitmap32::add,
                                            (x, y) -> x.or(y)));
        }

        // test predicate out of the value bound
        assertThat(bsi.lte(VALUE_LT_MIN)).isEqualTo(new RoaringBitmap32());
        assertThat(bsi.lte(VALUE_GT_MAX)).isEqualTo(bsi.isNotNull());
    }

    @Test
    public void testGT() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.gt(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value > predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap32::new,
                                            RoaringBitmap32::add,
                                            (x, y) -> x.or(y)));
        }

        // test predicate out of the value bound
        assertThat(bsi.gt(VALUE_LT_MIN)).isEqualTo(bsi.isNotNull());
        assertThat(bsi.gt(VALUE_GT_MAX)).isEqualTo(new RoaringBitmap32());
    }

    @Test
    public void testGTE() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.gte(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value >= predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap32::new,
                                            RoaringBitmap32::add,
                                            (x, y) -> x.or(y)));
        }

        // test predicate out of the value bound
        assertThat(bsi.gte(VALUE_LT_MIN)).isEqualTo(bsi.isNotNull());
        assertThat(bsi.gte(VALUE_GT_MAX)).isEqualTo(new RoaringBitmap32());
    }

    @Test
    public void testIsNotNull() {
        assertThat(bsi.isNotNull())
                .isEqualTo(
                        pairs.stream()
                                .filter(x -> x.value != null)
                                .map(x -> x.index)
                                .collect(
                                        RoaringBitmap32::new,
                                        RoaringBitmap32::add,
                                        (x, y) -> x.or(y)));
    }

    @Test
    public void testCompareUsingMinMax() {
        // a predicate in the value bound
        final int predicate = generateNextValue();
        final Optional<RoaringBitmap32> empty = Optional.of(new RoaringBitmap32());
        final Optional<RoaringBitmap32> notNul = Optional.of(bsi.isNotNull());
        final Optional<RoaringBitmap32> inBound = Optional.empty();

        // test eq & neq
        assertThat(bsi.compareUsingMinMax(EQ, predicate, null)).isEqualTo(inBound);
        assertThat(bsi.compareUsingMinMax(EQ, VALUE_LT_MIN, null)).isEqualTo(empty);
        assertThat(bsi.compareUsingMinMax(EQ, VALUE_GT_MAX, null)).isEqualTo(empty);
        assertThat(bsi.compareUsingMinMax(NEQ, predicate, null)).isEqualTo(inBound);
        assertThat(bsi.compareUsingMinMax(NEQ, VALUE_LT_MIN, null)).isEqualTo(notNul);
        assertThat(bsi.compareUsingMinMax(NEQ, VALUE_GT_MAX, null)).isEqualTo(notNul);

        // test lt & lte
        assertThat(bsi.compareUsingMinMax(LT, predicate, null)).isEqualTo(inBound);
        assertThat(bsi.compareUsingMinMax(LTE, predicate, null)).isEqualTo(inBound);
        assertThat(bsi.compareUsingMinMax(LT, VALUE_LT_MIN, null)).isEqualTo(empty);
        assertThat(bsi.compareUsingMinMax(LTE, VALUE_LT_MIN, null)).isEqualTo(empty);
        assertThat(bsi.compareUsingMinMax(LT, VALUE_GT_MAX, null)).isEqualTo(notNul);
        assertThat(bsi.compareUsingMinMax(LTE, VALUE_GT_MAX, null)).isEqualTo(notNul);

        // test gt & gte
        assertThat(bsi.compareUsingMinMax(GT, predicate, null)).isEqualTo(inBound);
        assertThat(bsi.compareUsingMinMax(GTE, predicate, null)).isEqualTo(inBound);
        assertThat(bsi.compareUsingMinMax(GT, VALUE_LT_MIN, null)).isEqualTo(notNul);
        assertThat(bsi.compareUsingMinMax(GTE, VALUE_LT_MIN, null)).isEqualTo(notNul);
        assertThat(bsi.compareUsingMinMax(GT, VALUE_GT_MAX, null)).isEqualTo(empty);
        assertThat(bsi.compareUsingMinMax(GT, VALUE_GT_MAX, null)).isEqualTo(empty);
    }

    private int generateNextValue() {
        // return a value in the range [1, VALUE_BOUND)
        return random.nextInt(VALUE_BOUND) + 1;
    }

    private static class Pair {
        int index;
        Long value;

        public Pair(int index, Long value) {
            this.index = index;
            this.value = value;
        }
    }
}
