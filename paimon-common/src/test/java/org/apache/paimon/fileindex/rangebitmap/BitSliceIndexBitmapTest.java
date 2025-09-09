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

package org.apache.paimon.fileindex.rangebitmap;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test case for {@link BitSliceIndexBitmap}. */
public class BitSliceIndexBitmapTest {

    private static final int CARDINALITY = 100;
    private static final int ROW_COUNT = 10000;

    @RepeatedTest(5)
    public void test() throws IOException {
        Random random = new Random();
        List<Pair<Integer, Integer>> pairs = new ArrayList<>();
        BitSliceIndexBitmap.Appender appender = new BitSliceIndexBitmap.Appender(0, CARDINALITY);
        for (int i = 0; i < ROW_COUNT; i++) {
            Integer value = random.nextBoolean() ? random.nextInt(CARDINALITY) : null;
            pairs.add(Pair.of(i, value));
            if (value != null) {
                appender.append(i, value);
            }
        }

        ByteBuffer serialize = appender.serialize();
        ByteArraySeekableStream in = new ByteArraySeekableStream(serialize.array());
        BitSliceIndexBitmap bsi = new BitSliceIndexBitmap(in, 0);

        // test eq
        for (int i = 0; i < 10; i++) {
            int code = random.nextInt(CARDINALITY);
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, Integer> pair : pairs) {
                if (Objects.equals(code, pair.getValue())) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(bsi.eq(code)).isEqualTo(bitmap);
        }

        // test gt
        for (int i = 0; i < 10; i++) {
            int code = random.nextInt(CARDINALITY);
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, Integer> pair : pairs) {
                if (pair.getValue() != null && pair.getValue() > code) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(bsi.gt(code)).isEqualTo(bitmap);
        }

        // test gte
        for (int i = 0; i < 10; i++) {
            int code = random.nextInt(CARDINALITY);
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, Integer> pair : pairs) {
                if (pair.getValue() != null && pair.getValue() >= code) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(bsi.gte(code)).isEqualTo(bitmap);
        }

        // test is not null
        {
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, Integer> pair : pairs) {
                if (pair.getValue() != null) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(bsi.isNotNull()).isEqualTo(bitmap);
        }

        // test get
        for (Pair<Integer, Integer> pair : pairs) {
            assertThat(bsi.get(pair.getKey())).isEqualTo(pair.getValue());
        }

        // test topK
        for (int i = 0; i < 10; i++) {
            int k = random.nextInt(CARDINALITY);

            // without found set
            RoaringBitmap32 expected = new RoaringBitmap32();
            pairs.stream()
                    .filter(pair -> pair.getValue() != null)
                    .sorted(
                            (x, y) -> {
                                int result = y.getValue().compareTo(x.getValue());
                                if (result == 0) {
                                    return y.getKey().compareTo(x.getKey());
                                }
                                return result;
                            })
                    .map(Pair::getKey)
                    .limit(k)
                    .forEach(expected::add);
            RoaringBitmap32 actual = bsi.topK(k, null, true);
            assertThat(actual).isEqualTo(expected);

            // with found set
            expected = new RoaringBitmap32();
            RoaringBitmap32 foundSet = new RoaringBitmap32();
            for (int j = 0; j < random.nextInt(CARDINALITY); j++) {
                foundSet.add(random.nextInt(ROW_COUNT));
            }
            pairs.stream()
                    .filter(pair -> pair.getValue() != null)
                    .filter(pair -> foundSet.contains(pair.getKey()))
                    .sorted(
                            (x, y) -> {
                                int result = y.getValue().compareTo(x.getValue());
                                if (result == 0) {
                                    return y.getKey().compareTo(x.getKey());
                                }
                                return result;
                            })
                    .map(Pair::getKey)
                    .limit(k)
                    .forEach(expected::add);
            actual = bsi.topK(k, foundSet, true);
            assertThat(actual).isEqualTo(expected);
        }

        // test bottomK
        for (int i = 0; i < 10; i++) {
            int k = random.nextInt(CARDINALITY);
            RoaringBitmap32 expected = new RoaringBitmap32();
            pairs.stream()
                    .filter(pair -> pair.getValue() != null)
                    .sorted(
                            (x, y) -> {
                                int result = x.getValue().compareTo(y.getValue());
                                if (result == 0) {
                                    return y.getKey().compareTo(x.getKey());
                                }
                                return result;
                            })
                    .map(Pair::getKey)
                    .limit(k)
                    .forEach(expected::add);
            RoaringBitmap32 actual = bsi.bottomK(k, null, true);
            assertThat(actual).isEqualTo(expected);

            // with found set
            expected = new RoaringBitmap32();
            RoaringBitmap32 foundSet = new RoaringBitmap32();
            for (int j = 0; j < random.nextInt(CARDINALITY); j++) {
                foundSet.add(random.nextInt(ROW_COUNT));
            }
            pairs.stream()
                    .filter(pair -> pair.getValue() != null)
                    .filter(pair -> foundSet.contains(pair.getKey()))
                    .sorted(
                            (x, y) -> {
                                int result = x.getValue().compareTo(y.getValue());
                                if (result == 0) {
                                    return y.getKey().compareTo(x.getKey());
                                }
                                return result;
                            })
                    .map(Pair::getKey)
                    .limit(k)
                    .forEach(expected::add);
            actual = bsi.bottomK(k, foundSet, true);
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    public void testSingle() throws IOException {
        int size = 5;
        BitSliceIndexBitmap.Appender appender = new BitSliceIndexBitmap.Appender(0, 0);
        for (int i = 0; i < size; i++) {
            appender.append(i, 0);
        }

        ByteBuffer serialize = appender.serialize();
        ByteArraySeekableStream in = new ByteArraySeekableStream(serialize.array());
        BitSliceIndexBitmap bsi = new BitSliceIndexBitmap(in, 0);

        assertThat(bsi.isNotNull()).isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 2, 3, 4));
        assertThat(bsi.eq(0)).isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 2, 3, 4));
        assertThat(bsi.gt(0)).isEqualTo(RoaringBitmap32.bitmapOf());
        assertThat(bsi.gte(0)).isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 2, 3, 4));
        for (int i = 0; i < 5; i++) {
            assertThat(bsi.get(i)).isEqualTo(0);
        }
    }

    @Test
    public void testBuildEmpty() throws IOException {
        BitSliceIndexBitmap.Appender appender = new BitSliceIndexBitmap.Appender(0, 0);

        ByteBuffer serialize = appender.serialize();
        ByteArraySeekableStream in = new ByteArraySeekableStream(serialize.array());
        BitSliceIndexBitmap bsi = new BitSliceIndexBitmap(in, 0);

        assertThat(bsi.isNotNull()).isEqualTo(RoaringBitmap32.bitmapOf());
        assertThat(bsi.eq(0)).isEqualTo(RoaringBitmap32.bitmapOf());
        assertThat(bsi.gt(0)).isEqualTo(RoaringBitmap32.bitmapOf());
        assertThat(bsi.gte(0)).isEqualTo(RoaringBitmap32.bitmapOf());
    }
}
