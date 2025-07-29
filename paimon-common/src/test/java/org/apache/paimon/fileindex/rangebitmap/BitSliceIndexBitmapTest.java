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
    }
}
