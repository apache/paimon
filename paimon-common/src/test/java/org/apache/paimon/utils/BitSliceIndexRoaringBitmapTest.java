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
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BitSliceIndexRoaringBitmap}. */
public class BitSliceIndexRoaringBitmapTest {

    private long base;
    private BitSliceIndexRoaringBitmap bsi;

    @BeforeEach
    public void setup() throws IOException {
        this.base = System.currentTimeMillis();
        BitSliceIndexRoaringBitmap.Appender appender =
                new BitSliceIndexRoaringBitmap.Appender(base, toPredicate(100));
        IntStream.range(0, 31).forEach(x -> appender.append(x, toPredicate(x)));
        IntStream.range(51, 100).forEach(x -> appender.append(x, toPredicate(x)));
        appender.append(100, toPredicate(30));
        this.bsi = appender.build();
    }

    @Test
    public void testSerde() throws IOException {
        BitSliceIndexRoaringBitmap.Appender appender =
                new BitSliceIndexRoaringBitmap.Appender(0, toPredicate(100));
        IntStream.range(0, 31).forEach(x -> appender.append(x, toPredicate(x)));
        IntStream.range(51, 100).forEach(x -> appender.append(x, toPredicate(x)));
        appender.append(100, toPredicate(30));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        appender.serialize(new DataOutputStream(out));

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        assertThat(BitSliceIndexRoaringBitmap.map(new DataInputStream(in)))
                .isEqualTo(appender.build());
    }

    @Test
    public void testEQ() {
        assertThat(bsi.eq(toPredicate(1))).isEqualTo(RoaringBitmap32.bitmapOf(1));
        assertThat(bsi.eq(toPredicate(32))).isEqualTo(RoaringBitmap32.bitmapOf());
        assertThat(bsi.eq(toPredicate(30))).isEqualTo(RoaringBitmap32.bitmapOf(30, 100));
    }

    @Test
    public void testLT() {
        assertThat(bsi.lt(toPredicate(30)))
                .isEqualTo(RoaringBitmap32.bitmapOf(IntStream.range(0, 30).toArray()));
        assertThat(bsi.lt(toPredicate(45)))
                .isEqualTo(
                        RoaringBitmap32.bitmapOf(
                                IntStream.concat(IntStream.range(0, 31), IntStream.range(100, 101))
                                        .toArray()));
    }

    @Test
    public void testLTE() {
        RoaringBitmap32 expected =
                RoaringBitmap32.bitmapOf(
                        IntStream.concat(IntStream.range(0, 31), IntStream.range(100, 101))
                                .toArray());
        assertThat(bsi.lte(toPredicate(30))).isEqualTo(expected);
        assertThat(bsi.lte(toPredicate(45))).isEqualTo(expected);
    }

    @Test
    public void testGT() {
        RoaringBitmap32 expected = RoaringBitmap32.bitmapOf(IntStream.range(51, 100).toArray());
        assertThat(bsi.gt(toPredicate(30))).isEqualTo(expected);
        assertThat(bsi.gt(toPredicate(45))).isEqualTo(expected);
    }

    @Test
    public void testGTE() {
        assertThat(bsi.gte(toPredicate(30)))
                .isEqualTo(
                        RoaringBitmap32.bitmapOf(
                                IntStream.concat(IntStream.range(30, 31), IntStream.range(51, 101))
                                        .toArray()));
        assertThat(bsi.gte(toPredicate(45)))
                .isEqualTo(RoaringBitmap32.bitmapOf(IntStream.range(51, 100).toArray()));
    }

    @Test
    public void testIsNotNull() {
        assertThat(bsi.isNotNull())
                .isEqualTo(
                        RoaringBitmap32.bitmapOf(
                                IntStream.concat(IntStream.range(0, 31), IntStream.range(51, 101))
                                        .toArray()));
    }

    private long toPredicate(long predicate) {
        return base + predicate;
    }
}
