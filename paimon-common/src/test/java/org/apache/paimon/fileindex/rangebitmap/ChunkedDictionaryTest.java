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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.rangebitmap.dictionary.chunked.ChunkedDictionary;
import org.apache.paimon.fileindex.rangebitmap.dictionary.chunked.KeyFactory;
import org.apache.paimon.fs.ByteArraySeekableStream;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test case for {@link ChunkedDictionary}. */
public class ChunkedDictionaryTest {

    private static final int CARDINALITY = 10000;
    private static final int BOUND = 1000000;

    @RepeatedTest(10)
    public void test() throws IOException {
        testFixedLengthChunkedDictionary(0); // test no chunked
        testFixedLengthChunkedDictionary(128); // test chunked

        testVariableLengthChunkedDictionary(0); // test no chunked
        testVariableLengthChunkedDictionary(512); // test chunked
    }

    @Test
    public void testBuildEmpty() throws IOException {
        KeyFactory.IntKeyFactory factory = new KeyFactory.IntKeyFactory();
        ChunkedDictionary.Appender appender = new ChunkedDictionary.Appender(factory, 0);
        ChunkedDictionary dictionary =
                new ChunkedDictionary(
                        new ByteArraySeekableStream(appender.serialize()), 0, factory);
        assertThat(dictionary.find(new Integer(0))).isEqualTo(-1);
        assertThat(dictionary.find(new Integer(1))).isEqualTo(-1);
        assertThat(dictionary.find(new Integer(3))).isEqualTo(-1);
    }

    private void testFixedLengthChunkedDictionary(int limitedSize) throws IOException {
        Random random = new Random();
        Set<Integer> set = new HashSet<>();
        for (int i = 0; i < CARDINALITY; i++) {
            int next;
            do {
                next = random.nextInt(BOUND);
                next = random.nextBoolean() ? next : -next;
            } while (set.contains(next));
            set.add(next);
        }

        List<Integer> expected = set.stream().sorted().collect(Collectors.toList());

        KeyFactory.IntKeyFactory factory = new KeyFactory.IntKeyFactory();
        ChunkedDictionary.Appender appender = new ChunkedDictionary.Appender(factory, limitedSize);
        for (int i = 0; i < expected.size(); i++) {
            appender.sortedAppend(expected.get(i), i);
        }

        ChunkedDictionary dictionary =
                new ChunkedDictionary(
                        new ByteArraySeekableStream(appender.serialize()), 0, factory);
        for (int i = 0; i < expected.size(); i++) {
            Integer value = expected.get(i);

            // find code by key
            int result = dictionary.find(value);
            assertThat(result).isEqualTo(i);
            assertThat(result).isEqualTo(Collections.binarySearch(expected, value));

            // find key by code
            assertThat(dictionary.find(i)).isEqualTo(value);
        }

        // find key by code out of range
        assertThatThrownBy(() -> dictionary.find(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> dictionary.find(expected.size()))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // not exists
        for (int i = 0; i < 10; i++) {
            Integer value = random.nextInt(BOUND) + BOUND;
            // find code by key
            int result = dictionary.find(value);
            assertThat(result).isNegative();
            assertThat(result).isEqualTo(Collections.binarySearch(expected, value));
        }
    }

    private void testVariableLengthChunkedDictionary(int limitedSize) throws IOException {
        List<BinaryString> expected = new ArrayList<>(CARDINALITY);
        for (int i = 0; i < CARDINALITY; i++) {
            expected.add(BinaryString.fromString(UUID.randomUUID().toString()));
        }
        expected.sort(Comparator.comparing(o -> o));

        KeyFactory.StringKeyFactory factory = new KeyFactory.StringKeyFactory();
        ChunkedDictionary.Appender appender = new ChunkedDictionary.Appender(factory, limitedSize);
        for (int i = 0; i < expected.size(); i++) {
            appender.sortedAppend(expected.get(i), i);
        }

        ChunkedDictionary dictionary =
                new ChunkedDictionary(
                        new ByteArraySeekableStream(appender.serialize()), 0, factory);
        for (int i = 0; i < expected.size(); i++) {
            BinaryString value = expected.get(i);

            // find code by key
            int result = dictionary.find(value);
            assertThat(result).isEqualTo(i);
            assertThat(result).isEqualTo(Collections.binarySearch(expected, value));

            // find key by code
            assertThat(dictionary.find(i)).isEqualTo(value);
        }

        // find key by code out of range
        assertThatThrownBy(() -> dictionary.find(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> dictionary.find(expected.size()))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // not exists
        for (int i = 0; i < 10; i++) {
            BinaryString value = BinaryString.fromString(UUID.randomUUID().toString() + i);
            // find code by key
            int result = dictionary.find(value);
            assertThat(result).isNegative();
            assertThat(result).isEqualTo(Collections.binarySearch(expected, value));
        }
    }
}
