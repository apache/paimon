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

package org.apache.paimon.globalindex.fmindex;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Mutation-oriented tests for the bounded-memory suffix-array builder used by FM index. */
public class FMIndexSuffixArrayTest {

    @Test
    public void testKnownText() {
        // banana plus its unique, lexicographically smallest terminator.
        int[] text = {2, 1, 3, 1, 3, 1, 0};
        assertThat(FMIndexSuffixArray.build(text, 3)).containsExactly(6, 5, 3, 1, 0, 4, 2);
    }

    @Test
    public void testRandomizedAgainstNaiveSuffixSort() {
        Random random = new Random(1847);
        for (int length = 1; length <= 80; length++) {
            for (int round = 0; round < 30; round++) {
                int alphabet = 1 + random.nextInt(12);
                int[] text = new int[length + 1];
                for (int i = 0; i < length; i++) {
                    text[i] = 1 + random.nextInt(alphabet);
                }
                text[length] = 0;
                assertThat(FMIndexSuffixArray.build(text, alphabet))
                        .as("length=%s, round=%s", length, round)
                        .containsExactly(naive(text));
                assertThat(FMIndexSuffixArray.build(toChars(text), alphabet))
                        .as("compact length=%s, round=%s", length, round)
                        .containsExactly(naive(text));
            }
        }
    }

    @Test
    public void testLargeRepetitiveInput() {
        int[] text = new int[200_001];
        Arrays.fill(text, 1);
        text[text.length - 1] = 0;

        int[] suffixArray = FMIndexSuffixArray.build(text, 1);
        assertThat(suffixArray).hasSize(text.length);
        for (int i = 0; i < suffixArray.length; i++) {
            assertThat(suffixArray[i]).isEqualTo(text.length - i - 1);
        }
    }

    private static char[] toChars(int[] text) {
        char[] result = new char[text.length];
        for (int i = 0; i < text.length; i++) {
            result[i] = (char) text[i];
        }
        return result;
    }

    private static int[] naive(int[] text) {
        Integer[] suffixes = new Integer[text.length];
        for (int i = 0; i < suffixes.length; i++) {
            suffixes[i] = i;
        }
        Arrays.sort(suffixes, suffixComparator(text));
        int[] result = new int[suffixes.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = suffixes[i];
        }
        return result;
    }

    private static Comparator<Integer> suffixComparator(int[] text) {
        return (left, right) -> {
            int l = left;
            int r = right;
            while (l < text.length && r < text.length) {
                int comparison = Integer.compare(text[l++], text[r++]);
                if (comparison != 0) {
                    return comparison;
                }
            }
            return Integer.compare(text.length - left, text.length - right);
        };
    }
}
