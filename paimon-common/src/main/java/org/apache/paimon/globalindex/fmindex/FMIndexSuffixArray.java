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

import org.apache.paimon.utils.Preconditions;

/** Bounded-memory linear-time suffix-array construction using SA-IS. */
final class FMIndexSuffixArray {

    private FMIndexSuffixArray() {}

    static int[] build(int[] text, int upper) {
        Preconditions.checkArgument(text.length > 0, "FM index text must not be empty.");
        Preconditions.checkArgument(upper >= 0, "FM index alphabet upper bound is invalid.");
        for (int value : text) {
            Preconditions.checkArgument(
                    value >= 0 && value <= upper,
                    "FM index symbol is outside the configured alphabet.");
        }
        return saIs(new IntSymbols(text), upper);
    }

    static int[] build(char[] text, int upper) {
        Preconditions.checkArgument(text.length > 0, "FM index text must not be empty.");
        Preconditions.checkArgument(upper >= 0, "FM index alphabet upper bound is invalid.");
        for (char value : text) {
            Preconditions.checkArgument(
                    value <= upper, "FM index symbol is outside the configured alphabet.");
        }
        return saIs(new CharSymbols(text), upper);
    }

    private static int[] saIs(Symbols text, int upper) {
        int length = text.length();
        if (length == 1) {
            return new int[] {0};
        }
        if (length == 2) {
            return text.get(0) < text.get(1) ? new int[] {0, 1} : new int[] {1, 0};
        }

        boolean[] sType = new boolean[length];
        for (int i = length - 2; i >= 0; i--) {
            int symbol = text.get(i);
            int nextSymbol = text.get(i + 1);
            sType[i] = symbol == nextSymbol ? sType[i + 1] : symbol < nextSymbol;
        }

        int[] bucketEnds = new int[upper + 1];
        int[] bucketStarts = new int[upper + 1];
        for (int i = 0; i < length; i++) {
            int symbol = text.get(i);
            if (sType[i]) {
                bucketStarts[symbol + 1]++;
            } else {
                bucketEnds[symbol]++;
            }
        }
        for (int symbol = 0; symbol <= upper; symbol++) {
            bucketEnds[symbol] += bucketStarts[symbol];
            if (symbol < upper) {
                bucketStarts[symbol + 1] += bucketEnds[symbol];
            }
        }

        LmsIndex lmsIndex = LmsIndex.create(sType);
        int lmsCount = lmsIndex.count();
        int[] lms = new int[lmsCount];
        int lmsPosition = 0;
        for (int i = 1; i < length; i++) {
            if (lmsIndex.ordinal(i) >= 0) {
                lms[lmsPosition++] = i;
            }
        }

        int[] suffixArray = new int[length];
        induce(text, upper, sType, bucketStarts, bucketEnds, lms, suffixArray);
        if (lmsCount == 0) {
            return suffixArray;
        }

        int[] sortedLms = new int[lmsCount];
        int sortedPosition = 0;
        for (int suffix : suffixArray) {
            if (suffix >= 0 && lmsIndex.ordinal(suffix) >= 0) {
                sortedLms[sortedPosition++] = suffix;
            }
        }
        Preconditions.checkState(sortedPosition == lmsCount, "FM index SA-IS lost an LMS suffix.");

        int[] reducedText = new int[lmsCount];
        int reducedUpper = 0;
        reducedText[lmsIndex.ordinal(sortedLms[0])] = 0;
        for (int i = 1; i < lmsCount; i++) {
            int previous = sortedLms[i - 1];
            int current = sortedLms[i];
            int previousOrdinal = lmsIndex.ordinal(previous);
            int currentOrdinal = lmsIndex.ordinal(current);
            int previousEnd =
                    previousOrdinal + 1 < lmsCount ? lms[previousOrdinal + 1] + 1 : length;
            int currentEnd = currentOrdinal + 1 < lmsCount ? lms[currentOrdinal + 1] + 1 : length;
            boolean same = previousEnd - previous == currentEnd - current;
            while (same && previous < previousEnd) {
                if (text.get(previous) != text.get(current) || sType[previous] != sType[current]) {
                    same = false;
                }
                previous++;
                current++;
            }
            if (!same) {
                reducedUpper++;
            }
            reducedText[currentOrdinal] = reducedUpper;
        }

        int[] reducedSuffixArray;
        if (reducedUpper + 1 == lmsCount) {
            reducedSuffixArray = new int[lmsCount];
            for (int i = 0; i < lmsCount; i++) {
                reducedSuffixArray[reducedText[i]] = i;
            }
        } else {
            reducedSuffixArray = saIs(new IntSymbols(reducedText), reducedUpper);
        }
        for (int i = 0; i < lmsCount; i++) {
            sortedLms[i] = lms[reducedSuffixArray[i]];
        }
        induce(text, upper, sType, bucketStarts, bucketEnds, sortedLms, suffixArray);
        return suffixArray;
    }

    private static void induce(
            Symbols text,
            int upper,
            boolean[] sType,
            int[] bucketStarts,
            int[] bucketEnds,
            int[] lms,
            int[] suffixArray) {
        java.util.Arrays.fill(suffixArray, -1);
        int[] buffer = java.util.Arrays.copyOf(bucketEnds, upper + 1);
        for (int suffix : lms) {
            if (suffix < text.length()) {
                suffixArray[buffer[text.get(suffix)]++] = suffix;
            }
        }
        buffer = java.util.Arrays.copyOf(bucketStarts, upper + 1);
        suffixArray[buffer[text.get(text.length() - 1)]++] = text.length() - 1;
        for (int i = 0; i < suffixArray.length; i++) {
            int suffix = suffixArray[i];
            if (suffix >= 1 && !sType[suffix - 1]) {
                suffixArray[buffer[text.get(suffix - 1)]++] = suffix - 1;
            }
        }
        buffer = java.util.Arrays.copyOf(bucketStarts, upper + 1);
        for (int i = suffixArray.length - 1; i >= 0; i--) {
            int suffix = suffixArray[i];
            if (suffix >= 1 && sType[suffix - 1]) {
                suffixArray[--buffer[text.get(suffix - 1) + 1]] = suffix - 1;
            }
        }
    }

    private interface Symbols {
        int length();

        int get(int position);
    }

    private static final class CharSymbols implements Symbols {
        private final char[] values;

        private CharSymbols(char[] values) {
            this.values = values;
        }

        @Override
        public int length() {
            return values.length;
        }

        @Override
        public int get(int position) {
            return values[position];
        }
    }

    private static final class IntSymbols implements Symbols {
        private final int[] values;

        private IntSymbols(int[] values) {
            this.values = values;
        }

        @Override
        public int length() {
            return values.length;
        }

        @Override
        public int get(int position) {
            return values[position];
        }
    }

    /** Compact membership and rank structure replacing an {@code int[length + 1]} LMS map. */
    private static final class LmsIndex {
        private final long[] words;
        private final int[] wordPrefixes;
        private final int count;

        private LmsIndex(long[] words, int[] wordPrefixes, int count) {
            this.words = words;
            this.wordPrefixes = wordPrefixes;
            this.count = count;
        }

        private static LmsIndex create(boolean[] sType) {
            long[] words = new long[(int) (((long) sType.length + Long.SIZE - 1) / Long.SIZE)];
            for (int i = 1; i < sType.length; i++) {
                if (!sType[i - 1] && sType[i]) {
                    words[i >>> 6] |= 1L << (i & 63);
                }
            }
            int[] wordPrefixes = new int[words.length];
            int count = 0;
            for (int i = 0; i < words.length; i++) {
                wordPrefixes[i] = count;
                count += Long.bitCount(words[i]);
            }
            return new LmsIndex(words, wordPrefixes, count);
        }

        private int count() {
            return count;
        }

        private int ordinal(int position) {
            if (position < 0) {
                return -1;
            }
            int wordIndex = position >>> 6;
            if (wordIndex >= words.length) {
                return -1;
            }
            long bit = 1L << (position & 63);
            long word = words[wordIndex];
            if ((word & bit) == 0) {
                return -1;
            }
            return wordPrefixes[wordIndex] + Long.bitCount(word & (bit - 1));
        }
    }
}
