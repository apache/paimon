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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link StringUtils}. */
class StringUtilsTest {

    @Nested
    class IsNullOrWhitespaceOnlyTests {

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {" ", "  ", "\t", "\n", "\r", " \t\n\r "})
        void testNullOrWhitespaceOnlyStrings(String input) {
            assertThat(StringUtils.isNullOrWhitespaceOnly(input)).isTrue();
        }

        @ParameterizedTest
        @ValueSource(strings = {"a", " a ", "hello", "hello world"})
        void testNonWhitespaceStrings(String input) {
            assertThat(StringUtils.isNullOrWhitespaceOnly(input)).isFalse();
        }
    }

    @Nested
    class ByteToHexStringTests {

        @Test
        void testByteToHexStringWithRange() {
            byte[] bytes = {0x00, 0x0F, (byte) 0xFF, 0x12, 0x34};
            String result = StringUtils.byteToHexString(bytes, 1, 4);
            assertThat(result).isEqualTo("0fff12");
        }

        @Test
        void testByteToHexStringFullArray() {
            byte[] bytes = {0x00, 0x0F, (byte) 0xFF};
            String result = StringUtils.byteToHexString(bytes);
            assertThat(result).isEqualTo("000fff");
        }

        @Test
        void testByteToHexStringEmptyRange() {
            byte[] bytes = {0x00, 0x0F, (byte) 0xFF};
            String result = StringUtils.byteToHexString(bytes, 1, 1);
            assertThat(result).isEmpty();
        }

        @Test
        void testByteToHexStringNullArray() {
            assertThatThrownBy(() -> StringUtils.byteToHexString(null, 0, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("bytes == null");
        }

        @Test
        void testByteToHexStringAllValues() {
            byte[] bytes = new byte[256];
            for (int i = 0; i < 256; i++) {
                bytes[i] = (byte) i;
            }
            String result = StringUtils.byteToHexString(bytes);
            assertThat(result).hasSize(512); // 256 bytes * 2 hex chars each
            assertThat(result).startsWith("000102");
            assertThat(result).endsWith("fdfeff");
        }
    }

    @Nested
    class BytesToBinaryStringTests {

        @Test
        void testBytesToBinaryString() {
            byte[] bytes = {0x00, 0x0F, (byte) 0xFF};
            String result = StringUtils.bytesToBinaryString(bytes);
            assertThat(result).isEqualTo("000000000000111111111111");
        }

        @Test
        void testBytesToBinaryStringEmptyArray() {
            byte[] bytes = {};
            String result = StringUtils.bytesToBinaryString(bytes);
            assertThat(result).isEmpty();
        }

        @Test
        void testBytesToBinaryStringSingleByte() {
            byte[] bytes = {(byte) 0xAA}; // 10101010
            String result = StringUtils.bytesToBinaryString(bytes);
            assertThat(result).isEqualTo("10101010");
        }
    }

    @Nested
    class GetRandomStringTests {

        @Test
        void testGetRandomStringWithinRange() {
            Random rnd = new Random(42);
            String result = StringUtils.getRandomString(rnd, 5, 10);
            assertThat(result.length()).isBetween(5, 10);
        }

        @Test
        void testGetRandomStringExactLength() {
            Random rnd = new Random(42);
            String result = StringUtils.getRandomString(rnd, 7, 7);
            assertThat(result).hasSize(7);
        }

        @Test
        void testGetRandomStringWithCharRange() {
            Random rnd = new Random(42);
            String result = StringUtils.getRandomString(rnd, 10, 10, 'a', 'z');
            assertThat(result).hasSize(10);
            for (char c : result.toCharArray()) {
                assertThat(c).isBetween('a', 'z');
            }
        }

        @Test
        void testGetRandomStringMinLength() {
            Random rnd = new Random(42);
            String result = StringUtils.getRandomString(rnd, 0, 5);
            assertThat(result.length()).isBetween(0, 5);
        }
    }

    @Nested
    class RepeatTests {

        @ParameterizedTest
        @CsvSource({"abc, 3, abcabcabc", "abc, 0, ''", "abc, 1, abc", "'', 5, ''"})
        void testRepeatValidCases(String input, int count, String expected) {
            String result = StringUtils.repeat(input, count);
            assertThat(result).isEqualTo(expected);
        }

        @Test
        void testRepeatNullString() {
            assertThatThrownBy(() -> StringUtils.repeat(null, 3))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void testRepeatNegativeCount() {
            assertThatThrownBy(() -> StringUtils.repeat("abc", -1))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void testRepeatLargeString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                sb.append("a");
            }
            String input = sb.toString();
            String result = StringUtils.repeat(input, 2);
            assertThat(result).hasSize(2000);
        }
    }

    @Nested
    class ReplaceTests {

        @ParameterizedTest
        @CsvSource({
            "aba, a, z, zbz",
            "aba, a, '', b",
            "abc, x, z, abc",
            "'', a, z, ''",
            "abc, '', z, abc"
        })
        void testReplaceBasicCases(
                String text, String search, String replacement, String expected) {
            String result = StringUtils.replace(text, search, replacement);
            assertThat(result).isEqualTo(expected);
        }

        @ParameterizedTest
        @CsvSource({
            "abaa, a, z, 1, zbaa",
            "abaa, a, z, 2, zbza",
            "abaa, a, z, -1, zbzz",
            "aba, a, z, 0, aba"
        })
        void testReplaceWithMaxCases(
                String text, String search, String replacement, int max, String expected) {
            String result = StringUtils.replace(text, search, replacement, max);
            assertThat(result).isEqualTo(expected);
        }

        @ParameterizedTest
        @CsvSource(
                value = {"null, a, z, null", "abc, null, z, abc", "aba, a, null, aba"},
                nullValues = "null")
        void testReplaceWithNullValues(
                String text, String search, String replacement, String expected) {
            String result = StringUtils.replace(text, search, replacement);
            assertThat(result).isEqualTo(expected);
        }
    }

    @Nested
    class IsEmptyTests {

        @Test
        void testEmptyOrNull() {
            assertThat(StringUtils.isEmpty(null)).isTrue();
            assertThat(StringUtils.isEmpty("")).isTrue();
        }

        @ParameterizedTest
        @ValueSource(strings = {" ", "a", "hello", "  hello  "})
        void testNonEmpty(String input) {
            assertThat(StringUtils.isEmpty(input)).isFalse();
        }

        @Test
        void testEmptyStringBuilder() {
            StringBuilder sb = new StringBuilder();
            assertThat(StringUtils.isEmpty(sb)).isTrue();
        }

        @Test
        void testNonEmptyStringBuilder() {
            StringBuilder sb = new StringBuilder("test");
            assertThat(StringUtils.isEmpty(sb)).isFalse();
        }
    }

    @Nested
    class RandomNumericStringTests {

        @Test
        void testRandomNumericStringLength() {
            String result = StringUtils.randomNumericString(5);
            assertThat(result).hasSize(5);
        }

        @Test
        void testRandomNumericStringContainsOnlyDigits() {
            String result = StringUtils.randomNumericString(10);
            assertThat(result).matches("\\d+");
        }

        @Test
        void testRandomNumericStringZeroLength() {
            String result = StringUtils.randomNumericString(0);
            assertThat(result).isEmpty();
        }

        @Test
        void testRandomNumericStringLargeLength() {
            String result = StringUtils.randomNumericString(100);
            assertThat(result).hasSize(100);
            assertThat(result).matches("\\d+");
        }
    }

    @Nested
    class SplitTests {

        @Test
        void testSplitBasicCases() {
            assertThat(StringUtils.split("ab:cd:ef", ":")).containsExactly("ab", "cd", "ef");
            assertThat(StringUtils.split("abc def", " ")).containsExactly("abc", "def");
            assertThat(StringUtils.split("abc  def", " ")).containsExactly("abc", "def");
            assertThat(StringUtils.split("a,b,c", ",")).containsExactly("a", "b", "c");
        }

        @Test
        void testSplitEdgeCases() {
            assertThat(StringUtils.split(null, ":")).isNull();
            assertThat(StringUtils.split("", ":")).isEmpty();
            assertThat(StringUtils.split("abc def", null)).containsExactly("abc", "def");
        }

        @Test
        void testSplitWithMax() {
            String[] result = StringUtils.split("a:b:c:d", ":", 2, false);
            assertThat(result).containsExactly("a", "b:c:d");
        }

        @Test
        void testSplitPreserveAllTokens() {
            String[] result = StringUtils.split("a::b", ":", -1, true);
            assertThat(result).containsExactly("a", "", "b");
        }
    }

    @Nested
    class JoinTests {

        @Test
        void testJoinIterableBasicCases() {
            assertThat(StringUtils.join(Arrays.asList("a", "b", "c"), ",")).isEqualTo("a,b,c");
            assertThat(StringUtils.join(Arrays.asList("a", "b", "c"), null)).isEqualTo("abc");
            assertThat(StringUtils.join(Arrays.asList("single"), ",")).isEqualTo("single");
            assertThat(StringUtils.join(Arrays.asList("a", null, "c"), ",")).isEqualTo("a,,c");
        }

        @Test
        void testJoinIterableEdgeCases() {
            assertThat(StringUtils.join((Iterable<?>) null, ",")).isNull();
            assertThat(StringUtils.join(Arrays.asList(), ",")).isEmpty();
        }

        @Test
        void testJoinIterator() {
            List<String> items = Arrays.asList("x", "y", "z");
            String result = StringUtils.join(items.iterator(), "-");
            assertThat(result).isEqualTo("x-y-z");
        }

        @Test
        void testJoinNullIterator() {
            String result = StringUtils.join((java.util.Iterator<?>) null, ",");
            assertThat(result).isNull();
        }
    }

    @Nested
    class QuoteTests {

        @ParameterizedTest
        @CsvSource({"hello, `hello`", "'', ``", "hello world!, `hello world!`"})
        void testQuote(String input, String expected) {
            String result = StringUtils.quote(input);
            assertThat(result).isEqualTo(expected);
        }
    }

    @Nested
    class ToLowerCaseIfNeedTests {

        @ParameterizedTest
        @CsvSource({
            "HELLO, true, HELLO",
            "HELLO, false, hello",
            "hello, false, hello",
            "HeLLo, false, hello"
        })
        void testToLowerCaseIfNeed(String input, boolean caseSensitive, String expected) {
            String result = StringUtils.toLowerCaseIfNeed(input, caseSensitive);
            assertThat(result).isEqualTo(expected);
        }
    }

    @Nested
    class IsNumericTests {

        @Test
        void testIsNumericNull() {
            assertThat(StringUtils.isNumeric(null)).isFalse();
        }

        @ParameterizedTest
        @ValueSource(
                strings = {
                    "0",
                    "1",
                    "123",
                    "999",
                    "-1",
                    "-123",
                    "-999",
                    "-0",
                    "123456789012345",
                    "-123456789012345"
                })
        void testIsNumericValidNumbers(String input) {
            assertThat(StringUtils.isNumeric(input)).isTrue();
        }

        @ParameterizedTest
        @ValueSource(
                strings = {"", " ", "abc", "12.3", "12a", "a12", " 12", "12 ", "+12", "-", "1 23"})
        void testIsNumericInvalidNumbers(String input) {
            assertThat(StringUtils.isNumeric(input)).isFalse();
        }
    }

    @Nested
    class TruncatedStringTests {

        @Test
        void testTruncatedStringWithinMaxFields() {
            List<String> items = Arrays.asList("a", "b", "c");
            String result = StringUtils.truncatedString(items, "[", ", ", "]", 5);
            assertThat(result).isEqualTo("[a, b, c]");
        }

        @Test
        void testTruncatedStringExactlyMaxFields() {
            List<String> items = Arrays.asList("a", "b", "c");
            String result = StringUtils.truncatedString(items, "[", ", ", "]", 3);
            assertThat(result).isEqualTo("[a, b, c]");
        }

        @Test
        void testTruncatedStringExceedsMaxFields() {
            List<String> items = Arrays.asList("a", "b", "c", "d", "e");
            String result = StringUtils.truncatedString(items, "[", ", ", "]", 3);
            assertThat(result).isEqualTo("[a, b, ... 3 more fields]");
        }

        @Test
        void testTruncatedStringExceedsMaxFieldsWithSeparator() {
            List<Integer> items = Arrays.asList(1, 2, 3, 4, 5, 6);
            String result = StringUtils.truncatedString(items, "(", "-", ")", 4);
            assertThat(result).isEqualTo("(1-2-3-... 3 more fields)");
        }

        @Test
        void testTruncatedStringEmptyCollection() {
            List<String> items = Arrays.asList();
            String result = StringUtils.truncatedString(items, "[", ", ", "]", 3);
            assertThat(result).isEqualTo("[]");
        }

        @Test
        void testTruncatedStringSingleElement() {
            List<String> items = Arrays.asList("only");
            String result = StringUtils.truncatedString(items, "[", ", ", "]", 5);
            assertThat(result).isEqualTo("[only]");
        }

        @Test
        void testTruncatedStringMaxFieldsZero() {
            List<String> items = Arrays.asList("a", "b", "c");
            String result = StringUtils.truncatedString(items, "[", ", ", "]", 0);
            assertThat(result).isEqualTo("[, ... 3 more fields]");
        }

        @Test
        void testTruncatedStringMaxFieldsOne() {
            List<String> items = Arrays.asList("a", "b", "c", "d");
            String result = StringUtils.truncatedString(items, "[", ", ", "]", 1);
            assertThat(result).isEqualTo("[, ... 4 more fields]");
        }

        @Test
        void testTruncatedStringLargeCollection() {
            List<Integer> items = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            String result = StringUtils.truncatedString(items, "{", ", ", "}", 5);
            assertThat(result).isEqualTo("{1, 2, 3, 4, ... 6 more fields}");
        }

        @Test
        void testTruncatedStringWithEmptyStrings() {
            List<String> items = Arrays.asList("", "a", "", "b", "");
            String result = StringUtils.truncatedString(items, "[", "|", "]", 3);
            assertThat(result).isEqualTo("[|a|... 3 more fields]");
        }

        @Test
        void testTruncatedStringWithNullElements() {
            List<String> items = Arrays.asList("a", null, "b", "c");
            String result = StringUtils.truncatedString(items, "[", ", ", "]", 3);
            assertThat(result).isEqualTo("[a, null, ... 2 more fields]");
        }

        @Test
        void testTruncatedStringWithCustomDelimiters() {
            List<String> items = Arrays.asList("apple", "banana", "cherry", "date");
            String result = StringUtils.truncatedString(items, "<", " | ", ">", 3);
            assertThat(result).isEqualTo("<apple | banana | ... 2 more fields>");
        }

        @Test
        void testTruncatedStringWithEmptyDelimiters() {
            List<String> items = Arrays.asList("a", "b", "c");
            String result = StringUtils.truncatedString(items, "", "", "", 5);
            assertThat(result).isEqualTo("abc");
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testLargeStringOperations() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 10000; i++) {
                sb.append("a");
            }
            String largeString = sb.toString();
            assertThat(StringUtils.isEmpty(largeString)).isFalse();
            assertThat(StringUtils.isNullOrWhitespaceOnly(largeString)).isFalse();
        }

        @Test
        void testUnicodeCharacters() {
            String unicode = "Hello 世界 🌍";
            assertThat(StringUtils.isEmpty(unicode)).isFalse();
            assertThat(StringUtils.isNullOrWhitespaceOnly(unicode)).isFalse();
            assertThat(StringUtils.quote(unicode)).isEqualTo("`Hello 世界 🌍`");
        }

        @Test
        void testSpecialWhitespaceCharacters() {
            // Test various Unicode whitespace characters that are recognized by
            // Character.isWhitespace()
            String specialWhitespace =
                    "\u0009\u000B\u000C\u001C\u001D\u001E\u001F"; // Tab, VT, FF, FS, GS, RS, US
            assertThat(StringUtils.isNullOrWhitespaceOnly(specialWhitespace)).isTrue();
        }
    }
}
