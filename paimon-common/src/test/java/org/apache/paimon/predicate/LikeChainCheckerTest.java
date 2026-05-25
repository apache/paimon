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

package org.apache.paimon.predicate;

import org.apache.paimon.data.BinaryString;

import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LikeChainChecker}. */
public class LikeChainCheckerTest {

    @Test
    public void testCompileRejectsUnderscore() {
        assertThat(LikeChainChecker.compile("a_b")).isNull();
        assertThat(LikeChainChecker.compile("_")).isNull();
        assertThat(LikeChainChecker.compile("%abc_def%")).isNull();
    }

    @Test
    public void testCompileRejectsBackslashEscape() {
        assertThat(LikeChainChecker.compile("a\\%b")).isNull();
        assertThat(LikeChainChecker.compile("\\")).isNull();
    }

    @Test
    public void testCompileAcceptsPureWildcardPatterns() {
        assertThat(LikeChainChecker.compile("")).isNotNull();
        assertThat(LikeChainChecker.compile("%")).isNotNull();
        assertThat(LikeChainChecker.compile("%%")).isNotNull();
        assertThat(LikeChainChecker.compile("abc")).isNotNull();
        assertThat(LikeChainChecker.compile("abc%")).isNotNull();
        assertThat(LikeChainChecker.compile("%abc")).isNotNull();
        assertThat(LikeChainChecker.compile("%abc%")).isNotNull();
        assertThat(LikeChainChecker.compile("abc%def")).isNotNull();
        assertThat(LikeChainChecker.compile("abc%def%ghi")).isNotNull();
        assertThat(LikeChainChecker.compile("%abc%def%")).isNotNull();
    }

    @Test
    public void testExactMatch() {
        LikeChainChecker checker = LikeChainChecker.compile("abc");
        assertThat(checker.test(b("abc"))).isTrue();
        assertThat(checker.test(b("abcd"))).isFalse();
        assertThat(checker.test(b("ab"))).isFalse();
        assertThat(checker.test(b(""))).isFalse();
    }

    @Test
    public void testStartsWith() {
        LikeChainChecker checker = LikeChainChecker.compile("abc%");
        assertThat(checker.test(b("abc"))).isTrue();
        assertThat(checker.test(b("abcdef"))).isTrue();
        assertThat(checker.test(b("ab"))).isFalse();
        assertThat(checker.test(b("xabc"))).isFalse();
    }

    @Test
    public void testEndsWith() {
        LikeChainChecker checker = LikeChainChecker.compile("%abc");
        assertThat(checker.test(b("abc"))).isTrue();
        assertThat(checker.test(b("xyzabc"))).isTrue();
        assertThat(checker.test(b("abcd"))).isFalse();
        assertThat(checker.test(b("ab"))).isFalse();
    }

    @Test
    public void testContains() {
        LikeChainChecker checker = LikeChainChecker.compile("%abc%");
        assertThat(checker.test(b("abc"))).isTrue();
        assertThat(checker.test(b("xabcy"))).isTrue();
        assertThat(checker.test(b("xabc"))).isTrue();
        assertThat(checker.test(b("abcy"))).isTrue();
        assertThat(checker.test(b("ab"))).isFalse();
        assertThat(checker.test(b(""))).isFalse();
    }

    @Test
    public void testBothAnchoredTwoSegments() {
        LikeChainChecker checker = LikeChainChecker.compile("abc%def");
        assertThat(checker.test(b("abcdef"))).isTrue();
        assertThat(checker.test(b("abcXdef"))).isTrue();
        assertThat(checker.test(b("abcXXXdef"))).isTrue();
        assertThat(checker.test(b("abc"))).isFalse();
        assertThat(checker.test(b("def"))).isFalse();
        assertThat(checker.test(b("abdef"))).isFalse();
        assertThat(checker.test(b("abcde"))).isFalse();
    }

    @Test
    public void testLeftAnchoredOnly() {
        LikeChainChecker checker = LikeChainChecker.compile("abc%def%");
        assertThat(checker.test(b("abcdef"))).isTrue();
        assertThat(checker.test(b("abcdefX"))).isTrue();
        assertThat(checker.test(b("abcXdef"))).isTrue();
        assertThat(checker.test(b("abcXdefY"))).isTrue();
        assertThat(checker.test(b("abc"))).isFalse();
        assertThat(checker.test(b("xabcdef"))).isFalse();
    }

    @Test
    public void testRightAnchoredOnly() {
        LikeChainChecker checker = LikeChainChecker.compile("%abc%def");
        assertThat(checker.test(b("abcdef"))).isTrue();
        assertThat(checker.test(b("XabcXdef"))).isTrue();
        assertThat(checker.test(b("abcXXXdef"))).isTrue();
        assertThat(checker.test(b("abc"))).isFalse();
        assertThat(checker.test(b("def"))).isFalse();
        assertThat(checker.test(b("abcdefX"))).isFalse();
    }

    @Test
    public void testNeitherAnchored() {
        LikeChainChecker checker = LikeChainChecker.compile("%abc%def%");
        assertThat(checker.test(b("abcdef"))).isTrue();
        assertThat(checker.test(b("XabcXdefX"))).isTrue();
        assertThat(checker.test(b("abcXdef"))).isTrue();
        assertThat(checker.test(b("abcdefY"))).isTrue();
        assertThat(checker.test(b("abc"))).isFalse();
        assertThat(checker.test(b("def"))).isFalse();
        assertThat(checker.test(b("defXabc"))).isFalse();
    }

    @Test
    public void testThreeSegmentsBothAnchored() {
        LikeChainChecker checker = LikeChainChecker.compile("/api/%/users/%/profile");
        assertThat(checker.test(b("/api/v1/users/123/profile"))).isTrue();
        assertThat(checker.test(b("/api/X/users/Y/profile"))).isTrue();
        assertThat(checker.test(b("/api//users//profile"))).isTrue();
        assertThat(checker.test(b("/api/v1/profile"))).isFalse();
        assertThat(checker.test(b("/api/v1/users/123/profile/extra"))).isFalse();
        assertThat(checker.test(b("X/api/v1/users/123/profile"))).isFalse();
    }

    @Test
    public void testLongChain() {
        LikeChainChecker checker = LikeChainChecker.compile("a%b%c%d%e");
        assertThat(checker.test(b("abcde"))).isTrue();
        assertThat(checker.test(b("aXbYcZdWe"))).isTrue();
        assertThat(checker.test(b("aXXXbYYYcZZZdWWWe"))).isTrue();
        assertThat(checker.test(b("aebcd"))).isFalse();
        assertThat(checker.test(b("abcd"))).isFalse();
    }

    @Test
    public void testEmptyPattern() {
        LikeChainChecker checker = LikeChainChecker.compile("");
        assertThat(checker.test(b(""))).isTrue();
        assertThat(checker.test(b("a"))).isFalse();
    }

    @Test
    public void testJustPercent() {
        LikeChainChecker checker = LikeChainChecker.compile("%");
        assertThat(checker.test(b(""))).isTrue();
        assertThat(checker.test(b("a"))).isTrue();
        assertThat(checker.test(b("hello world"))).isTrue();
    }

    @Test
    public void testConsecutivePercents() {
        LikeChainChecker checker = LikeChainChecker.compile("%%");
        assertThat(checker.test(b(""))).isTrue();
        assertThat(checker.test(b("anything"))).isTrue();

        LikeChainChecker checker2 = LikeChainChecker.compile("a%%b");
        assertThat(checker2.test(b("ab"))).isTrue();
        assertThat(checker2.test(b("aXb"))).isTrue();
        assertThat(checker2.test(b("aXXXb"))).isTrue();
        assertThat(checker2.test(b("a"))).isFalse();
        assertThat(checker2.test(b("b"))).isFalse();
    }

    @Test
    public void testOverlapRejection() {
        LikeChainChecker checker = LikeChainChecker.compile("a%a");
        assertThat(checker.test(b("a"))).isFalse();
        assertThat(checker.test(b("aa"))).isTrue();
        assertThat(checker.test(b("aXa"))).isTrue();
        assertThat(checker.test(b("aXXa"))).isTrue();
    }

    @Test
    public void testInputShorterThanPattern() {
        LikeChainChecker checker = LikeChainChecker.compile("abc%def");
        assertThat(checker.test(b("abc"))).isFalse();
        assertThat(checker.test(b("def"))).isFalse();
        assertThat(checker.test(b("ab"))).isFalse();
        assertThat(checker.test(b(""))).isFalse();
    }

    @Test
    public void testUtf8MultiByteCharacters() {
        LikeChainChecker checker = LikeChainChecker.compile("café%");
        assertThat(checker.test(b("café latte"))).isTrue();
        assertThat(checker.test(b("café"))).isTrue();
        assertThat(checker.test(b("cafe"))).isFalse();

        LikeChainChecker checker2 = LikeChainChecker.compile("%日本%");
        assertThat(checker2.test(b("こんにちは日本語"))).isTrue();
        assertThat(checker2.test(b("hello world"))).isFalse();
    }

    @Test
    public void testParityWithRegex() {
        String[] patterns = {
            "",
            "%",
            "%%",
            "abc",
            "abc%",
            "%abc",
            "%abc%",
            "abc%def",
            "abc%def%",
            "%abc%def",
            "%abc%def%",
            "a%b%c",
            "%a%b%c%",
            "a%a",
            "/api/%/users/%/profile",
            "%hello%",
            "prefix%",
            "%suffix",
        };
        String[] inputs = {
            "",
            "a",
            "abc",
            "abcdef",
            "abc def",
            "xabcy",
            "/api/v1/users/123/profile",
            "/api//users//profile",
            "aaa",
            "ababab",
            "hello world",
            "the quick brown fox",
            "a%b%c",
            "log_x.txt",
        };

        for (String pattern : patterns) {
            LikeChainChecker checker = LikeChainChecker.compile(pattern);
            assertThat(checker).as("compile should succeed for pattern: %s", pattern).isNotNull();
            String regex = sqlLikeToRegex(pattern);
            Pattern p = Pattern.compile(regex, Pattern.DOTALL);
            for (String input : inputs) {
                boolean expected = p.matcher(input).matches();
                boolean actual = checker.test(b(input));
                assertThat(actual)
                        .as("pattern=[%s] input=[%s] regex=[%s]", pattern, input, regex)
                        .isEqualTo(expected);
            }
        }
    }

    private static BinaryString b(String s) {
        return BinaryString.fromString(s);
    }

    private static String sqlLikeToRegex(String sqlPattern) {
        StringBuilder out = new StringBuilder(sqlPattern.length() * 2);
        for (int i = 0; i < sqlPattern.length(); i++) {
            char c = sqlPattern.charAt(i);
            if ("[]()|^-+*?{}$\\.".indexOf(c) >= 0) {
                out.append('\\');
                out.append(c);
            } else if (c == '%') {
                out.append("(?s:.*)");
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
