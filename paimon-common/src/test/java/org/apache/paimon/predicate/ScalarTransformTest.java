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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Behavioral tests for scalar transforms. */
class ScalarTransformTest {

    @Test
    void testBitLengthUsesUtf8BytesAndPropagatesNull() {
        BitLengthTransform transform =
                new BitLengthTransform(
                        Collections.singletonList(new FieldRef(0, "s", DataTypes.STRING())));

        assertThat(transform.transform(GenericRow.of(string("é😀")))).isEqualTo(48);
        assertThat(transform.transform(GenericRow.of((Object) null))).isNull();
    }

    @Test
    void testTranslateCodePointAndDuplicateSemantics() {
        assertThat(
                        new TranslateTransform(
                                        Arrays.asList(string("A😀B"), string("😀"), string("界")))
                                .transform(GenericRow.of()))
                .isEqualTo(string("A界B"));

        // The first duplicate mapping wins even when it maps to deletion.
        assertThat(
                        new TranslateTransform(
                                        Arrays.asList(string("aba"), string("aa"), string("")))
                                .transform(GenericRow.of()))
                .isEqualTo(string("b"));

        // A NUL replacement code point acts as the deletion sentinel.
        assertThat(
                        new TranslateTransform(
                                        Arrays.asList(string("aba"), string("a"), string("\u0000")))
                                .transform(GenericRow.of()))
                .isEqualTo(string("b"));

        assertThat(
                        new TranslateTransform(Arrays.asList(null, string("a"), string("b")))
                                .transform(GenericRow.of()))
                .isNull();
    }

    @Test
    void testOverlayDefaultExplicitAndUnicodeSemantics() {
        assertThat(
                        new OverlayTransform(Arrays.asList(string("Hello SQL"), string("_"), 6))
                                .transform(GenericRow.of()))
                .isEqualTo(string("Hello_SQL"));
        assertThat(
                        new OverlayTransform(
                                        Arrays.asList(string("Hello SQL"), string("ANSI "), 7, 0))
                                .transform(GenericRow.of()))
                .isEqualTo(string("Hello ANSI SQL"));
        assertThat(
                        new OverlayTransform(Arrays.asList(string("a😀c"), string("界"), 2, 1))
                                .transform(GenericRow.of()))
                .isEqualTo(string("a界c"));
        assertThat(
                        new OverlayTransform(Arrays.asList(string("abc"), null, 1, 1))
                                .transform(GenericRow.of()))
                .isNull();
    }

    @Test
    void testPadForPaddingTruncationAndEmptyPad() {
        assertThat(pad(PadTransform.Direction.LEFT, "hi", 5, "??")).isEqualTo(string("???hi"));
        assertThat(pad(PadTransform.Direction.RIGHT, "hi", 5, "??")).isEqualTo(string("hi???"));
        assertThat(pad(PadTransform.Direction.LEFT, "a😀c", 2, "x")).isEqualTo(string("a😀"));
        assertThat(pad(PadTransform.Direction.RIGHT, "hi", 5, "")).isEqualTo(string("hi"));
        assertThat(pad(PadTransform.Direction.LEFT, "hi", -1, "?")).isEqualTo(string(""));
    }

    @Test
    void testDateAddAndDiffWithLiteralAndFieldInputs() {
        FieldRef date = new FieldRef(0, "date", DataTypes.DATE());
        FieldRef days = new FieldRef(1, "days", DataTypes.SMALLINT());
        int january15 = epochDay("2025-01-15");

        DateAddTransform add = new DateAddTransform(Arrays.asList(date, days));
        assertThat(add.transform(GenericRow.of(january15, (short) 2)))
                .isEqualTo(epochDay("2025-01-17"));
        assertThat(add.transform(GenericRow.of(january15, null))).isNull();

        DateDiffTransform diff = new DateDiffTransform(Arrays.asList(date, epochDay("2025-01-01")));
        assertThat(diff.transform(GenericRow.of(january15))).isEqualTo(14);
        assertThat(diff.transform(GenericRow.of((Object) null))).isNull();
    }

    @Test
    void testDateTruncSupportsEveryDateLevel() {
        FieldRef date = new FieldRef(0, "date", DataTypes.DATE());
        GenericRow row = GenericRow.of(epochDay("2025-05-18"));

        assertThat(trunc(date, "week").transform(row)).isEqualTo(epochDay("2025-05-12"));
        assertThat(trunc(date, "mon").transform(row)).isEqualTo(epochDay("2025-05-01"));
        assertThat(trunc(date, "quarter").transform(row)).isEqualTo(epochDay("2025-04-01"));
        assertThat(trunc(date, "yyyy").transform(row)).isEqualTo(epochDay("2025-01-01"));
        assertThat(trunc(date, "day").transform(row)).isNull();
        assertThat(
                        new DateTruncTransform(Arrays.asList(date, null))
                                .transform(GenericRow.of(epochDay("2025-05-18"))))
                .isNull();
    }

    @Test
    void testInputTypesAreValidated() {
        assertThatThrownBy(
                        () ->
                                new DateAddTransform(
                                        Arrays.asList(new FieldRef(0, "s", DataTypes.STRING()), 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("first input must be a date");
        assertThatThrownBy(
                        () ->
                                new PadTransform(
                                        Arrays.asList(string("x"), string("1"), string("_")),
                                        PadTransform.Direction.LEFT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("length must be an integer");
    }

    private static Object pad(
            PadTransform.Direction direction, String value, int length, String padding) {
        return new PadTransform(Arrays.asList(string(value), length, string(padding)), direction)
                .transform(GenericRow.of());
    }

    private static DateTruncTransform trunc(FieldRef date, String format) {
        return new DateTruncTransform(Arrays.asList(date, string(format)));
    }

    private static BinaryString string(String value) {
        return BinaryString.fromString(value);
    }

    private static int epochDay(String value) {
        return Math.toIntExact(LocalDate.parse(value).toEpochDay());
    }
}
