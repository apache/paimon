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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransformJsonSerdeTest {

    private static Stream<TestSpec> testData() {
        return Stream.of(
                // FieldTransform
                TestSpec.forTransform(new FieldTransform(new FieldRef(0, "f0", new IntType())))
                        .expectJson(
                                "{\"name\":\"FIELD_REF\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}}"),

                // FieldTransform - String type
                TestSpec.forTransform(new FieldTransform(new FieldRef(1, "f1", DataTypes.STRING())))
                        .expectJson(
                                "{\"name\":\"FIELD_REF\",\"fieldRef\":{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"}}"),

                // CastTransform - INT to BIGINT
                TestSpec.forTransform(
                                new CastTransform(
                                        new FieldRef(0, "f0", new IntType()), DataTypes.BIGINT()))
                        .expectJson(
                                "{\"name\":\"CAST\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"},\"type\":\"BIGINT\"}"),

                // CastTransform - STRING to INT
                TestSpec.forTransform(
                                new CastTransform(
                                        new FieldRef(2, "f2", DataTypes.STRING()), DataTypes.INT()))
                        .expectJson(
                                "{\"name\":\"CAST\",\"fieldRef\":{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"},\"type\":\"INT\"}"),

                // UpperTransform
                TestSpec.forTransform(
                                new UpperTransform(
                                        Collections.singletonList(
                                                new FieldRef(1, "f1", DataTypes.STRING()))))
                        .expectJson(
                                "{\"name\":\"UPPER\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"}]}"),
                TestSpec.forTransform(
                                new LowerTransform(
                                        Collections.singletonList(
                                                new FieldRef(1, "f1", DataTypes.STRING()))))
                        .expectJson(
                                "{\"name\":\"LOWER\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"}]}"),
                TestSpec.forTransform(NullTransform.INSTANCE).expectJson("{\"name\":\"NULL\"}"),

                // ConcatTransform - two fields
                TestSpec.forTransform(
                                new ConcatTransform(
                                        Arrays.asList(
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                new FieldRef(2, "f2", DataTypes.STRING()))))
                        .expectJson(
                                "{\"name\":\"CONCAT\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}]}"),

                // ConcatTransform - with literals and nulls
                TestSpec.forTransform(
                                new ConcatTransform(
                                        Arrays.asList(
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                BinaryString.fromString("-"),
                                                new FieldRef(2, "f2", DataTypes.STRING()),
                                                null)))
                        .expectJson(
                                "{\"name\":\"CONCAT\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},\"-\",{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"},null]}"),

                // ConcatWsTransform - with separator
                TestSpec.forTransform(
                                new ConcatWsTransform(
                                        Arrays.asList(
                                                BinaryString.fromString("|"),
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                new FieldRef(2, "f2", DataTypes.STRING()))))
                        .expectJson(
                                "{\"name\":\"CONCAT_WS\",\"inputs\":[\"|\",{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}]}"),

                // ConcatWsTransform - with literals and nulls
                TestSpec.forTransform(
                                new ConcatWsTransform(
                                        Arrays.asList(
                                                BinaryString.fromString("|"),
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                BinaryString.fromString("X"),
                                                null,
                                                new FieldRef(2, "f2", DataTypes.STRING()))))
                        .expectJson(
                                "{\"name\":\"CONCAT_WS\",\"inputs\":[\"|\",{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},\"X\",null,{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}]}"),

                // DateExtractTransform - YEAR on DATE, MINUTE on TIMESTAMP
                TestSpec.forTransform(new YearTransform(new FieldRef(0, "d0", DataTypes.DATE())))
                        .expectJson(
                                "{\"name\":\"YEAR\",\"fieldRef\":{\"index\":0,\"name\":\"d0\",\"type\":\"DATE\"}}"),
                TestSpec.forTransform(
                                new MinuteTransform(new FieldRef(1, "t1", DataTypes.TIMESTAMP(3))))
                        .expectJson(
                                "{\"name\":\"MINUTE\",\"fieldRef\":{\"index\":1,\"name\":\"t1\",\"type\":\"TIMESTAMP(3)\"}}"),
                TestSpec.forTransform(new QuarterTransform(new FieldRef(0, "d0", DataTypes.DATE())))
                        .expectJson(
                                "{\"name\":\"QUARTER\",\"fieldRef\":{\"index\":0,\"name\":\"d0\",\"type\":\"DATE\"}}"),
                TestSpec.forTransform(
                                new IsoDayOfWeekTransform(new FieldRef(0, "d0", DataTypes.DATE())))
                        .expectJson(
                                "{\"name\":\"ISO_DAY_OF_WEEK\",\"fieldRef\":{\"index\":0,\"name\":\"d0\",\"type\":\"DATE\"}}"),
                TestSpec.forTransform(
                                new DayOfWeekTransform(new FieldRef(0, "d0", DataTypes.DATE())))
                        .expectJson(
                                "{\"name\":\"DAY_OF_WEEK\",\"fieldRef\":{\"index\":0,\"name\":\"d0\",\"type\":\"DATE\"}}"),
                TestSpec.forTransform(new WeekdayTransform(new FieldRef(0, "d0", DataTypes.DATE())))
                        .expectJson(
                                "{\"name\":\"WEEKDAY\",\"fieldRef\":{\"index\":0,\"name\":\"d0\",\"type\":\"DATE\"}}"),
                TestSpec.forTransform(
                                new DayOfYearTransform(new FieldRef(0, "d0", DataTypes.DATE())))
                        .expectJson(
                                "{\"name\":\"DAY_OF_YEAR\",\"fieldRef\":{\"index\":0,\"name\":\"d0\",\"type\":\"DATE\"}}"),
                TestSpec.forTransform(new WeekTransform(new FieldRef(0, "d0", DataTypes.DATE())))
                        .expectJson(
                                "{\"name\":\"WEEK\",\"fieldRef\":{\"index\":0,\"name\":\"d0\",\"type\":\"DATE\"}}"),
                TestSpec.forTransform(
                                new YearOfWeekTransform(new FieldRef(0, "d0", DataTypes.DATE())))
                        .expectJson(
                                "{\"name\":\"YEAR_OF_WEEK\",\"fieldRef\":{\"index\":0,\"name\":\"d0\",\"type\":\"DATE\"}}"),
                TestSpec.forTransform(
                                new SubstringTransform(
                                        Arrays.asList(
                                                new FieldRef(1, "f1", DataTypes.STRING()), 8, 4)))
                        .expectJson(
                                "{\"name\":\"SUBSTRING\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},8,4]}"),
                TestSpec.forTransform(
                                new SubstringTransform(
                                        Arrays.asList(
                                                new FieldRef(1, "f1", DataTypes.STRING()), 8)))
                        .expectJson(
                                "{\"name\":\"SUBSTRING\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},8]}"),
                TestSpec.forTransform(
                                new SubstringTransform(
                                        Arrays.asList(
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                new FieldRef(3, "f3", DataTypes.INT()),
                                                new FieldRef(4, "f4", DataTypes.INT()))))
                        .expectJson(
                                "{\"name\":\"SUBSTRING\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},{\"index\":3,\"name\":\"f3\",\"type\":\"INT\"},{\"index\":4,\"name\":\"f4\",\"type\":\"INT\"}]}"),
                TestSpec.forTransform(
                                new SubstringTransform(
                                        Arrays.asList(BinaryString.fromString("hello"), 2, 3)))
                        .expectJson("{\"name\":\"SUBSTRING\",\"inputs\":[\"hello\",2,3]}"),
                TestSpec.forTransform(new SubstringTransform(Arrays.asList(null, 1)))
                        .expectJson("{\"name\":\"SUBSTRING\",\"inputs\":[null,1]}"),
                TestSpec.forTransform(
                                new TrimTransform(
                                        Collections.singletonList(
                                                new FieldRef(1, "f1", DataTypes.STRING())),
                                        TrimTransform.Flag.BOTH))
                        .expectJson(
                                "{\"name\":\"TRIM\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"}],\"trimFlag\":\"BOTH\"}"),
                TestSpec.forTransform(
                                new TrimTransform(
                                        Collections.singletonList(
                                                new FieldRef(1, "f1", DataTypes.STRING())),
                                        TrimTransform.Flag.LEADING))
                        .expectJson(
                                "{\"name\":\"TRIM\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"}],\"trimFlag\":\"LEADING\"}"),
                TestSpec.forTransform(
                                new TrimTransform(
                                        Arrays.asList(
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                BinaryString.fromString("x")),
                                        TrimTransform.Flag.TRAILING))
                        .expectJson(
                                "{\"name\":\"TRIM\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},\"x\"],\"trimFlag\":\"TRAILING\"}"),

                // LengthTransform
                TestSpec.forTransform(
                                new LengthTransform(
                                        Collections.singletonList(
                                                new FieldRef(1, "f1", DataTypes.STRING()))))
                        .expectJson(
                                "{\"name\":\"LENGTH\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"}]}"),
                TestSpec.forTransform(
                                new LengthTransform(
                                        Collections.singletonList(
                                                BinaryString.fromString("hello"))))
                        .expectJson("{\"name\":\"LENGTH\",\"inputs\":[\"hello\"]}"),
                TestSpec.forTransform(new LengthTransform(Collections.singletonList(null)))
                        .expectJson("{\"name\":\"LENGTH\",\"inputs\":[null]}"),

                // Additional scalar transforms
                TestSpec.forTransform(
                                new BitLengthTransform(
                                        Collections.singletonList(
                                                new FieldRef(1, "f1", DataTypes.STRING()))))
                        .expectJson(
                                "{\"name\":\"BIT_LENGTH\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"}]}"),
                TestSpec.forTransform(
                                new TranslateTransform(
                                        Arrays.asList(
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                BinaryString.fromString("ab"),
                                                BinaryString.fromString("xy"))))
                        .expectJson(
                                "{\"name\":\"TRANSLATE\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},\"ab\",\"xy\"]}"),
                TestSpec.forTransform(
                                new OverlayTransform(
                                        Arrays.asList(
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                BinaryString.fromString("x"),
                                                2,
                                                1)))
                        .expectJson(
                                "{\"name\":\"OVERLAY\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},\"x\",2,1]}"),
                TestSpec.forTransform(
                                new PadTransform(
                                        Arrays.asList(
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                5,
                                                BinaryString.fromString("_")),
                                        PadTransform.Direction.LEFT))
                        .expectJson(
                                "{\"name\":\"PAD\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},5,\"_\"],\"direction\":\"LEFT\"}"),
                TestSpec.forTransform(
                                new DateAddTransform(
                                        Arrays.asList(new FieldRef(0, "d0", DataTypes.DATE()), 1)))
                        .expectJson(
                                "{\"name\":\"DATE_ADD\",\"inputs\":[{\"index\":0,\"name\":\"d0\",\"type\":\"DATE\"},1]}"),
                TestSpec.forTransform(
                                new DateDiffTransform(
                                        Arrays.asList(
                                                new FieldRef(0, "d0", DataTypes.DATE()),
                                                new FieldRef(1, "d1", DataTypes.DATE()))))
                        .expectJson(
                                "{\"name\":\"DATE_DIFF\",\"inputs\":[{\"index\":0,\"name\":\"d0\",\"type\":\"DATE\"},{\"index\":1,\"name\":\"d1\",\"type\":\"DATE\"}]}"),
                TestSpec.forTransform(
                                new DateTruncTransform(
                                        Arrays.asList(
                                                new FieldRef(0, "d0", DataTypes.DATE()),
                                                BinaryString.fromString("MONTH"))))
                        .expectJson(
                                "{\"name\":\"DATE_TRUNC\",\"inputs\":[{\"index\":0,\"name\":\"d0\",\"type\":\"DATE\"},\"MONTH\"]}"),

                // error message testing
                TestSpec.forJson("{\"name\":\"invalid\"}")
                        .expectErrorMessage("Could not resolve type id 'invalid'"),
                TestSpec.forJson(
                                "{\"name\":\"TRIM\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"}]}")
                        .expectErrorMessage("trimFlag must not be null"),
                TestSpec.forJson("{\"name\":\"SUBSTRING\",\"inputs\":[true,1]}")
                        .expectErrorMessage("Unsupported StringTransform input JSON"),
                TestSpec.forJson(
                                "{\"name\":\"SUBSTRING\",\"inputs\":[{\"index\":0,\"name\":\"f0\",\"type\":\"STRING\"},1.5]}")
                        .expectErrorMessage("position must be an integer"),
                TestSpec.forJson("{\"name\":\"SUBSTRING\",\"inputs\":[123,1,1]}")
                        .expectErrorMessage(
                                "SUBSTRING source must be a string or a field reference"),
                TestSpec.forJson(
                                "{\"name\":\"YEAR\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"STRING\"}}")
                        .expectErrorMessage(
                                "YEAR requires a DATE or TIMESTAMP field, found STRING"),
                TestSpec.forJson("{\"name\":\"LENGTH\",\"inputs\":[]}")
                        .expectErrorMessage("LENGTH requires exactly one input"),
                TestSpec.forJson(
                                "{\"name\":\"LENGTH\",\"inputs\":[{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}]}")
                        .expectErrorMessage("LENGTH input must be a string field"),
                TestSpec.forJson(
                                "{\"name\":\"LENGTH\",\"inputs\":[{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"},{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"}]}")
                        .expectErrorMessage("LENGTH requires exactly one input"),
                TestSpec.forJson("{\"name\":\"LENGTH\",\"inputs\":[5]}")
                        .expectErrorMessage("Unsupported StringTransform input JSON"));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testParsing(TestSpec testSpec) {
        if (testSpec.expectedJson != null) {
            Transform parsed = parse(testSpec.expectedJson);
            assertThat(parsed).isEqualTo(testSpec.transform);
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testJsonParsing(TestSpec testSpec) {
        if (testSpec.expectedJson != null) {
            Transform parsed = parse(toJson(testSpec.transform));
            assertThat(parsed).isEqualTo(testSpec.transform);
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testSerializedText(TestSpec testSpec) {
        if (testSpec.expectedJson != null) {
            assertThat(toJson(testSpec.transform)).isEqualTo(testSpec.expectedJson);
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testErrorMessage(TestSpec testSpec) {
        if (testSpec.expectedErrorMessage != null) {
            assertThatThrownBy(() -> parse(testSpec.jsonString))
                    .hasMessageContaining(testSpec.expectedErrorMessage);
        }
    }

    @Test
    void testSubstringRoundTripKeepsPositions() {
        FieldRef ssn = new FieldRef(0, "ssn", DataTypes.VARCHAR(64));
        assertRoundTrip(
                new SubstringTransform(Arrays.asList(ssn, 8, 4)),
                GenericRow.of(BinaryString.fromString("123-45-6789")),
                BinaryString.fromString("6789"));

        FieldRef phone = new FieldRef(0, "phone", DataTypes.VARCHAR(64));
        assertRoundTrip(
                new SubstringTransform(Arrays.asList(phone, 1, 3)),
                GenericRow.of(BinaryString.fromString("13812348000")),
                BinaryString.fromString("138"));

        assertRoundTrip(
                new SubstringTransform(
                        Arrays.asList(
                                new FieldRef(0, "f0", DataTypes.STRING()),
                                new FieldRef(1, "f1", DataTypes.INT()),
                                new FieldRef(2, "f2", DataTypes.INT()))),
                GenericRow.of(BinaryString.fromString("123-45-6789"), 8, 4),
                BinaryString.fromString("6789"));

        assertRoundTrip(
                new SubstringTransform(Arrays.asList(BinaryString.fromString("123-45-6789"), 8)),
                GenericRow.of(),
                BinaryString.fromString("6789"));
    }

    @Test
    void testPositionsOutsideTheIntegerRangeAreNotTruncated() {
        // canConvertToInt is false beyond an int, so the value stays wide and the
        // per-row parse rejects it rather than silently truncating
        Transform parsed = parse("{\"name\":\"SUBSTRING\",\"inputs\":[\"abcdef\",3000000000]}");
        assertThatThrownBy(() -> parsed.transform(GenericRow.of()))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void testTextualPositionIsParsedPerRow() {
        // Jackson keeps a textual position as a string; Java parses it when a row
        // reaches it, which is the contract the Python client mirrors
        Transform parsed =
                parse("{\"name\":\"SUBSTRING\",\"inputs\":[\"123-45-6789\",\"8\",\"4\"]}");
        assertThat(parsed.transform(GenericRow.of())).isEqualTo(BinaryString.fromString("6789"));

        Transform bad = parse("{\"name\":\"SUBSTRING\",\"inputs\":[\"abcdef\",\"1_0\"]}");
        assertThatThrownBy(() -> bad.transform(GenericRow.of()))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void testTrimSourceTypeIsCheckedWhenTheRuleIsRead() {
        for (String type : new String[] {"INT", "VARCHAR(0)", "STRING ARRAY"}) {
            assertThatThrownBy(
                            () ->
                                    parse(
                                            "{\"name\":\"TRIM\",\"inputs\":[{\"index\":0,\"name\":\"s\",\"type\":\""
                                                    + type
                                                    + "\"}],\"trimFlag\":\"BOTH\"}"))
                    .isInstanceOf(RuntimeException.class);
        }
        for (String type :
                new String[] {"STRING", "STRING NULL", "CHAR(3) NOT NULL", "VARCHAR(10)NULL"}) {
            assertThat(
                            parse(
                                    "{\"name\":\"TRIM\",\"inputs\":[{\"index\":0,\"name\":\"s\",\"type\":\""
                                            + type
                                            + "\"}],\"trimFlag\":\"BOTH\"}"))
                    .isNotNull();
        }
    }

    @Test
    void testWrongArityIsRejected() {
        for (String inputs : new String[] {"[\"abc\"]", "[\"abc\",1,2,3]"}) {
            assertThatThrownBy(() -> parse("{\"name\":\"SUBSTRING\",\"inputs\":" + inputs + "}"))
                    .isInstanceOf(RuntimeException.class);
        }
        for (String inputs : new String[] {"[]", "[\"a\",\"b\",\"c\"]"}) {
            assertThatThrownBy(
                            () ->
                                    parse(
                                            "{\"name\":\"TRIM\",\"inputs\":"
                                                    + inputs
                                                    + ",\"trimFlag\":\"BOTH\"}"))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    void testCopyWithNewInputsKeepsTheFlag() {
        // auth remapping rebuilds every transform through copyWithNewInputs, so a flag
        // lost there would silently turn LEADING into BOTH
        FieldRef f0 = new FieldRef(0, "f0", DataTypes.STRING());
        GenericRow row = GenericRow.of(BinaryString.fromString("  x  "));
        for (TrimTransform.Flag flag : TrimTransform.Flag.values()) {
            Transform copied =
                    new TrimTransform(Collections.singletonList(BinaryString.fromString("")), flag)
                            .copyWithNewInputs(Collections.singletonList(f0));
            assertThat(copied.transform(row))
                    .isEqualTo(
                            new TrimTransform(Collections.singletonList(f0), flag).transform(row));
        }
        assertThat(
                        new TrimTransform(Collections.singletonList(f0), TrimTransform.Flag.LEADING)
                                .copyWithNewInputs(Collections.singletonList(f0))
                                .transform(row))
                .isEqualTo(BinaryString.fromString("x  "));
    }

    @Test
    void testTrimFlagMustBeItsName() {
        for (String flag :
                new String[] {"0", "\"0\"", "2", "\"LTRIM\"", "null", "\"both\"", "\"Both\""}) {
            assertThatThrownBy(
                            () ->
                                    parse(
                                            "{\"name\":\"TRIM\",\"inputs\":[\"  x  \"],\"trimFlag\":"
                                                    + flag
                                                    + "}"))
                    .isInstanceOf(RuntimeException.class);
        }

        assertThat(parse("{\"name\":\"TRIM\",\"inputs\":[\"  x  \"],\"trimFlag\":\"LEADING\"}"))
                .isEqualTo(
                        new TrimTransform(
                                Collections.singletonList(BinaryString.fromString("  x  ")),
                                TrimTransform.Flag.LEADING));
    }

    @Test
    void testTrimRoundTripKeepsFlag() {
        FieldRef f0 = new FieldRef(0, "f0", DataTypes.STRING());
        GenericRow row = GenericRow.of(BinaryString.fromString("  x  "));

        assertRoundTrip(
                new TrimTransform(Collections.singletonList(f0), TrimTransform.Flag.BOTH),
                row,
                BinaryString.fromString("x"));
        assertRoundTrip(
                new TrimTransform(Collections.singletonList(f0), TrimTransform.Flag.LEADING),
                row,
                BinaryString.fromString("x  "));
        assertRoundTrip(
                new TrimTransform(Collections.singletonList(f0), TrimTransform.Flag.TRAILING),
                row,
                BinaryString.fromString("  x"));

        assertThat(new TrimTransform(Collections.singletonList(f0), TrimTransform.Flag.LEADING))
                .isNotEqualTo(
                        new TrimTransform(Collections.singletonList(f0), TrimTransform.Flag.BOTH));
    }

    private static void assertRoundTrip(Transform transform, InternalRow row, Object expected) {
        assertThat(transform.transform(row)).isEqualTo(expected);

        Transform parsed = parse(toJson(transform));
        assertThat(parsed.transform(row)).isEqualTo(expected);
        assertThat(parsed).isEqualTo(transform);
        assertThat(toJson(parsed)).isEqualTo(toJson(transform));
    }

    private static String toJson(Transform transform) {
        return JsonSerdeUtil.toFlatJson(transform);
    }

    private static Transform parse(String json) {
        return JsonSerdeUtil.fromJson(json, Transform.class);
    }

    private static class TestSpec {

        private final Transform transform;

        private final String jsonString;

        private @Nullable String expectedJson;

        private @Nullable String expectedErrorMessage;

        private TestSpec(Transform transform) {
            this.transform = transform;
            this.jsonString = null;
        }

        private TestSpec(String jsonString) {
            this.transform = null;
            this.jsonString = jsonString;
        }

        static TestSpec forTransform(Transform transform) {
            return new TestSpec(transform);
        }

        static TestSpec forJson(String jsonString) {
            return new TestSpec(jsonString);
        }

        TestSpec expectJson(String expectedJson) {
            this.expectedJson = expectedJson;
            return this;
        }

        TestSpec expectErrorMessage(String expectedErrorMessage) {
            this.expectedErrorMessage = expectedErrorMessage;
            return this;
        }

        @Override
        public String toString() {
            return transform != null ? transform.toString() : jsonString;
        }
    }
}
