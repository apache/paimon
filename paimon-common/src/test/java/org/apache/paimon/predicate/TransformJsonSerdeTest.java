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
                                "SUBSTRING source must be a string or a field reference"));
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
    void testTrimFlagMustBeItsName() {
        // Jackson reads an enum from a number as an ordinal, which would make 0 a valid
        // LEADING that the Python client rejects
        for (String flag : new String[] {"0", "\"0\"", "2", "\"LTRIM\"", "null"}) {
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
