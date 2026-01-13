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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.JsonSerdeUtil;

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
                                "{\"type\":\"FIELD_REF\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}}"),

                // FieldTransform - String type
                TestSpec.forTransform(new FieldTransform(new FieldRef(1, "f1", DataTypes.STRING())))
                        .expectJson(
                                "{\"type\":\"FIELD_REF\",\"fieldRef\":{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"}}"),

                // CastTransform - INT to BIGINT
                TestSpec.forTransform(
                                new CastTransform(
                                        new FieldRef(0, "f0", new IntType()), DataTypes.BIGINT()))
                        .expectJson(
                                "{\"type\":\"CAST\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"},\"type\":\"BIGINT\"}"),

                // CastTransform - STRING to INT
                TestSpec.forTransform(
                                new CastTransform(
                                        new FieldRef(2, "f2", DataTypes.STRING()), DataTypes.INT()))
                        .expectJson(
                                "{\"type\":\"CAST\",\"fieldRef\":{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"},\"type\":\"INT\"}"),

                // UpperTransform
                TestSpec.forTransform(
                                new UpperTransform(
                                        Collections.singletonList(
                                                new FieldRef(1, "f1", DataTypes.STRING()))))
                        .expectJson(
                                "{\"type\":\"UPPER\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"}]}"),

                // ConcatTransform - two fields
                TestSpec.forTransform(
                                new ConcatTransform(
                                        Arrays.asList(
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                new FieldRef(2, "f2", DataTypes.STRING()))))
                        .expectJson(
                                "{\"type\":\"CONCAT\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}]}"),

                // ConcatTransform - with literals and nulls
                TestSpec.forTransform(
                                new ConcatTransform(
                                        Arrays.asList(
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                BinaryString.fromString("-"),
                                                new FieldRef(2, "f2", DataTypes.STRING()),
                                                null)))
                        .expectJson(
                                "{\"type\":\"CONCAT\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},\"-\",{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"},null]}"),

                // ConcatWsTransform - with separator
                TestSpec.forTransform(
                                new ConcatWsTransform(
                                        Arrays.asList(
                                                BinaryString.fromString("|"),
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                new FieldRef(2, "f2", DataTypes.STRING()))))
                        .expectJson(
                                "{\"type\":\"CONCAT_WS\",\"inputs\":[\"|\",{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}]}"),

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
                                "{\"type\":\"CONCAT_WS\",\"inputs\":[\"|\",{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},\"X\",null,{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}]}"),

                // error message testing
                TestSpec.forJson("{\"type\":\"invalid\"}")
                        .expectErrorMessage("Could not resolve type id 'invalid'"));
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
    void testErrorMessage(TestSpec testSpec) {
        if (testSpec.expectedErrorMessage != null) {
            assertThatThrownBy(() -> parse(testSpec.jsonString))
                    .hasMessageContaining(testSpec.expectedErrorMessage);
        }
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
