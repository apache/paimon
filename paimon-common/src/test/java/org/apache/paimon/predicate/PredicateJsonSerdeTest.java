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
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PredicateJsonSerdeTest {

    private static Stream<TestSpec> testData() {
        PredicateBuilder builder = newBuilder();

        return Stream.of(
                // LeafPredicate - Equal
                TestSpec.forPredicate(builder.equal(0, 1))
                        .expectJson(
                                "{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"equal\"},\"literals\":[1]}"),

                // LeafPredicate - NotEqual
                TestSpec.forPredicate(builder.notEqual(0, 1))
                        .expectJson(
                                "{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"notEqual\"},\"literals\":[1]}"),

                // LeafPredicate - LessThan
                TestSpec.forPredicate(builder.lessThan(0, 10))
                        .expectJson(
                                "{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"lessThan\"},\"literals\":[10]}"),

                // LeafPredicate - LessOrEqual
                TestSpec.forPredicate(builder.lessOrEqual(0, 10))
                        .expectJson(
                                "{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"lessOrEqual\"},\"literals\":[10]}"),

                // LeafPredicate - GreaterThan
                TestSpec.forPredicate(builder.greaterThan(0, 5))
                        .expectJson(
                                "{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"greaterThan\"},\"literals\":[5]}"),

                // LeafPredicate - GreaterOrEqual
                TestSpec.forPredicate(builder.greaterOrEqual(0, 5))
                        .expectJson(
                                "{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"greaterOrEqual\"},\"literals\":[5]}"),

                // LeafPredicate - IsNull
                TestSpec.forPredicate(builder.isNull(0))
                        .expectJson(
                                "{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"isNull\"},\"literals\":[]}"),

                // LeafPredicate - IsNotNull
                TestSpec.forPredicate(builder.isNotNull(0))
                        .expectJson(
                                "{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"isNotNull\"},\"literals\":[]}"),

                // LeafPredicate - In
                TestSpec.forPredicate(builder.in(0, Arrays.asList(1, 2, 3)))
                        .expectJson(
                                "{\"type\":\"compound\",\"function\":{\"type\":\"or\"},\"children\":[{\"type\":\"compound\",\"function\":{\"type\":\"or\"},\"children\":[{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"equal\"},\"literals\":[1]},{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"equal\"},\"literals\":[2]}]},{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"equal\"},\"literals\":[3]}]}"),

                // LeafPredicate - NotIn
                TestSpec.forPredicate(builder.notIn(0, Arrays.asList(1, 2, 3)))
                        .expectJson(
                                "{\"type\":\"compound\",\"function\":{\"type\":\"and\"},\"children\":[{\"type\":\"compound\",\"function\":{\"type\":\"and\"},\"children\":[{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"notEqual\"},\"literals\":[1]},{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"notEqual\"},\"literals\":[2]}]},{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"notEqual\"},\"literals\":[3]}]}"),

                // TransformPredicate - CastTransform
                TestSpec.forPredicate(
                                builder.greaterThan(
                                        new CastTransform(
                                                new FieldRef(0, "f0", new IntType()),
                                                DataTypes.BIGINT()),
                                        10L))
                        .expectJson(
                                "{\"type\":\"transform\",\"transform\":{\"type\":\"cast\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"},\"type\":\"BIGINT\"},\"function\":{\"type\":\"greaterThan\"},\"literals\":[10]}"),

                // TransformPredicate - UpperTransform
                TestSpec.forPredicate(
                                builder.startsWith(
                                        new UpperTransform(
                                                Collections.singletonList(
                                                        new FieldRef(2, "f2", DataTypes.STRING()))),
                                        BinaryString.fromString("ABC")))
                        .expectJson(
                                "{\"type\":\"transform\",\"transform\":{\"type\":\"upper\",\"inputs\":[{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}]},\"function\":{\"type\":\"startsWith\"},\"literals\":[\"ABC\"]}"),

                // TransformPredicate - ConcatTransform
                TestSpec.forPredicate(
                                builder.contains(
                                        new ConcatTransform(
                                                Arrays.asList(
                                                        new FieldRef(1, "f1", DataTypes.STRING()),
                                                        BinaryString.fromString("-"),
                                                        new FieldRef(2, "f2", DataTypes.STRING()),
                                                        null)),
                                        BinaryString.fromString("m")))
                        .expectJson(
                                "{\"type\":\"transform\",\"transform\":{\"type\":\"concat\",\"inputs\":[{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},\"-\",{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"},null]},\"function\":{\"type\":\"contains\"},\"literals\":[\"m\"]}"),

                // TransformPredicate - ConcatWsTransform
                TestSpec.forPredicate(
                                builder.endsWith(
                                        new ConcatWsTransform(
                                                Arrays.asList(
                                                        BinaryString.fromString("|"),
                                                        new FieldRef(1, "f1", DataTypes.STRING()),
                                                        BinaryString.fromString("X"),
                                                        null,
                                                        new FieldRef(2, "f2", DataTypes.STRING()))),
                                        BinaryString.fromString("z")))
                        .expectJson(
                                "{\"type\":\"transform\",\"transform\":{\"type\":\"concat_ws\",\"inputs\":[\"|\",{\"index\":1,\"name\":\"f1\",\"type\":\"STRING\"},\"X\",null,{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}]},\"function\":{\"type\":\"endsWith\"},\"literals\":[\"z\"]}"),

                // LeafPredicate - Like (non-negatable)
                TestSpec.forPredicate(builder.like(2, BinaryString.fromString("%a%b%")))
                        .expectJson(
                                "{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}},\"function\":{\"type\":\"like\"},\"literals\":[\"%a%b%\"]}"),

                // TransformPredicate - In with many values including nulls
                TestSpec.forPredicate(
                                builder.in(
                                        new UpperTransform(
                                                Collections.singletonList(
                                                        new FieldRef(2, "f2", DataTypes.STRING()))),
                                        manyUpperStringsWithNulls()))
                        .expectJson(
                                "{\"type\":\"transform\",\"transform\":{\"type\":\"upper\",\"inputs\":[{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}]},\"function\":{\"type\":\"in\"},\"literals\":[null,\"S1\",\"S2\",\"S3\",\"S4\",null,\"S6\",\"S7\",\"S8\",\"S9\",null,\"S11\",\"S12\",\"S13\",\"S14\",null,\"S16\",\"S17\",\"S18\",\"S19\",null]}"),

                // CompoundPredicate - Complex combination with empty list
                TestSpec.forPredicate(
                                PredicateBuilder.and(
                                        builder.equal(0, 1),
                                        builder.in(3, manyInts()),
                                        builder.in(3, Collections.emptyList()),
                                        builder.like(2, BinaryString.fromString("%a%b%")),
                                        PredicateBuilder.or(
                                                builder.equal(0, 7), builder.isNotNull(2))))
                        .expectJson(
                                "{\"type\":\"compound\",\"function\":{\"type\":\"and\"},\"children\":[{\"type\":\"compound\",\"function\":{\"type\":\"and\"},\"children\":[{\"type\":\"compound\",\"function\":{\"type\":\"and\"},\"children\":[{\"type\":\"compound\",\"function\":{\"type\":\"and\"},\"children\":[{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"equal\"},\"literals\":[1]},{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":3,\"name\":\"f3\",\"type\":\"INT\"}},\"function\":{\"type\":\"in\"},\"literals\":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]}]},{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":3,\"name\":\"f3\",\"type\":\"INT\"}},\"function\":{\"type\":\"in\"},\"literals\":[]}]},{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}},\"function\":{\"type\":\"like\"},\"literals\":[\"%a%b%\"]}]},{\"type\":\"compound\",\"function\":{\"type\":\"or\"},\"children\":[{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":0,\"name\":\"f0\",\"type\":\"INT\"}},\"function\":{\"type\":\"equal\"},\"literals\":[7]},{\"type\":\"leaf\",\"transform\":{\"type\":\"field\",\"fieldRef\":{\"index\":2,\"name\":\"f2\",\"type\":\"STRING\"}},\"function\":{\"type\":\"isNotNull\"},\"literals\":[]}]}]}"),

                // error message testing
                TestSpec.forJson("{\"type\":\"invalid\"}")
                        .expectErrorMessage("Could not resolve type id 'invalid'"),
                TestSpec.forJson("{\"type\":\"leaf\",\"function\":{\"type\":\"unknown\"}}")
                        .expectErrorMessage("Could not resolve type id 'unknown'"));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testParsing(TestSpec testSpec) {
        if (testSpec.expectedJson != null) {
            Predicate parsed = parse(testSpec.expectedJson);
            assertThat(parsed).isEqualTo(testSpec.predicate);
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testJsonParsing(TestSpec testSpec) {
        if (testSpec.expectedJson != null) {
            Predicate parsed = parse(toJson(testSpec.predicate));
            assertThat(parsed).isEqualTo(testSpec.predicate);
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

    private static PredicateBuilder newBuilder() {
        return new PredicateBuilder(
                RowType.of(new IntType(), DataTypes.STRING(), DataTypes.STRING(), new IntType()));
    }

    private static List<Object> manyInts() {
        List<Object> ints = new ArrayList<>();
        for (int i = 0; i < 21; i++) {
            ints.add(i);
        }
        return ints;
    }

    private static List<Object> manyUpperStringsWithNulls() {
        List<Object> strings = new ArrayList<>();
        for (int i = 0; i < 21; i++) {
            strings.add(i % 5 == 0 ? null : BinaryString.fromString("S" + i));
        }
        return strings;
    }

    private static String toJson(Predicate predicate) {
        return JsonSerdeUtil.toFlatJson(predicate);
    }

    private static Predicate parse(String json) {
        return JsonSerdeUtil.fromJson(json, Predicate.class);
    }

    private static class TestSpec {

        private final Predicate predicate;

        private final String jsonString;

        private @Nullable String expectedJson;

        private @Nullable String expectedErrorMessage;

        private TestSpec(Predicate predicate) {
            this.predicate = predicate;
            this.jsonString = null;
        }

        private TestSpec(String jsonString) {
            this.predicate = null;
            this.jsonString = jsonString;
        }

        static TestSpec forPredicate(Predicate predicate) {
            return new TestSpec(predicate);
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
            return predicate != null ? predicate.toString() : jsonString;
        }
    }
}
