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

package org.apache.paimon.schema;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeJsonParser;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataTypeJsonParser}. */
public class DataTypeJsonParserTest {

    private static Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forString("CHAR").expectType(new CharType()),
                TestSpec.forString("CHAR NOT NULL").expectType(new CharType().copy(false)),
                TestSpec.forString("char not null").expectType(new CharType().copy(false)),
                TestSpec.forString("CHAR NULL").expectType(new CharType()),
                TestSpec.forString("CHAR(33)").expectType(new CharType(33)),
                TestSpec.forString("VARCHAR").expectType(new VarCharType()),
                TestSpec.forString("VARCHAR(33)").expectType(new VarCharType(33)),
                TestSpec.forString("STRING").expectType(VarCharType.STRING_TYPE),
                TestSpec.forString("BOOLEAN").expectType(new BooleanType()),
                TestSpec.forString("BINARY").expectType(new BinaryType()),
                TestSpec.forString("BINARY(33)").expectType(new BinaryType(33)),
                TestSpec.forString("VARBINARY").expectType(new VarBinaryType()),
                TestSpec.forString("VARBINARY(33)").expectType(new VarBinaryType(33)),
                TestSpec.forString("BYTES").expectType(new VarBinaryType(VarBinaryType.MAX_LENGTH)),
                TestSpec.forString("DECIMAL").expectType(new DecimalType()),
                TestSpec.forString("DEC").expectType(new DecimalType()),
                TestSpec.forString("NUMERIC").expectType(new DecimalType()),
                TestSpec.forString("DECIMAL(10)").expectType(new DecimalType(10)),
                TestSpec.forString("DEC(10)").expectType(new DecimalType(10)),
                TestSpec.forString("NUMERIC(10)").expectType(new DecimalType(10)),
                TestSpec.forString("DECIMAL(10, 3)").expectType(new DecimalType(10, 3)),
                TestSpec.forString("DEC(10, 3)").expectType(new DecimalType(10, 3)),
                TestSpec.forString("NUMERIC(10, 3)").expectType(new DecimalType(10, 3)),
                TestSpec.forString("TINYINT").expectType(new TinyIntType()),
                TestSpec.forString("SMALLINT").expectType(new SmallIntType()),
                TestSpec.forString("INTEGER").expectType(new IntType()),
                TestSpec.forString("INT").expectType(new IntType()),
                TestSpec.forString("BIGINT").expectType(new BigIntType()),
                TestSpec.forString("FLOAT").expectType(new FloatType()),
                TestSpec.forString("DOUBLE").expectType(new DoubleType()),
                TestSpec.forString("DOUBLE PRECISION").expectType(new DoubleType()),
                TestSpec.forString("DATE").expectType(new DateType()),
                TestSpec.forString("TIME").expectType(new TimeType()),
                TestSpec.forString("TIME(3)").expectType(new TimeType(3)),
                TestSpec.forString("TIME WITHOUT TIME ZONE").expectType(new TimeType()),
                TestSpec.forString("TIME(3) WITHOUT TIME ZONE").expectType(new TimeType(3)),
                TestSpec.forString("TIMESTAMP").expectType(new TimestampType()),
                TestSpec.forString("TIMESTAMP(3)").expectType(new TimestampType(3)),
                TestSpec.forString("TIMESTAMP WITHOUT TIME ZONE").expectType(new TimestampType()),
                TestSpec.forString("TIMESTAMP(3) WITHOUT TIME ZONE")
                        .expectType(new TimestampType(3)),
                TestSpec.forString("TIMESTAMP WITH LOCAL TIME ZONE")
                        .expectType(new LocalZonedTimestampType()),
                TestSpec.forString("TIMESTAMP_LTZ").expectType(new LocalZonedTimestampType()),
                TestSpec.forString("TIMESTAMP(3) WITH LOCAL TIME ZONE")
                        .expectType(new LocalZonedTimestampType(3)),
                TestSpec.forString("TIMESTAMP_LTZ(3)").expectType(new LocalZonedTimestampType(3)),
                TestSpec.forString(
                                "{\"type\":\"ARRAY\",\"element\":\"TIMESTAMP(3) WITH LOCAL TIME ZONE\"}")
                        .expectType(new ArrayType(new LocalZonedTimestampType(3))),
                TestSpec.forString("{\"type\":\"ARRAY\",\"element\":\"INT NOT NULL\"}")
                        .expectType(new ArrayType(new IntType(false))),
                TestSpec.forString("{\"type\":\"ARRAY\",\"element\":\"INT\"}")
                        .expectType(new ArrayType(new IntType())),
                TestSpec.forString("{\"type\":\"ARRAY\",\"element\":\"INT NOT NULL\"}")
                        .expectType(new ArrayType(new IntType(false))),
                TestSpec.forString("{\"type\":\"ARRAY NOT NULL\",\"element\":\"INT\"}")
                        .expectType(new ArrayType(false, new IntType())),
                TestSpec.forString("{\"type\":\"MULTISET\",\"element\":\"INT NOT NULL\"}")
                        .expectType(new MultisetType(new IntType(false))),
                TestSpec.forString("{\"type\":\"MULTISET\",\"element\":\"INT\"}")
                        .expectType(new MultisetType(new IntType())),
                TestSpec.forString("{\"type\":\"MULTISET\",\"element\":\"INT NOT NULL\"}")
                        .expectType(new MultisetType(new IntType(false))),
                TestSpec.forString("{\"type\":\"MULTISET NOT NULL\",\"element\":\"INT\"}")
                        .expectType(new MultisetType(false, new IntType())),
                TestSpec.forString("{\"type\":\"MAP\",\"key\":\"BIGINT\",\"value\":\"BOOLEAN\"}")
                        .expectType(new MapType(new BigIntType(), new BooleanType())),
                TestSpec.forString(
                                "{\"type\":\"ROW\",\"fields\":[{\"id\":0,\"name\":\"f0\",\"type\":\"INT NOT NULL\"},{\"id\":1,\"name\":\"f1\",\"type\":\"BOOLEAN\"}]}")
                        .expectType(
                                new RowType(
                                        Arrays.asList(
                                                new DataField(0, "f0", new IntType(false)),
                                                new DataField(1, "f1", new BooleanType())))),
                TestSpec.forString(
                                "{\"type\":\"ROW\",\"fields\":[{\"id\":0,\"name\":\"f0\",\"type\":\"INT NOT NULL\"},{\"id\":1,\"name\":\"f1\",\"type\":\"BOOLEAN\"}]}")
                        .expectType(
                                new RowType(
                                        Arrays.asList(
                                                new DataField(0, "f0", new IntType(false)),
                                                new DataField(1, "f1", new BooleanType())))),
                TestSpec.forString(
                                "{\"type\":\"ROW\",\"fields\":[{\"id\":0,\"name\":\"f0\",\"type\":\"INT\"}]}")
                        .expectType(
                                new RowType(
                                        Collections.singletonList(
                                                new DataField(0, "f0", new IntType())))),
                TestSpec.forString("{\"type\":\"ROW\",\"fields\":[]}")
                        .expectType(new RowType(Collections.emptyList())),
                TestSpec.forString(
                                "{\"type\":\"ROW\",\"fields\":[{\"id\":0,\"name\":\"f0\",\"type\":\"INT NOT NULL\",\"description\":\"This is a comment.\"},{\"id\":1,\"name\":\"f1\",\"type\":\"BOOLEAN\",\"description\":\"This as well.\"}]}")
                        .expectType(
                                new RowType(
                                        Arrays.asList(
                                                new DataField(
                                                        0,
                                                        "f0",
                                                        new IntType(false),
                                                        "This is a comment."),
                                                new DataField(
                                                        1,
                                                        "f1",
                                                        new BooleanType(),
                                                        "This as well.")))),

                // error message testing

                TestSpec.forString("VARCHAR(test)").expectErrorMessage("<LITERAL_INT> expected"),
                TestSpec.forString("VARCHAR(33333333333)")
                        .expectErrorMessage("Invalid integer value"));
    }

    @ParameterizedTest(name = "{index}: [From: {0}, To: {1}]")
    @MethodSource("testData")
    void testParsing(TestSpec testSpec) {
        if (testSpec.expectedType != null) {
            assertThat(parse(testSpec.jsonString)).isEqualTo(testSpec.expectedType);
        }
    }

    @ParameterizedTest(name = "{index}: [From: {0}, To: {1}]")
    @MethodSource("testData")
    void testJsonParsing(TestSpec testSpec) {
        if (testSpec.expectedType != null) {
            assertThat(parse(toJson(testSpec.expectedType))).isEqualTo(testSpec.expectedType);
        }
    }

    @ParameterizedTest(name = "{index}: [From: {0}, To: {1}]")
    @MethodSource("testData")
    void testErrorMessage(TestSpec testSpec) {
        if (testSpec.expectedErrorMessage != null) {
            assertThatThrownBy(() -> parse(testSpec.jsonString))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(testSpec.expectedErrorMessage);
        }
    }

    @Test
    void testParingRowTypeWithoutFieldId() {
        String jsonString1 =
                "{\"type\":\"ROW\",\"fields\":[{\"name\":\"field1\",\"type\":\"INT\"},{\"name\":\"field2\",\"type\":\"STRING\"}]}";
        RowType rowType1 =
                RowType.builder()
                        .fields(
                                new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                                new String[] {"field1", "field2"})
                        .build();
        assertThat(parse(jsonString1)).isEqualTo(rowType1);

        String jsonString2 =
                "{\"type\":\"ROW\",\"fields\":[{\"name\":\"field1\",\"type\":\"INT\"},{\"name\":\"field2\",\"type\":{\"type\":\"ROW\",\"fields\":[{\"name\":\"s1\",\"type\":\"INT\"},{\"name\":\"s2\",\"type\":\"STRING\"}]}}]}";
        RowType rowType2 =
                RowType.builder()
                        .fields(
                                new DataType[] {
                                    DataTypes.INT(),
                                    RowType.builder(new AtomicInteger(1))
                                            .fields(
                                                    new DataType[] {
                                                        DataTypes.INT(), DataTypes.STRING()
                                                    },
                                                    new String[] {"s1", "s2"})
                                            .build()
                                },
                                new String[] {"field1", "field2"})
                        .build();
        assertThat(parse(jsonString2)).isEqualTo(rowType2);

        String jsonString3 =
                "{\"type\":\"ROW\",\"fields\":[{\"name\":\"field1\",\"type\":\"INT\"},{\"id\":1, \"name\":\"field2\",\"type\":\"STRING\"}]}";
        assertThatThrownBy(() -> parse(jsonString3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Partial field id is not allowed.");
    }

    private static String toJson(DataType type) {
        return JsonSerdeUtil.toFlatJson(type);
    }

    private static DataType parse(String json) {
        if (!json.startsWith("\"") && !json.startsWith("{")) {
            json = "\"" + json + "\"";
        }
        return JsonSerdeUtil.fromJson(json, DataType.class);
    }

    // --------------------------------------------------------------------------------------------

    private static class TestSpec {

        private final String jsonString;

        private @Nullable DataType expectedType;

        private @Nullable String expectedErrorMessage;

        private TestSpec(String jsonString) {
            this.jsonString = jsonString;
        }

        static TestSpec forString(String jsonString) {
            return new TestSpec(jsonString);
        }

        TestSpec expectType(DataType expectedType) {
            this.expectedType = expectedType;
            return this;
        }

        TestSpec expectErrorMessage(String expectedErrorMessage) {
            this.expectedErrorMessage = expectedErrorMessage;
            return this;
        }
    }
}
