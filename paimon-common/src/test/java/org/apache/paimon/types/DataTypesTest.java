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

package org.apache.paimon.types;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for subclasses of {@link DataType}. */
public class DataTypesTest {

    @Test
    void testCharType() {
        assertThat(new CharType(33))
                .satisfies(baseAssertions("CHAR(33)", new CharType(Integer.MAX_VALUE)));
    }

    @Test
    void testVarCharType() {
        assertThat(new VarCharType(33))
                .satisfies(baseAssertions("VARCHAR(33)", new VarCharType(12)));
    }

    @Test
    void testVarCharTypeWithMaximumLength() {
        assertThat(new VarCharType(Integer.MAX_VALUE))
                .satisfies(baseAssertions("STRING", new VarCharType(12)));
    }

    @Test
    void testBooleanType() {
        assertThat(new BooleanType()).satisfies(baseAssertions("BOOLEAN", new BooleanType(false)));
    }

    @Test
    void testBinaryType() {
        assertThat(new BinaryType(22)).satisfies(baseAssertions("BINARY(22)", new BinaryType()));
    }

    @Test
    void testVarBinaryType() {
        assertThat(new VarBinaryType(22))
                .satisfies(baseAssertions("VARBINARY(22)", new VarBinaryType()));
    }

    @Test
    void testVarBinaryTypeWithMaximumLength() {
        assertThat(new VarBinaryType(Integer.MAX_VALUE))
                .satisfies(baseAssertions("BYTES", new VarBinaryType(12)));
    }

    @Test
    void testDecimalType() {
        assertThat(new DecimalType(10, 2))
                .satisfies(baseAssertions("DECIMAL(10, 2)", new DecimalType()));
    }

    @Test
    void testTinyIntType() {
        assertThat(new TinyIntType()).satisfies(baseAssertions("TINYINT", new TinyIntType(false)));
    }

    @Test
    void testSmallIntType() {
        assertThat(new SmallIntType())
                .satisfies(baseAssertions("SMALLINT", new SmallIntType(false)));
    }

    @Test
    void testIntType() {
        assertThat(new IntType()).satisfies(baseAssertions("INT", new IntType(false)));
    }

    @Test
    void testBigIntType() {
        assertThat(new BigIntType()).satisfies(baseAssertions("BIGINT", new BigIntType(false)));
    }

    @Test
    void testFloatType() {
        assertThat(new FloatType()).satisfies(baseAssertions("FLOAT", new FloatType(false)));
    }

    @Test
    void testDoubleType() {
        assertThat(new DoubleType()).satisfies(baseAssertions("DOUBLE", new DoubleType(false)));
    }

    @Test
    void testDateType() {
        assertThat(new DateType()).satisfies(baseAssertions("DATE", new DateType(false)));
    }

    @Test
    void testTimeType() {
        assertThat(new TimeType(9)).satisfies(baseAssertions("TIME(9)", new TimeType()));
    }

    @Test
    void testTimestampType() {
        assertThat(new TimestampType(9))
                .satisfies(baseAssertions("TIMESTAMP(9)", new TimestampType(3)));
    }

    @Test
    void testLocalZonedTimestampType() {
        assertThat(new LocalZonedTimestampType(9))
                .satisfies(
                        baseAssertions(
                                "TIMESTAMP(9) WITH LOCAL TIME ZONE",
                                new LocalZonedTimestampType(3)));
    }

    @Test
    void testArrayType() {
        assertThat(new ArrayType(new TimestampType()))
                .satisfies(
                        baseAssertions("ARRAY<TIMESTAMP(6)>", new ArrayType(new SmallIntType())));

        assertThat(new ArrayType(new ArrayType(new TimestampType())))
                .satisfies(
                        baseAssertions(
                                "ARRAY<ARRAY<TIMESTAMP(6)>>",
                                new ArrayType(new ArrayType(new SmallIntType()))));
    }

    @Test
    void testMultisetType() {
        assertThat(new MultisetType(new TimestampType()))
                .satisfies(
                        baseAssertions(
                                "MULTISET<TIMESTAMP(6)>", new MultisetType(new SmallIntType())));

        assertThat(new MultisetType(new MultisetType(new TimestampType())))
                .satisfies(
                        baseAssertions(
                                "MULTISET<MULTISET<TIMESTAMP(6)>>",
                                new MultisetType(new MultisetType(new SmallIntType()))));
    }

    @Test
    void testMapType() {
        assertThat(new MapType(new VarCharType(20), new TimestampType()))
                .satisfies(
                        baseAssertions(
                                "MAP<VARCHAR(20), TIMESTAMP(6)>",
                                new MapType(new VarCharType(99), new TimestampType())));
    }

    @Test
    void testRowType() {
        assertThat(
                        new RowType(
                                Arrays.asList(
                                        new DataField(0, "a", new VarCharType(), "Someone's desc."),
                                        new DataField(1, "b`", new TimestampType()))))
                .satisfies(
                        baseAssertions(
                                "ROW<`a` VARCHAR(1) 'Someone''s desc.', `b``` TIMESTAMP(6)>",
                                new RowType(
                                        Arrays.asList(
                                                new DataField(
                                                        0,
                                                        "a",
                                                        new VarCharType(),
                                                        "Different desc."),
                                                new DataField(1, "b`", new TimestampType())))));

        assertThatThrownBy(
                        () ->
                                new RowType(
                                        Arrays.asList(
                                                new DataField(0, "b", new VarCharType()),
                                                new DataField(1, "b", new VarCharType()),
                                                new DataField(2, "a", new VarCharType()),
                                                new DataField(3, "a", new TimestampType()))))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(
                        () ->
                                new RowType(
                                        Collections.singletonList(
                                                new DataField(0, "", new VarCharType()))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --------------------------------------------------------------------------------------------

    private static ThrowingConsumer<DataType> baseAssertions(String sqlString, DataType otherType) {
        return nullableType ->
                assertThat(nullableType)
                        .satisfies(nonEqualityCheckWithOtherType(otherType))
                        .satisfies(DataTypesTest::nullability)
                        .isJavaSerializable()
                        .hasSQLString(sqlString);
    }

    private static ThrowingConsumer<DataType> nonEqualityCheckWithOtherType(DataType otherType) {
        return nullableType -> {
            assertThat(nullableType)
                    .isNullable()
                    .isEqualTo(nullableType)
                    .isEqualTo(nullableType.copy())
                    .isNotEqualTo(otherType);

            assertThat(nullableType.hashCode())
                    .isEqualTo(nullableType.hashCode())
                    .isNotEqualTo(otherType.hashCode());
        };
    }

    private static void nullability(DataType nullableType) {
        final DataType notNullInstance = nullableType.copy(false);
        assertThat(notNullInstance).isNotNullable();
        assertThat(nullableType).isNotEqualTo(notNullInstance);
    }

    private static final DataType UDT_NAME_TYPE = new VarCharType();

    private static final DataType UDT_SETTING_TYPE = new IntType();

    private static final DataType UDT_TIMESTAMP_TYPE = new TimestampType();

    public static DataTypeAssert assertThat(DataType actual) {
        return new DataTypeAssert(actual);
    }

    public static <T> ObjectAssert<T> assertThat(T actual) {
        return Assertions.assertThat(actual);
    }
}
