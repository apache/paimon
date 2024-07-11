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

package org.apache.paimon.spark;

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** Test for {@link SparkFilterConverter}. */
public class SparkFilterConverterTest {

    @Test
    public void testAll() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));
        SparkFilterConverter converter = new SparkFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        String field = "id";
        IsNull isNull = IsNull.apply(field);
        Predicate expectedIsNull = builder.isNull(0);
        Predicate actualIsNull = converter.convert(isNull);
        assertThat(actualIsNull).isEqualTo(expectedIsNull);

        IsNotNull isNotNull = IsNotNull.apply(field);
        Predicate expectedIsNotNull = builder.isNotNull(0);
        Predicate actualIsNotNull = converter.convert(isNotNull);
        assertThat(actualIsNotNull).isEqualTo(expectedIsNotNull);

        LessThan lt = LessThan.apply(field, 1);
        Predicate expectedLt = builder.lessThan(0, 1);
        Predicate actualLt = converter.convert(lt);
        assertThat(actualLt).isEqualTo(expectedLt);

        LessThan ltNull = LessThan.apply(field, null);
        Predicate expectedLtNull = builder.lessThan(0, null);
        Predicate actualLtNull = converter.convert(ltNull);
        assertThat(actualLtNull).isEqualTo(expectedLtNull);

        LessThanOrEqual ltEq = LessThanOrEqual.apply(field, 1);
        Predicate expectedLtEq = builder.lessOrEqual(0, 1);
        Predicate actualLtEq = converter.convert(ltEq);
        assertThat(actualLtEq).isEqualTo(expectedLtEq);

        LessThanOrEqual ltEqNull = LessThanOrEqual.apply(field, null);
        Predicate expectedLtEqNull = builder.lessOrEqual(0, null);
        Predicate actualLtEqNull = converter.convert(ltEqNull);
        assertThat(actualLtEqNull).isEqualTo(expectedLtEqNull);

        GreaterThan gt = GreaterThan.apply(field, 1);
        Predicate expectedGt = builder.greaterThan(0, 1);
        Predicate actualGt = converter.convert(gt);
        assertThat(actualGt).isEqualTo(expectedGt);

        GreaterThan gtNull = GreaterThan.apply(field, null);
        Predicate expectedGtNull = builder.greaterThan(0, null);
        Predicate actualGtNull = converter.convert(gtNull);
        assertThat(actualGtNull).isEqualTo(expectedGtNull);

        GreaterThanOrEqual gtEq = GreaterThanOrEqual.apply(field, 1);
        Predicate expectedGtEq = builder.greaterOrEqual(0, 1);
        Predicate actualGtEq = converter.convert(gtEq);
        assertThat(actualGtEq).isEqualTo(expectedGtEq);

        GreaterThanOrEqual gtEqNull = GreaterThanOrEqual.apply(field, null);
        Predicate expectedGtEqNull = builder.greaterOrEqual(0, null);
        Predicate actualGtEqNull = converter.convert(gtEqNull);
        assertThat(actualGtEqNull).isEqualTo(expectedGtEqNull);

        EqualTo eq = EqualTo.apply(field, 1);
        Predicate expectedEq = builder.equal(0, 1);
        Predicate actualEq = converter.convert(eq);
        assertThat(actualEq).isEqualTo(expectedEq);

        EqualTo eqNull = EqualTo.apply(field, null);
        Predicate expectedEqNull = builder.equal(0, null);
        Predicate actualEqNull = converter.convert(eqNull);
        assertThat(actualEqNull).isEqualTo(expectedEqNull);

        EqualNullSafe eqSafe = EqualNullSafe.apply(field, 1);
        Predicate expectedEqSafe = builder.equal(0, 1);
        Predicate actualEqSafe = converter.convert(eqSafe);
        assertThat(actualEqSafe).isEqualTo(expectedEqSafe);

        EqualNullSafe eqNullSafe = EqualNullSafe.apply(field, null);
        Predicate expectEqNullSafe = builder.isNull(0);
        Predicate actualEqNullSafe = converter.convert(eqNullSafe);
        assertThat(actualEqNullSafe).isEqualTo(expectEqNullSafe);

        In in = In.apply(field, new Object[] {1, null, 2});
        Predicate expectedIn = builder.in(0, Arrays.asList(1, null, 2));
        Predicate actualIn = converter.convert(in);
        assertThat(actualIn).isEqualTo(expectedIn);

        Object[] literals = new Object[30];
        literals[0] = null;
        for (int i = 1; i < literals.length; i++) {
            literals[i] = i * 100;
        }
        In largeIn = In.apply(field, literals);
        Predicate expectedLargeIn = builder.in(0, Arrays.asList(literals));
        Predicate actualLargeIn = converter.convert(largeIn);
        assertThat(actualLargeIn).isEqualTo(expectedLargeIn);

        RowType rowType01 =
                new RowType(Collections.singletonList(new DataField(0, "id", new VarCharType())));
        SparkFilterConverter converter01 = new SparkFilterConverter(rowType01);
        StringEndsWith endsWith = StringEndsWith.apply("id", "abc");
        Predicate endsWithPre = converter01.convert(endsWith);
        GenericRow row = GenericRow.of(fromString("aabc"));
        GenericRow max = GenericRow.of(fromString("xasxwsa"));
        GenericRow min = GenericRow.of(fromString("aaaaa"));
        boolean test = endsWithPre.test(row);
        Integer[] nullCount = {null};
        boolean test1 = endsWithPre.test(10, min, max, new GenericArray(nullCount));
        assertThat(test).isEqualTo(true);
        assertThat(test1).isEqualTo(true);
    }

    @Test
    public void testTimestamp() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "x", new TimestampType())));
        SparkFilterConverter converter = new SparkFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2018-10-18 00:00:57.907");
        LocalDateTime localDateTime = LocalDateTime.parse("2018-10-18T00:00:57.907");
        Instant instant = localDateTime.toInstant(ZoneOffset.UTC);

        Predicate instantExpression = converter.convert(GreaterThan.apply("x", instant));
        Predicate timestampExpression = converter.convert(GreaterThan.apply("x", timestamp));
        Predicate rawExpression =
                builder.greaterThan(0, Timestamp.fromLocalDateTime(localDateTime));

        assertThat(timestampExpression).isEqualTo(rawExpression);
        assertThat(instantExpression).isEqualTo(rawExpression);
    }

    @Test
    public void testDate() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "x", new DateType())));
        SparkFilterConverter converter = new SparkFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        LocalDate localDate = LocalDate.parse("2018-10-18");
        Date date = Date.valueOf(localDate);
        int epochDay = (int) localDate.toEpochDay();

        Predicate localDateExpression = converter.convert(GreaterThan.apply("x", localDate));
        Predicate dateExpression = converter.convert(GreaterThan.apply("x", date));
        Predicate rawExpression = builder.greaterThan(0, epochDay);

        assertThat(dateExpression).isEqualTo(rawExpression);
        assertThat(localDateExpression).isEqualTo(rawExpression);
    }

    @Test
    public void testIgnoreFailure() {
        List<DataField> dataFields = new ArrayList<>();
        dataFields.add(new DataField(0, "id", new IntType()));
        dataFields.add(new DataField(1, "name", new VarCharType(VarCharType.MAX_LENGTH)));
        RowType rowType = new RowType(dataFields);
        SparkFilterConverter converter = new SparkFilterConverter(rowType);

        Not not = Not.apply(StringStartsWith.apply("name", "paimon"));
        catchThrowableOfType(() -> converter.convert(not), UnsupportedOperationException.class);
        assertThat(converter.convertIgnoreFailure(not)).isNull();
    }
}
