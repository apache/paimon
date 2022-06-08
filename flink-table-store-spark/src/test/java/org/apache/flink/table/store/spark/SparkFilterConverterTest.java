/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.spark;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.store.file.predicate.Literal;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SparkFilterConverter}. */
public class SparkFilterConverterTest {

    @Test
    public void testAll() {
        SparkFilterConverter converter =
                new SparkFilterConverter(
                        new RowType(
                                Collections.singletonList(
                                        new RowType.RowField("id", new IntType()))));
        String field = "id";
        IsNull isNull = IsNull.apply(field);
        Predicate expectedIsNull = PredicateBuilder.isNull(0);
        Predicate actualIsNull = converter.convert(isNull);
        assertThat(actualIsNull).isEqualTo(expectedIsNull);

        IsNotNull isNotNull = IsNotNull.apply(field);
        Predicate expectedIsNotNull = PredicateBuilder.isNotNull(0);
        Predicate actualIsNotNull = converter.convert(isNotNull);
        assertThat(actualIsNotNull).isEqualTo(expectedIsNotNull);

        LessThan lt = LessThan.apply(field, 1);
        Predicate expectedLt = PredicateBuilder.lessThan(0, new Literal(new IntType(), 1));
        Predicate actualLt = converter.convert(lt);
        assertThat(actualLt).isEqualTo(expectedLt);

        LessThanOrEqual ltEq = LessThanOrEqual.apply(field, 1);
        Predicate expectedLtEq = PredicateBuilder.lessOrEqual(0, new Literal(new IntType(), 1));
        Predicate actualLtEq = converter.convert(ltEq);
        assertThat(actualLtEq).isEqualTo(expectedLtEq);

        GreaterThan gt = GreaterThan.apply(field, 1);
        Predicate expectedGt = PredicateBuilder.greaterThan(0, new Literal(new IntType(), 1));
        Predicate actualGt = converter.convert(gt);
        assertThat(actualGt).isEqualTo(expectedGt);

        GreaterThanOrEqual gtEq = GreaterThanOrEqual.apply(field, 1);
        Predicate expectedGtEq = PredicateBuilder.greaterOrEqual(0, new Literal(new IntType(), 1));
        Predicate actualGtEq = converter.convert(gtEq);
        assertThat(actualGtEq).isEqualTo(expectedGtEq);

        EqualTo eq = EqualTo.apply(field, 1);
        Predicate expectedEq = PredicateBuilder.equal(0, new Literal(new IntType(), 1));
        Predicate actualEq = converter.convert(eq);
        assertThat(actualEq).isEqualTo(expectedEq);
    }

    @Test
    public void testTimestamp() {
        SparkFilterConverter converter =
                new SparkFilterConverter(
                        new RowType(
                                Collections.singletonList(
                                        new RowType.RowField("x", new TimestampType()))));

        Timestamp timestamp = Timestamp.valueOf("2018-10-18 00:00:57.907");
        LocalDateTime localDateTime = LocalDateTime.parse("2018-10-18T00:00:57.907");
        Instant instant = localDateTime.toInstant(ZoneOffset.UTC);

        Predicate instantExpression = converter.convert(GreaterThan.apply("x", instant));
        Predicate timestampExpression = converter.convert(GreaterThan.apply("x", timestamp));
        Predicate rawExpression =
                PredicateBuilder.greaterThan(
                        0,
                        new Literal(
                                new TimestampType(),
                                TimestampData.fromLocalDateTime(localDateTime)));

        assertThat(timestampExpression).isEqualTo(rawExpression);
        assertThat(instantExpression).isEqualTo(rawExpression);
    }

    @Test
    public void testDate() {
        SparkFilterConverter converter =
                new SparkFilterConverter(
                        new RowType(
                                Collections.singletonList(
                                        new RowType.RowField("x", new DateType()))));

        LocalDate localDate = LocalDate.parse("2018-10-18");
        Date date = Date.valueOf(localDate);
        int epochDay = (int) localDate.toEpochDay();

        Predicate localDateExpression = converter.convert(GreaterThan.apply("x", localDate));
        Predicate dateExpression = converter.convert(GreaterThan.apply("x", date));
        Predicate rawExpression =
                PredicateBuilder.greaterThan(0, new Literal(new DateType(), epochDay));

        assertThat(dateExpression).isEqualTo(rawExpression);
        assertThat(localDateExpression).isEqualTo(rawExpression);
    }
}
