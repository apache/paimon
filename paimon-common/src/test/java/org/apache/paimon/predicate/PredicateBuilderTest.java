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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.paimon.predicate.SimpleColStatsTestUtils.test;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PredicateBuilder}. */
public class PredicateBuilderTest {

    @Test
    public void testBetween() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.between(0, 1, 3);

        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(2))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(3))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(2, 5, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 2, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testBetweenNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.between(0, 1, null);

        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(2))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(3))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(2, 5, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 2, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testSplitAnd() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        RowType.of(
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType()));

        Predicate child1 =
                PredicateBuilder.or(builder.isNull(0), builder.isNull(1), builder.isNull(2));
        Predicate child2 =
                PredicateBuilder.and(builder.isNull(3), builder.isNull(4), builder.isNull(5));
        Predicate child3 = builder.isNull(6);

        assertThat(PredicateBuilder.splitAnd(PredicateBuilder.and(child1, child2, child3)))
                .isEqualTo(
                        Arrays.asList(
                                child1,
                                builder.isNull(3),
                                builder.isNull(4),
                                builder.isNull(5),
                                child3));
    }

    @Test
    public void testIn() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.in(0, new ArrayList<>());
        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(false);
        predicate = builder.in(0, Arrays.asList(1, 2));
        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(10))).isEqualTo(false);
    }

    @Test
    public void testConvertToJavaObjectRoundTrip() {
        // VARCHAR
        DataType varcharType = new VarCharType();
        Object internalVarchar = PredicateBuilder.convertJavaObject(varcharType, "hello");
        assertThat(PredicateBuilder.convertToJavaObject(varcharType, internalVarchar))
                .isEqualTo("hello");

        // DECIMAL
        DecimalType decimalType = new DecimalType(10, 2);
        BigDecimal decimal = new BigDecimal("12.34");
        Object internalDecimal = PredicateBuilder.convertJavaObject(decimalType, decimal);
        assertThat(PredicateBuilder.convertToJavaObject(decimalType, internalDecimal))
                .isEqualTo(decimal);

        // DATE
        DataType dateType = new DateType();
        LocalDate date = LocalDate.of(2024, 1, 2);
        Object internalDate = PredicateBuilder.convertJavaObject(dateType, date);
        assertThat(PredicateBuilder.convertToJavaObject(dateType, internalDate)).isEqualTo(date);

        // TIME
        DataType timeType = new TimeType(3);
        LocalTime time = LocalTime.of(1, 2, 3, 400_000_000);
        Object internalTime = PredicateBuilder.convertJavaObject(timeType, time);
        assertThat(PredicateBuilder.convertToJavaObject(timeType, internalTime))
                .isEqualTo(LocalTime.of(1, 2, 3, 400_000_000));

        // TIMESTAMP (without time zone)
        DataType tsType = new TimestampType(3);
        LocalDateTime ts = LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_000_000);
        Object internalTs = PredicateBuilder.convertJavaObject(tsType, ts);
        assertThat(PredicateBuilder.convertToJavaObject(tsType, internalTs)).isEqualTo(ts);

        // TIMESTAMP_LTZ
        DataType ltzType = new LocalZonedTimestampType(3);
        Instant instant = Instant.parse("2024-01-02T03:04:05.123Z");
        Object internalLtz = PredicateBuilder.convertJavaObject(ltzType, instant);
        assertThat(PredicateBuilder.convertToJavaObject(ltzType, internalLtz)).isEqualTo(instant);
    }
}
