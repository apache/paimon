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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DateExtractTransformTest {

    private static int epochDay(int year, int month, int day) {
        return (int) LocalDate.of(year, month, day).toEpochDay();
    }

    private static Object extract(
            DataType type, Function<FieldRef, DateExtractTransform> factory, Object value) {
        return factory.apply(new FieldRef(0, "f0", type)).transform(GenericRow.of(value));
    }

    @Test
    public void testExtractFromDate() {
        int value = epochDay(2023, 7, 15);
        assertThat(extract(DataTypes.DATE(), YearTransform::new, value)).isEqualTo(2023);
        assertThat(extract(DataTypes.DATE(), MonthTransform::new, value)).isEqualTo(7);
        assertThat(extract(DataTypes.DATE(), DayTransform::new, value)).isEqualTo(15);
        assertThat(extract(DataTypes.DATE(), QuarterTransform::new, value)).isEqualTo(3);
        assertThat(extract(DataTypes.DATE(), IsoDayOfWeekTransform::new, value)).isEqualTo(6);
        assertThat(extract(DataTypes.DATE(), DayOfWeekTransform::new, value)).isEqualTo(7);
        assertThat(extract(DataTypes.DATE(), WeekdayTransform::new, value)).isEqualTo(5);
        assertThat(extract(DataTypes.DATE(), DayOfYearTransform::new, value)).isEqualTo(196);
        assertThat(extract(DataTypes.DATE(), WeekTransform::new, value)).isEqualTo(28);
        assertThat(extract(DataTypes.DATE(), YearOfWeekTransform::new, value)).isEqualTo(2023);
    }

    @Test
    public void testIsoWeekCrossesCalendarYear() {
        int value = epochDay(2024, 12, 30);
        assertThat(extract(DataTypes.DATE(), WeekTransform::new, value)).isEqualTo(1);
        assertThat(extract(DataTypes.DATE(), YearOfWeekTransform::new, value)).isEqualTo(2025);
        assertThat(extract(DataTypes.DATE(), IsoDayOfWeekTransform::new, value)).isEqualTo(1);
        assertThat(extract(DataTypes.DATE(), DayOfWeekTransform::new, value)).isEqualTo(2);
        assertThat(extract(DataTypes.DATE(), WeekdayTransform::new, value)).isEqualTo(0);
    }

    @Test
    public void testExtractFromTimestamp() {
        Timestamp value = Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 3, 5, 14, 30, 45));
        assertThat(extract(DataTypes.TIMESTAMP(3), YearTransform::new, value)).isEqualTo(2024);
        assertThat(extract(DataTypes.TIMESTAMP(3), MonthTransform::new, value)).isEqualTo(3);
        assertThat(extract(DataTypes.TIMESTAMP(3), DayTransform::new, value)).isEqualTo(5);
        assertThat(extract(DataTypes.TIMESTAMP(3), HourTransform::new, value)).isEqualTo(14);
        assertThat(extract(DataTypes.TIMESTAMP(3), MinuteTransform::new, value)).isEqualTo(30);
        assertThat(extract(DataTypes.TIMESTAMP(3), SecondTransform::new, value)).isEqualTo(45);
    }

    @Test
    public void testTimeFieldsOfDateAreZero() {
        int value = epochDay(2024, 1, 1);
        assertThat(extract(DataTypes.DATE(), HourTransform::new, value)).isEqualTo(0);
        assertThat(extract(DataTypes.DATE(), MinuteTransform::new, value)).isEqualTo(0);
        assertThat(extract(DataTypes.DATE(), SecondTransform::new, value)).isEqualTo(0);
    }

    @Test
    public void testNullYieldsNull() {
        DateExtractTransform transform = new YearTransform(new FieldRef(0, "d0", DataTypes.DATE()));
        assertThat(transform.transform(GenericRow.of((Object) null))).isNull();
    }

    @Test
    public void testOutputTypeIsInt() {
        DateExtractTransform transform =
                new HourTransform(new FieldRef(0, "t0", DataTypes.TIMESTAMP(6)));
        assertThat(transform.outputType()).isEqualTo(DataTypes.INT());
    }

    @Test
    public void testExtractPredicateFiltersRows() {
        PredicateBuilder builder =
                new PredicateBuilder(RowType.of(DataTypes.DATE(), DataTypes.TIMESTAMP(3)));
        Predicate predicate =
                builder.equal(new YearTransform(new FieldRef(0, "d0", DataTypes.DATE())), 2023);

        assertThat(predicate.test(GenericRow.of(epochDay(2023, 7, 15), null))).isTrue();
        assertThat(predicate.test(GenericRow.of(epochDay(2024, 1, 1), null))).isFalse();
        assertThat(predicate.test(GenericRow.of((Object) null, null))).isFalse();
    }

    @Test
    public void testCopyWithNewInputsRemapsField() {
        DateExtractTransform transform =
                new MonthTransform(new FieldRef(0, "d0", DataTypes.DATE()));
        DateExtractTransform copied =
                (DateExtractTransform)
                        transform.copyWithNewInputs(
                                Collections.singletonList(new FieldRef(1, "d1", DataTypes.DATE())));
        assertThat(copied.transform(GenericRow.of((Object) null, epochDay(2024, 5, 20))))
                .isEqualTo(5);
    }

    @Test
    public void testUnsupportedFieldTypeIsRejected() {
        for (DataType type :
                new DataType[] {DataTypes.STRING(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)}) {
            FieldRef fieldRef = new FieldRef(0, "f0", type);
            assertThatThrownBy(() -> new YearTransform(fieldRef))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("YEAR requires a DATE or TIMESTAMP field, found " + type);
            assertThat(YearTransform.tryCreate(fieldRef)).isEqualTo(Optional.empty());
        }

        FieldRef dateRef = new FieldRef(0, "d0", DataTypes.DATE());
        assertThat(YearTransform.tryCreate(dateRef).isPresent()).isTrue();
    }

    @Test
    public void testToString() {
        DateExtractTransform transform = new YearTransform(new FieldRef(0, "d0", DataTypes.DATE()));
        assertThat(transform.toString()).isEqualTo("YEAR(d0)");
    }
}
