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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests casting DATE, TIME and TIMESTAMP to a bounded or non-nullable character string. Those
 * targets resolve to no rule until the rules are keyed on the string family, and the unbounded
 * cases cover the branch that skips the trim and pad.
 */
public class DateTimeToCharacterStringCastRuleTest {

    @Test
    public void testDateToBoundedString() {
        DateType date = new DateType();

        assertThat(cast(date, new VarCharType(4), 0)).isEqualTo("1970");
        assertThat(cast(date, new CharType(12), 0)).isEqualTo("1970-01-01  ");
        assertThat(cast(date, VarCharType.STRING_TYPE, 0)).isEqualTo("1970-01-01");
        assertThat(cast(date, VarCharType.stringType(false), 0)).isEqualTo("1970-01-01");
    }

    @Test
    public void testTimeToBoundedString() {
        assertThat(cast(new TimeType(0), new VarCharType(5), 3661000)).isEqualTo("01:01");
        assertThat(cast(new TimeType(0), new CharType(10), 3661000)).isEqualTo("01:01:01  ");
        assertThat(cast(new TimeType(0), VarCharType.STRING_TYPE, 3661000)).isEqualTo("01:01:01");

        // the input precision has to survive: a bounded target must cut the fraction, not the rule
        assertThat(cast(new TimeType(3), VarCharType.STRING_TYPE, 3661123))
                .isEqualTo("01:01:01.123");
        assertThat(cast(new TimeType(3), new VarCharType(8), 3661123)).isEqualTo("01:01:01");
    }

    @Test
    public void testTimestampToBoundedString() {
        TimestampType timestamp = new TimestampType(3);
        Timestamp value = Timestamp.fromEpochMillis(0);

        assertThat(cast(timestamp, new VarCharType(10), value)).isEqualTo("1970-01-01");
        assertThat(cast(timestamp, new CharType(25), value)).isEqualTo("1970-01-01 00:00:00.000  ");
        assertThat(cast(timestamp, VarCharType.STRING_TYPE, value))
                .isEqualTo("1970-01-01 00:00:00.000");
        assertThat(cast(timestamp, VarCharType.stringType(false), value))
                .isEqualTo("1970-01-01 00:00:00.000");
    }

    @Test
    public void testLocalZonedTimestampToBoundedString() {
        // this input keeps the default time zone rather than UTC, so pin the bounded result
        // against the unbounded one instead of a fixed instant
        LocalZonedTimestampType ltz = new LocalZonedTimestampType(3);
        Timestamp value = Timestamp.fromEpochMillis(0);

        String unbounded = cast(ltz, VarCharType.STRING_TYPE, value);
        assertThat(unbounded).hasSize(23);
        assertThat(cast(ltz, new VarCharType(10), value)).isEqualTo(unbounded.substring(0, 10));
        assertThat(cast(ltz, new CharType(25), value)).isEqualTo(unbounded + "  ");
    }

    @SuppressWarnings("unchecked")
    private static <T> String cast(DataType inputType, DataType targetType, T value) {
        CastExecutor<T, BinaryString> executor =
                (CastExecutor<T, BinaryString>) CastExecutors.resolve(inputType, targetType);
        assertThat(executor).as("no cast rule for %s to %s", inputType, targetType).isNotNull();
        return executor.cast(value).toString();
    }
}
