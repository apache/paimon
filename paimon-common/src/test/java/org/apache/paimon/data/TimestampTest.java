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

package org.apache.paimon.data;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Timestamp}. */
public class TimestampTest {

    @Test
    public void testNormal() {
        // From long to TimestampData and vice versa
        assertThat(Timestamp.fromEpochMillis(1123L).getMillisecond()).isEqualTo(1123L);
        assertThat(Timestamp.fromEpochMillis(-1123L).getMillisecond()).isEqualTo(-1123L);

        assertThat(Timestamp.fromEpochMillis(1123L, 45678).getMillisecond()).isEqualTo(1123L);
        assertThat(Timestamp.fromEpochMillis(1123L, 45678).getNanoOfMillisecond()).isEqualTo(45678);

        assertThat(Timestamp.fromEpochMillis(-1123L, 45678).getMillisecond()).isEqualTo(-1123L);
        assertThat(Timestamp.fromEpochMillis(-1123L, 45678).getNanoOfMillisecond())
                .isEqualTo(45678);

        // From TimestampData to TimestampData and vice versa
        java.sql.Timestamp t19 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
        java.sql.Timestamp t16 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456");
        java.sql.Timestamp t13 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123");
        java.sql.Timestamp t10 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00");

        assertThat(Timestamp.fromSQLTimestamp(t19).toSQLTimestamp()).isEqualTo(t19);
        assertThat(Timestamp.fromSQLTimestamp(t16).toSQLTimestamp()).isEqualTo(t16);
        assertThat(Timestamp.fromSQLTimestamp(t13).toSQLTimestamp()).isEqualTo(t13);
        assertThat(Timestamp.fromSQLTimestamp(t10).toSQLTimestamp()).isEqualTo(t10);

        java.sql.Timestamp t2 = java.sql.Timestamp.valueOf("1979-01-02 00:00:00.123456");
        assertThat(Timestamp.fromSQLTimestamp(t2).toSQLTimestamp()).isEqualTo(t2);

        java.sql.Timestamp t3 = new java.sql.Timestamp(1572333940000L);
        assertThat(Timestamp.fromSQLTimestamp(t3).toSQLTimestamp()).isEqualTo(t3);

        // From LocalDateTime to TimestampData and vice versa
        LocalDateTime ldt19 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
        LocalDateTime ldt16 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456000);
        LocalDateTime ldt13 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123000000);
        LocalDateTime ldt10 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);

        assertThat(Timestamp.fromLocalDateTime(ldt19).toLocalDateTime()).isEqualTo(ldt19);
        assertThat(Timestamp.fromLocalDateTime(ldt16).toLocalDateTime()).isEqualTo(ldt16);
        assertThat(Timestamp.fromLocalDateTime(ldt13).toLocalDateTime()).isEqualTo(ldt13);
        assertThat(Timestamp.fromLocalDateTime(ldt10).toLocalDateTime()).isEqualTo(ldt10);

        LocalDateTime ldt2 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);
        assertThat(Timestamp.fromLocalDateTime(ldt2).toLocalDateTime()).isEqualTo(ldt2);

        LocalDateTime ldt3 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
        assertThat(Timestamp.fromLocalDateTime(ldt3).toLocalDateTime()).isEqualTo(ldt3);

        LocalDateTime ldt4 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
        java.sql.Timestamp t4 = java.sql.Timestamp.valueOf(ldt4);
        assertThat(Timestamp.fromSQLTimestamp(t4)).isEqualTo(Timestamp.fromLocalDateTime(ldt4));

        // From Instant to TimestampData and vice versa
        Instant instant1 = Instant.ofEpochMilli(123L);
        Instant instant2 = Instant.ofEpochSecond(0L, 123456789L);
        Instant instant3 = Instant.ofEpochSecond(-2L, 123456789L);

        assertThat(Timestamp.fromInstant(instant1).toInstant()).isEqualTo(instant1);
        assertThat(Timestamp.fromInstant(instant2).toInstant()).isEqualTo(instant2);
        assertThat(Timestamp.fromInstant(instant3).toInstant()).isEqualTo(instant3);
    }

    @Test
    public void testDaylightSavingTime() {
        TimeZone tz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

        java.sql.Timestamp dstBegin2018 = java.sql.Timestamp.valueOf("2018-03-11 03:00:00");
        assertThat(Timestamp.fromSQLTimestamp(dstBegin2018).toSQLTimestamp())
                .isEqualTo(dstBegin2018);

        java.sql.Timestamp dstBegin2019 = java.sql.Timestamp.valueOf("2019-03-10 02:00:00");
        assertThat(Timestamp.fromSQLTimestamp(dstBegin2019).toSQLTimestamp())
                .isEqualTo(dstBegin2019);

        TimeZone.setDefault(tz);
    }

    @Test
    public void testToString() {

        java.sql.Timestamp t = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
        assertThat(Timestamp.fromSQLTimestamp(t).toString())
                .isEqualTo("1969-01-02T00:00:00.123456789");

        assertThat(Timestamp.fromEpochMillis(123L).toString()).isEqualTo("1970-01-01T00:00:00.123");
        assertThat(Timestamp.fromEpochMillis(123L, 456789).toString())
                .isEqualTo("1970-01-01T00:00:00.123456789");

        assertThat(Timestamp.fromEpochMillis(-123L).toString())
                .isEqualTo("1969-12-31T23:59:59.877");
        assertThat(Timestamp.fromEpochMillis(-123L, 456789).toString())
                .isEqualTo("1969-12-31T23:59:59.877456789");

        LocalDateTime ldt = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
        assertThat(Timestamp.fromLocalDateTime(ldt).toString())
                .isEqualTo("1969-01-02T00:00:00.123456789");

        Instant instant = Instant.ofEpochSecond(0L, 123456789L);
        assertThat(Timestamp.fromInstant(instant).toString())
                .isEqualTo("1970-01-01T00:00:00.123456789");
    }

    @Test
    public void testToMicros() {
        java.sql.Timestamp t = java.sql.Timestamp.valueOf("2005-01-02 00:00:00.123456789");
        assertThat(Timestamp.fromSQLTimestamp(t).toString())
                .isEqualTo("2005-01-02T00:00:00.123456789");
        assertThat(Timestamp.fromMicros(Timestamp.fromSQLTimestamp(t).toMicros()).toString())
                .isEqualTo("2005-01-02T00:00:00.123456");
    }
}
