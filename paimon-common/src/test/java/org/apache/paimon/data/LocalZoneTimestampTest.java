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
import java.time.ZoneId;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LocalZoneTimestamp}. */
public class LocalZoneTimestampTest {

    @Test
    public void testNormal() {
        // From long to TimestampData and vice versa
        assertThat(LocalZoneTimestamp.fromEpochMillis(1123L).getMillisecond()).isEqualTo(1123L);
        assertThat(LocalZoneTimestamp.fromEpochMillis(-1123L).getMillisecond()).isEqualTo(-1123L);

        assertThat(LocalZoneTimestamp.fromEpochMillis(1123L, 45678).getMillisecond())
                .isEqualTo(1123L);
        assertThat(LocalZoneTimestamp.fromEpochMillis(1123L, 45678).getNanoOfMillisecond())
                .isEqualTo(45678);

        assertThat(LocalZoneTimestamp.fromEpochMillis(-1123L, 45678).getMillisecond())
                .isEqualTo(-1123L);
        assertThat(LocalZoneTimestamp.fromEpochMillis(-1123L, 45678).getNanoOfMillisecond())
                .isEqualTo(45678);

        // From TimestampData to TimestampData and vice versa
        java.sql.Timestamp t19 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
        java.sql.Timestamp t16 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456");
        java.sql.Timestamp t13 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123");
        java.sql.Timestamp t10 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00");

        assertThat(LocalZoneTimestamp.fromSQLTimestamp(t19).toSQLTimestamp()).isEqualTo(t19);
        assertThat(LocalZoneTimestamp.fromSQLTimestamp(t16).toSQLTimestamp()).isEqualTo(t16);
        assertThat(LocalZoneTimestamp.fromSQLTimestamp(t13).toSQLTimestamp()).isEqualTo(t13);
        assertThat(LocalZoneTimestamp.fromSQLTimestamp(t10).toSQLTimestamp()).isEqualTo(t10);

        java.sql.Timestamp t2 = java.sql.Timestamp.valueOf("1979-01-02 00:00:00.123456");
        assertThat(LocalZoneTimestamp.fromSQLTimestamp(t2).toSQLTimestamp()).isEqualTo(t2);

        java.sql.Timestamp t3 = new java.sql.Timestamp(1572333940000L);
        assertThat(LocalZoneTimestamp.fromSQLTimestamp(t3).toSQLTimestamp()).isEqualTo(t3);

        // From Instant to TimestampData and vice versa
        Instant instant1 = Instant.ofEpochMilli(123L);
        Instant instant2 = Instant.ofEpochSecond(0L, 123456789L);
        Instant instant3 = Instant.ofEpochSecond(-2L, 123456789L);

        assertThat(LocalZoneTimestamp.fromInstant(instant1).toInstant()).isEqualTo(instant1);
        assertThat(LocalZoneTimestamp.fromInstant(instant2).toInstant()).isEqualTo(instant2);
        assertThat(LocalZoneTimestamp.fromInstant(instant3).toInstant()).isEqualTo(instant3);
    }

    @Test
    public void testDaylightSavingTime() {
        TimeZone tz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

        java.sql.Timestamp dstBegin2018 = java.sql.Timestamp.valueOf("2018-03-11 03:00:00");
        assertThat(LocalZoneTimestamp.fromSQLTimestamp(dstBegin2018).toSQLTimestamp())
                .isEqualTo(dstBegin2018);

        java.sql.Timestamp dstBegin2019 = java.sql.Timestamp.valueOf("2019-03-10 02:00:00");
        assertThat(LocalZoneTimestamp.fromSQLTimestamp(dstBegin2019).toSQLTimestamp())
                .isEqualTo(dstBegin2019);

        TimeZone.setDefault(tz);
    }

    @Test
    public void testToString() {

        java.sql.Timestamp t = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
        assertThat(LocalZoneTimestamp.fromSQLTimestamp(t).toString())
                .isEqualTo("1969-01-02T00:00:00.123456789");

        assertThat(LocalZoneTimestamp.fromEpochMillis(123L).toString())
                .isEqualTo(
                        Instant.ofEpochMilli(123)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDateTime()
                                .toString());

        Instant instant = Instant.ofEpochSecond(0L, 123456789L);
        assertThat(LocalZoneTimestamp.fromInstant(instant).toString())
                .isEqualTo(instant.atZone(ZoneId.systemDefault()).toLocalDateTime().toString());
    }

    @Test
    public void testToMicros() {
        java.sql.Timestamp t = java.sql.Timestamp.valueOf("2005-01-02 00:00:00.123456789");
        assertThat(LocalZoneTimestamp.fromSQLTimestamp(t).toString())
                .isEqualTo("2005-01-02T00:00:00.123456789");
        assertThat(
                        LocalZoneTimestamp.fromMicros(
                                        LocalZoneTimestamp.fromSQLTimestamp(t).toMicros())
                                .toString())
                .isEqualTo("2005-01-02T00:00:00.123456");
    }
}
