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

package org.apache.paimon.utils;

import org.apache.paimon.data.Timestamp;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DateTimeUtils}. */
public class DateTimeUtilsTest {

    @Test
    public void testFormatLocalDateTime() {
        LocalDateTime time = LocalDateTime.of(2023, 8, 30, 12, 30, 59, 999_999_999);
        String[] expectations = new String[10];
        expectations[0] = "2023-08-30 12:30:59";
        expectations[1] = "2023-08-30 12:30:59.9";
        for (int i = 2; i <= 9; i++) {
            expectations[i] = expectations[i - 1] + "9";
        }

        for (int precision = 0; precision <= 9; precision++) {
            assertThat(DateTimeUtils.formatLocalDateTime(time, precision))
                    .isEqualTo(expectations[precision]);
        }
    }

    @Test
    public void testAutoFormatDatetimeTz() {
        long millisPerHour = 3600000L;
        String testDatetime = "2023-12-12 10:12:12";
        long testTimestamp = 1702375932000L;

        Timestamp utc = DateTimeUtils.autoFormatToTimestamp(testDatetime + "/UTC");
        Timestamp utc1 = DateTimeUtils.autoFormatToTimestamp(testDatetime + "/UTC+1");
        Timestamp utcMinus1 = DateTimeUtils.autoFormatToTimestamp(testDatetime + "/UTC-1");
        Timestamp utc8 = DateTimeUtils.autoFormatToTimestamp(testDatetime + "/UTC+8");
        Timestamp utcMinus8 = DateTimeUtils.autoFormatToTimestamp(testDatetime + "/UTC-8");

        assertThat(utc.getMillisecond()).isEqualTo(testTimestamp);
        assertThat(utc1.getMillisecond()).isEqualTo(testTimestamp - millisPerHour);
        assertThat(utcMinus1.getMillisecond()).isEqualTo(testTimestamp + millisPerHour);
        assertThat(utc8.getMillisecond()).isEqualTo(testTimestamp - millisPerHour * 8);
        assertThat(utcMinus8.getMillisecond()).isEqualTo(testTimestamp + millisPerHour * 8);
    }

    @Test
    public void testTimestamp() {
        int nanos = 100;
        java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
        for (int i = 0; i < 2000; i++) {
            timestamp = new java.sql.Timestamp(timestamp.getTime() + 60 * 1000);
            timestamp.setNanos(nanos + timestamp.getNanos());

            Timestamp t1 = Timestamp.fromSQLTimestamp(timestamp);
            Timestamp t2 = DateTimeUtils.toInternal(timestamp.getTime(), nanos);
            assertThat(t1).isEqualTo(t2);
        }
    }
}
