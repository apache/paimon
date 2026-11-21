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

package org.apache.paimon.tag;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.apache.paimon.CoreOptions.TagPeriodFormatter.WITHOUT_DASHES;
import static org.apache.paimon.CoreOptions.TagPeriodFormatter.WITHOUT_DASHES_AND_SPACES;
import static org.apache.paimon.CoreOptions.TagPeriodFormatter.WITH_DASHES;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for TagPeriodHandler. */
public class TagPeriodHandlerTest {

    @ParameterizedTest
    @CsvSource({
        "1969-12-31T23:59:59.999, 60, 1969-12-31T23:00",
        "1970-01-01T01:14:00, 37, 1970-01-01T00:37",
        "1970-01-01T01:14:00.000999999, 37, 1970-01-01T00:37",
        "+54696732-08-30T20:26:40, 60, +54696732-08-30T19:00",
        "+11761245-09-30T20:26:40, 60, +11761245-09-30T19:00",
        "+292278994-08-17T21:12:55.807, 60, +292278994-08-17T20:00",
        "-292275055-05-16T04:47:04.193, 60, -292275055-05-16T04:00"
    })
    public void testNormalizeForWatermarkValidation(String input, long minutes, String expected) {
        TagPeriodHandler handler =
                new TagPeriodHandler.PeriodDurationTagPeriodHandler(Duration.ofMinutes(minutes));
        assertThat(handler.normalizeForWatermarkValidation(LocalDateTime.parse(input)))
                .isEqualTo(LocalDateTime.parse(expected));
    }

    @Test
    public void testDailyTagPeriodHandler() {
        TagPeriodHandler.DailyTagPeriodHandler dailyTagPeriodHandler =
                new TagPeriodHandler.DailyTagPeriodHandler(WITH_DASHES);
        assertThat(dailyTagPeriodHandler.timeToTag(LocalDateTime.of(2023, 1, 1, 0, 0)))
                .isEqualTo("2023-01-01");
        assertThat(dailyTagPeriodHandler.tagToTime("2023-01-01"))
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 0, 0));
        assertThat(dailyTagPeriodHandler.isAutoTag("2023-01-01")).isTrue();
        assertThat(dailyTagPeriodHandler.isAutoTag("20230101")).isFalse();

        dailyTagPeriodHandler = new TagPeriodHandler.DailyTagPeriodHandler(WITHOUT_DASHES);
        assertThat(dailyTagPeriodHandler.timeToTag(LocalDateTime.of(2023, 1, 1, 0, 0)))
                .isEqualTo("20230101");
        assertThat(dailyTagPeriodHandler.tagToTime("20230101"))
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 0, 0));
        assertThat(dailyTagPeriodHandler.isAutoTag("20230101")).isTrue();
        assertThat(dailyTagPeriodHandler.isAutoTag("2023-01-01")).isFalse();
    }

    @Test
    public void testHourlyTagPeriodHandler() {
        TagPeriodHandler.HourlyTagPeriodHandler hourlyTagPeriodHandler =
                new TagPeriodHandler.HourlyTagPeriodHandler(WITH_DASHES);
        assertThat(hourlyTagPeriodHandler.timeToTag(LocalDateTime.of(2023, 1, 1, 1, 0)))
                .isEqualTo("2023-01-01 01");
        assertThat(hourlyTagPeriodHandler.tagToTime("2023-01-01 01"))
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 1, 0));
        assertThat(hourlyTagPeriodHandler.isAutoTag("2023-01-01 01")).isTrue();
        assertThat(hourlyTagPeriodHandler.isAutoTag("20230101 01")).isFalse();

        hourlyTagPeriodHandler = new TagPeriodHandler.HourlyTagPeriodHandler(WITHOUT_DASHES);
        assertThat(hourlyTagPeriodHandler.timeToTag(LocalDateTime.of(2023, 1, 1, 1, 0)))
                .isEqualTo("20230101 01");
        assertThat(hourlyTagPeriodHandler.tagToTime("20230101 01"))
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 1, 0));
        assertThat(hourlyTagPeriodHandler.isAutoTag("20230101 01")).isTrue();
        assertThat(hourlyTagPeriodHandler.isAutoTag("2023-01-01 01")).isFalse();

        hourlyTagPeriodHandler =
                new TagPeriodHandler.HourlyTagPeriodHandler(WITHOUT_DASHES_AND_SPACES);
        assertThat(hourlyTagPeriodHandler.timeToTag(LocalDateTime.of(2023, 1, 1, 1, 0)))
                .isEqualTo("2023010101");
        assertThat(hourlyTagPeriodHandler.tagToTime("2023010101"))
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 1, 0));
        assertThat(hourlyTagPeriodHandler.isAutoTag("2023010101")).isTrue();
        assertThat(hourlyTagPeriodHandler.isAutoTag("20230101 01")).isFalse();
    }

    @Test
    public void testPeriodDurationTagPeriodHandler() {
        TagPeriodHandler.PeriodDurationTagPeriodHandler periodDurationTagPeriodHandler =
                new TagPeriodHandler.PeriodDurationTagPeriodHandler(Duration.ofMinutes(5));
        assertThat(periodDurationTagPeriodHandler.timeToTag(LocalDateTime.of(2023, 1, 1, 1, 1)))
                .isEqualTo("202301010101");
        assertThat(periodDurationTagPeriodHandler.tagToTime("202301010101"))
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 1, 1));
        assertThat(periodDurationTagPeriodHandler.isAutoTag("202301010101")).isTrue();
        assertThat(periodDurationTagPeriodHandler.isAutoTag("20230101 0101")).isFalse();
    }
}
