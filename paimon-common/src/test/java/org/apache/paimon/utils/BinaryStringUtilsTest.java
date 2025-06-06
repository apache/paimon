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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.DateTimeException;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link BinaryStringUtils}.
 */
class BinaryStringUtilsTest {
    @ParameterizedTest
    @CsvSource({
            "0, 0, 0, 0", // Unix epoch
            "86400, 0, 86400000, 0", // One day in seconds
            "3600000, 3, 3600000, 0", // One hour in milliseconds
            "3600000000, 6, 3600000, 0", // One hour in microseconds
            "3600000000000, 9, 3600000, 0", // One hour in nanoseconds
            "1609459200123456789, 9, 1609459200123, 456789", // 2021-01-01 00:00:00.123456789 UTC
            "1609459200123456, 6, 1609459200123, 456000", // 2021-01-01 00:00:00.123456 UTC
            "1609459200123, 3, 1609459200123, 000000", // 2021-01-01 00:00:00 UTC
            "1609459200, 0, 1609459200000, 000000", // 2021-01-01 00:00:00 UTC
            "-1, 0, -1000, 0", // One second before epoch
            "-1000, 3, -1000, 0", // One second before epoch in milliseconds
            "-1000000, 6, -1000, 0", // One second before epoch in microseconds
            "-1000000000, 9, -1000, 0", // One second before epoch in nanoseconds
            // One second and one nanosecond before epoch in nanoseconds
            // The negative nanosecond gets flipped and the milliseconds decremented
            "-1000000001, 9, -1001, 999999",
            "-86400123456, 6, -86400124, 544000"
    })
    void testToTimestamp(
            String input, int precision, long expectedMillis, int expectedNanos) {
        BinaryString binaryInput = BinaryString.fromString(input);
        Timestamp result = BinaryStringUtils.toTimestamp(binaryInput, precision);

        assertThat(result.getMillisecond()).isEqualTo(expectedMillis);
        assertThat(result.getNanoOfMillisecond()).isEqualTo(expectedNanos);
    }

    void testUnsupportedPrecision() {
        BinaryString input = BinaryString.fromString("1609459200");

        assertThatThrownBy(() -> BinaryStringUtils.toTimestamp(input, 5))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unsupported precision: 5");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 5, 7, 8, 10, -1})
    void testInvalidPrecisions(int precision) {
        BinaryString input = BinaryString.fromString("1609459200");

        assertThatThrownBy(() -> BinaryStringUtils.toTimestamp(input, precision))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unsupported precision: " + precision);
    }

    @Test
    void testDateStringInput() {
        // Test with date string input - should delegate to DateTimeUtils.parseTimestampData
        BinaryString input = BinaryString.fromString("2021-01-01 12:30:45");
        Timestamp result = BinaryStringUtils.toTimestamp(input, 3);

        // Verify it's not null and has reasonable values
        assertThat(result).isNotNull();
        assertThat(result.getMillisecond()).isGreaterThan(0);
    }

    @Test
    void testDateOnlyStringInput() {
        // Test with date-only string input
        BinaryString input = BinaryString.fromString("2021-01-01");
        Timestamp result = BinaryStringUtils.toTimestamp(input, 3);

        assertThat(result).isNotNull();
        assertThat(result.getMillisecond()).isGreaterThan(0);
    }

    @Test
    void testInvalidStringInput() {
        // Test with invalid string input
        BinaryString input = BinaryString.fromString("invalid-date");

        assertThatThrownBy(() -> BinaryStringUtils.toTimestamp(input, 3))
                .isInstanceOf(DateTimeException.class);
    }


    @Test
    void testToTimestampWithTimeZone() {
        // Test the timezone variant of toTimestamp method
        BinaryString input = BinaryString.fromString("2021-01-01 12:30:45");
        TimeZone timeZone = TimeZone.getTimeZone("UTC");

        Timestamp result = BinaryStringUtils.toTimestamp(input, 3, timeZone);

        assertThat(result).isNotNull();
        assertThat(result.getMillisecond()).isGreaterThan(0);
    }

    @Test
    void testToTimestampWithDifferentTimeZones() {
        BinaryString input = BinaryString.fromString("2021-01-01 12:30:45");

        Timestamp utcResult =
                BinaryStringUtils.toTimestamp(input, 3, TimeZone.getTimeZone("UTC"));
        Timestamp estResult =
                BinaryStringUtils.toTimestamp(
                        input, 3, TimeZone.getTimeZone("America/New_York"));

        assertThat(utcResult).isNotNull();
        assertThat(estResult).isNotNull();
        // The results should be different due to timezone offset
        assertThat(utcResult.getMillisecond()).isNotEqualTo(estResult.getMillisecond());
    }
}
