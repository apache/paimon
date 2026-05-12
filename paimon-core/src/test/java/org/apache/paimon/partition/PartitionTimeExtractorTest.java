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

package org.apache.paimon.partition;

import org.apache.paimon.testutils.assertj.PaimonAssertions;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PartitionTimeExtractor}. */
public class PartitionTimeExtractorTest {

    @Test
    public void testDefault() {
        PartitionTimeExtractor extractor = new PartitionTimeExtractor(null, null);
        assertThat(
                        extractor.extract(
                                Collections.emptyList(),
                                Collections.singletonList("2023-01-01 20:08:08")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T20:08:08"));

        assertThat(
                        extractor.extract(
                                Collections.emptyList(),
                                Collections.singletonList("2023-1-1 20:08:08")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T20:08:08"));

        assertThat(
                        extractor.extract(
                                Collections.emptyList(), Collections.singletonList("2023-01-01")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));

        assertThat(
                        extractor.extract(
                                Collections.emptyList(), Collections.singletonList("2023-1-1")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));
    }

    @Test
    public void testPattern() {
        PartitionTimeExtractor extractor =
                new PartitionTimeExtractor("$year-$month-$day 00:00:00", null);
        assertThat(
                        extractor.extract(
                                Arrays.asList("year", "month", "day"),
                                Arrays.asList("2023", "01", "01")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));

        extractor = new PartitionTimeExtractor("$year-$month-$day $hour:00:00", null);
        assertThat(
                        extractor.extract(
                                Arrays.asList("year", "month", "day", "hour"),
                                Arrays.asList("2023", "01", "01", "01")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T01:00:00"));

        extractor = new PartitionTimeExtractor("$dt", null);
        assertThat(
                        extractor.extract(
                                Arrays.asList("other", "dt"), Arrays.asList("dummy", "2023-01-01")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));
    }

    @Test
    public void testFormatter() {
        PartitionTimeExtractor extractor = new PartitionTimeExtractor(null, "yyyyMMdd");
        assertThat(
                        extractor.extract(
                                Collections.emptyList(), Collections.singletonList("20230101")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));
    }

    @Test
    public void testExtractNonDateFormattedPartition() {
        PartitionTimeExtractor extractor = new PartitionTimeExtractor("$ds", "yyyyMMdd");
        assertThatThrownBy(
                        () ->
                                extractor.extract(
                                        Collections.singletonList("ds"),
                                        Collections.singletonList("unknown")))
                .satisfies(
                        PaimonAssertions.anyCauseMatches(
                                DateTimeParseException.class,
                                "Text 'unknown' could not be parsed at index 0"));
    }

    @Test
    public void testExtractFromIntegerDateType() {
        // Test INTEGER type representing days since epoch (DATE type in Paimon)
        // 2025-10-10 = 20371 days since 1970-01-01
        PartitionTimeExtractor extractor = new PartitionTimeExtractor(null, null);
        assertThat(
                        extractor.extract(
                                Collections.singletonList("order_date"),
                                Collections.singletonList(20371)))
                .isEqualTo(LocalDateTime.parse("2025-10-10T00:00:00"));

        // Test with pattern
        extractor = new PartitionTimeExtractor("$order_date", "yyyy-MM-dd");
        assertThat(
                        extractor.extract(
                                Collections.singletonList("order_date"),
                                Collections.singletonList(20371)))
                .isEqualTo(LocalDateTime.parse("2025-10-10T00:00:00"));
    }

    @Test
    public void testExtractFromLocalDateType() {
        // Test java.time.LocalDate type
        PartitionTimeExtractor extractor = new PartitionTimeExtractor(null, null);
        assertThat(
                        extractor.extract(
                                Collections.singletonList("dt"),
                                Collections.singletonList(java.time.LocalDate.of(2023, 1, 15))))
                .isEqualTo(LocalDateTime.parse("2023-01-15T00:00:00"));
    }

    @Test
    public void testExtractFromLocalDateTimeType() {
        // Test java.time.LocalDateTime type
        PartitionTimeExtractor extractor = new PartitionTimeExtractor(null, null);
        assertThat(
                        extractor.extract(
                                Collections.singletonList("ts"),
                                Collections.singletonList(
                                        java.time.LocalDateTime.of(2023, 1, 15, 10, 30, 45))))
                .isEqualTo(LocalDateTime.parse("2023-01-15T10:30:45"));
    }

    @Test
    public void testExtractFromMultiplePartitionWithTypedDate() {
        // Test pattern with multiple partitions, one of which is DATE type
        PartitionTimeExtractor extractor =
                new PartitionTimeExtractor("$year-$month-$day 00:00:00", null);
        assertThat(
                        extractor.extract(
                                Arrays.asList("year", "month", "day"),
                                Arrays.asList("2023", "01", "15")))
                .isEqualTo(LocalDateTime.parse("2023-01-15T00:00:00"));
    }
}
