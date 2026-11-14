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
    public void testPatternWithPrefixVariableNames() {
        PartitionTimeExtractor extractor =
                new PartitionTimeExtractor("$dt1-$dt-$dt2 00:00:00", null);
        assertThat(
                        extractor.extract(
                                Arrays.asList("dt1", "dt", "dt2"),
                                Arrays.asList("2023", "01", "02")))
                .isEqualTo(LocalDateTime.parse("2023-01-02T00:00:00"));

        extractor = new PartitionTimeExtractor("$t $t2:$t3:$t4", null);
        assertThat(
                        extractor.extract(
                                Arrays.asList("t", "t4", "t2", "t3"),
                                Arrays.asList("2023-01-01", "03", "01", "02")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T01:02:03"));

        extractor = new PartitionTimeExtractor("$dt1$dt", "MMyyyydd");
        assertThat(extractor.extract(Arrays.asList("dt", "dt1"), Arrays.asList("01", "022023")))
                .isEqualTo(LocalDateTime.parse("2023-02-01T00:00:00"));
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
    public void testEpochDayCastWithHourAndFormatter() {
        PartitionTimeExtractor extractor =
                new PartitionTimeExtractor("$pd::epoch_day $ph:00:00", "yyyy-MM-dd H:mm:ss");
        assertThat(extractor.extract(Arrays.asList("pd", "ph"), Arrays.asList(20477, 0)))
                .isEqualTo(LocalDateTime.parse("2026-01-24T00:00:00"));
        assertThat(extractor.extract(Arrays.asList("pd", "ph"), Arrays.asList(20477L, "00")))
                .isEqualTo(LocalDateTime.parse("2026-01-24T00:00:00"));
        assertThat(extractor.extract(Arrays.asList("pd", "ph"), Arrays.asList(20478, 23)))
                .isEqualTo(LocalDateTime.parse("2026-01-25T23:00:00"));
    }

    @Test
    public void testEpochDayCastWithHourWithoutFormatter() {
        PartitionTimeExtractor extractor =
                new PartitionTimeExtractor("$pd::epoch_day $ph:00:00", null);
        assertThat(extractor.extract(Arrays.asList("pd", "ph"), Arrays.asList(20477, 0)))
                .isEqualTo(LocalDateTime.parse("2026-01-24T00:00:00"));
        assertThat(extractor.extract(Arrays.asList("pd", "ph"), Arrays.asList(20479, 1)))
                .isEqualTo(LocalDateTime.parse("2026-01-26T01:00:00"));
    }

    @Test
    public void testEpochDayCastAlone() {
        PartitionTimeExtractor extractor = new PartitionTimeExtractor("$pd::epoch_day", null);
        assertThat(
                        extractor.extract(
                                Collections.singletonList("pd"), Collections.singletonList(20477)))
                .isEqualTo(LocalDateTime.parse("2026-01-24T00:00:00"));
        assertThat(
                        extractor.extract(
                                Collections.singletonList("pd"), Collections.singletonList(-1)))
                .isEqualTo(LocalDateTime.parse("1969-12-31T00:00:00"));
    }

    @Test
    public void testEpochDayCastKeepsFormattedDate() {
        // A DATE value read from a partition path is already a date string.
        PartitionTimeExtractor extractor =
                new PartitionTimeExtractor("$pd::epoch_day $ph:00:00", null);
        assertThat(extractor.extract(Arrays.asList("pd", "ph"), Arrays.asList("2026-01-24", "5")))
                .isEqualTo(LocalDateTime.parse("2026-01-24T05:00:00"));
    }

    @Test
    public void testEpochDayCastWithPrefixVariableNames() {
        PartitionTimeExtractor extractor =
                new PartitionTimeExtractor("$dt::epoch_day $dt1:00:00", null);
        assertThat(extractor.extract(Arrays.asList("dt", "dt1"), Arrays.asList(20477, 7)))
                .isEqualTo(LocalDateTime.parse("2026-01-24T07:00:00"));
    }

    @Test
    public void testIntegerWithoutEpochDayCastIsUnchanged() {
        PartitionTimeExtractor extractor = new PartitionTimeExtractor("$dt", "yyyyMMdd");
        assertThat(
                        extractor.extract(
                                Collections.singletonList("dt"),
                                Collections.singletonList(20230115)))
                .isEqualTo(LocalDateTime.parse("2023-01-15T00:00:00"));
    }
}
