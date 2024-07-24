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
                                Collections.singletonList("2023-01-01 20:08:08"),
                                true))
                .isEqualTo(LocalDateTime.parse("2023-01-01T20:08:08"));

        assertThat(
                        extractor.extract(
                                Collections.emptyList(),
                                Collections.singletonList("2023-1-1 20:08:08"),
                                true))
                .isEqualTo(LocalDateTime.parse("2023-01-01T20:08:08"));

        assertThat(
                        extractor.extract(
                                Collections.emptyList(),
                                Collections.singletonList("2023-01-01"),
                                true))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));

        assertThat(
                        extractor.extract(
                                Collections.emptyList(),
                                Collections.singletonList("2023-1-1"),
                                true))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));
    }

    @Test
    public void testPattern() {
        PartitionTimeExtractor extractor =
                new PartitionTimeExtractor("$year-$month-$day 00:00:00", null);
        assertThat(
                        extractor.extract(
                                Arrays.asList("year", "month", "day"),
                                Arrays.asList("2023", "01", "01"),
                                true))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));

        extractor = new PartitionTimeExtractor("$year-$month-$day $hour:00:00", null);
        assertThat(
                        extractor.extract(
                                Arrays.asList("year", "month", "day", "hour"),
                                Arrays.asList("2023", "01", "01", "01"),
                                true))
                .isEqualTo(LocalDateTime.parse("2023-01-01T01:00:00"));

        extractor = new PartitionTimeExtractor("$dt", null);
        assertThat(
                        extractor.extract(
                                Arrays.asList("other", "dt"),
                                Arrays.asList("dummy", "2023-01-01"),
                                true))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));
    }

    @Test
    public void testFormatter() {
        PartitionTimeExtractor extractor = new PartitionTimeExtractor(null, "yyyyMMdd");
        assertThat(
                        extractor.extract(
                                Collections.emptyList(),
                                Collections.singletonList("20230101"),
                                true))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));
    }

    @Test
    public void testExtractNonDateFormattedPartition() {
        PartitionTimeExtractor extractor = new PartitionTimeExtractor("$ds", "yyyyMMdd");
        assertThat(
                        extractor.extract(
                                Collections.singletonList("ds"),
                                Collections.singletonList("unknown"),
                                true))
                .isNull();
        assertThatThrownBy(
                        () ->
                                extractor.extract(
                                        Collections.singletonList("ds"),
                                        Collections.singletonList("unknown"),
                                        false))
                .satisfies(
                        PaimonAssertions.anyCauseMatches(
                                RuntimeException.class,
                                "Can't extract datetime from partition 'ds:unknown' with formatter 'yyyyMMdd' and pattern '$ds'."));
    }
}
