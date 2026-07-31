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

/** Test for {@link PartitionTimeResolvable} fallback behavior (unconfigured pattern/formatter). */
public class FallbackPartitionTimeResolverTest {

    @Test
    public void testDefault() {
        PartitionTimeResolvable resolver =
                PartitionTimeResolvable.create(Collections.emptyList(), null, null);
        assertThat(resolver.parsePartitionValues(Collections.singletonList("2023-01-01 20:08:08")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T20:08:08"));

        assertThat(
                        resolver.parsePartitionValues(
                                Collections.singletonList("2023-01-01 20:08:08.12")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T20:08:08.12"));

        assertThat(resolver.parsePartitionValues(Collections.singletonList("2023-1-1 20:08:08")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T20:08:08"));

        assertThat(resolver.parsePartitionValues(Collections.singletonList("2023-01-01")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));

        assertThat(resolver.parsePartitionValues(Collections.singletonList("2023-1-1")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));
    }

    @Test
    public void testPattern() {
        PartitionTimeResolvable resolver =
                PartitionTimeResolvable.create(
                        Arrays.asList("year", "month", "day"),
                        "$year-$month-$day 00:00:00.12",
                        null);
        assertThat(resolver.parsePartitionValues(Arrays.asList("2023", "01", "01")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00.12"));

        resolver =
                PartitionTimeResolvable.create(
                        Arrays.asList("year", "month", "day", "hour"),
                        "$year-$month-$day $hour:00:00",
                        null);
        assertThat(resolver.parsePartitionValues(Arrays.asList("2023", "01", "01", "01")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T01:00:00"));

        resolver = PartitionTimeResolvable.create(Arrays.asList("other", "dt"), "$dt", null);
        assertThat(resolver.parsePartitionValues(Arrays.asList("dummy", "2023-01-01")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));

        // One column name is a prefix of another ("t" vs "t1"). The longest match must be used
        // so that "$t1" is not broken by "$t" replacement.
        resolver = PartitionTimeResolvable.create(Arrays.asList("t", "t1"), "$t $t1", null);
        assertThat(resolver.parsePartitionValues(Arrays.asList("2023-01-01", "00:00:00")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));
    }

    @Test
    public void testFormatter() {
        PartitionTimeResolvable resolver =
                PartitionTimeResolvable.create(Collections.emptyList(), null, "yyyyMMdd");
        assertThat(resolver.parsePartitionValues(Collections.singletonList("20230101")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));
    }

    @Test
    public void testExtractNonDateFormattedPartition() {
        PartitionTimeResolvable resolver =
                PartitionTimeResolvable.create(Collections.singletonList("ds"), "$ds", "yyyyMMdd");
        assertThatThrownBy(
                        () -> resolver.parsePartitionValues(Collections.singletonList("unknown")))
                .satisfies(
                        PaimonAssertions.anyCauseMatches(
                                DateTimeParseException.class,
                                "Text 'unknown' could not be parsed at index 0"));
    }
}
