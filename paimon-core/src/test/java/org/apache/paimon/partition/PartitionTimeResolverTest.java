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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChainPartitionProjector;
import org.apache.paimon.utils.ChainTableUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link PartitionTimeResolver}. */
public class PartitionTimeResolverTest {

    private TemporalAmount extractMinStep(
            String pattern, String formatter, String... partitionKeys) {
        return new PartitionTimeResolver(Arrays.asList(partitionKeys), pattern, formatter)
                .extractMinStep();
    }

    /** Extract a string value from a BinaryRow at the given position. */
    private static String getString(BinaryRow row, int pos) {
        return row.getString(pos).toString();
    }

    private static BinaryRow row(List<String> values) {
        BinaryRow row = new BinaryRow(values.size());
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < values.size(); i++) {
            writer.writeString(i, BinaryString.fromString(values.get(i)));
        }
        writer.complete();
        return row;
    }

    @Test
    public void testExtractMinStep() {
        assertThat(extractMinStep("$y$M$d$H$m$s", "yyyyMMddHHmmss", "y", "M", "d", "H", "m", "s"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("$y$M$d $H$m$s", "yyyyMMdd HHmmss", "y", "M", "d", "H", "m", "s"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(
                        extractMinStep(
                                "$y-$M-$d $H:$m:$s",
                                "yyyy-MM-dd HH:mm:ss",
                                "y",
                                "M",
                                "d",
                                "H",
                                "m",
                                "s"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(
                        extractMinStep(
                                "$y-$M-$d T $H:$m:$s",
                                "yyyy-MM-dd 'T' HH:mm:ss",
                                "y",
                                "M",
                                "d",
                                "H",
                                "m",
                                "s"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("$a", "yyyyMMddHHmmss", "a")).isEqualTo(Duration.ofSeconds(1));

        assertThat(
                        extractMinStep(
                                "$a $aaT$aaa $a4Z", "yyMM dd'T'HHmm ss'Z'", "a4", "aa", "a", "aaa"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("$a12$aaT$aaa00Z", "yyyyMMdd'T'HHmmss'Z'", "aa", "aaa", "a"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("$aT$a1$a200", "yyyyMMdd'T'HHmmss", "a", "a1", "a2"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("$aT$aa", "yyyyMMdd'T'HHmm", "a", "aa"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("$a", "yyyyMMdd'T'HHmmss", "a")).isEqualTo(Duration.ofSeconds(1));

        assertThat(
                        extractMinStep(
                                "$ab $c $d:$e:$f", "yyyyMM dd HH:mm:ss", "ab", "c", "d", "e", "f"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("$day $a:$b", "yyyyMMdd HH:mm", "day", "a", "b"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("$aa $a", "yyyy/MM/dd HH", "aa", "a"))
                .isEqualTo(Duration.ofHours(1));

        assertThat(extractMinStep("$a $b", "HH:mm:ss yyyyMMdd", "a", "b"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("12:$a $b", "HH:mm:ss yyyyMMdd", "b", "a"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("12:$a:01 $b", "HH:mm:ss yyyyMMdd", "a", "b"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("12:02:01 $b", "HH:mm:ss yyyyMMdd", "b"))
                .isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("$hour:00:00 $date", "HH:mm:ss yyyyMMdd", "date", "hour"))
                .isEqualTo(Duration.ofHours(1));
        assertThat(extractMinStep("00:00:00 $b", "HH:mm:ss yyyyMMdd", "b"))
                .isEqualTo(Duration.ofDays(1));
        assertThat(
                        extractMinStep(
                                "$hour_minute:01 $date",
                                "HH:mm:ss yyyyMMdd",
                                "hour_minute",
                                "date"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("12:$a $b", "HH:mm:ss yyMMdd", "b", "a"))
                .isEqualTo(Duration.ofSeconds(1));

        // Unused partition columns should not affect the extracted minimum step.
        assertThat(extractMinStep("$dt", "yyyy-MM-dd", "other", "dt"))
                .isEqualTo(Duration.ofDays(1));
        assertThat(
                        extractMinStep(
                                "$dt $hour:$minute:00",
                                "yyyy-MM-dd HH:mm:ss",
                                "region",
                                "dt",
                                "hour",
                                "minute"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(
                        extractMinStep(
                                "$hour:00:00 $date", "HH:mm:ss yyyyMMdd", "date", "extra", "hour"))
                .isEqualTo(Duration.ofHours(1));

        assertThat(extractMinStep("$a-01-$b", "yyyy-MM-dd", "a", "b"))
                .isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("$a-01", "yyyy-MM-dd", "a")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$y/$m/$d", "yyyy/MM/dd", "d", "y", "m"))
                .isEqualTo(Duration.ofDays(1));

        assertThat(extractMinStep("$a", "yyyyMMdd", "a")).isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("$a01", "yyyyMMdd", "a")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$a $aa", "yyyyMM dd", "a", "aa")).isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("202601$a", "yyyyMMdd", "a")).isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("2026$a01", "yyyyMMdd", "a")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$a1201", "yyyyMMdd", "a")).isEqualTo(Period.ofYears(1));
        assertThat(extractMinStep("$a01", "yyyyMMdd", "a")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$a1201", "yyyyMMdd", "a")).isEqualTo(Period.ofYears(1));

        assertThat(extractMinStep("$a01", "yyMMdd", "a")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$a1", "yyMd", "a")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$a1201", "yyMMdd", "a")).isEqualTo(Period.ofYears(1));
        assertThat(extractMinStep("$a-12-1", "yy-M-d", "a")).isEqualTo(Period.ofYears(1));
        assertThat(extractMinStep("$a $aa", "yyMM dd", "a", "aa")).isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("$a'", "yyMMdd''", "a")).isEqualTo(Duration.ofDays(1));

        assertThat(extractMinStep("$dt", "yyyy-DDD", "dt")).isEqualTo(Duration.ofDays(1));

        // 'k' = clock-hour-of-day
        assertThat(extractMinStep("$dt", "yyyyMMddkk", "dt")).isEqualTo(Duration.ofHours(1));
    }

    @Test
    public void testResolvePartitionValues() {
        Map<String, String> partitionValues =
                new PartitionTimeResolver(
                                Arrays.asList("dt", "hour"), "$dt $hour:00:00", "yyyyMMdd HH:mm:ss")
                        .resolvePartitionValues(LocalDateTime.of(2023, 1, 1, 12, 0, 0));
        assertEquals(ImmutableMap.of("dt", "20230101", "hour", "12"), partitionValues);

        partitionValues =
                new PartitionTimeResolver(Arrays.asList("dt", "hr"), "$dt $hr", "yyyyMMdd HH")
                        .resolvePartitionValues(LocalDateTime.of(2023, 1, 2, 3, 0, 0));
        assertEquals(ImmutableMap.of("dt", "20230102", "hr", "03"), partitionValues);

        partitionValues =
                new PartitionTimeResolver(Arrays.asList("dt"), "$dt", "yyyyMMdd")
                        .resolvePartitionValues(LocalDateTime.of(2023, 1, 1, 0, 0, 0));
        assertEquals(ImmutableMap.of("dt", "20230101"), partitionValues);

        partitionValues =
                new PartitionTimeResolver(Arrays.asList("dt", "t"), "$dtT$t", "yy-M-d'T'H:m:ss")
                        .resolvePartitionValues(LocalDateTime.of(2023, 12, 1, 11, 2, 3));
        assertEquals(ImmutableMap.of("dt", "23-12-1", "t", "11:2:03"), partitionValues);

        partitionValues =
                new PartitionTimeResolver(Arrays.asList("dt"), "$dt", "yy-MMM-d")
                        .resolvePartitionValues(LocalDateTime.of(2023, 12, 1, 11, 2, 3));
        assertEquals(ImmutableMap.of("dt", "23-Dec-1"), partitionValues);

        // Partition columns that are not referenced by the pattern should not appear in the result.
        partitionValues =
                new PartitionTimeResolver(Arrays.asList("other", "dt"), "$dt", "yyyy-MM-dd")
                        .resolvePartitionValues(LocalDateTime.of(2023, 1, 1, 0, 0, 0));
        assertEquals(ImmutableMap.of("dt", "2023-01-01"), partitionValues);

        partitionValues =
                new PartitionTimeResolver(
                                Arrays.asList("region", "dt", "hour"),
                                "$dt $hour:00:00",
                                "yyyy-MM-dd HH:mm:ss")
                        .resolvePartitionValues(LocalDateTime.of(2023, 1, 1, 10, 0, 0));
        assertEquals(ImmutableMap.of("dt", "2023-01-01", "hour", "10"), partitionValues);

        // Day-of-year is also a valid complete date.
        partitionValues =
                new PartitionTimeResolver(Arrays.asList("dt"), "$dt", "yyyy-DDD")
                        .resolvePartitionValues(LocalDateTime.of(2026, 8, 10, 15, 30, 0));
        assertEquals(ImmutableMap.of("dt", "2026-222"), partitionValues);

        // 'u' = year, same as 'y'
        partitionValues =
                new PartitionTimeResolver(Arrays.asList("dt"), "$dt", "uuuuMMdd")
                        .resolvePartitionValues(LocalDateTime.of(2023, 1, 20, 0, 0));
        assertEquals(ImmutableMap.of("dt", "20230120"), partitionValues);

        // 'L' = month-of-year, same as 'M'
        partitionValues =
                new PartitionTimeResolver(Arrays.asList("dt"), "$dt", "yyyy-L-dd")
                        .resolvePartitionValues(LocalDateTime.of(2023, 1, 20, 0, 0));
        assertEquals(ImmutableMap.of("dt", "2023-1-20"), partitionValues);

        // 'k' = clock-hour-of-day
        partitionValues =
                new PartitionTimeResolver(Arrays.asList("dt", "hour"), "$dt $hour", "yyyyMMdd kk")
                        .resolvePartitionValues(LocalDateTime.of(2023, 1, 1, 12, 0));
        assertEquals(ImmutableMap.of("dt", "20230101", "hour", "12"), partitionValues);
    }

    @Test
    public void testParsePartitionValuesWithHourMinuteGranularity() {
        // partition keys: (region, dt, hour_minute), chain keys: (dt, hour_minute)
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("dt", DataTypes.STRING().notNull())
                        .field("hour_minute", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 2);

        // Compare chain partition (dt, hour_minute) lexicographically
        RecordComparator chainComparator = (a, b) -> a.getString(1).compareTo(b.getString(1));

        Options opts = new Options();
        opts.set(CoreOptions.PARTITION_TIMESTAMP_PATTERN, "$dtT$hour_minute");
        opts.set(CoreOptions.PARTITION_TIMESTAMP_FORMATTER, "yyyyMMdd'T'HHmm");
        CoreOptions options = new CoreOptions(opts);

        BinaryRow begin = row(Lists.newArrayList("CN", "20260609", "1010"));
        BinaryRow end = row(Lists.newArrayList("CN", "20260609", "1015"));

        List<BinaryRow> deltas =
                ChainTableUtils.getDeltaPartitionsWithProjector(
                        begin, end, options, chainComparator, projector);

        assertThat(deltas).hasSize(5);
        for (BinaryRow delta : deltas) {
            assertThat(getString(delta, 0)).isEqualTo("CN");
            assertThat(getString(delta, 1)).isEqualTo("20260609");
        }
        assertThat(getString(deltas.get(0), 2)).isEqualTo("1011");
        assertThat(getString(deltas.get(1), 2)).isEqualTo("1012");
        assertThat(getString(deltas.get(2), 2)).isEqualTo("1013");
        assertThat(getString(deltas.get(3), 2)).isEqualTo("1014");
        assertThat(getString(deltas.get(4), 2)).isEqualTo("1015");
    }

    @Test
    public void testParsePartitionValuesWithSeparateHourAndMinute() {
        // partition keys: (region, dt, hour, minute), chain keys: (dt, hour, minute)
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("dt", DataTypes.STRING().notNull())
                        .field("hour", DataTypes.STRING().notNull())
                        .field("minute", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 3);

        // Compare chain partition (dt, hour, minute) lexicographically
        RecordComparator chainComparator = (a, b) -> a.getString(2).compareTo(b.getString(2));

        Options opts = new Options();
        opts.set(CoreOptions.PARTITION_TIMESTAMP_PATTERN, "$dtT$hour$minute00");
        opts.set(CoreOptions.PARTITION_TIMESTAMP_FORMATTER, "yyyyMMdd'T'HHmmss");
        CoreOptions options = new CoreOptions(opts);

        BinaryRow begin = row(Lists.newArrayList("CN", "20260609", "10", "10"));
        BinaryRow end = row(Lists.newArrayList("CN", "20260609", "10", "15"));

        List<BinaryRow> deltas =
                ChainTableUtils.getDeltaPartitionsWithProjector(
                        begin, end, options, chainComparator, projector);

        assertThat(deltas).hasSize(5);
        for (BinaryRow delta : deltas) {
            assertThat(getString(delta, 0)).isEqualTo("CN");
            assertThat(getString(delta, 1)).isEqualTo("20260609");
            assertThat(getString(delta, 2)).isEqualTo("10");
        }
        assertThat(getString(deltas.get(0), 3)).isEqualTo("11");
        assertThat(getString(deltas.get(1), 3)).isEqualTo("12");
        assertThat(getString(deltas.get(2), 3)).isEqualTo("13");
        assertThat(getString(deltas.get(3), 3)).isEqualTo("14");
        assertThat(getString(deltas.get(4), 3)).isEqualTo("15");
    }

    @Test
    public void testUnsupportedOrIncompleteGranularity() {
        // Incomplete date: year only / year-month / quarter.
        assertThatThrownBy(() -> extractMinStep("$dt", "yyyy", "dt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not specify a complete date");
        assertThatThrownBy(() -> extractMinStep("$dt", "yyyyMM", "dt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not specify a complete date");
        assertThatThrownBy(() -> extractMinStep("$dt", "yyyy-'Q'q", "dt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported formatter pattern letter");

        // Non-unique fields.
        assertThatThrownBy(() -> extractMinStep("$dt $ampm", "yyyyMMdd a", "dt", "ampm"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported formatter pattern letter");
        assertThatThrownBy(() -> extractMinStep("$dt", "yyyy-MM-W", "dt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported formatter pattern letter");

        // Week-based year.
        assertThatThrownBy(() -> extractMinStep("$dt", "YYYY-'W'ww-e", "dt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported formatter pattern letter");

        // Sub-second step.
        assertThatThrownBy(() -> extractMinStep("$dt", "yyyyMMddHHmmss.SSS", "dt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported formatter pattern letter");

        // Zone offset currently is not support
        assertThatThrownBy(() -> extractMinStep("$dt", "yyyyMMddHHmmssZ", "dt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported formatter pattern letter");
    }
}
