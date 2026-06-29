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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionTimeResolver;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link PartitionTimeResolver}. */
public class PartitionTimeResolverTest {

    private TemporalAmount extractMinStep(
            String pattern, String formatter, String... partitionColumns) {
        return new PartitionTimeResolver(Arrays.asList(partitionColumns), pattern, formatter)
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
    public void testExtractMinStepWithDuration() {
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

        assertThat(extractMinStep("$a$aaT$aaa$a4Z", "yyMMdd'T'HHmmss'Z'", "a", "aa", "aaa", "a4"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("$a12$aaT$aaa00Z", "yyyyMMdd'T'HHmmss'Z'", "a", "aa", "aaa"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("$aT$a1$a200", "yyyyMMdd'T'HHmmss", "a", "a1", "a2"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("$aT$aa", "yyyyMMdd'T'HHmm", "a", "aa"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("$a", "yyyyMMdd'T'HHmmss", "a")).isEqualTo(Duration.ofSeconds(1));

        assertThat(extractMinStep("$ab$c $d:$e:$f", "yyyyMMdd HH:mm:ss", "ab", "c", "d", "e", "f"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("$day $a:$b", "yyyyMMdd HH:mm", "day", "a", "b"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("$aa$a", "yyyy/MM/ddHH", "aa", "a"))
                .isEqualTo(Duration.ofHours(1));

        assertThat(extractMinStep("$a $b", "HH:mm:ss yyyyMMdd", "a", "b"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("12:$a $b", "HH:mm:ss yyyyMMdd", "a", "b"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("12:$a:01 $b", "HH:mm:ss yyyyMMdd", "a", "b"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("12:02:01 $b", "HH:mm:ss yyyyMMdd", "b"))
                .isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("$hour:00:00 $date", "HH:mm:ss yyyyMMdd", "hour", "date"))
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
        assertThat(extractMinStep("12:$a $b", "HH:mm:ss yyMMdd", "a", "b"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("12$b", "HHmmss", "b")).isEqualTo(Duration.ofSeconds(1));

        assertThat(extractMinStep("$a", "yyyyMMddhh", "a")).isEqualTo(Duration.ofHours(1));
        assertThat(extractMinStep("$date $time", "yyyyMMdd hh", "date", "time"))
                .isEqualTo(Duration.ofHours(1));
        assertThat(extractMinStep("$a$b", "yyyyMMddhh", "a", "b")).isEqualTo(Duration.ofHours(1));
    }

    @Test
    public void testExtractMinStepWithPeriod() {
        assertThat(extractMinStep("$a-01-$b", "yyyy-MM-dd", "a", "b"))
                .isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("$a-01", "yyyy-MM-dd", "a")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$y/$m/$d", "yyyy/MM/dd", "y", "m", "d"))
                .isEqualTo(Duration.ofDays(1));

        assertThat(extractMinStep("$a", "yyyyMMdd", "a")).isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("$a01", "yyyyMMdd", "a")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$a$aa", "yyyyMMdd", "a", "aa")).isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("202601$a", "yyyyMMdd", "a")).isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("2026$a01", "yyyyMMdd", "a")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$a1201", "yyyyMMdd", "a")).isEqualTo(Period.ofYears(1));
        assertThat(extractMinStep("$a01", "yyyyMMdd", "a")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$a1201", "yyyyMMdd", "a")).isEqualTo(Period.ofYears(1));

        assertThat(extractMinStep("$a01", "yyMMdd", "a")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$a1201", "yyMMdd", "a")).isEqualTo(Period.ofYears(1));
        assertThat(extractMinStep("$a$aa", "yyMMdd", "a", "aa")).isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("$a'", "yyMMdd''", "a")).isEqualTo(Duration.ofDays(1));
    }

    @Test
    public void testResolvePartitionValues() {
        LinkedHashMap<String, String> partitionValues =
                new PartitionTimeResolver(
                                Arrays.asList("dt", "hour"), "$dt $hour:00:00", "yyyyMMdd HH:mm:ss")
                        .resolvePartitionValues(LocalDateTime.of(2023, 1, 1, 12, 0, 0));
        assertEquals(
                new LinkedHashMap<String, String>() {
                    {
                        put("dt", "20230101");
                        put("hour", "12");
                    }
                },
                partitionValues);

        partitionValues =
                new PartitionTimeResolver(Arrays.asList("dt"), "$dt", "yyyyMMdd")
                        .resolvePartitionValues(LocalDateTime.of(2023, 1, 1, 0, 0, 0));
        assertEquals(
                new LinkedHashMap<String, String>() {
                    {
                        put("dt", "20230101");
                    }
                },
                partitionValues);
    }

    @Test
    public void testParsePartitionValues() {
        PartitionTimeResolver resolver =
                new PartitionTimeResolver(
                        Arrays.asList("a", "aa", "aaa"),
                        "$a-$aa-$aaa 00:00:00",
                        "yyyy-MM-dd HH:mm:ss");
        assertThat(resolver.parsePartitionValues(Arrays.asList("2023", "01", "01")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));

        resolver =
                new PartitionTimeResolver(
                        Arrays.asList("aa", "a", "aaa"), "$aa$a$aaaT000000", "yyyyMMdd'T'HHmmss");
        assertThat(resolver.parsePartitionValues(Arrays.asList("2023", "01", "01")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));

        resolver =
                new PartitionTimeResolver(Arrays.asList("aa", "a", "aaa"), "$aa$a$aaa", "yyyyMMdd");
        assertThat(resolver.parsePartitionValues(Arrays.asList("2023", "01", "01")))
                .isEqualTo(LocalDateTime.parse("2023-01-01T00:00:00"));
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
}
