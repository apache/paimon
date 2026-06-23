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

import org.junit.Test;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChainPartitionPatternResolver}. */
public class ChainPartitionPatternResolverTest {

    private TemporalAmount extractMinStep(
            String pattern, String formatter, String... partitionColumns) {
        return new ChainPartitionPatternResolver(
                        Arrays.asList(partitionColumns), pattern, formatter)
                .extractMinStep();
    }

    @Test
    public void testVariablePatternsWithDuration() {
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
    public void testVariablePatternsWithPeriod() {
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
}
