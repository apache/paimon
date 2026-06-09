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

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChainPartitionStepExtractor}. */
public class ChainPartitionStepExtractorTest {
    private TemporalAmount extractMinStep(@Nullable String pattern, @Nullable String formatter) {
        return new ChainPartitionStepExtractor(pattern, formatter).extractMinStep();
    }

    @Test
    public void testVariablePatternsWithDuration() {
        assertThat(extractMinStep("$ab$c $d:$e:$f", "yyyyMMdd HH:mm:ss"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("$day $a:$b", "yyyyMMdd HH:mm")).isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("$day$a", "yyyy/MM/ddHH")).isEqualTo(Duration.ofHours(1));

        assertThat(extractMinStep("$a $b", "HH:mm:ss yyyyMMdd")).isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("12:$a $b", "HH:mm:ss yyyyMMdd"))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("12:$a:01 $b", "HH:mm:ss yyyyMMdd"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("12:02:01 $b", "HH:mm:ss yyyyMMdd"))
                .isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("$hour:00:00 $date", "HH:mm:ss yyyyMMdd"))
                .isEqualTo(Duration.ofHours(1));
        assertThat(extractMinStep("00:00:00 $b", "HH:mm:ss yyyyMMdd"))
                .isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("$hour_minute:01 $date", "HH:mm:ss yyyyMMdd"))
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(extractMinStep("12:$a $b", "HH:mm:ss yyMMdd")).isEqualTo(Duration.ofSeconds(1));
        assertThat(extractMinStep("12$b", "HHmmss")).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    public void testVariablePatternsWithPeriod() {
        assertThat(extractMinStep("$a$b", "yyyyMMdd")).isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("$a01", "yyyyMMdd")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("202601$a", "yyyyMMdd")).isEqualTo(Duration.ofDays(1));
        assertThat(extractMinStep("2026$a01", "yyyyMMdd")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$a1201", "yyyyMMdd")).isEqualTo(Period.ofYears(1));
        assertThat(extractMinStep("$year_month01", "yyyyMMdd")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$year1201", "yyyyMMdd")).isEqualTo(Period.ofYears(1));

        assertThat(extractMinStep("$a01", "yyMMdd")).isEqualTo(Period.ofMonths(1));
        assertThat(extractMinStep("$a1201", "yyMMdd")).isEqualTo(Period.ofYears(1));
        assertThat(extractMinStep("$year_month$day", "yyMMdd")).isEqualTo(Duration.ofDays(1));
    }
}
