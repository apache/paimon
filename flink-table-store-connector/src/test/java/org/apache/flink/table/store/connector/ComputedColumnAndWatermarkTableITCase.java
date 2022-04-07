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

package org.apache.flink.table.store.connector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.store.log.LogOptions;

import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.rates;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;

/** Table store IT case when the managed table has computed column and watermark spec. */
public class ComputedColumnAndWatermarkTableITCase extends ReadWriteTableTestBase {

    @Test
    public void testBatchSelectComputedColumn() throws Exception {
        // input is rates()
        collectAndCheckUnderSameEnv(
                false,
                false,
                true,
                Collections.emptyList(), // partition
                Collections.emptyList(), // pk
                Collections.singletonList(Tuple2.of("capital_currency", "UPPER(currency)")),
                null,
                true,
                Collections.emptyMap(),
                null,
                Collections.singletonList("capital_currency"),
                rates().stream()
                        .map(
                                row ->
                                        changelogRow(
                                                row.getKind().shortString(),
                                                ((String) row.getField(0)).toUpperCase()))
                        .collect(Collectors.toList()));

        // input is rates()
        collectAndCheckUnderSameEnv(
                false,
                false,
                true,
                Collections.emptyList(), // partition
                Collections.singletonList("currency"), // pk
                Collections.singletonList(Tuple2.of("capital_currency", "LOWER(currency)")),
                null,
                true,
                Collections.emptyMap(),
                null,
                Collections.singletonList("capital_currency"),
                Arrays.asList(
                        changelogRow("+I", "us dollar"),
                        changelogRow("+I", "yen"),
                        changelogRow("+I", "euro")));

        // input is hourlyRates()
        collectAndCheckUnderSameEnv(
                false,
                false,
                true,
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("currency", "dt", "hh"), // pk
                Collections.singletonList(Tuple2.of("dth", "dt || ' ' || hh")),
                null,
                true,
                Collections.emptyMap(),
                "dth = '2022-01-02 12'",
                Collections.singletonList("dth"),
                Collections.singletonList(changelogRow("+I", "2022-01-02 12")));

        // test proctime
        collectAndCheckUnderSameEnv(
                false,
                false,
                true,
                Collections.emptyList(), // partition
                Collections.singletonList("currency"), // pk
                Collections.singletonList(Tuple2.of("ptime", "PROCTIME()")),
                null,
                true,
                Collections.emptyMap(),
                "currency = 'US Dollar'",
                Collections.singletonList("CHAR_LENGTH(DATE_FORMAT(ptime, 'yyyy-MM-dd HH:mm'))"),
                Collections.singletonList(changelogRow("+I", 16)));
    }

    @Test
    public void testStreamingSelectComputedColumn() throws Exception {
        // input is ratesChangelogWithUB()
        collectAndCheckUnderSameEnv(
                        true,
                        true,
                        false,
                        Collections.emptyList(), // partition
                        Collections.emptyList(), // pk
                        Arrays.asList(
                                Tuple2.of("capital_currency", "UPPER(currency)"),
                                Tuple2.of("ptime", "PROCTIME()")),
                        null,
                        true,
                        Collections.emptyMap(),
                        "currency IS NULL",
                        Arrays.asList(
                                "capital_currency",
                                "CHAR_LENGTH(DATE_FORMAT(ptime, 'yyyy-MM-dd HH:mm'))"),
                        Collections.singletonList(changelogRow("+I", null, 16)))
                .f1
                .close();

        // input is dailyExchangeRatesChangelogWithoutUB()
        collectAndCheckUnderSameEnv(
                        true,
                        true,
                        false,
                        Collections.singletonList("dt"), // partition
                        Arrays.asList("from_currency", "to_currency", "dt"), // pk
                        Arrays.asList(
                                Tuple2.of(
                                        "corrected_rate_by_to_currency",
                                        "COALESCE(rate_by_to_currency, 1)"),
                                Tuple2.of("ptime", "PROCTIME()")),
                        null,
                        false,
                        Collections.singletonMap(
                                LOG_PREFIX + LogOptions.SCAN.key(),
                                LogOptions.LogStartupMode.LATEST.name().toLowerCase()),
                        "rate_by_to_currency IS NULL",
                        Arrays.asList(
                                "corrected_rate_by_to_currency",
                                "CHAR_LENGTH(DATE_FORMAT(ptime, 'yyyy-MM-dd HH:mm'))"),
                        Collections.singletonList(changelogRow("+I", 1d, 16)))
                .f1
                .close();
    }

    @Test
    public void testStreamingSelectWithWatermark() throws Exception {
        String lateEventFilter = "CURRENT_WATERMARK(ts) IS NULL OR ts > CURRENT_WATERMARK(ts)";
        // input is ratesWithTimestamp()
        collectAndCheckUnderSameEnv(
                        true,
                        true,
                        true,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        WatermarkSpec.of("ts", "ts - INTERVAL '3' YEAR"),
                        false,
                        Collections.singletonMap(
                                LOG_PREFIX + LogOptions.SCAN.key(),
                                LogOptions.LogStartupMode.LATEST.name().toLowerCase()),
                        lateEventFilter,
                        Collections.emptyList(),
                        Collections.singletonList(
                                changelogRow(
                                        "+I",
                                        "US Dollar",
                                        102L,
                                        LocalDateTime.parse("1990-04-07T10:00:11.120"))))
                .f1
                .close();

        // query both event time and processing time
        collectAndCheckUnderSameEnv(
                        true,
                        true,
                        true,
                        Collections.emptyList(), // partition
                        Collections.emptyList(), // pk
                        Collections.singletonList(
                                Tuple2.of("ptime", "PROCTIME()")), // computed column
                        WatermarkSpec.of("ts", "ts - INTERVAL '3' YEAR"),
                        false,
                        Collections.singletonMap(
                                LOG_PREFIX + LogOptions.SCAN.key(),
                                LogOptions.LogStartupMode.LATEST.name().toLowerCase()),
                        lateEventFilter,
                        Arrays.asList(
                                "currency",
                                "rate",
                                "ts",
                                "CHAR_LENGTH(DATE_FORMAT(ptime, 'yyyy-MM-dd HH:mm'))"),
                        Collections.singletonList(
                                changelogRow(
                                        "+I",
                                        "US Dollar",
                                        102L,
                                        LocalDateTime.parse("1990-04-07T10:00:11.120"),
                                        16)))
                .f1
                .close();
    }
}
