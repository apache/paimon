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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.planner.factories.TestValuesTableFactory.registerData;
import static org.apache.flink.table.store.connector.ShowCreateUtil.buildShowCreateTable;
import static org.apache.flink.table.store.connector.TableStoreTestBase.createResolvedTable;

/** Data util for {@link ReadWriteTableITCase}. */
public class ReadWriteTableTestUtil {

    public static String prepareHelperSourceWithInsertOnlyRecords(
            String sourceTable,
            List<String> partitionKeys,
            List<String> primaryKeys,
            boolean assignWatermark) {
        return prepareHelperSourceRecords(
                RuntimeExecutionMode.BATCH,
                sourceTable,
                partitionKeys,
                primaryKeys,
                assignWatermark);
    }

    public static String prepareHelperSourceWithChangelogRecords(
            String sourceTable,
            List<String> partitionKeys,
            List<String> primaryKeys,
            boolean assignWatermark) {
        return prepareHelperSourceRecords(
                RuntimeExecutionMode.STREAMING,
                sourceTable,
                partitionKeys,
                primaryKeys,
                assignWatermark);
    }

    /**
     * Prepare helper source table ddl according to different input parameter.
     *
     * <pre> E.g. pk with partition
     *   {@code
     *   CREATE TABLE source_table (
     *     currency STRING,
     *     rate BIGINT,
     *     dt STRING) PARTITIONED BY (dt)
     *    WITH (
     *      'connector' = 'values',
     *      'bounded' = executionMode == RuntimeExecutionMode.BATCH,
     *      'partition-list' = '...'
     *     )
     *   }
     * </pre>
     *
     * @param executionMode is used to calculate {@code bounded}
     * @param sourceTable source table name
     * @param partitionKeys is used to calculate {@code partition-list}
     * @param primaryKeys
     * @param assignWatermark
     * @return helper source ddl
     */
    private static String prepareHelperSourceRecords(
            RuntimeExecutionMode executionMode,
            String sourceTable,
            List<String> partitionKeys,
            List<String> primaryKeys,
            boolean assignWatermark) {
        Map<String, String> tableOptions = new HashMap<>();
        boolean bounded = executionMode == RuntimeExecutionMode.BATCH;
        String changelogMode = bounded ? "I" : primaryKeys.size() > 0 ? "I,UA,D" : "I,UA,UB,D";
        List<String> exclusivePk = new ArrayList<>(primaryKeys);
        exclusivePk.removeAll(partitionKeys);
        Tuple2<List<Row>, String> tuple =
                prepareHelperSource(
                        bounded, exclusivePk.size(), partitionKeys.size(), assignWatermark);
        String dataId = registerData(tuple.f0);
        String partitionList = tuple.f1;
        tableOptions.put("connector", TestValuesTableFactory.IDENTIFIER);
        tableOptions.put("bounded", String.valueOf(bounded));
        tableOptions.put("disable-lookup", String.valueOf(true));
        tableOptions.put("changelog-mode", changelogMode);
        tableOptions.put("data-id", dataId);
        if (partitionList != null) {
            tableOptions.put("partition-list", partitionList);
        }
        ResolvedCatalogTable table;
        if (assignWatermark) {
            table = prepareRateSourceWithTimestamp(tableOptions);
        } else {
            if (exclusivePk.size() > 1) {
                table = prepareExchangeRateSource(partitionKeys, primaryKeys, tableOptions);
            } else {
                table = prepareRateSource(partitionKeys, primaryKeys, tableOptions);
            }
        }
        return buildShowCreateTable(
                table,
                ObjectIdentifier.of("default_catalog", "default_database", sourceTable),
                true);
    }

    private static Tuple2<List<Row>, String> prepareHelperSource(
            boolean bounded, int pkSize, int partitionSize, boolean assignWatermark) {
        List<Row> input;
        String partitionList = null;
        if (assignWatermark) {
            return Tuple2.of(ratesWithTimestamp(), null);
        }
        if (pkSize == 2) {
            if (bounded) {
                if (partitionSize == 0) {
                    // multi-pk, non-partitioned
                    input = exchangeRates();
                } else if (partitionSize == 1) {
                    // multi-pk, single-partitioned
                    input = dailyExchangeRates().f0;
                    partitionList = dailyExchangeRates().f1;
                } else {
                    // multi-pk, multi-partitioned
                    input = hourlyExchangeRates().f0;
                    partitionList = hourlyExchangeRates().f1;
                }
            } else {
                if (partitionSize == 0) {
                    // multi-pk, non-partitioned
                    input = exchangeRatesChangelogWithoutUB();
                } else if (partitionSize == 1) {
                    // multi-pk, single-partitioned
                    input = dailyExchangeRatesChangelogWithoutUB().f0;
                    partitionList = dailyExchangeRatesChangelogWithoutUB().f1;
                } else {
                    // multi-pk, multi-partitioned
                    input = hourlyExchangeRatesChangelogWithoutUB().f0;
                    partitionList = hourlyExchangeRatesChangelogWithoutUB().f1;
                }
            }
        } else if (pkSize == 1) {
            if (bounded) {
                if (partitionSize == 0) {
                    // single-pk, non-partitioned
                    input = rates();
                } else if (partitionSize == 1) {
                    // single-pk, single-partitioned
                    input = dailyRates().f0;
                    partitionList = dailyRates().f1;
                } else {
                    // single-pk, multi-partitioned
                    input = hourlyRates().f0;
                    partitionList = hourlyRates().f1;
                }
            } else {
                if (partitionSize == 0) {
                    // single-pk, non-partitioned
                    input = ratesChangelogWithoutUB();
                } else if (partitionSize == 1) {
                    // single-pk, single-partitioned
                    input = dailyRatesChangelogWithoutUB().f0;
                    partitionList = dailyRatesChangelogWithoutUB().f1;
                } else {
                    // single-pk, multi-partitioned
                    input = hourlyRatesChangelogWithoutUB().f0;
                    partitionList = hourlyRatesChangelogWithoutUB().f1;
                }
            }
        } else {
            if (bounded) {
                if (partitionSize == 0) {
                    // without pk, non-partitioned
                    input = rates();
                } else if (partitionSize == 1) {
                    // without, single-partitioned
                    input = dailyRates().f0;
                    partitionList = dailyRates().f1;
                } else {
                    // without pk, multi-partitioned
                    input = hourlyRates().f0;
                    partitionList = hourlyRates().f1;
                }
            } else {
                if (partitionSize == 0) {
                    // without pk, non-partitioned
                    input = ratesChangelogWithUB();
                } else if (partitionSize == 1) {
                    // without pk, single-partitioned
                    input = dailyRatesChangelogWithUB().f0;
                    partitionList = dailyRatesChangelogWithUB().f1;
                } else {
                    // without pk, multi-partitioned
                    input = hourlyRatesChangelogWithUB().f0;
                    partitionList = hourlyRatesChangelogWithUB().f1;
                }
            }
        }
        return Tuple2.of(input, partitionList);
    }

    private static ResolvedCatalogTable prepareRateSource(
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> tableOptions) {
        List<Column> resolvedColumns = new ArrayList<>();
        resolvedColumns.add(Column.physical("currency", STRING()));
        resolvedColumns.add(Column.physical("rate", BIGINT()));
        if (partitionKeys.size() > 0) {
            resolvedColumns.add(Column.physical("dt", STRING()));
        }
        if (partitionKeys.size() == 2) {
            resolvedColumns.add(Column.physical("hh", STRING()));
        }
        return createResolvedTable(tableOptions, resolvedColumns, partitionKeys, primaryKeys);
    }

    private static ResolvedCatalogTable prepareRateSourceWithTimestamp(
            Map<String, String> tableOptions) {
        List<Column> resolvedColumns = new ArrayList<>();
        resolvedColumns.add(Column.physical("currency", STRING()));
        resolvedColumns.add(Column.physical("rate", BIGINT()));
        resolvedColumns.add(Column.physical("ts", TIMESTAMP(3)));
        return createResolvedTable(
                tableOptions, resolvedColumns, Collections.emptyList(), Collections.emptyList());
    }

    private static ResolvedCatalogTable prepareExchangeRateSource(
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> tableOptions) {
        List<Column> resolvedColumns = new ArrayList<>();
        resolvedColumns.add(Column.physical("from_currency", STRING()));
        resolvedColumns.add(Column.physical("to_currency", STRING()));
        resolvedColumns.add(Column.physical("rate_by_to_currency", DOUBLE()));
        if (partitionKeys.size() > 0) {
            resolvedColumns.add(Column.physical("dt", STRING()));
        }
        if (partitionKeys.size() == 2) {
            resolvedColumns.add(Column.physical("hh", STRING()));
        }
        return createResolvedTable(tableOptions, resolvedColumns, partitionKeys, primaryKeys);
    }

    public static List<Row> rates() {
        // currency, rate
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Euro", 119L));
    }

    public static List<Row> ratesWithTimestamp() {
        // currency, rate, timestamp
        return Arrays.asList(
                changelogRow(
                        "+I", "US Dollar", 102L, LocalDateTime.parse("1990-04-07T10:00:11.120")),
                changelogRow("+I", "Euro", 119L, LocalDateTime.parse("2020-04-07T10:10:11.120")),
                changelogRow("+I", "Yen", 1L, LocalDateTime.parse("2022-04-07T09:54:11.120")));
    }

    public static List<Row> exchangeRates() {
        // from_currency, to_currency, rate_by_to_currency
        return Arrays.asList(
                // to_currency is USD
                changelogRow("+I", "US Dollar", "US Dollar", 1.0d),
                changelogRow("+I", "Euro", "US Dollar", 1.11d),
                changelogRow("+I", "HK Dollar", "US Dollar", 0.13d),
                changelogRow("+I", "Yen", "US Dollar", 0.0082d),
                changelogRow("+I", "Singapore Dollar", "US Dollar", 0.74d),
                changelogRow("+I", "Yen", "US Dollar", 0.0081d),
                changelogRow("+I", "US Dollar", "US Dollar", 1.0d),
                // to_currency is Euro
                changelogRow("+I", "US Dollar", "Euro", 0.9d),
                changelogRow("+I", "Singapore Dollar", "Euro", 0.67d),
                // to_currency is Yen
                changelogRow("+I", "Yen", "Yen", 1.0d),
                changelogRow("+I", "Chinese Yuan", "Yen", 19.25d),
                changelogRow("+I", "Singapore Dollar", "Yen", 90.32d),
                changelogRow("+I", "Singapore Dollar", "Yen", 122.46d));
    }

    public static Tuple2<List<Row>, String> dailyRates() {
        // currency, rate, dt
        return Tuple2.of(
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")),
                "dt:2022-01-01;dt:2022-01-02");
    }

    public static Tuple2<List<Row>, String> dailyExchangeRates() {
        // from_currency, to_currency, rate_by_to_currency, dt
        return Tuple2.of(
                Arrays.asList(
                        // to_currency is USD
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01"),
                        changelogRow("+I", "Euro", "US Dollar", 1.11d, "2022-01-01"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.13d, "2022-01-01"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0082d, "2022-01-01"),
                        changelogRow("+I", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-01"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-02"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-02"),
                        // to_currency is Euro
                        changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-01"),
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-01"),
                        // to_currency is Yen
                        changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-01"),
                        changelogRow("+I", "Chinese Yuan", "Yen", 19.25d, "2022-01-01"),
                        changelogRow("+I", "Singapore Dollar", "Yen", 90.32d, "2022-01-01"),
                        changelogRow("+I", "Singapore Dollar", "Yen", 122.46d, "2022-01-01")),
                "dt:2022-01-01;dt:2022-01-02");
    }

    public static Tuple2<List<Row>, String> hourlyRates() {
        // currency, rate, dt, hh
        return Tuple2.of(
                Arrays.asList(
                        // dt = 2022-01-01, hh = 00
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01", "00"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "00"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "00"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "00"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01", "00"),
                        // dt = 2022-01-01, hh = 20
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01", "20"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "20"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "20"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "20"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01", "20"),
                        // dt = 2022-01-02, hh = 12
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "12")),
                "dt:2022-01-01,hh:00;dt:2022-01-01,hh:20;dt:2022-01-02,hh:12");
    }

    public static Tuple2<List<Row>, String> hourlyExchangeRates() {
        // from_currency, to_currency, rate_by_to_currency, dt, hh
        return Tuple2.of(
                Arrays.asList(
                        // to_currency is USD, dt = 2022-01-01, hh = 11
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "11"),
                        changelogRow("+I", "Euro", "US Dollar", 1.11d, "2022-01-01", "11"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.13d, "2022-01-01", "11"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0082d, "2022-01-01", "11"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-01", "11"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-01", "11"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "11"),
                        // to_currency is USD, dt = 2022-01-01, hh = 12
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "12"),
                        changelogRow("+I", "Euro", "US Dollar", 1.12d, "2022-01-01", "12"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.129d, "2022-01-01", "12"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-01", "12"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.741d, "2022-01-01", "12"),
                        changelogRow("+I", "Yen", "US Dollar", 0.00812d, "2022-01-01", "12"),
                        // to_currency is Euro, dt = 2022-01-02, hh = 23
                        changelogRow("+I", "US Dollar", "Euro", 0.918d, "2022-01-02", "23"),
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-02", "23")),
                "dt:2022-01-01,hh:11;dt:2022-01-01,hh:12;dt:2022-01-02,hh:23");
    }

    public static List<Row> ratesChangelogWithoutUB() {
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("+U", "Euro", 116L),
                changelogRow("-D", "Euro", 116L),
                changelogRow("+I", "Euro", 119L),
                changelogRow("+U", "Euro", 119L),
                changelogRow("-D", "Yen", 1L));
    }

    public static List<Row> exchangeRatesChangelogWithoutUB() {
        // from_currency, to_currency, rate_by_to_currency
        return Arrays.asList(
                // to_currency is USD
                changelogRow("+I", "US Dollar", "US Dollar", 1.0d),
                changelogRow("+I", "Euro", "US Dollar", 1.11d),
                changelogRow("+I", "HK Dollar", "US Dollar", 0.13d),
                changelogRow("+U", "Euro", "US Dollar", 1.12d),
                changelogRow("+I", "Yen", "US Dollar", 0.0082d),
                changelogRow("+I", "Singapore Dollar", "US Dollar", 0.74d),
                changelogRow("+U", "Yen", "US Dollar", 0.0081d),
                changelogRow("-D", "US Dollar", "US Dollar", 1.0d),
                changelogRow("-D", "Yen", "US Dollar", 0.0081d),
                // to_currency is Euro
                changelogRow("+I", "US Dollar", "Euro", 0.9d),
                changelogRow("+I", "Singapore Dollar", "Euro", 0.67d),
                changelogRow("+U", "Singapore Dollar", "Euro", 0.69d),
                // to_currency is Yen
                changelogRow("+I", "Yen", "Yen", 1.0d),
                changelogRow("+I", "Chinese Yuan", "Yen", 19.25d),
                changelogRow("+I", "Singapore Dollar", "Yen", 90.32d),
                changelogRow("-D", "Yen", "Yen", 1.0d),
                changelogRow("+U", "Singapore Dollar", "Yen", 122.46d),
                changelogRow("+U", "Singapore Dollar", "Yen", 122d));
    }

    public static Tuple2<List<Row>, String> dailyRatesChangelogWithoutUB() {
        return Tuple2.of(
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+U", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02"),
                        changelogRow("+U", "Euro", 119L, "2022-01-02")),
                "dt:2022-01-01;dt:2022-01-02");
    }

    public static Tuple2<List<Row>, String> dailyExchangeRatesChangelogWithoutUB() {
        // from_currency, to_currency, rate_by_to_currency, dt
        return Tuple2.of(
                Arrays.asList(
                        // to_currency is USD
                        changelogRow("+I", "US Dollar", "US Dollar", null, "2022-01-01"),
                        changelogRow("+I", "Euro", "US Dollar", null, "2022-01-01"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.13d, "2022-01-01"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0082d, "2022-01-01"),
                        changelogRow("-D", "Yen", "US Dollar", 0.0082d, "2022-01-01"),
                        changelogRow("+I", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-01"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-01"),
                        changelogRow("+U", "Euro", "US Dollar", 1.11d, "2022-01-01"),
                        changelogRow("+U", "US Dollar", "US Dollar", 1.0d, "2022-01-01"),
                        // to_currency is Euro
                        changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02"),
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-02"),
                        changelogRow("-D", "Singapore Dollar", "Euro", 0.67d, "2022-01-02"),
                        // to_currency is Yen
                        changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-02"),
                        changelogRow("+I", "Chinese Yuan", "Yen", 19.25d, "2022-01-02"),
                        changelogRow("+I", "Singapore Dollar", "Yen", 90.32d, "2022-01-02"),
                        changelogRow("+U", "Singapore Dollar", "Yen", 122.46d, "2022-01-02")),
                "dt:2022-01-01;dt:2022-01-02");
    }

    public static Tuple2<List<Row>, String> hourlyRatesChangelogWithoutUB() {
        return Tuple2.of(
                Arrays.asList(
                        // dt = 2022-01-01, hh = "15"
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01", "15"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "15"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "15"),
                        changelogRow("+U", "Euro", 116L, "2022-01-01", "15"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01", "15"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01", "15"),
                        // dt = 2022-01-02, hh = "23"
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "23"),
                        changelogRow("+U", "Euro", 119L, "2022-01-02", "23")),
                "dt:2022-01-01,hh:15;dt:2022-01-02,hh:23");
    }

    public static Tuple2<List<Row>, String> hourlyExchangeRatesChangelogWithoutUB() {
        // from_currency, to_currency, rate_by_to_currency, dt, hh
        return Tuple2.of(
                Arrays.asList(
                        // to_currency is USD
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-02", "20"),
                        changelogRow("+I", "Euro", "US Dollar", null, "2022-01-02", "20"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.13d, "2022-01-02", "20"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0082d, "2022-01-02", "20"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-02", "20"),
                        changelogRow("+U", "Yen", "US Dollar", 0.0081d, "2022-01-02", "20"),
                        changelogRow("-D", "US Dollar", "US Dollar", 1.0d, "2022-01-02", "20"),
                        changelogRow("-D", "Euro", "US Dollar", null, "2022-01-02", "20"),
                        changelogRow(
                                "+U", "Singapore Dollar", "US Dollar", 0.76d, "2022-01-02", "20"),
                        // to_currency is Euro
                        changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02", "20"),
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-02", "20"),
                        changelogRow("+U", "Singapore Dollar", "Euro", null, "2022-01-02", "20"),
                        // to_currency is Yen
                        changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-02", "20"),
                        changelogRow("+I", "Chinese Yuan", "Yen", 19.25d, "2022-01-02", "20"),
                        changelogRow("+U", "Chinese Yuan", "Yen", 25.6d, "2022-01-02", "20"),
                        changelogRow("+I", "Singapore Dollar", "Yen", 90.32d, "2022-01-02", "20"),
                        changelogRow("+I", "US Dollar", "Yen", 122.46d, "2022-01-02", "21"),
                        changelogRow("+U", "Singapore Dollar", "Yen", 90.1d, "2022-01-02", "20")),
                "dt:2022-01-02,hh:20;dt:2022-01-02,hh:21");
    }

    public static List<Row> ratesChangelogWithUB() {
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("-U", "Euro", 114L),
                changelogRow("+U", "Euro", 116L),
                changelogRow("-D", "Euro", 116L),
                changelogRow("+I", "Euro", 119L),
                changelogRow("-U", "Euro", 119L),
                changelogRow("+U", "Euro", 119L),
                changelogRow("-D", "Yen", 1L),
                changelogRow("+I", null, 100L),
                changelogRow("+I", "HK Dollar", null));
    }

    public static Tuple2<List<Row>, String> dailyRatesChangelogWithUB() {
        return Tuple2.of(
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 114L, "2022-01-02"),
                        changelogRow("-U", "Euro", 114L, "2022-01-02"),
                        changelogRow("+U", "Euro", 119L, "2022-01-02"),
                        changelogRow("-D", "Euro", 119L, "2022-01-02"),
                        changelogRow("+I", "Euro", 115L, "2022-01-02")),
                "dt:2022-01-01;dt:2022-01-02");
    }

    public static Tuple2<List<Row>, String> hourlyRatesChangelogWithUB() {
        return Tuple2.of(
                Arrays.asList(
                        // dt = 2022-01-01, hh = 15
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01", "15"),
                        changelogRow("+I", "Euro", 116L, "2022-01-01", "15"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01", "15"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "15"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01", "15"),
                        // dt = 2022-01-02, hh = 20
                        changelogRow("+I", "Euro", 114L, "2022-01-02", "20"),
                        changelogRow("-U", "Euro", 114L, "2022-01-02", "20"),
                        changelogRow("+U", "Euro", 119L, "2022-01-02", "20"),
                        changelogRow("-D", "Euro", 119L, "2022-01-02", "20"),
                        changelogRow("+I", "Euro", 115L, "2022-01-02", "20")),
                "dt:2022-01-01,hh:15;dt:2022-01-02,hh:20");
    }
}
