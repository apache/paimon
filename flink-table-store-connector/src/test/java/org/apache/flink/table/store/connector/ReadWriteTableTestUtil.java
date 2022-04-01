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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.planner.factories.TestValuesTableFactory.registerData;
import static org.apache.flink.table.store.connector.TableStoreTestBase.createResolvedTable;
import static org.assertj.core.api.Assertions.assertThat;

/** Util for {@link ReadWriteTableITCase}. */
public class ReadWriteTableTestUtil {

    static void assertNoMoreRecords(BlockingIterator<Row, Row> iterator) {
        List<Row> expectedRecords = Collections.emptyList();
        try {
            // set expectation size to 1 to let time pass by until timeout
            // just wait 5s to avoid too long time
            expectedRecords = iterator.collect(1, 5L, TimeUnit.SECONDS);
            iterator.close();
        } catch (Exception ignored) {
            // don't throw exception
        }
        assertThat(expectedRecords).isEmpty();
    }

    static String prepareManagedTableDdl(
            String sourceTableName, String managedTableName, Map<String, String> tableOptions) {
        StringBuilder ddl =
                new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                        .append(String.format("`%s`", managedTableName));
        ddl.append(prepareOptions(tableOptions))
                .append(String.format(" LIKE `%s` (EXCLUDING OPTIONS)\n", sourceTableName));
        return ddl.toString();
    }

    static String prepareOptions(Map<String, String> tableOptions) {
        StringBuilder with = new StringBuilder();
        if (tableOptions.size() > 0) {
            with.append(" WITH (\n");
            tableOptions.forEach(
                    (k, v) ->
                            with.append("  ")
                                    .append(String.format("'%s'", k))
                                    .append(" = ")
                                    .append(String.format("'%s',\n", v)));
            int len = with.length();
            with.delete(len - 2, len);
            with.append(")");
        }
        return with.toString();
    }

    static String prepareInsertIntoQuery(String sourceTableName, String managedTableName) {
        return prepareInsertIntoQuery(
                sourceTableName, managedTableName, Collections.emptyMap(), Collections.emptyMap());
    }

    static String prepareInsertIntoQuery(
            String sourceTableName,
            String managedTableName,
            Map<String, String> partitions,
            Map<String, String> hints) {
        StringBuilder insertDmlBuilder =
                new StringBuilder(String.format("INSERT INTO `%s`", managedTableName));
        if (partitions.size() > 0) {
            insertDmlBuilder.append(" PARTITION (");
            partitions.forEach(
                    (k, v) -> {
                        insertDmlBuilder.append(String.format("'%s'", k));
                        insertDmlBuilder.append(" = ");
                        insertDmlBuilder.append(String.format("'%s', ", v));
                    });
            int len = insertDmlBuilder.length();
            insertDmlBuilder.deleteCharAt(len - 1);
            insertDmlBuilder.append(")");
        }
        insertDmlBuilder.append(String.format("\n SELECT * FROM `%s`", sourceTableName));
        insertDmlBuilder.append(buildHints(hints));

        return insertDmlBuilder.toString();
    }

    static String prepareInsertOverwriteQuery(
            String managedTableName,
            Map<String, String> staticPartitions,
            List<String[]> overwriteRecords) {
        StringBuilder insertDmlBuilder =
                new StringBuilder(String.format("INSERT OVERWRITE `%s`", managedTableName));
        if (staticPartitions.size() > 0) {
            String partitionString =
                    staticPartitions.entrySet().stream()
                            .map(
                                    entry ->
                                            String.format(
                                                    "%s = %s", entry.getKey(), entry.getValue()))
                            .collect(Collectors.joining(", "));
            insertDmlBuilder.append(String.format("PARTITION (%s)", partitionString));
        }
        insertDmlBuilder.append("\n VALUES ");
        overwriteRecords.forEach(
                record -> {
                    int arity = record.length;
                    insertDmlBuilder.append("(");
                    IntStream.range(0, arity)
                            .forEach(i -> insertDmlBuilder.append(record[i]).append(", "));

                    if (arity > 0) {
                        int len = insertDmlBuilder.length();
                        insertDmlBuilder.delete(len - 2, len);
                    }
                    insertDmlBuilder.append("), ");
                });
        int len = insertDmlBuilder.length();
        insertDmlBuilder.delete(len - 2, len);
        return insertDmlBuilder.toString();
    }

    static String prepareSimpleSelectQuery(String tableName, Map<String, String> hints) {
        return prepareSelectQuery(tableName, hints, null, Collections.emptyList());
    }

    static String prepareSelectQuery(
            String tableName,
            Map<String, String> hints,
            @Nullable String filter,
            List<String> projections) {
        StringBuilder queryBuilder =
                new StringBuilder(
                        String.format(
                                "SELECT %s FROM `%s` %s",
                                projections.isEmpty() ? "*" : String.join(", ", projections),
                                tableName,
                                buildHints(hints)));
        if (filter != null) {
            queryBuilder.append("\nWHERE ").append(filter);
        }
        return queryBuilder.toString();
    }

    static String buildHints(Map<String, String> hints) {
        if (hints.size() > 0) {
            String hintString =
                    hints.entrySet().stream()
                            .map(
                                    entry ->
                                            String.format(
                                                    "'%s' = '%s'",
                                                    entry.getKey(), entry.getValue()))
                            .collect(Collectors.joining(", "));
            return "/*+ OPTIONS (" + hintString + ") */";
        }
        return "";
    }

    static String prepareHelperSourceWithInsertOnlyRecords(
            String sourceTable, List<String> partitionKeys, List<String> primaryKeys) {
        return prepareHelperSourceRecords(
                RuntimeExecutionMode.BATCH, sourceTable, partitionKeys, primaryKeys);
    }

    static String prepareHelperSourceWithChangelogRecords(
            String sourceTable, List<String> partitionKeys, List<String> primaryKeys) {
        return prepareHelperSourceRecords(
                RuntimeExecutionMode.STREAMING, sourceTable, partitionKeys, primaryKeys);
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
     * @return helper source ddl
     */
    static String prepareHelperSourceRecords(
            RuntimeExecutionMode executionMode,
            String sourceTable,
            List<String> partitionKeys,
            List<String> primaryKeys) {
        Map<String, String> tableOptions = new HashMap<>();
        boolean bounded = executionMode == RuntimeExecutionMode.BATCH;
        String changelogMode = bounded ? "I" : primaryKeys.size() > 0 ? "I,UA,D" : "I,UA,UB,D";
        List<String> exclusivePk = new ArrayList<>(primaryKeys);
        exclusivePk.removeAll(partitionKeys);
        Tuple2<List<Row>, String> tuple =
                prepareHelperSource(bounded, exclusivePk.size(), partitionKeys.size());
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
        if (exclusivePk.size() > 1) {
            table = prepareExchangeRateSource(primaryKeys, partitionKeys, tableOptions);
        } else {
            table = prepareRateSource(primaryKeys, partitionKeys, tableOptions);
        }
        return ShowCreateUtil.buildShowCreateTable(
                table,
                ObjectIdentifier.of("default_catalog", "default_database", sourceTable),
                true);
    }

    static Tuple2<List<Row>, String> prepareHelperSource(
            boolean bounded, int pkSize, int partitionSize) {
        List<Row> input;
        String partitionList = null;
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

    static ResolvedCatalogTable prepareRateSource(
            List<String> primaryKeys,
            List<String> partitionKeys,
            Map<String, String> tableOptions) {
        RowType rowType;
        if (partitionKeys.isEmpty()) {
            rowType =
                    RowType.of(
                            new LogicalType[] {VarCharType.STRING_TYPE, new BigIntType()},
                            new String[] {"currency", "rate"});
        } else if (partitionKeys.size() == 1) {
            rowType =
                    RowType.of(
                            new LogicalType[] {
                                VarCharType.STRING_TYPE, new BigIntType(), VarCharType.STRING_TYPE
                            },
                            new String[] {"currency", "rate", "dt"});
        } else {
            rowType =
                    RowType.of(
                            new LogicalType[] {
                                VarCharType.STRING_TYPE,
                                new BigIntType(),
                                VarCharType.STRING_TYPE,
                                VarCharType.STRING_TYPE
                            },
                            new String[] {"currency", "rate", "dt", "hh"});
        }
        return createResolvedTable(tableOptions, rowType, partitionKeys, primaryKeys);
    }

    static ResolvedCatalogTable prepareExchangeRateSource(
            List<String> primaryKeys,
            List<String> partitionKeys,
            Map<String, String> tableOptions) {
        RowType rowType;
        if (partitionKeys.isEmpty()) {
            rowType =
                    RowType.of(
                            new LogicalType[] {
                                VarCharType.STRING_TYPE, VarCharType.STRING_TYPE, new DoubleType()
                            },
                            new String[] {"from_currency", "to_currency", "rate_by_to_currency"});
        } else if (partitionKeys.size() == 1) {
            rowType =
                    RowType.of(
                            new LogicalType[] {
                                VarCharType.STRING_TYPE,
                                VarCharType.STRING_TYPE,
                                new DoubleType(),
                                VarCharType.STRING_TYPE
                            },
                            new String[] {
                                "from_currency", "to_currency", "rate_by_to_currency", "dt"
                            });
        } else {
            rowType =
                    RowType.of(
                            new LogicalType[] {
                                VarCharType.STRING_TYPE,
                                VarCharType.STRING_TYPE,
                                new DoubleType(),
                                VarCharType.STRING_TYPE,
                                VarCharType.STRING_TYPE
                            },
                            new String[] {
                                "from_currency", "to_currency", "rate_by_to_currency", "dt", "hh"
                            });
        }
        return createResolvedTable(tableOptions, rowType, partitionKeys, primaryKeys);
    }

    static List<Row> rates() {
        // currency, rate
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Euro", 119L));
    }

    static List<Row> exchangeRates() {
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

    static Tuple2<List<Row>, String> dailyRates() {
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

    static Tuple2<List<Row>, String> dailyExchangeRates() {
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

    static Tuple2<List<Row>, String> hourlyRates() {
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

    static Tuple2<List<Row>, String> hourlyExchangeRates() {
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

    static List<Row> ratesChangelogWithoutUB() {
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

    static List<Row> exchangeRatesChangelogWithoutUB() {
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

    static Tuple2<List<Row>, String> dailyRatesChangelogWithoutUB() {
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

    static Tuple2<List<Row>, String> dailyExchangeRatesChangelogWithoutUB() {
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

    static Tuple2<List<Row>, String> hourlyRatesChangelogWithoutUB() {
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

    static Tuple2<List<Row>, String> hourlyExchangeRatesChangelogWithoutUB() {
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

    static List<Row> ratesChangelogWithUB() {
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

    static Tuple2<List<Row>, String> dailyRatesChangelogWithUB() {
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

    static Tuple2<List<Row>, String> hourlyRatesChangelogWithUB() {
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

    static StreamExecutionEnvironment buildStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100);
        env.setParallelism(2);
        return env;
    }

    static StreamExecutionEnvironment buildBatchEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);
        return env;
    }
}
