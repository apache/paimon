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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyExchangeRates;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyExchangeRatesChangelogWithoutUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyRatesChangelogWithUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyRatesChangelogWithoutUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.exchangeRatesChangelogWithoutUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.hourlyExchangeRates;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.hourlyExchangeRatesChangelogWithoutUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.hourlyRates;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.hourlyRatesChangelogWithoutUB;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Table store IT case when the managed table has composite primary keys and multiple partition
 * fields.
 */
public class CompositePkAndMultiPartitionedTableITCase extends ReadWriteTableTestBase {

    @Test
    public void testBatchWriteWithMultiPartitionedRecordsWithMultiPk() throws Exception {
        // input is hourlyExchangeRates()
        List<Row> expectedRecords =
                Arrays.asList(
                        // to_currency is USD, dt = 2022-01-01, hh = 11
                        changelogRow("+I", "Euro", "US Dollar", 1.11d, "2022-01-01", "11"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.13d, "2022-01-01", "11"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-01", "11"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-01", "11"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "11"),
                        // to_currency is USD, dt = 2022-01-01, hh = 12
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "12"),
                        changelogRow("+I", "Euro", "US Dollar", 1.12d, "2022-01-01", "12"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.129d, "2022-01-01", "12"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.741d, "2022-01-01", "12"),
                        changelogRow("+I", "Yen", "US Dollar", 0.00812d, "2022-01-01", "12"),
                        // to_currency is Euro, dt = 2022-01-02, hh = 23
                        changelogRow("+I", "US Dollar", "Euro", 0.918d, "2022-01-02", "23"),
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-02", "23"));
        // test batch read
        String managedTable =
                collectAndCheckBatchReadWrite(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        null,
                        Collections.emptyList(),
                        expectedRecords);

        checkFileStorePath(tEnv, managedTable, hourlyExchangeRates().f1);

        // test streaming read
        final StreamTableEnvironment streamTableEnv =
                StreamTableEnvironment.create(buildStreamEnv());
        registerTable(streamTableEnv, managedTable);
        BlockingIterator<Row, Row> streamIter =
                collectAndCheck(
                        streamTableEnv,
                        managedTable,
                        Collections.emptyMap(),
                        null,
                        expectedRecords);

        // overwrite static partition dt = 2022-01-02 and hh = 23
        Map<String, String> overwritePartition = new LinkedHashMap<>();
        overwritePartition.put("dt", "'2022-01-02'");
        overwritePartition.put("hh", "'23'");
        prepareEnvAndOverwrite(
                managedTable,
                overwritePartition,
                Collections.singletonList(new String[] {"'US Dollar'", "'Thai Baht'", "33.51"}));

        // streaming iter will not receive any changelog
        assertNoMoreRecords(streamIter);

        // batch read to check partition refresh
        collectAndCheck(
                        tEnv,
                        managedTable,
                        Collections.emptyMap(),
                        "dt = '2022-01-02' AND hh = '23'",
                        Collections.singletonList(
                                changelogRow(
                                        "+I",
                                        "US Dollar",
                                        "Thai Baht",
                                        33.51d,
                                        "2022-01-02",
                                        "23")))
                .close();

        // test partition filter
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                "dt = '2022-01-01'",
                Collections.emptyList(),
                Arrays.asList(
                        // to_currency is USD, dt = 2022-01-01, hh = 11
                        changelogRow("+I", "Euro", "US Dollar", 1.11d, "2022-01-01", "11"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.13d, "2022-01-01", "11"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-01", "11"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-01", "11"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "11"),
                        // to_currency is USD, dt = 2022-01-01, hh = 12
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "12"),
                        changelogRow("+I", "Euro", "US Dollar", 1.12d, "2022-01-01", "12"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.129d, "2022-01-01", "12"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.741d, "2022-01-01", "12"),
                        changelogRow("+I", "Yen", "US Dollar", 0.00812d, "2022-01-01", "12")));

        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                "dt = '2022-01-01' AND hh = '12'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "12"),
                        changelogRow("+I", "Euro", "US Dollar", 1.12d, "2022-01-01", "12"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.129d, "2022-01-01", "12"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.741d, "2022-01-01", "12"),
                        changelogRow("+I", "Yen", "US Dollar", 0.00812d, "2022-01-01", "12")));

        // test field filter
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                "to_currency = 'Euro'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", "Euro", 0.918d, "2022-01-02", "23"),
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-02", "23")));

        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                "from_currency = 'HK Dollar'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.13d, "2022-01-01", "11"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.129d, "2022-01-01", "12")));

        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                "rate_by_to_currency > 0.5",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Euro", "US Dollar", 1.11d, "2022-01-01", "11"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-01", "11"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "11"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "12"),
                        changelogRow("+I", "Euro", "US Dollar", 1.12d, "2022-01-01", "12"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.741d, "2022-01-01", "12"),
                        changelogRow("+I", "US Dollar", "Euro", 0.918d, "2022-01-02", "23"),
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-02", "23")));

        // test partition and field filter
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                "rate_by_to_currency > 0.9 AND hh = '23'",
                Collections.emptyList(),
                Collections.singletonList(
                        changelogRow("+I", "US Dollar", "Euro", 0.918d, "2022-01-02", "23")));

        // test projection
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                null,
                Arrays.asList("from_currency", "dt", "hh"),
                Arrays.asList(
                        // to_currency is USD, dt = 2022-01-01, hh = 11
                        changelogRow("+I", "Euro", "2022-01-01", "11"),
                        changelogRow("+I", "HK Dollar", "2022-01-01", "11"),
                        changelogRow("+I", "Singapore Dollar", "2022-01-01", "11"),
                        changelogRow("+I", "Yen", "2022-01-01", "11"),
                        changelogRow("+I", "US Dollar", "2022-01-01", "11"),
                        // to_currency is USD, dt = 2022-01-01, hh = 12
                        changelogRow("+I", "US Dollar", "2022-01-01", "12"),
                        changelogRow("+I", "Euro", "2022-01-01", "12"),
                        changelogRow("+I", "HK Dollar", "2022-01-01", "12"),
                        changelogRow("+I", "Singapore Dollar", "2022-01-01", "12"),
                        changelogRow("+I", "Yen", "2022-01-01", "12"),
                        // to_currency is Euro, dt = 2022-01-02, hh = 23
                        changelogRow("+I", "US Dollar", "2022-01-02", "23"),
                        changelogRow("+I", "Singapore Dollar", "2022-01-02", "23")));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                "dt = '2022-01-01' AND hh >= '12' OR rate_by_to_currency > 2",
                Arrays.asList("from_currency", "to_currency"),
                Arrays.asList(
                        // to_currency is USD, dt = 2022-01-01, hh = 12
                        changelogRow("+I", "US Dollar", "US Dollar"),
                        changelogRow("+I", "Euro", "US Dollar"),
                        changelogRow("+I", "HK Dollar", "US Dollar"),
                        changelogRow("+I", "Singapore Dollar", "US Dollar"),
                        changelogRow("+I", "Yen", "US Dollar")));
    }

    @Test
    public void testBatchWriteWithSinglePartitionedRecordsWithMultiPk() throws Exception {
        // input is dailyExchangeRates()
        List<Row> expectedRecords =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01"),
                        changelogRow("+I", "Euro", "US Dollar", 1.11d, "2022-01-01"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.13d, "2022-01-01"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0082d, "2022-01-01"),
                        changelogRow("+I", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-01"),
                        changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-01"),
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-01"),
                        changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-01"),
                        changelogRow("+I", "Chinese Yuan", "Yen", 19.25d, "2022-01-01"),
                        changelogRow("+I", "Singapore Dollar", "Yen", 122.46d, "2022-01-01"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-02"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-02"));
        // test batch read
        String managedTable =
                collectAndCheckBatchReadWrite(
                        Collections.singletonList("dt"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        null,
                        Collections.emptyList(),
                        expectedRecords);

        checkFileStorePath(tEnv, managedTable, dailyExchangeRates().f1);

        // test streaming read
        final StreamTableEnvironment streamTableEnv =
                StreamTableEnvironment.create(buildStreamEnv());
        registerTable(streamTableEnv, managedTable);
        BlockingIterator<Row, Row> streamIter =
                collectAndCheck(
                        streamTableEnv,
                        managedTable,
                        Collections.emptyMap(),
                        null,
                        expectedRecords);

        // overwrite dynamic partition
        prepareEnvAndOverwrite(
                managedTable,
                Collections.emptyMap(),
                Collections.singletonList(
                        new String[] {"'US Dollar'", "'Thai Baht'", "33.51", "'2022-01-01'"}));

        // streaming iter will not receive any changelog
        assertNoMoreRecords(streamIter);

        // batch read to check partition refresh
        collectAndCheck(
                        tEnv,
                        managedTable,
                        Collections.emptyMap(),
                        "dt = '2022-01-01'",
                        Collections.singletonList(
                                changelogRow("+I", "US Dollar", "Thai Baht", 33.51d, "2022-01-01")))
                .close();

        // test partition filter
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"), // partition
                Arrays.asList("from_currency", "to_currency", "dt"), // pk
                "dt = '2022-01-02'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-02"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-02")));

        // test field filter
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"), // partition
                Arrays.asList("from_currency", "to_currency", "dt"), // pk
                "rate_by_to_currency < 0.1",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Yen", "US Dollar", 0.0082d, "2022-01-01"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-02")));

        // test partition and field filter
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"), // partition
                Arrays.asList("from_currency", "to_currency", "dt"), // pk
                "rate_by_to_currency > 0.9 OR dt = '2022-01-02'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01"),
                        changelogRow("+I", "Euro", "US Dollar", 1.11d, "2022-01-01"),
                        changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-01"),
                        changelogRow("+I", "Chinese Yuan", "Yen", 19.25d, "2022-01-01"),
                        changelogRow("+I", "Singapore Dollar", "Yen", 122.46d, "2022-01-01"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-02"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-02")));

        // test projection
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"), // partition
                Arrays.asList("from_currency", "to_currency", "dt"), // pk
                null,
                Arrays.asList("rate_by_to_currency", "dt"),
                Arrays.asList(
                        changelogRow("+I", 1.0d, "2022-01-01"), // US Dollar，US Dollar
                        changelogRow("+I", 1.11d, "2022-01-01"), // Euro，US Dollar
                        changelogRow("+I", 0.13d, "2022-01-01"), // HK Dollar，US Dollar
                        changelogRow("+I", 0.0082d, "2022-01-01"), // Yen，US Dollar
                        changelogRow("+I", 0.74d, "2022-01-01"), // Singapore Dollar，US Dollar
                        changelogRow("+I", 0.9d, "2022-01-01"), // US Dollar，Euro
                        changelogRow("+I", 0.67d, "2022-01-01"), // Singapore Dollar，Euro
                        changelogRow("+I", 1.0d, "2022-01-01"), // Yen，Yen
                        changelogRow("+I", 19.25d, "2022-01-01"), // Chinese Yuan，Yen
                        changelogRow("+I", 122.46d, "2022-01-01"), // Singapore Dollar，Yen
                        changelogRow("+I", 0.0081d, "2022-01-02"), // Yen，US Dollar
                        changelogRow("+I", 1.0d, "2022-01-02") // US Dollar，US Dollar
                        ));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"), // partition
                Arrays.asList("from_currency", "to_currency", "dt"), // pk
                "dt = '2022-01-02' OR rate_by_to_currency > 100",
                Arrays.asList("from_currency", "to_currency"),
                Arrays.asList(
                        changelogRow("+I", "Yen", "US Dollar"),
                        changelogRow("+I", "US Dollar", "US Dollar"),
                        changelogRow("+I", "Singapore Dollar", "Yen")));
    }

    @Test
    public void testBatchWriteWithNonPartitionedRecordsWithMultiPk() throws Exception {
        // input is exchangeRates()
        List<Row> expectedRecords =
                Arrays.asList(
                        // to_currency is USD
                        changelogRow("+I", "Euro", "US Dollar", 1.11d),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.13d),
                        changelogRow("+I", "Singapore Dollar", "US Dollar", 0.74d),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d),
                        // to_currency is Euro
                        changelogRow("+I", "US Dollar", "Euro", 0.9d),
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d),
                        // to_currency is Yen
                        changelogRow("+I", "Yen", "Yen", 1.0d),
                        changelogRow("+I", "Chinese Yuan", "Yen", 19.25d),
                        changelogRow("+I", "Singapore Dollar", "Yen", 122.46d));
        // test batch read
        String managedTable =
                collectAndCheckBatchReadWrite(
                        Collections.emptyList(), // partition
                        Arrays.asList("from_currency", "to_currency"), // pk
                        null,
                        Collections.emptyList(),
                        expectedRecords);

        checkFileStorePath(tEnv, managedTable, null);

        // test streaming read
        final StreamTableEnvironment streamTableEnv =
                StreamTableEnvironment.create(buildStreamEnv());
        registerTable(streamTableEnv, managedTable);
        BlockingIterator<Row, Row> streamIter =
                collectAndCheck(
                        streamTableEnv,
                        managedTable,
                        Collections.emptyMap(),
                        null,
                        expectedRecords);

        // overwrite the whole table
        prepareEnvAndOverwrite(
                managedTable,
                Collections.emptyMap(),
                Collections.singletonList(new String[] {"'US Dollar'", "'Thai Baht'", "33.51"}));

        // streaming iter will not receive any changelog
        assertNoMoreRecords(streamIter);

        // batch read to check data refresh
        collectAndCheck(
                        tEnv,
                        managedTable,
                        Collections.emptyMap(),
                        null,
                        Collections.singletonList(
                                changelogRow("+I", "US Dollar", "Thai Baht", 33.51d)))
                .close();

        // test field filter
        collectAndCheckBatchReadWrite(
                Collections.emptyList(), // partition
                Arrays.asList("from_currency", "to_currency"), // pk
                "rate_by_to_currency < 0.1",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Yen", "US Dollar", 0.0081d)));

        // test projection
        collectAndCheckBatchReadWrite(
                Collections.emptyList(), // partition
                Arrays.asList("from_currency", "to_currency"), // pk
                null,
                Collections.singletonList("rate_by_to_currency"),
                Arrays.asList(
                        changelogRow("+I", 1.11d), // Euro, US Dollar
                        changelogRow("+I", 0.13d), // HK Dollar, US Dollar
                        changelogRow("+I", 0.74d), // Singapore Dollar, US Dollar
                        changelogRow("+I", 0.0081d), // Yen, US Dollar
                        changelogRow("+I", 1.0d), // US Dollar, US Dollar
                        changelogRow("+I", 0.9d), // US Dollar, Euro
                        changelogRow("+I", 0.67d), // Singapore Dollar, Euro
                        changelogRow("+I", 1.0d), // Yen, Yen
                        changelogRow("+I", 19.25d), // Chinese Yuan, Yen
                        changelogRow("+I", 122.46d) // Singapore Dollar, Yen
                        ));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                Collections.emptyList(), // partition
                Arrays.asList("from_currency", "to_currency"), // pk
                "rate_by_to_currency > 100",
                Arrays.asList("from_currency", "to_currency"),
                Collections.singletonList(changelogRow("+I", "Singapore Dollar", "Yen")));
    }

    @Test
    public void testBatchWriteMultiPartitionedRecordsWithOnePk() throws Exception {
        // input is hourlyRates()
        List<Row> expectedRecords =
                Arrays.asList(
                        // dt = 2022-01-01, hh = 00
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "00"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "00"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01", "00"),
                        // dt = 2022-01-01, hh = 20
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "20"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "20"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01", "20"),
                        // dt = 2022-01-02, hh = 12
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "12"));
        // test batch read
        String managedTable =
                collectAndCheckBatchReadWrite(
                        Arrays.asList("dt", "hh"), // partition
                        Arrays.asList("currency", "dt", "hh"), // pk
                        null,
                        Collections.emptyList(),
                        expectedRecords);

        checkFileStorePath(tEnv, managedTable, hourlyRates().f1);

        // test streaming read
        final StreamTableEnvironment streamTableEnv =
                StreamTableEnvironment.create(buildStreamEnv());
        registerTable(streamTableEnv, managedTable);
        BlockingIterator<Row, Row> streamIter =
                collectAndCheck(
                        streamTableEnv,
                        managedTable,
                        Collections.emptyMap(),
                        null,
                        expectedRecords);

        // dynamic overwrite
        // INSERT OVERWRITE `manged_table-${uuid}` VALUES(...)
        prepareEnvAndOverwrite(
                managedTable,
                Collections.emptyMap(),
                Collections.singletonList(
                        new String[] {"'HK Dollar'", "80", "'2022-01-01'", "'00'"}));

        // INSERT OVERWRITE `manged_table-${uuid}` PARTITION (dt = '2022-01-02') VALUES(...)
        prepareEnvAndOverwrite(
                managedTable,
                Collections.singletonMap("dt", "'2022-01-02'"),
                Collections.singletonList(new String[] {"'Euro'", "120", "'12'"}));

        // batch read to check data refresh
        collectAndCheck(
                tEnv,
                managedTable,
                Collections.emptyMap(),
                "hh = '00' OR hh = '12'",
                Arrays.asList(
                        changelogRow("+I", "HK Dollar", 80L, "2022-01-01", "00"),
                        changelogRow("+I", "Euro", 120L, "2022-01-02", "12")));

        // streaming iter will not receive any changelog
        assertNoMoreRecords(streamIter);

        // test partition filter
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("currency", "dt", "hh"), // pk
                "hh >= '10'",
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01, hh = 20
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "20"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "20"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01", "20"),
                        // dt = 2022-01-02, hh = 12
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "12")));

        // test field filter
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("currency", "dt", "hh"), // pk
                "rate >= 119",
                Collections.emptyList(),
                Collections.singletonList(
                        // dt = 2022-01-02, hh = 12
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "12")));

        // test projection
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("currency", "dt", "hh"), // pk
                null,
                Collections.singletonList("hh"),
                Arrays.asList(
                        changelogRow("+I", "00"), // Yen, 1L, 2022-01-01
                        changelogRow("+I", "00"), // Euro, 114L, 2022-01-01
                        changelogRow("+I", "00"), // US Dollar, 114L, 2022-01-01
                        changelogRow("+I", "20"), // Yen, 1L, 2022-01-01
                        changelogRow("+I", "20"), // Euro, 114L, 2022-01-01
                        changelogRow("+I", "20"), // US Dollar, 114L, 2022-01-01
                        changelogRow("+I", "12") // Euro, 119L, "2022-01-02
                        ));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("currency", "dt", "hh"), // pk
                "rate > 100 AND hh >= '20'",
                Collections.singletonList("rate"),
                Collections.singletonList(changelogRow("+I", 114L)));
    }

    @Test
    public void testBatchWriteMultiPartitionedRecordsWithoutPk() throws Exception {
        // input is hourlyRates()

        // test batch read
        String managedTable =
                collectAndCheckBatchReadWrite(
                        Arrays.asList("dt", "hh"), // partition
                        Collections.emptyList(), // pk
                        null,
                        Collections.emptyList(),
                        hourlyRates().f0);

        checkFileStorePath(tEnv, managedTable, hourlyRates().f1);

        // test streaming read
        final StreamTableEnvironment streamTableEnv =
                StreamTableEnvironment.create(buildStreamEnv());
        registerTable(streamTableEnv, managedTable);
        BlockingIterator<Row, Row> streamIter =
                collectAndCheck(
                        streamTableEnv,
                        managedTable,
                        Collections.emptyMap(),
                        null,
                        hourlyRates().f0);

        // dynamic overwrite the whole table with null partitions
        String query =
                String.format(
                        "INSERT OVERWRITE `%s` SELECT 'Yen', 1, CAST(null AS STRING), CAST(null AS STRING) FROM `%s`",
                        managedTable, managedTable);
        prepareEnvAndOverwrite(managedTable, query);
        // check new partition path
        checkFileStorePath(tEnv, managedTable, "dt:null,hh:null");

        // batch read to check data refresh
        collectAndCheck(
                tEnv,
                managedTable,
                Collections.emptyMap(),
                null,
                Collections.singletonList(changelogRow("+I", "Yen", 1L, null, null)));

        // streaming iter will not receive any changelog
        assertNoMoreRecords(streamIter);

        // test partition filter
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Collections.emptyList(), // pk
                "hh >= '10'",
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01, hh = 20
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01", "20"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "20"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "20"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "20"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01", "20"),
                        // dt = 2022-01-02, hh = 12
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "12")));

        // test field filter
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Collections.emptyList(), // pk
                "rate >= 119",
                Collections.emptyList(),
                Collections.singletonList(
                        // dt = 2022-01-02, hh = 12
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "12")));

        // test projection
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Collections.emptyList(), // pk
                null,
                Collections.singletonList("currency"),
                Arrays.asList(
                        // dt = 2022-01-01, hh = 00
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "US Dollar"),
                        // dt = 2022-01-01, hh = 20
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "US Dollar"),
                        // dt = 2022-01-02, hh = 12
                        changelogRow("+I", "Euro")));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                Arrays.asList("dt", "hh"), // partition
                Collections.emptyList(), // pk
                "rate > 100 AND hh >= '20'",
                Collections.singletonList("rate"),
                Collections.singletonList(changelogRow("+I", 114L)));
    }

    @Test
    public void testEnableLogAndStreamingReadWriteSinglePartitionedRecordsWithMultiPk()
            throws Exception {
        // input is dailyExchangeRatesChangelogWithoutUB()
        // test hybrid read
        Tuple2<String, BlockingIterator<Row, Row>> tuple =
                collectAndCheckStreamingReadWriteWithoutClose(
                        Collections.singletonList("dt"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                changelogRow("+I", "HK Dollar", "US Dollar", 0.13d, "2022-01-01"),
                                changelogRow(
                                        "+I", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-01"),
                                changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-01"),
                                changelogRow("+I", "Euro", "US Dollar", 1.11d, "2022-01-01"),
                                changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01"),
                                changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02"),
                                changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-02"),
                                changelogRow("+I", "Chinese Yuan", "Yen", 19.25d, "2022-01-02"),
                                changelogRow(
                                        "+I", "Singapore Dollar", "Yen", 122.46d, "2022-01-02")));
        String managedTable = tuple.f0;
        checkFileStorePath(tEnv, managedTable, dailyRatesChangelogWithoutUB().f1);
        BlockingIterator<Row, Row> streamIter = tuple.f1;

        // test streaming consume changelog
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s` PARTITION (dt = '2022-01-03')\n"
                                        + "VALUES('Chinese Yuan', 'HK Dollar', 1.231)\n",
                                managedTable))
                .await();

        assertThat(streamIter.collect(1, 5, TimeUnit.SECONDS))
                .containsExactlyInAnyOrderElementsOf(
                        Collections.singletonList(
                                changelogRow(
                                        "+I", "Chinese Yuan", "HK Dollar", 1.231d, "2022-01-03")));

        // dynamic overwrite the whole table
        String query =
                String.format(
                        "INSERT OVERWRITE `%s` SELECT 'US Dollar', 'US Dollar', 1, '2022-04-02' FROM `%s`",
                        managedTable, managedTable);
        prepareEnvAndOverwrite(managedTable, query);
        checkFileStorePath(tEnv, managedTable, "dt:2022-04-02");

        // batch read to check data refresh
        final StreamTableEnvironment batchTableEnv =
                StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        registerTable(batchTableEnv, managedTable);
        collectAndCheck(
                batchTableEnv,
                managedTable,
                Collections.emptyMap(),
                null,
                Collections.singletonList(
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-04-02")));

        // check no changelog generated for streaming read
        assertNoMoreRecords(streamIter);

        // filter on partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Arrays.asList("from_currency", "to_currency", "dt"),
                Collections.emptyMap(),
                "dt = '2022-01-02' AND from_currency = 'US Dollar'",
                Collections.emptyList(),
                Collections.singletonList(
                        changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02")));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Arrays.asList("from_currency", "to_currency", "dt"),
                Collections.emptyMap(),
                "dt = '2022-01-01' AND rate_by_to_currency IS NULL",
                Arrays.asList("from_currency", "to_currency"),
                Collections.emptyList());
    }

    @Test
    public void testEnableLogAndStreamingReadWriteMultiPartitionedRecordsWithMultiPk()
            throws Exception {
        // input is hourlyExchangeRatesChangelogWithoutUB()
        // test hybrid read
        Tuple2<String, BlockingIterator<Row, Row>> tuple =
                collectAndCheckStreamingReadWriteWithoutClose(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                changelogRow(
                                        "+I", "HK Dollar", "US Dollar", 0.13d, "2022-01-02", "20"),
                                changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-02", "20"),
                                changelogRow(
                                        "+I",
                                        "Singapore Dollar",
                                        "US Dollar",
                                        0.76d,
                                        "2022-01-02",
                                        "20"),
                                changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02", "20"),
                                changelogRow(
                                        "+I", "Singapore Dollar", "Euro", null, "2022-01-02", "20"),
                                changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-02", "20"),
                                changelogRow(
                                        "+I", "Chinese Yuan", "Yen", 25.6d, "2022-01-02", "20"),
                                changelogRow("+I", "US Dollar", "Yen", 122.46d, "2022-01-02", "21"),
                                changelogRow(
                                        "+I",
                                        "Singapore Dollar",
                                        "Yen",
                                        90.1d,
                                        "2022-01-02",
                                        "20")));
        String managedTable = tuple.f0;
        checkFileStorePath(tEnv, managedTable, hourlyExchangeRatesChangelogWithoutUB().f1);
        BlockingIterator<Row, Row> streamIter = tuple.f1;

        // test streaming consume changelog
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s` \n"
                                        + "VALUES('Chinese Yuan', 'HK Dollar', 1.231, '2022-01-03', '15')\n",
                                managedTable))
                .await();

        assertThat(streamIter.collect(1, 5, TimeUnit.SECONDS))
                .containsExactlyInAnyOrderElementsOf(
                        Collections.singletonList(
                                changelogRow(
                                        "+I",
                                        "Chinese Yuan",
                                        "HK Dollar",
                                        1.231d,
                                        "2022-01-03",
                                        "15")));

        // dynamic overwrite the whole table
        String query =
                String.format(
                        "INSERT OVERWRITE `%s` SELECT 'US Dollar', 'US Dollar', 1, '2022-04-02', '10' FROM `%s`",
                        managedTable, managedTable);
        prepareEnvAndOverwrite(managedTable, query);
        checkFileStorePath(tEnv, managedTable, "dt:2022-04-02,hh:10");

        // batch read to check data refresh
        final StreamTableEnvironment batchTableEnv =
                StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        registerTable(batchTableEnv, managedTable);
        collectAndCheck(
                batchTableEnv,
                managedTable,
                Collections.emptyMap(),
                null,
                Collections.singletonList(
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-04-02", "10")));

        // check no changelog generated for streaming read
        assertNoMoreRecords(streamIter);

        // filter on partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Arrays.asList("dt", "hh"),
                Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                Collections.emptyMap(),
                "dt = '2022-01-02' AND from_currency = 'US Dollar'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02", "20"),
                        changelogRow("+I", "US Dollar", "Yen", 122.46d, "2022-01-02", "21")));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Arrays.asList("dt", "hh"),
                Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                Collections.emptyMap(),
                "dt = '2022-01-02' AND from_currency = 'US Dollar'",
                Arrays.asList("from_currency", "to_currency"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", "Euro"),
                        changelogRow("+I", "US Dollar", "Yen")));
    }

    @Test
    public void testEnableLogAndStreamingReadWriteMultiPartitionedRecordsWithoutPk()
            throws Exception {
        // input is dailyExchangeRatesChangelogWithUB()
        // test hybrid read
        Tuple2<String, BlockingIterator<Row, Row>> tuple =
                collectAndCheckStreamingReadWriteWithoutClose(
                        Collections.singletonList("dt"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                // dt = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // dt = 2022-01-02
                                changelogRow("+I", "Euro", 115L, "2022-01-02")));
        String managedTable = tuple.f0;
        checkFileStorePath(tEnv, managedTable, dailyRatesChangelogWithUB().f1);
        BlockingIterator<Row, Row> streamIter = tuple.f1;

        // test streaming consume changelog
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s` PARTITION (dt = '2022-04-02')\n"
                                        + "VALUES('Euro', 116)\n",
                                managedTable))
                .await();

        assertThat(streamIter.collect(1, 5, TimeUnit.SECONDS))
                .containsExactlyInAnyOrderElementsOf(
                        Collections.singletonList(changelogRow("+I", "Euro", 116L, "2022-04-02")));

        // dynamic overwrite the whole table
        String query =
                String.format(
                        "INSERT OVERWRITE `%s` SELECT 'US Dollar', 103, '2022-04-02' FROM `%s`",
                        managedTable, managedTable);
        prepareEnvAndOverwrite(managedTable, query);
        checkFileStorePath(tEnv, managedTable, "dt:2022-04-02");

        // batch read to check data refresh
        final StreamTableEnvironment batchTableEnv =
                StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        registerTable(batchTableEnv, managedTable);
        collectAndCheck(
                batchTableEnv,
                managedTable,
                Collections.emptyMap(),
                null,
                Collections.singletonList(changelogRow("+I", "US Dollar", 103L, "2022-04-02")));

        // check no changelog generated for streaming read
        assertNoMoreRecords(streamIter);

        // filter on partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Collections.emptyList(),
                Collections.emptyMap(),
                "dt = '2022-01-01' AND currency = 'Yen'",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Collections.emptyList(),
                Collections.emptyMap(),
                "dt = '2022-01-01' AND rate = 103",
                Collections.singletonList("currency"),
                Collections.emptyList());
    }

    @Test
    public void testReadLatestChangelogOfMultiPartitionedRecordsWithMultiPk() throws Exception {
        // input is hourlyExchangeRatesChangelogWithoutUB()
        List<Row> expectedRecords = new ArrayList<>(hourlyExchangeRatesChangelogWithoutUB().f0);
        expectedRecords.add(changelogRow("-U", "Yen", "US Dollar", 0.0082d, "2022-01-02", "20"));
        expectedRecords.add(
                changelogRow("-U", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-02", "20"));
        expectedRecords.add(
                changelogRow("-U", "Singapore Dollar", "Euro", 0.67d, "2022-01-02", "20"));
        expectedRecords.add(changelogRow("-U", "Chinese Yuan", "Yen", 19.25d, "2022-01-02", "20"));
        expectedRecords.add(
                changelogRow("-U", "Singapore Dollar", "Yen", 90.32d, "2022-01-02", "20"));
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                null,
                Collections.emptyList(),
                expectedRecords);

        // test partition filter
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                "dt = '2022-01-02' AND hh = '21'",
                Collections.emptyList(),
                Collections.singletonList(
                        changelogRow("+I", "US Dollar", "Yen", 122.46d, "2022-01-02", "21")));

        // test field filter
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"),
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                "rate_by_to_currency IS NOT NULL AND from_currency = 'US Dollar'",
                Collections.emptyList(),
                Arrays.asList(
                        // to_currency is USD
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-02", "20"),
                        changelogRow("-D", "US Dollar", "US Dollar", 1.0d, "2022-01-02", "20"),
                        // to_currency is Euro
                        changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02", "20"),
                        // to_currency is Yen
                        changelogRow("+I", "US Dollar", "Yen", 122.46d, "2022-01-02", "21")));

        // test partition and field filter
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"),
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                "hh = '21' AND from_currency = 'US Dollar'",
                Collections.emptyList(),
                Collections.singletonList(
                        changelogRow("+I", "US Dollar", "Yen", 122.46d, "2022-01-02", "21")));

        // test projection
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"),
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                null,
                Arrays.asList("from_currency", "to_currency"),
                Arrays.asList(
                        // to_currency is USD
                        changelogRow("+I", "US Dollar", "US Dollar"),
                        changelogRow("+I", "Euro", "US Dollar"),
                        changelogRow("+I", "HK Dollar", "US Dollar"),
                        changelogRow("+I", "Yen", "US Dollar"),
                        changelogRow("+I", "Singapore Dollar", "US Dollar"),
                        changelogRow("-D", "US Dollar", "US Dollar"),
                        changelogRow("-D", "Euro", "US Dollar"),
                        // to_currency is Euro
                        changelogRow("+I", "US Dollar", "Euro"),
                        changelogRow("+I", "Singapore Dollar", "Euro"),
                        // to_currency is Yen
                        changelogRow("+I", "Yen", "Yen"),
                        changelogRow("+I", "Chinese Yuan", "Yen"),
                        changelogRow("+I", "Singapore Dollar", "Yen"),
                        changelogRow("+I", "US Dollar", "Yen")));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"),
                Arrays.asList("from_currency", "to_currency", "dt", "hh"), // pk
                "rate_by_to_currency > 100",
                Arrays.asList("from_currency", "to_currency"),
                Collections.singletonList(changelogRow("+I", "US Dollar", "Yen")));
    }

    @Test
    public void testReadLatestChangelogOfSinglePartitionedRecordsWithMultiPk() throws Exception {
        // input is dailyExchangeRatesChangelogWithoutUB()
        List<Row> expectedRecords = new ArrayList<>(dailyExchangeRatesChangelogWithoutUB().f0);
        expectedRecords.add(changelogRow("-U", "Euro", "US Dollar", null, "2022-01-01"));
        expectedRecords.add(changelogRow("-U", "US Dollar", "US Dollar", null, "2022-01-01"));
        expectedRecords.add(changelogRow("-U", "Singapore Dollar", "Yen", 90.32d, "2022-01-02"));

        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"), // partition
                Arrays.asList("from_currency", "to_currency", "dt"), // pk
                null,
                Collections.emptyList(),
                expectedRecords);

        // test partition filter
        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"), // partition
                Arrays.asList("from_currency", "to_currency", "dt"), // pk
                "dt = '2022-01-02'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02"),
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-02"),
                        changelogRow("-D", "Singapore Dollar", "Euro", 0.67d, "2022-01-02"),
                        changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-02"),
                        changelogRow("+I", "Chinese Yuan", "Yen", 19.25d, "2022-01-02"),
                        changelogRow("+I", "Singapore Dollar", "Yen", 90.32d, "2022-01-02"),
                        changelogRow("+U", "Singapore Dollar", "Yen", 122.46d, "2022-01-02"),
                        changelogRow("-U", "Singapore Dollar", "Yen", 90.32d, "2022-01-02")));

        // test field filter
        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"), // partition
                Arrays.asList("from_currency", "to_currency", "dt"), // pk
                "rate_by_to_currency IS NULL",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", "US Dollar", null, "2022-01-01"),
                        changelogRow("+I", "Euro", "US Dollar", null, "2022-01-01"),
                        changelogRow("-U", "Euro", "US Dollar", null, "2022-01-01"),
                        changelogRow("-U", "US Dollar", "US Dollar", null, "2022-01-01")));

        // test partition and field filter
        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"), // partition
                Arrays.asList("from_currency", "to_currency", "dt"), // pk
                "dt = '2022-01-02' AND from_currency = 'Yen'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-02")));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"), // partition
                Arrays.asList("from_currency", "to_currency", "dt"), // pk
                "rate_by_to_currency > 100",
                Arrays.asList("from_currency", "to_currency"),
                Collections.singletonList(changelogRow("+U", "Singapore Dollar", "Yen")));
    }

    @Test
    public void testReadLatestChangelogOfNonPartitionedRecordsWithMultiPk() throws Exception {
        // input is exchangeRatesChangelogWithoutUB()
        List<Row> expectedRecords = new ArrayList<>(exchangeRatesChangelogWithoutUB());
        expectedRecords.add(changelogRow("-U", "Yen", "US Dollar", 0.0082d));
        expectedRecords.add(changelogRow("-U", "Euro", "US Dollar", 1.11d));
        expectedRecords.add(changelogRow("-U", "Singapore Dollar", "Euro", 0.67d));
        expectedRecords.add(changelogRow("-U", "Singapore Dollar", "Yen", 90.32d));
        expectedRecords.add(changelogRow("-U", "Singapore Dollar", "Yen", 122.46d));

        collectLatestLogAndCheck(
                false,
                Collections.emptyList(), // partition
                Arrays.asList("from_currency", "to_currency"), // pk
                null,
                Collections.emptyList(),
                expectedRecords);

        // test field filter
        collectLatestLogAndCheck(
                false,
                Collections.emptyList(), // partition
                Arrays.asList("from_currency", "to_currency"), // pk
                "rate_by_to_currency < 1 OR rate_by_to_currency > 100",
                Collections.emptyList(),
                Arrays.asList(
                        // to_currency is USD
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.13d),
                        changelogRow("+I", "Yen", "US Dollar", 0.0082d),
                        changelogRow("-U", "Yen", "US Dollar", 0.0082d),
                        changelogRow("+I", "Singapore Dollar", "US Dollar", 0.74d),
                        changelogRow("+U", "Yen", "US Dollar", 0.0081d),
                        changelogRow("-D", "Yen", "US Dollar", 0.0081d),
                        // to_currency is Euro
                        changelogRow("+I", "US Dollar", "Euro", 0.9d),
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d),
                        changelogRow("-U", "Singapore Dollar", "Euro", 0.67d),
                        changelogRow("+U", "Singapore Dollar", "Euro", 0.69d),
                        // to_currency is Yen
                        changelogRow("+U", "Singapore Dollar", "Yen", 122.46d),
                        changelogRow("-U", "Singapore Dollar", "Yen", 122.46d),
                        changelogRow("+U", "Singapore Dollar", "Yen", 122d)));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                Collections.emptyList(), // partition
                Arrays.asList("from_currency", "to_currency"), // pk
                "rate_by_to_currency < 1 OR rate_by_to_currency > 100",
                Arrays.asList("from_currency", "to_currency"),
                Arrays.asList(
                        // to_currency is USD
                        changelogRow("+I", "HK Dollar", "US Dollar"),
                        changelogRow("+I", "Yen", "US Dollar"),
                        changelogRow("-U", "Yen", "US Dollar"),
                        changelogRow("+I", "Singapore Dollar", "US Dollar"),
                        changelogRow("+U", "Yen", "US Dollar"),
                        changelogRow("-D", "Yen", "US Dollar"),
                        // to_currency is Euro
                        changelogRow("+I", "US Dollar", "Euro"),
                        changelogRow("+I", "Singapore Dollar", "Euro"),
                        changelogRow("-U", "Singapore Dollar", "Euro"),
                        changelogRow("+U", "Singapore Dollar", "Euro"),
                        // to_currency is Yen
                        changelogRow("+U", "Singapore Dollar", "Yen"),
                        changelogRow("-U", "Singapore Dollar", "Yen"),
                        changelogRow("+U", "Singapore Dollar", "Yen")));
    }

    @Test
    public void testReadLatestChangelogOfMultiPartitionedRecordsWithOnePk() throws Exception {
        // input is hourlyRatesChangelogWithoutUB()
        List<Row> expectedRecords = new ArrayList<>(hourlyRatesChangelogWithoutUB().f0);
        expectedRecords.remove(changelogRow("+U", "Euro", 119L, "2022-01-02", "23"));
        expectedRecords.add(changelogRow("-U", "Euro", 114L, "2022-01-01", "15"));

        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("currency", "dt", "hh"), // pk
                null,
                Collections.emptyList(),
                expectedRecords);

        // test partition filter
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("currency", "dt", "hh"), // pk
                "dt >= '2022-01-02'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Euro", 119L, "2022-01-02", "23")));

        // test field filter
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("currency", "dt", "hh"), // pk
                "rate = 1",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "15"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01", "15")));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"), // partition
                Arrays.asList("currency", "dt", "hh"), // pk
                "rate = 1",
                Collections.singletonList("currency"),
                Arrays.asList(changelogRow("+I", "Yen"), changelogRow("-D", "Yen")));
    }

    @Test
    public void testReadLatestChangelogOfMultiPartitionedRecordsWithoutPk() throws Exception {
        // input is hourlyRatesChangelogWithUB()
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"), // partition
                Collections.emptyList(), // pk
                null,
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01, hh = 15
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01", "15"),
                        changelogRow("+I", "Euro", 116L, "2022-01-01", "15"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01", "15"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "15"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01", "15"),
                        // dt = 2022-01-02, hh = 20
                        changelogRow("+I", "Euro", 114L, "2022-01-02", "20"),
                        changelogRow("-D", "Euro", 114L, "2022-01-02", "20"),
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "20"),
                        changelogRow("-D", "Euro", 119L, "2022-01-02", "20"),
                        changelogRow("+I", "Euro", 115L, "2022-01-02", "20")));

        // test partition filter
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"), // partition
                Collections.emptyList(), // pk
                "hh <> '20'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01", "15"),
                        changelogRow("+I", "Euro", 116L, "2022-01-01", "15"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01", "15"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "15"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01", "15")));

        // test field filter
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"), // partition
                Collections.emptyList(), // pk
                "rate = 1",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "15"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01", "15")));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                Arrays.asList("dt", "hh"), // partition
                Collections.emptyList(), // pk
                "rate = 1",
                Collections.singletonList("currency"),
                Arrays.asList(changelogRow("+I", "Yen"), changelogRow("-D", "Yen")));
    }
}
