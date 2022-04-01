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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.store.kafka.KafkaTableTestBase;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.junit.Test;

import javax.annotation.Nullable;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.assertNoMoreRecords;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.buildBatchEnv;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.buildStreamEnv;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyExchangeRates;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyExchangeRatesChangelogWithoutUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyRates;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyRatesChangelogWithUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyRatesChangelogWithoutUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.exchangeRatesChangelogWithoutUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.hourlyExchangeRates;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.hourlyExchangeRatesChangelogWithoutUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.hourlyRates;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.hourlyRatesChangelogWithoutUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.prepareHelperSourceWithChangelogRecords;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.prepareHelperSourceWithInsertOnlyRecords;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.prepareInsertIntoQuery;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.prepareInsertOverwriteQuery;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.prepareManagedTableDdl;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.prepareSelectQuery;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.prepareSimpleSelectQuery;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.rates;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.LOG_SYSTEM;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.SCAN_PARALLELISM;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.SINK_PARALLELISM;
import static org.apache.flink.table.store.connector.TableStoreTestBase.createResolvedTable;
import static org.apache.flink.table.store.kafka.KafkaLogOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for managed table dml. */
public class ReadWriteTableITCase extends KafkaTableTestBase {

    private String rootPath;

    @Test
    public void testBatchWriteWithSinglePartitionedRecordsWithExclusiveOnePk() throws Exception {
        // input is dailyRates()
        List<Row> expectedRecords =
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02"));
        // test batch read
        String managedTable =
                collectAndCheckBatchReadWrite(
                        Collections.singletonList("dt"),
                        Arrays.asList("currency", "dt"),
                        null,
                        Collections.emptyList(),
                        expectedRecords);
        checkFileStorePath(tEnv, managedTable, dailyRates().f1);

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

        // overwrite static partition 2022-01-02
        prepareEnvAndOverwrite(
                managedTable,
                Collections.singletonMap("dt", "'2022-01-02'"),
                Arrays.asList(new String[] {"'Euro'", "100"}, new String[] {"'Yen'", "1"}));

        // streaming iter will not receive any changelog
        assertNoMoreRecords(streamIter);

        // batch read to check partition refresh
        collectAndCheck(
                        tEnv,
                        managedTable,
                        Collections.emptyMap(),
                        "dt = '2022-01-02'",
                        Arrays.asList(
                                // dt = 2022-01-02
                                changelogRow("+I", "Euro", 100L, "2022-01-02"),
                                changelogRow("+I", "Yen", 1L, "2022-01-02")))
                .close();

        // test partition filter
        List<Row> expected =
                Arrays.asList(
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"));
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                "dt <> '2022-01-02'",
                Collections.emptyList(),
                expected);

        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                "dt = '2022-01-01'",
                Collections.emptyList(),
                expected);

        // test field filter
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                "rate >= 100",
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test partition and field filter
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                "rate >= 100 AND dt = '2022-01-02'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test projection
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                null,
                Collections.singletonList("dt"),
                Arrays.asList(
                        changelogRow("+I", "2022-01-01"), // US Dollar
                        changelogRow("+I", "2022-01-01"), // Yen
                        changelogRow("+I", "2022-01-01"), // Euro
                        changelogRow("+I", "2022-01-02"))); // Euro

        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                null,
                Collections.singletonList("dt, currency, rate"),
                Arrays.asList(
                        changelogRow("+I", "2022-01-01", "US Dollar", 114L), // US Dollar
                        changelogRow("+I", "2022-01-01", "Yen", 1L), // Yen
                        changelogRow("+I", "2022-01-01", "Euro", 114L), // Euro
                        changelogRow("+I", "2022-01-02", "Euro", 119L))); // Euro

        // test projection and filter
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                "rate = 114",
                Collections.singletonList("rate"),
                Arrays.asList(
                        changelogRow("+I", 114L), // US Dollar
                        changelogRow("+I", 114L) // Euro
                        ));
    }

    @Test
    public void testBatchWriteWithSinglePartitionedRecordsWithoutPk() throws Exception {
        // input is dailyRates()

        // test batch read
        String managedTable =
                collectAndCheckBatchReadWrite(
                        Collections.singletonList("dt"),
                        Collections.emptyList(),
                        null,
                        Collections.emptyList(),
                        dailyRates().f0);
        checkFileStorePath(tEnv, managedTable, dailyRates().f1);

        // overwrite dynamic partition
        prepareEnvAndOverwrite(
                managedTable,
                Collections.emptyMap(),
                Arrays.asList(
                        new String[] {"'Euro'", "90", "'2022-01-01'"},
                        new String[] {"'Yen'", "2", "'2022-01-02'"}));

        // test streaming read
        final StreamTableEnvironment streamTableEnv =
                StreamTableEnvironment.create(buildStreamEnv());
        registerTable(streamTableEnv, managedTable);
        collectAndCheck(
                        streamTableEnv,
                        managedTable,
                        Collections.emptyMap(),
                        null,
                        Arrays.asList(
                                // dt = 2022-01-01
                                changelogRow("+I", "Euro", 90L, "2022-01-01"),
                                // dt = 2022-01-02
                                changelogRow("+I", "Yen", 2L, "2022-01-02")))
                .close();

        // test partition filter
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Collections.emptyList(),
                "dt >= '2022-01-01'",
                Collections.emptyList(),
                dailyRates().f0);

        // test field filter
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Collections.emptyList(),
                "currency = 'US Dollar'",
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01")));

        // test partition and field filter
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Collections.emptyList(),
                "dt = '2022-01-01' OR rate > 115",
                Collections.emptyList(),
                dailyRates().f0);

        // test projection
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Collections.emptyList(),
                null,
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro")));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                Collections.singletonList("dt"),
                Collections.emptyList(),
                "rate = 119",
                Arrays.asList("currency", "dt"),
                Collections.singletonList(changelogRow("+I", "Euro", "2022-01-02")));
    }

    @Test
    public void testBatchWriteWithNonPartitionedRecordsWithOnePk() throws Exception {
        // input is rates()
        String managedTable =
                collectAndCheckBatchReadWrite(
                        Collections.emptyList(),
                        Collections.singletonList("currency"),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Yen", 1L),
                                changelogRow("+I", "Euro", 119L)));
        checkFileStorePath(tEnv, managedTable, null);

        // overwrite the whole table
        prepareEnvAndOverwrite(
                managedTable,
                Collections.emptyMap(),
                Collections.singletonList(new String[] {"'Euro'", "100"}));
        collectAndCheck(
                tEnv,
                managedTable,
                Collections.emptyMap(),
                null,
                Collections.singletonList(changelogRow("+I", "Euro", 100L)));

        // test field filter
        collectAndCheckBatchReadWrite(
                Collections.emptyList(),
                Collections.singletonList("currency"),
                "currency = 'Euro'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Euro", 119L)));

        collectAndCheckBatchReadWrite(
                Collections.emptyList(),
                Collections.singletonList("currency"),
                "119 >= rate AND 102 < rate",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Euro", 119L)));

        // test projection
        collectAndCheckBatchReadWrite(
                Collections.emptyList(),
                Collections.singletonList("currency"),
                null,
                Arrays.asList("rate", "currency"),
                Arrays.asList(
                        changelogRow("+I", 102L, "US Dollar"),
                        changelogRow("+I", 1L, "Yen"),
                        changelogRow("+I", 119L, "Euro")));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                Collections.emptyList(),
                Collections.singletonList("currency"),
                "currency = 'Yen'",
                Collections.singletonList("rate"),
                Collections.singletonList(changelogRow("+I", 1L)));
    }

    @Test
    public void testBatchWriteNonPartitionedRecordsWithoutPk() throws Exception {
        // input is rates()
        String managedTable =
                collectAndCheckBatchReadWrite(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        null,
                        Collections.emptyList(),
                        rates());
        checkFileStorePath(tEnv, managedTable, null);

        // test field filter
        collectAndCheckBatchReadWrite(
                Collections.emptyList(),
                Collections.emptyList(),
                "currency = 'Euro'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 119L)));

        collectAndCheckBatchReadWrite(
                Collections.emptyList(),
                Collections.emptyList(),
                "rate >= 1",
                Collections.emptyList(),
                rates());

        // test projection
        collectAndCheckBatchReadWrite(
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "US Dollar")));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                Collections.emptyList(),
                Collections.emptyList(),
                "rate > 100 OR currency = 'Yen'",
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "US Dollar")));
    }

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
    public void testBatchWriteMultiPartitionedRecordsWithExclusiveOnePk() throws Exception {
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
    public void testEnableLogAndStreamingReadWriteSinglePartitionedRecordsWithExclusiveOnePk()
            throws Exception {
        // input is dailyRatesChangelogWithoutUB()
        // test hybrid read
        Tuple2<String, BlockingIterator<Row, Row>> tuple =
                collectAndCheckStreamingReadWriteWithoutClose(
                        Collections.singletonList("dt"),
                        Arrays.asList("currency", "dt"),
                        Collections.emptyMap(),
                        "dt >= '2022-01-01' AND dt <= '2022-01-03' OR currency = 'HK Dollar'",
                        Collections.emptyList(),
                        Arrays.asList(
                                // dt = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // dt = 2022-01-02
                                changelogRow("+I", "Euro", 119L, "2022-01-02")));
        String managedTable = tuple.f0;
        checkFileStorePath(tEnv, managedTable, dailyRatesChangelogWithoutUB().f1);
        BlockingIterator<Row, Row> streamIter = tuple.f1;

        // test log store in hybrid mode accepts all filters
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s` PARTITION (dt = '2022-01-03')\n"
                                        + "VALUES('HK Dollar', 100), ('Yen', 20)\n",
                                managedTable))
                .await();

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s` PARTITION (dt = '2022-01-04')\n"
                                        + "VALUES('Yen', 20)\n",
                                managedTable))
                .await();

        assertThat(streamIter.collect(2, 10, TimeUnit.SECONDS))
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList(
                                changelogRow("+I", "HK Dollar", 100L, "2022-01-03"),
                                changelogRow("+I", "Yen", 20L, "2022-01-03")));

        // overwrite partition 2022-01-02
        prepareEnvAndOverwrite(
                managedTable,
                Collections.singletonMap("dt", "'2022-01-02'"),
                Arrays.asList(new String[] {"'Euro'", "100"}, new String[] {"'Yen'", "1"}));

        // batch read to check data refresh
        final StreamTableEnvironment batchTableEnv =
                StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        registerTable(batchTableEnv, managedTable);
        collectAndCheck(
                batchTableEnv,
                managedTable,
                Collections.emptyMap(),
                null,
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 100L, "2022-01-02"),
                        changelogRow("+I", "Yen", 1L, "2022-01-02"),
                        // part = 2022-01-03
                        changelogRow("+I", "HK Dollar", 100L, "2022-01-03"),
                        changelogRow("+I", "Yen", 20L, "2022-01-03"),
                        // part = 2022-01-04
                        changelogRow("+I", "Yen", 20L, "2022-01-04")));

        // check no changelog generated for streaming read
        assertNoMoreRecords(streamIter);

        // filter on partition
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                Collections.emptyMap(),
                "dt = '2022-01-01'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                Collections.emptyMap(),
                "currency = 'US Dollar'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                Collections.emptyMap(),
                "dt = '2022-01-01' AND rate = 1",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                Collections.emptyMap(),
                "dt = '2022-01-02' AND currency = 'Euro'",
                Arrays.asList("rate", "dt", "currency"),
                Collections.singletonList(changelogRow("+I", 119L, "2022-01-02", "Euro")));
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
                        changelogRow("+I", "US Dollar", "Euro", 0.9, "2022-01-02")));

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
    public void testDisableLogAndStreamingReadWriteSinglePartitionedRecordsWithExclusiveOnePk()
            throws Exception {
        // input is dailyRatesChangelogWithoutUB()
        // file store continuous read
        // will not merge, at least collect two records
        checkFileStorePath(
                tEnv,
                collectAndCheckStreamingReadWriteWithClose(
                        false,
                        Collections.singletonList("dt"),
                        Arrays.asList("currency", "dt"),
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                // dt = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // dt = 2022-01-02
                                changelogRow("+I", "Euro", 119L, "2022-01-02"))),
                dailyRatesChangelogWithoutUB().f1);

        // test partition filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                Collections.emptyMap(),
                "dt < '2022-01-02'",
                Collections.emptyList(),
                Collections.singletonList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                Collections.emptyMap(),
                "rate = 102",
                Collections.emptyList(),
                Collections.singletonList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                Collections.emptyMap(),
                "rate = 102 or dt < '2022-01-02'",
                Collections.emptyList(),
                Collections.singletonList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                Collections.emptyMap(),
                "rate = 102 or dt < '2022-01-02'",
                Collections.singletonList("currency"),
                Collections.singletonList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar")));
    }

    @Test
    public void testStreamingReadWriteSinglePartitionedRecordsWithoutPk() throws Exception {
        // input is dailyRatesChangelogWithUB()
        // enable log store, file store bounded read with merge
        checkFileStorePath(
                tEnv,
                collectAndCheckStreamingReadWriteWithClose(
                        true,
                        Collections.singletonList("dt"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                // dt = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // dt = 2022-01-02
                                changelogRow("+I", "Euro", 115L, "2022-01-02"))),
                dailyRatesChangelogWithUB().f1);

        // test partition filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Collections.emptyList(),
                Collections.emptyMap(),
                "dt IS NOT NULL",
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));

        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Collections.emptyList(),
                Collections.emptyMap(),
                "dt IS NULL",
                Collections.emptyList(),
                Collections.emptyList());

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Collections.emptyList(),
                Collections.emptyMap(),
                "currency = 'US Dollar' OR rate = 115",
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));

        // test partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Collections.emptyList(),
                Collections.emptyMap(),
                "(dt = '2022-01-02' AND currency = 'US Dollar') OR (dt = '2022-01-01' AND rate = 115)",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Collections.emptyList(),
                Collections.emptyMap(),
                null,
                Collections.singletonList("rate"),
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", 102L),
                        // dt = 2022-01-02
                        changelogRow("+I", 115L)));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.singletonList("dt"),
                Collections.emptyList(),
                Collections.emptyMap(),
                "dt <> '2022-01-01'",
                Collections.singletonList("rate"),
                Collections.singletonList(
                        // dt = 2022-01-02, Euro
                        changelogRow("+I", 115L)));
    }

    @Test
    public void testStreamingReadWriteNonPartitionedRecordsWithExclusiveOnePk() throws Exception {
        // input is ratesChangelogWithoutUB()
        // enable log store, file store bounded read with merge
        checkFileStorePath(
                tEnv,
                collectAndCheckStreamingReadWriteWithClose(
                        true,
                        Collections.emptyList(),
                        Collections.singletonList("currency"),
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 119L))),
                null);

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                Collections.emptyMap(),
                "currency = 'Yen'",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                Collections.emptyMap(),
                null,
                Collections.singletonList("currency"),
                Arrays.asList(changelogRow("+I", "US Dollar"), changelogRow("+I", "Euro")));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                Collections.emptyMap(),
                "rate = 102",
                Collections.singletonList("currency"),
                Collections.singletonList(changelogRow("+I", "US Dollar")));
    }

    @Test
    public void testStreamingReadWriteNonPartitionedRecordsWithoutPk() throws Exception {
        // input is ratesChangelogWithUB()
        // enable log store, with default full scan mode, will merge
        checkFileStorePath(
                tEnv,
                collectAndCheckStreamingReadWriteWithClose(
                        true,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 119L),
                                changelogRow("+I", null, 100L),
                                changelogRow("+I", "HK Dollar", null))),
                null);

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                "currency IS NOT NULL",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("+I", "HK Dollar", null)));

        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                "rate IS NOT NULL",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("+I", null, 100L)));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                "currency IS NOT NULL AND rate is NOT NULL",
                Collections.singletonList("rate"),
                Arrays.asList(
                        changelogRow("+I", 102L), // US Dollar
                        changelogRow("+I", 119L)) // Euro
                );
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
    public void testReadLatestChangelogOfMultiPartitionedRecordsWithExclusiveOnePk()
            throws Exception {
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

    @Test
    public void testReadLatestChangelogOfSinglePartitionedRecordsWithExclusiveOnePk()
            throws Exception {
        // input is dailyRatesChangelogWithoutUB()
        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                null,
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-U", "Euro", 114L, "2022-01-01"),
                        changelogRow("+U", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test partition filter
        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                "dt = '2022-01-01'",
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-U", "Euro", 114L, "2022-01-01"),
                        changelogRow("+U", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01")));

        // test field filter
        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                "currency = 'Yen'",
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01")));

        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                "rate = 114",
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("-U", "Euro", 114L, "2022-01-01")));

        // test partition and field filter
        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                "rate = 114 AND dt = '2022-01-02'",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection
        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                null,
                Collections.singletonList("rate"),
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", 102L), // US Dollar
                        changelogRow("+I", 114L), // Euro
                        changelogRow("+I", 1L), // Yen
                        changelogRow("-U", 114L), // Euro
                        changelogRow("+U", 116L), // Euro
                        changelogRow("-D", 1L), // Yen
                        changelogRow("-D", 116L), // Euro
                        // dt = 2022-01-02
                        changelogRow("+I", 119L) // Euro
                        ));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                "dt = '2022-01-02'",
                Collections.singletonList("rate"),
                Collections.singletonList(
                        // dt = 2022-01-02, Euro
                        changelogRow("+I", 119L)));
    }

    @Test
    public void testReadLatestChangelogOfSinglePartitionedRecordsWithoutPk() throws Exception {
        // input is dailyRatesChangelogWithUB()
        collectLatestLogAndCheck(
                false,
                Collections.singletonList("dt"),
                Collections.emptyList(),
                null,
                Collections.emptyList(),
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 114L, "2022-01-02"),
                        changelogRow("-D", "Euro", 114L, "2022-01-02"),
                        changelogRow("+I", "Euro", 119L, "2022-01-02"),
                        changelogRow("-D", "Euro", 119L, "2022-01-02"),
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));
    }

    @Test
    public void testReadLatestChangelogOfNonPartitionedRecordsWithExclusiveOnePk()
            throws Exception {
        // input is ratesChangelogWithoutUB()
        collectLatestLogAndCheck(
                false,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                null,
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-U", "Euro", 114L),
                        changelogRow("+U", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Yen", 1L)));

        // test field filter
        collectLatestLogAndCheck(
                false,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                "currency = 'Euro'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("-U", "Euro", 114L),
                        changelogRow("+U", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L)));

        // test projection
        collectLatestLogAndCheck(
                false,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                null,
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("-D", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("-D", "Yen")));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                "currency = 'Euro'",
                Collections.singletonList("rate"),
                Arrays.asList(
                        changelogRow("+I", 114L),
                        changelogRow("-U", 114L),
                        changelogRow("+U", 116L),
                        changelogRow("-D", 116L),
                        changelogRow("+I", 119L)));
    }

    @Test
    public void testReadLatestChangelogOfNonPartitionedRecordsWithoutPk() throws Exception {
        // input is ratesChangelogWithUB()
        collectLatestLogAndCheck(
                false,
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-D", "Euro", 114L),
                        changelogRow("+I", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Euro", 119L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Yen", 1L),
                        changelogRow("+I", null, 100L),
                        changelogRow("+I", "HK Dollar", null)));

        // test field filter
        collectLatestLogAndCheck(
                false,
                Collections.emptyList(),
                Collections.emptyList(),
                "currency = 'Euro'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("-D", "Euro", 114L),
                        changelogRow("+I", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Euro", 119L),
                        changelogRow("+I", "Euro", 119L)));

        // test projection
        collectLatestLogAndCheck(
                false,
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                Arrays.asList("currency", "rate"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-D", "Euro", 114L),
                        changelogRow("+I", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Euro", 119L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Yen", 1L),
                        changelogRow("+I", null, 100L),
                        changelogRow("+I", "HK Dollar", null)));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                Collections.emptyList(),
                Collections.emptyList(),
                "currency IS NOT NULL",
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("-D", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("-D", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("-D", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("-D", "Yen"),
                        changelogRow("+I", "HK Dollar")));

        collectLatestLogAndCheck(
                false,
                Collections.emptyList(),
                Collections.emptyList(),
                "rate = 119",
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "Euro"),
                        changelogRow("-D", "Euro"),
                        changelogRow("+I", "Euro")));
    }

    @Test
    public void testReadLatestChangelogOfInsertOnlyRecords() throws Exception {
        // input is rates()
        List<Row> expected =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-U", "Euro", 114L),
                        changelogRow("+U", "Euro", 119L));

        // currency as pk
        collectLatestLogAndCheck(
                true,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                null,
                Collections.emptyList(),
                expected);

        // without pk
        collectLatestLogAndCheck(
                true,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                null,
                Collections.emptyList(),
                expected);

        // test field filter
        collectLatestLogAndCheck(
                true,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                "rate = 114",
                Collections.emptyList(),
                Arrays.asList(changelogRow("+I", "Euro", 114L), changelogRow("-U", "Euro", 114L)));

        // test projection
        collectLatestLogAndCheck(
                true,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                null,
                Collections.singletonList("rate"),
                Arrays.asList(
                        changelogRow("+I", 102L),
                        changelogRow("+I", 114L),
                        changelogRow("+I", 1L),
                        changelogRow("-U", 114L),
                        changelogRow("+U", 119L)));

        // test projection and filter
        collectLatestLogAndCheck(
                true,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                "rate = 114",
                Collections.singletonList("currency"),
                Arrays.asList(changelogRow("+I", "Euro"), changelogRow("-U", "Euro")));
    }

    @Test
    public void testReadInsertOnlyChangelogFromTimestamp() throws Exception {
        // input is dailyRates()
        collectChangelogFromTimestampAndCheck(
                true,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                0,
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("-U", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+U", "US Dollar", 114L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));

        collectChangelogFromTimestampAndCheck(
                true, Collections.singletonList("dt"), Collections.emptyList(), 0, dailyRates().f0);

        // input is rates()
        collectChangelogFromTimestampAndCheck(
                true,
                Collections.emptyList(),
                Collections.singletonList("currency"),
                0,
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-U", "Euro", 114L),
                        changelogRow("+U", "Euro", 119L)));

        collectChangelogFromTimestampAndCheck(
                true, Collections.emptyList(), Collections.emptyList(), 0, rates());

        collectChangelogFromTimestampAndCheck(
                true,
                Collections.emptyList(),
                Collections.emptyList(),
                Long.MAX_VALUE - 1,
                Collections.emptyList());
    }

    @Test
    public void testReadRetractChangelogFromTimestamp() throws Exception {
        // input is dailyRatesChangelogWithUB()
        collectChangelogFromTimestampAndCheck(
                false,
                Collections.singletonList("dt"),
                Collections.emptyList(),
                0,
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 114L, "2022-01-02"),
                        changelogRow("-D", "Euro", 114L, "2022-01-02"),
                        changelogRow("+I", "Euro", 119L, "2022-01-02"),
                        changelogRow("-D", "Euro", 119L, "2022-01-02"),
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));

        // input is dailyRatesChangelogWithoutUB()
        collectChangelogFromTimestampAndCheck(
                false,
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                0,
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-U", "Euro", 114L, "2022-01-01"),
                        changelogRow("+U", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));
    }

    @Test
    public void testSourceParallelism() throws Exception {
        String managedTable =
                createSourceAndManagedTable(
                                false,
                                false,
                                false,
                                Collections.emptyList(),
                                Collections.emptyList())
                        .f1;

        // without hint
        String query = prepareSimpleSelectQuery(managedTable, Collections.emptyMap());
        assertThat(sourceParallelism(query)).isEqualTo(env.getParallelism());

        // with hint
        query =
                prepareSimpleSelectQuery(
                        managedTable, Collections.singletonMap(SCAN_PARALLELISM.key(), "66"));
        assertThat(sourceParallelism(query)).isEqualTo(66);
    }

    @Test
    public void testSinkParallelism() {
        testSinkParallelism(null, env.getParallelism());
        testSinkParallelism(23, 23);
    }

    // ------------------------ Tools ----------------------------------

    private String collectAndCheckBatchReadWrite(
            List<String> partitions,
            List<String> primaryKeys,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        return collectAndCheckUnderSameEnv(
                        false,
                        false,
                        true,
                        partitions,
                        primaryKeys,
                        true,
                        Collections.emptyMap(),
                        filter,
                        projection,
                        expected)
                .f0;
    }

    private String collectAndCheckStreamingReadWriteWithClose(
            boolean enableLogStore,
            List<String> partitions,
            List<String> primaryKeys,
            Map<String, String> readHints,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        Tuple2<String, BlockingIterator<Row, Row>> tuple =
                collectAndCheckUnderSameEnv(
                        true,
                        enableLogStore,
                        false,
                        partitions,
                        primaryKeys,
                        true,
                        readHints,
                        filter,
                        projection,
                        expected);
        tuple.f1.close();
        return tuple.f0;
    }

    private Tuple2<String, BlockingIterator<Row, Row>>
            collectAndCheckStreamingReadWriteWithoutClose(
                    List<String> partitions,
                    List<String> primaryKeys,
                    Map<String, String> readHints,
                    @Nullable String filter,
                    List<String> projection,
                    List<Row> expected)
                    throws Exception {
        return collectAndCheckUnderSameEnv(
                true,
                true,
                false,
                partitions,
                primaryKeys,
                true,
                readHints,
                filter,
                projection,
                expected);
    }

    private void collectLatestLogAndCheck(
            boolean insertOnly,
            List<String> partitionKeys,
            List<String> primaryKeys,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        Map<String, String> hints = new HashMap<>();
        hints.put(
                LOG_PREFIX + LogOptions.SCAN.key(),
                LogOptions.LogStartupMode.LATEST.name().toLowerCase());
        collectAndCheckUnderSameEnv(
                        true,
                        true,
                        insertOnly,
                        partitionKeys,
                        primaryKeys,
                        false,
                        hints,
                        filter,
                        projection,
                        expected)
                .f1
                .close();
    }

    private void collectChangelogFromTimestampAndCheck(
            boolean insertOnly,
            List<String> partitionKeys,
            List<String> primaryKeys,
            long timestamp,
            List<Row> expected)
            throws Exception {
        Map<String, String> hints = new HashMap<>();
        hints.put(LOG_PREFIX + LogOptions.SCAN.key(), "from-timestamp");
        hints.put(LOG_PREFIX + LogOptions.SCAN_TIMESTAMP_MILLS.key(), String.valueOf(timestamp));
        collectAndCheckUnderSameEnv(
                        true,
                        true,
                        insertOnly,
                        partitionKeys,
                        primaryKeys,
                        true,
                        hints,
                        null,
                        Collections.emptyList(),
                        expected)
                .f1
                .close();
    }

    private Tuple2<String, String> createSourceAndManagedTable(
            boolean streaming,
            boolean enableLogStore,
            boolean insertOnly,
            List<String> partitionKeys,
            List<String> primaryKeys)
            throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        rootPath = TEMPORARY_FOLDER.newFolder().getPath();
        tableOptions.put(FileStoreOptions.FILE_PATH.key(), rootPath);
        if (enableLogStore) {
            tableOptions.put(LOG_SYSTEM.key(), "kafka");
            tableOptions.put(LOG_PREFIX + BOOTSTRAP_SERVERS.key(), getBootstrapServers());
        }
        String sourceTable = "source_table_" + UUID.randomUUID();
        String managedTable = "managed_table_" + UUID.randomUUID();
        EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance().inStreamingMode();
        String helperTableDdl;
        if (streaming) {
            helperTableDdl =
                    insertOnly
                            ? prepareHelperSourceWithInsertOnlyRecords(
                                    sourceTable, partitionKeys, primaryKeys)
                            : prepareHelperSourceWithChangelogRecords(
                                    sourceTable, partitionKeys, primaryKeys);
            env = buildStreamEnv();
            builder.inStreamingMode();
        } else {
            helperTableDdl =
                    prepareHelperSourceWithInsertOnlyRecords(
                            sourceTable, partitionKeys, primaryKeys);
            env = buildBatchEnv();
            builder.inBatchMode();
        }
        String managedTableDdl = prepareManagedTableDdl(sourceTable, managedTable, tableOptions);

        tEnv = StreamTableEnvironment.create(env, builder.build());
        tEnv.executeSql(helperTableDdl);
        tEnv.executeSql(managedTableDdl);
        return new Tuple2<>(sourceTable, managedTable);
    }

    private Tuple2<String, BlockingIterator<Row, Row>> collectAndCheckUnderSameEnv(
            boolean streaming,
            boolean enableLogStore,
            boolean insertOnly,
            List<String> partitionKeys,
            List<String> primaryKeys,
            boolean writeFirst,
            Map<String, String> readHints,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        Tuple2<String, String> tables =
                createSourceAndManagedTable(
                        streaming, enableLogStore, insertOnly, partitionKeys, primaryKeys);
        String sourceTable = tables.f0;
        String managedTable = tables.f1;

        String insertQuery = prepareInsertIntoQuery(sourceTable, managedTable);
        String selectQuery = prepareSelectQuery(managedTable, readHints, filter, projection);

        BlockingIterator<Row, Row> iterator;
        if (writeFirst) {
            tEnv.executeSql(insertQuery).await();
            iterator = BlockingIterator.of(tEnv.executeSql(selectQuery).collect());
        } else {
            iterator = BlockingIterator.of(tEnv.executeSql(selectQuery).collect());
            tEnv.executeSql(insertQuery).await();
        }
        if (expected.isEmpty()) {
            assertNoMoreRecords(iterator);
        } else {
            assertThat(iterator.collect(expected.size(), 10, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
        return Tuple2.of(managedTable, iterator);
    }

    private void prepareEnvAndOverwrite(
            String managedTable,
            Map<String, String> staticPartitions,
            List<String[]> overwriteRecords)
            throws Exception {
        prepareEnvAndOverwrite(
                managedTable,
                prepareInsertOverwriteQuery(managedTable, staticPartitions, overwriteRecords));
    }

    private void prepareEnvAndOverwrite(String managedTable, String query) throws Exception {
        final StreamTableEnvironment batchEnv =
                StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        registerTable(batchEnv, managedTable);
        batchEnv.executeSql(query).await();
    }

    private void registerTable(StreamTableEnvironment tEnvToRegister, String managedTable)
            throws Exception {
        String cat = this.tEnv.getCurrentCatalog();
        String db = this.tEnv.getCurrentDatabase();
        ObjectPath objectPath = new ObjectPath(db, managedTable);
        CatalogBaseTable table = this.tEnv.getCatalog(cat).get().getTable(objectPath);
        tEnvToRegister.getCatalog(cat).get().createTable(objectPath, table, false);
    }

    private BlockingIterator<Row, Row> collect(
            StreamTableEnvironment tEnv, String selectQuery, int expectedSize, List<Row> actual)
            throws Exception {
        TableResult result = tEnv.executeSql(selectQuery);
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(result.collect());
        actual.addAll(iterator.collect(expectedSize));
        return iterator;
    }

    private BlockingIterator<Row, Row> collectAndCheck(
            StreamTableEnvironment tEnv,
            String managedTable,
            Map<String, String> hints,
            @Nullable String filter,
            List<Row> expectedRecords)
            throws Exception {
        List<Row> actual = new ArrayList<>();
        BlockingIterator<Row, Row> iterator =
                collect(
                        tEnv,
                        filter == null
                                ? prepareSimpleSelectQuery(managedTable, hints)
                                : prepareSelectQuery(
                                        managedTable, hints, filter, Collections.emptyList()),
                        expectedRecords.size(),
                        actual);
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRecords);
        return iterator;
    }

    private void checkFileStorePath(
            StreamTableEnvironment tEnv, String managedTable, @Nullable String partitionList) {
        String relativeFilePath =
                FileStoreOptions.relativeTablePath(
                        ObjectIdentifier.of(
                                tEnv.getCurrentCatalog(), tEnv.getCurrentDatabase(), managedTable));
        // check snapshot file path
        assertThat(Paths.get(rootPath, relativeFilePath, "snapshot")).exists();
        // check manifest file path
        assertThat(Paths.get(rootPath, relativeFilePath, "manifest")).exists();
        // check sst file path
        if (partitionList == null) {
            // at least exists bucket-0
            assertThat(Paths.get(rootPath, relativeFilePath, "bucket-0")).exists();
        } else {
            Arrays.stream(partitionList.split(";"))
                    .map(str -> str.replaceAll(":", "="))
                    .map(str -> str.replaceAll(",", "/"))
                    .map(str -> str.replaceAll("null", "__DEFAULT_PARTITION__"))
                    .collect(Collectors.toList())
                    .forEach(
                            partition -> {
                                assertThat(Paths.get(rootPath, relativeFilePath, partition))
                                        .exists();
                                assertThat(
                                                Paths.get(
                                                        rootPath,
                                                        relativeFilePath,
                                                        partition,
                                                        "bucket-0"))
                                        .exists();
                            });
        }
    }

    private int sourceParallelism(String sql) {
        DataStream<Row> stream = tEnv.toChangelogStream(tEnv.sqlQuery(sql));
        return stream.getParallelism();
    }

    private void testSinkParallelism(Integer configParallelism, int expectedParallelism) {
        // 1. create a mock table sink
        Map<String, String> options = new HashMap<>();
        if (configParallelism != null) {
            options.put(SINK_PARALLELISM.key(), configParallelism.toString());
        }

        DynamicTableFactory.Context context =
                new FactoryUtil.DefaultDynamicTableContext(
                        ObjectIdentifier.of("default", "default", "t1"),
                        createResolvedTable(
                                options,
                                RowType.of(
                                        new LogicalType[] {new VarCharType(Integer.MAX_VALUE)},
                                        new String[] {"a"}),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        Collections.emptyMap(),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        DynamicTableSink tableSink = new TableStoreFactory().createDynamicTableSink(context);
        assertThat(tableSink).isInstanceOf(TableStoreSink.class);

        // 2. get sink provider
        DynamicTableSink.SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider).isInstanceOf(DataStreamSinkProvider.class);
        DataStreamSinkProvider sinkProvider = (DataStreamSinkProvider) provider;

        // 3. assert parallelism from transformation
        DataStream<RowData> mockSource =
                env.fromCollection(Collections.singletonList(GenericRowData.of()));
        DataStreamSink<?> sink = sinkProvider.consumeDataStream(null, mockSource);
        Transformation<?> transformation = sink.getTransformation();
        // until a PartitionTransformation, see TableStore.SinkBuilder.build()
        while (!(transformation instanceof PartitionTransformation)) {
            assertThat(transformation.getParallelism()).isIn(1, expectedParallelism);
            transformation = transformation.getInputs().get(0);
        }
    }
}
