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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.junit.Test;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.planner.factories.TestValuesTableFactory.registerData;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.ROOT_PATH;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.SCAN_PARALLELISM;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyRates;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyRatesChangelogWithUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.dailyRatesChangelogWithoutUB;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.rates;
import static org.apache.flink.table.store.connector.ShowCreateUtil.buildSimpleSelectQuery;
import static org.apache.flink.table.store.connector.TableStoreTestBase.createResolvedTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for managed table dml. */
public class ReadWriteTableITCase extends ReadWriteTableTestBase {

    @Test
    public void testBatchWriteWithPartitionedRecordsWithPk() throws Exception {
        // input is dailyRates()
        List<Row> expectedRecords =
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02"));
        // test batch read
        String managedTable =
                collectAndCheckBatchReadWrite(
                        true, true, null, Collections.emptyList(), expectedRecords);
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
                        "dt IN ('2022-01-02')",
                        Arrays.asList(
                                // part = 2022-01-01
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
                true, true, "dt <> '2022-01-02'", Collections.emptyList(), expected);

        collectAndCheckBatchReadWrite(
                true, true, "dt IN ('2022-01-01')", Collections.emptyList(), expected);

        // test field filter
        collectAndCheckBatchReadWrite(
                true,
                true,
                "rate >= 100",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test partition and field filter
        collectAndCheckBatchReadWrite(
                true,
                true,
                "rate >= 100 AND dt = '2022-01-02'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test projection
        collectAndCheckBatchReadWrite(
                true,
                true,
                null,
                Collections.singletonList("dt"),
                Arrays.asList(
                        changelogRow("+I", "2022-01-01"), // US Dollar
                        changelogRow("+I", "2022-01-01"), // Yen
                        changelogRow("+I", "2022-01-01"), // Euro
                        changelogRow("+I", "2022-01-02"))); // Euro

        collectAndCheckBatchReadWrite(
                true,
                true,
                null,
                Collections.singletonList("dt, currency, rate"),
                Arrays.asList(
                        changelogRow("+I", "2022-01-01", "US Dollar", 114L), // US Dollar
                        changelogRow("+I", "2022-01-01", "Yen", 1L), // Yen
                        changelogRow("+I", "2022-01-01", "Euro", 114L), // Euro
                        changelogRow("+I", "2022-01-02", "Euro", 119L))); // Euro

        // test projection and filter
        collectAndCheckBatchReadWrite(
                true,
                true,
                "rate = 114",
                Collections.singletonList("rate"),
                Arrays.asList(
                        changelogRow("+I", 114L), // US Dollar
                        changelogRow("+I", 114L) // Euro
                        ));
    }

    @Test
    public void testBatchWriteWithPartitionedRecordsWithoutPk() throws Exception {
        // input is dailyRates()

        // test batch read
        String managedTable =
                collectAndCheckBatchReadWrite(
                        true, false, null, Collections.emptyList(), dailyRates().f0);
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
                        "dt IN ('2022-01-01', '2022-01-02')",
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "Euro", 90L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Yen", 2L, "2022-01-02")))
                .close();

        // test partition filter
        collectAndCheckBatchReadWrite(
                true, false, "dt >= '2022-01-01'", Collections.emptyList(), dailyRates().f0);

        // test field filter
        collectAndCheckBatchReadWrite(
                true,
                false,
                "currency = 'US Dollar'",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01")));

        // test partition and field filter
        collectAndCheckBatchReadWrite(
                true,
                false,
                "dt = '2022-01-01' OR rate > 115",
                Collections.emptyList(),
                dailyRates().f0);

        // test projection
        collectAndCheckBatchReadWrite(
                true,
                false,
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
                true,
                false,
                "rate = 119",
                Arrays.asList("currency", "dt"),
                Collections.singletonList(changelogRow("+I", "Euro", "2022-01-02")));
    }

    @Test
    public void testBatchWriteWithNonPartitionedRecordsWithPk() throws Exception {
        // input is rates()
        String managedTable =
                collectAndCheckBatchReadWrite(
                        false,
                        true,
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
                false,
                true,
                "currency = 'Euro'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Euro", 119L)));

        collectAndCheckBatchReadWrite(
                false,
                true,
                "119 >= rate AND 102 < rate",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Euro", 119L)));

        // test projection
        collectAndCheckBatchReadWrite(
                false,
                true,
                null,
                Arrays.asList("rate", "currency"),
                Arrays.asList(
                        changelogRow("+I", 102L, "US Dollar"),
                        changelogRow("+I", 1L, "Yen"),
                        changelogRow("+I", 119L, "Euro")));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                false,
                true,
                "currency IN ('Yen')",
                Collections.singletonList("rate"),
                Collections.singletonList(changelogRow("+I", 1L)));
    }

    @Test
    public void testBatchWriteNonPartitionedRecordsWithoutPk() throws Exception {
        // input is rates()
        String managedTable =
                collectAndCheckBatchReadWrite(false, false, null, Collections.emptyList(), rates());
        checkFileStorePath(tEnv, managedTable, null);

        // test field filter
        collectAndCheckBatchReadWrite(
                false,
                false,
                "currency = 'Euro'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 119L)));

        collectAndCheckBatchReadWrite(false, false, "rate >= 1", Collections.emptyList(), rates());

        // test projection
        collectAndCheckBatchReadWrite(
                false,
                false,
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
                false,
                false,
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
    public void testEnableLogAndStreamingReadWritePartitionedRecordsWithPk() throws Exception {
        // input is dailyRatesChangelogWithoutUB()
        // test hybrid read
        Tuple2<String, BlockingIterator<Row, Row>> tuple =
                collectAndCheckStreamingReadWriteWithoutClose(
                        Collections.emptyMap(),
                        "dt >= '2022-01-01' AND dt <= '2022-01-03' OR currency = 'HK Dollar'",
                        Collections.emptyList(),
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // part = 2022-01-02
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
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-02
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
                true,
                true,
                Collections.emptyMap(),
                "dt = '2022-01-01'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                true,
                Collections.emptyMap(),
                "currency = 'US Dollar'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                true,
                Collections.emptyMap(),
                "dt = '2022-01-01' AND rate = 1",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                true,
                Collections.emptyMap(),
                "dt = '2022-01-02' AND currency = 'Euro'",
                Arrays.asList("rate", "dt", "currency"),
                Collections.singletonList(changelogRow("+I", 119L, "2022-01-02", "Euro")));
    }

    @Test
    public void testDisableLogAndStreamingReadWritePartitionedRecordsWithPk() throws Exception {
        // input is dailyRatesChangelogWithoutUB()
        // file store continuous read
        // will not merge, at least collect two records
        checkFileStorePath(
                tEnv,
                collectAndCheckStreamingReadWriteWithClose(
                        false,
                        true,
                        true,
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Euro", 119L, "2022-01-02"))),
                dailyRatesChangelogWithoutUB().f1);

        // test partition filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                true,
                true,
                Collections.emptyMap(),
                "dt < '2022-01-02'",
                Collections.emptyList(),
                Collections.singletonList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                true,
                true,
                Collections.emptyMap(),
                "rate = 102",
                Collections.emptyList(),
                Collections.singletonList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                true,
                true,
                Collections.emptyMap(),
                "rate = 102 or dt < '2022-01-02'",
                Collections.emptyList(),
                Collections.singletonList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                true,
                true,
                Collections.emptyMap(),
                "rate = 102 or dt < '2022-01-02'",
                Collections.singletonList("currency"),
                Collections.singletonList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar")));
    }

    @Test
    public void testStreamingReadWritePartitionedRecordsWithoutPk() throws Exception {
        // input is dailyRatesChangelogWithUB()
        // enable log store, file store bounded read with merge
        checkFileStorePath(
                tEnv,
                collectAndCheckStreamingReadWriteWithClose(
                        true,
                        true,
                        false,
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Euro", 115L, "2022-01-02"))),
                dailyRatesChangelogWithUB().f1);

        // test partition filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                "dt IS NOT NULL",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));

        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                "dt IS NULL",
                Collections.emptyList(),
                Collections.emptyList());

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                "currency = 'US Dollar' OR rate = 115",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));

        // test partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                "(dt = '2022-01-02' AND currency = 'US Dollar') OR (dt = '2022-01-01' AND rate = 115)",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                null,
                Collections.singletonList("rate"),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", 102L),
                        // part = 2022-01-02
                        changelogRow("+I", 115L)));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                "dt <> '2022-01-01'",
                Collections.singletonList("rate"),
                Collections.singletonList(
                        // part = 2022-01-02, Euro
                        changelogRow("+I", 115L)));
    }

    @Test
    public void testStreamingReadWriteNonPartitionedRecordsWithPk() throws Exception {
        // input is ratesChangelogWithoutUB()
        // enable log store, file store bounded read with merge
        checkFileStorePath(
                tEnv,
                collectAndCheckStreamingReadWriteWithClose(
                        true,
                        false,
                        true,
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
                false,
                true,
                Collections.emptyMap(),
                "currency = 'Yen'",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection
        collectAndCheckStreamingReadWriteWithClose(
                true,
                false,
                true,
                Collections.emptyMap(),
                null,
                Collections.singletonList("currency"),
                Arrays.asList(changelogRow("+I", "US Dollar"), changelogRow("+I", "Euro")));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                false,
                true,
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
                        false,
                        false,
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
                false,
                false,
                Collections.emptyMap(),
                "currency IS NOT NULL",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("+I", "HK Dollar", null)));

        collectAndCheckStreamingReadWriteWithClose(
                true,
                false,
                false,
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
                false,
                false,
                Collections.emptyMap(),
                "currency IS NOT NULL AND rate is NOT NULL",
                Collections.singletonList("rate"),
                Arrays.asList(
                        changelogRow("+I", 102L), // US Dollar
                        changelogRow("+I", 119L)) // Euro
                );
    }

    @Test
    public void testReadLatestChangelogOfPartitionedRecordsWithPk() throws Exception {
        // input is dailyRatesChangelogWithoutUB()
        collectLatestLogAndCheck(
                false,
                true,
                true,
                null,
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-U", "Euro", 114L, "2022-01-01"),
                        changelogRow("+U", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test partition filter
        collectLatestLogAndCheck(
                false,
                true,
                true,
                "dt = '2022-01-01'",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
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
                true,
                true,
                "currency = 'Yen'",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01")));

        collectLatestLogAndCheck(
                false,
                true,
                true,
                "rate = 114",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("-U", "Euro", 114L, "2022-01-01")));

        // test partition and field filter
        collectLatestLogAndCheck(
                false,
                true,
                true,
                "rate = 114 AND dt = '2022-01-02'",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection
        collectLatestLogAndCheck(
                false,
                true,
                true,
                null,
                Collections.singletonList("rate"),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", 102L), // US Dollar
                        changelogRow("+I", 114L), // Euro
                        changelogRow("+I", 1L), // Yen
                        changelogRow("-U", 114L), // Euro
                        changelogRow("+U", 116L), // Euro
                        changelogRow("-D", 1L), // Yen
                        changelogRow("-D", 116L), // Euro
                        // part = 2022-01-02
                        changelogRow("+I", 119L) // Euro
                        ));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                true,
                true,
                "dt = '2022-01-02'",
                Collections.singletonList("rate"),
                Collections.singletonList(
                        // part = 2022-01-02, Euro
                        changelogRow("+I", 119L)));
    }

    @Test
    public void testReadLatestChangelogOfPartitionedRecordsWithoutPk() throws Exception {
        // input is dailyRatesChangelogWithUB()
        collectLatestLogAndCheck(
                false,
                true,
                false,
                null,
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 114L, "2022-01-02"),
                        changelogRow("-D", "Euro", 114L, "2022-01-02"),
                        changelogRow("+I", "Euro", 119L, "2022-01-02"),
                        changelogRow("-D", "Euro", 119L, "2022-01-02"),
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));
    }

    @Test
    public void testReadLatestChangelogOfNonPartitionedRecordsWithPk() throws Exception {
        // input is ratesChangelogWithoutUB()
        collectLatestLogAndCheck(
                false,
                false,
                true,
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
                false,
                true,
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
                false,
                true,
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
                false,
                true,
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
                false,
                false,
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
                false,
                false,
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
                false,
                false,
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
                false,
                false,
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
                false,
                false,
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
        collectLatestLogAndCheck(true, false, true, null, Collections.emptyList(), expected);

        // without pk
        collectLatestLogAndCheck(true, false, true, null, Collections.emptyList(), expected);

        // test field filter
        collectLatestLogAndCheck(
                true,
                false,
                true,
                "rate = 114",
                Collections.emptyList(),
                Arrays.asList(changelogRow("+I", "Euro", 114L), changelogRow("-U", "Euro", 114L)));

        // test projection
        collectLatestLogAndCheck(
                true,
                false,
                true,
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
                false,
                true,
                "rate = 114",
                Collections.singletonList("currency"),
                Arrays.asList(changelogRow("+I", "Euro"), changelogRow("-U", "Euro")));
    }

    @Test
    public void testReadInsertOnlyChangelogFromTimestamp() throws Exception {
        // input is dailyRates()
        collectChangelogFromTimestampAndCheck(
                true,
                true,
                true,
                0,
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("-U", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+U", "US Dollar", 114L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));

        collectChangelogFromTimestampAndCheck(true, true, false, 0, dailyRates().f0);

        // input is rates()
        collectChangelogFromTimestampAndCheck(
                true,
                false,
                true,
                0,
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-U", "Euro", 114L),
                        changelogRow("+U", "Euro", 119L)));

        collectChangelogFromTimestampAndCheck(true, false, false, 0, rates());

        collectChangelogFromTimestampAndCheck(
                true, false, false, Long.MAX_VALUE - 1, Collections.emptyList());
    }

    @Test
    public void testReadRetractChangelogFromTimestamp() throws Exception {
        // input is dailyRatesChangelogWithUB()
        collectChangelogFromTimestampAndCheck(
                false,
                true,
                false,
                0,
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 114L, "2022-01-02"),
                        changelogRow("-D", "Euro", 114L, "2022-01-02"),
                        changelogRow("+I", "Euro", 119L, "2022-01-02"),
                        changelogRow("-D", "Euro", 119L, "2022-01-02"),
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));

        // input is dailyRatesChangelogWithoutUB()
        collectChangelogFromTimestampAndCheck(
                false,
                true,
                true,
                0,
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-U", "Euro", 114L, "2022-01-01"),
                        changelogRow("+U", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        // part = 2022-01-02
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
                                Collections.emptyList(),
                                Collections.emptyList(),
                                null)
                        .f1;

        // without hint
        String query = buildSimpleSelectQuery(managedTable, Collections.emptyMap());
        assertThat(sourceParallelism(query)).isEqualTo(env.getParallelism());

        // with hint
        query =
                buildSimpleSelectQuery(
                        managedTable, Collections.singletonMap(SCAN_PARALLELISM.key(), "66"));
        assertThat(sourceParallelism(query)).isEqualTo(66);
    }

    @Test
    public void testSinkParallelism() throws IOException {
        testSinkParallelism(null, env.getParallelism());
        testSinkParallelism(23, 23);
    }

    @Test
    public void testQueryContainsDefaultFieldName() throws Exception {
        rootPath = TEMPORARY_FOLDER.newFolder().getPath();
        tEnv = StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        String id = registerData(Collections.singletonList(changelogRow("+I", 1, "abc")));
        tEnv.executeSql(
                String.format(
                        "create table dummy_source ("
                                + "f0 int, "
                                + "f1 string) with ("
                                + "'connector' = 'values', "
                                + "'bounded' = 'true', "
                                + "'data-id' = '%s')",
                        id));
        tEnv.executeSql(
                String.format(
                        "create table managed_table with ('root-path' = '%s') "
                                + "like dummy_source (excluding options)",
                        rootPath));
        tEnv.executeSql("insert into managed_table select * from dummy_source").await();
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(tEnv.executeSql("select * from managed_table").collect());
        assertThat(iterator.collect(1, 5, TimeUnit.SECONDS))
                .containsOnly(changelogRow("+I", 1, "abc"));
    }

    @Test
    public void testLike() throws Exception {
        rootPath = TEMPORARY_FOLDER.newFolder().getPath();
        tEnv = StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        List<Row> input =
                Arrays.asList(
                        changelogRow("+I", 1, "test_1"),
                        changelogRow("+I", 2, "test_2"),
                        changelogRow("+I", 1, "test_%"),
                        changelogRow("+I", 2, "test%2"),
                        changelogRow("+I", 3, "university"),
                        changelogRow("+I", 4, "very"),
                        changelogRow("+I", 5, "yield"));
        String id = registerData(input);
        UUID randomSourceId = UUID.randomUUID();
        tEnv.executeSql(
                String.format(
                        "create table `helper_source_%s` (f0 int, f1 string) with ("
                                + "'connector' = 'values', "
                                + "'bounded' = 'true', "
                                + "'data-id' = '%s')",
                        randomSourceId, id));

        UUID randomSinkId = UUID.randomUUID();
        tEnv.executeSql(
                String.format(
                        "create table `managed_table_%s` with ("
                                + "'%s' = '%s'"
                                + ") like `helper_source_%s` "
                                + "(excluding options)",
                        randomSinkId, ROOT_PATH.key(), rootPath, randomSourceId));

        // insert multiple times
        tEnv.executeSql(
                        String.format(
                                "insert into `managed_table_%s` select * from `helper_source_%s`",
                                randomSinkId, randomSourceId))
                .await();

        tEnv.executeSql(
                        String.format(
                                "insert into `managed_table_%s` values (7, 'villa'), (8, 'tests'), (20, 'test_123')",
                                randomSinkId))
                .await();

        tEnv.executeSql(
                        "insert into `managed_table_"
                                + randomSinkId
                                + "` values (9, 'valley'), (10, 'tested'), (100, 'test%fff')")
                .await();

        collectAndCheck(
                tEnv,
                "managed_table_" + randomSinkId,
                Collections.emptyMap(),
                "f1 like 'test%'",
                Arrays.asList(
                        changelogRow("+I", 1, "test_1"),
                        changelogRow("+I", 2, "test_2"),
                        changelogRow("+I", 1, "test_%"),
                        changelogRow("+I", 2, "test%2"),
                        changelogRow("+I", 8, "tests"),
                        changelogRow("+I", 10, "tested"),
                        changelogRow("+I", 20, "test_123")));
        collectAndCheck(
                tEnv,
                "managed_table_" + randomSinkId,
                Collections.emptyMap(),
                "f1 like 'v%'",
                Arrays.asList(
                        changelogRow("+I", 4, "very"),
                        changelogRow("+I", 7, "villa"),
                        changelogRow("+I", 9, "valley")));
        collectAndCheck(
                tEnv,
                "managed_table_" + randomSinkId,
                Collections.emptyMap(),
                "f1 like 'test=_%' escape '='",
                Arrays.asList(
                        changelogRow("+I", 1, "test_1"),
                        changelogRow("+I", 2, "test_2"),
                        changelogRow("+I", 1, "test_%"),
                        changelogRow("+I", 20, "test_123")));
        collectAndCheck(
                tEnv,
                "managed_table_" + randomSinkId,
                Collections.emptyMap(),
                "f1 like 'test=__' escape '='",
                Arrays.asList(
                        changelogRow("+I", 1, "test_1"),
                        changelogRow("+I", 2, "test_2"),
                        changelogRow("+I", 1, "test_%")));
        collectAndCheck(
                tEnv,
                "managed_table_" + randomSinkId,
                Collections.emptyMap(),
                "f1 like 'test$%%' escape '$'",
                Arrays.asList(
                        changelogRow("+I", 2, "test%2"), changelogRow("+I", 100, "test%fff")));
    }

    @Test
    public void testIn() throws Exception {
        rootPath = TEMPORARY_FOLDER.newFolder().getPath();
        tEnv = StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        List<Row> input =
                Arrays.asList(
                        changelogRow("+I", 1, "aaa"),
                        changelogRow("+I", 2, "bbb"),
                        changelogRow("+I", 3, "ccc"),
                        changelogRow("+I", 4, "ddd"),
                        changelogRow("+I", 5, "eee"),
                        changelogRow("+I", 6, "aaa"),
                        changelogRow("+I", 7, "bbb"),
                        changelogRow("+I", 8, "ccc"),
                        changelogRow("+I", 9, "ddd"),
                        changelogRow("+I", 10, "eee"),
                        changelogRow("+I", 11, "aaa"),
                        changelogRow("+I", 12, "bbb"),
                        changelogRow("+I", 13, "ccc"),
                        changelogRow("+I", 14, "ddd"),
                        changelogRow("+I", 15, "eee"),
                        changelogRow("+I", 16, "aaa"),
                        changelogRow("+I", 17, "bbb"),
                        changelogRow("+I", 18, "ccc"),
                        changelogRow("+I", 19, "ddd"),
                        changelogRow("+I", 20, "eee"),
                        changelogRow("+I", 21, "fff"));

        String id = registerData(input);
        UUID randomSourceId = UUID.randomUUID();
        tEnv.executeSql(
                String.format(
                        "create table `helper_source_%s` (f0 int, f1 string) with ("
                                + "'connector' = 'values', "
                                + "'bounded' = 'true', "
                                + "'data-id' = '%s')",
                        randomSourceId, id));

        UUID randomSinkId = UUID.randomUUID();
        tEnv.executeSql(
                String.format(
                        "create table `managed_table_%s` with ("
                                + "'%s' = '%s'"
                                + ") like `helper_source_%s` "
                                + "(excluding options)",
                        randomSinkId, ROOT_PATH.key(), rootPath, randomSourceId));
        tEnv.executeSql(
                        String.format(
                                "insert into `managed_table_%s` select * from `helper_source_%s`",
                                randomSinkId, randomSourceId))
                .await();

        collectAndCheck(
                tEnv,
                "managed_table_" + randomSinkId,
                Collections.emptyMap(),
                "f0 in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, "
                        + "11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)",
                input);

        collectAndCheck(
                tEnv,
                "managed_table_" + randomSinkId,
                Collections.emptyMap(),
                "f1 in ('aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff')",
                input);
    }

    @Test
    public void testUnsupportedPredicate() throws Exception {
        // test unsupported filter
        collectAndCheckBatchReadWrite(
                true,
                true,
                "currency similar to 'Euro'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));
    }

    @Test
    public void testChangeBucketNumber() throws Exception {
        rootPath = TEMPORARY_FOLDER.newFolder().getPath();
        tEnv = StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE IF NOT EXISTS rates (\n"
                                + "currency STRING,\n"
                                + " rate BIGINT,\n"
                                + " dt STRING\n"
                                + ") PARTITIONED BY (dt)\n"
                                + "WITH (\n"
                                + " 'bucket' = '2',\n"
                                + " 'root-path' = '%s'\n"
                                + ")",
                        rootPath));
        tEnv.executeSql("INSERT INTO rates VALUES('US Dollar', 102, '2022-06-20')").await();

        // increase bucket num from 2 to 3
        assertChangeBucketWithoutRescale(3);

        // decrease bucket num from 3 to 1
        assertChangeBucketWithoutRescale(1);
    }

    private void assertChangeBucketWithoutRescale(int bucketNum) throws Exception {
        tEnv.executeSql(String.format("ALTER TABLE rates SET ('bucket' = '%d')", bucketNum));
        // read is ok
        assertThat(BlockingIterator.of(tEnv.executeSql("SELECT * FROM rates").collect()).collect())
                .containsExactlyInAnyOrder(changelogRow("+I", "US Dollar", 102L, "2022-06-20"));
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO rates VALUES('US Dollar', 102, '2022-06-20')")
                                        .await())
                .getRootCause()
                .isInstanceOf(TableException.class)
                .hasMessage(
                        String.format(
                                "Try to write partition {dt=2022-06-20} with a new bucket num %d, but the previous bucket num is 2. "
                                        + "Please switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.",
                                bucketNum));
    }

    @Test
    public void testSuccessiveWriteAndRead() throws Exception {
        String managedTable =
                collectAndCheckBatchReadWrite(false, false, null, Collections.emptyList(), rates());
        // write rates() twice
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s`"
                                        + " VALUES ('US Dollar', 102),\n"
                                        + "('Euro', 114),\n"
                                        + "('Yen', 1),\n"
                                        + "('Euro', 114),\n"
                                        + "('Euro', 119)",
                                managedTable))
                .await();
        collectAndCheck(
                tEnv,
                managedTable,
                Collections.emptyMap(),
                "currency = 'Yen'",
                Collections.nCopies(2, changelogRow("+I", "Yen", 1L)));
    }

    @Test
    public void testStreamingInsertOverwrite() throws Exception {
        rootPath = TEMPORARY_FOLDER.newFolder().getPath();
        tEnv =
                StreamTableEnvironment.create(
                        buildStreamEnv(), EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE IF NOT EXISTS rates (\n"
                                + "currency STRING,\n"
                                + " rate BIGINT,\n"
                                + " dt STRING\n"
                                + ") PARTITIONED BY (dt)\n"
                                + "WITH (\n"
                                + " 'bucket' = '2',\n"
                                + " 'root-path' = '%s'\n"
                                + ")",
                        rootPath));
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "INSERT OVERWRITE rates VALUES('US Dollar', 102, '2022-06-20')"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Table store doesn't support streaming INSERT OVERWRITE.");
    }

    // ------------------------ Tools ----------------------------------

    private String collectAndCheckBatchReadWrite(
            boolean partitioned,
            boolean hasPk,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        return collectAndCheckBatchReadWrite(
                partitioned ? Collections.singletonList("dt") : Collections.emptyList(),
                hasPk
                        ? partitioned
                                ? Arrays.asList("currency", "dt")
                                : Collections.singletonList("currency")
                        : Collections.emptyList(),
                filter,
                projection,
                expected);
    }

    private String collectAndCheckStreamingReadWriteWithClose(
            boolean enableLogStore,
            boolean partitioned,
            boolean hasPk,
            Map<String, String> readHints,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        return collectAndCheckStreamingReadWriteWithClose(
                enableLogStore,
                partitioned ? Collections.singletonList("dt") : Collections.emptyList(),
                hasPk
                        ? partitioned
                                ? Arrays.asList("currency", "dt")
                                : Collections.singletonList("currency")
                        : Collections.emptyList(),
                readHints,
                filter,
                projection,
                expected);
    }

    private Tuple2<String, BlockingIterator<Row, Row>>
            collectAndCheckStreamingReadWriteWithoutClose(
                    Map<String, String> readHints,
                    @Nullable String filter,
                    List<String> projection,
                    List<Row> expected)
                    throws Exception {
        return collectAndCheckStreamingReadWriteWithoutClose(
                Collections.singletonList("dt"),
                Arrays.asList("currency", "dt"),
                readHints,
                filter,
                projection,
                expected);
    }

    private void collectLatestLogAndCheck(
            boolean insertOnly,
            boolean partitioned,
            boolean hasPk,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        collectLatestLogAndCheck(
                insertOnly,
                partitioned ? Collections.singletonList("dt") : Collections.emptyList(),
                hasPk
                        ? partitioned
                                ? Arrays.asList("currency", "dt")
                                : Collections.singletonList("currency")
                        : Collections.emptyList(),
                filter,
                projection,
                expected);
    }

    private void collectChangelogFromTimestampAndCheck(
            boolean insertOnly,
            boolean partitioned,
            boolean hasPk,
            long timestamp,
            List<Row> expected)
            throws Exception {
        collectChangelogFromTimestampAndCheck(
                insertOnly,
                partitioned ? Collections.singletonList("dt") : Collections.emptyList(),
                hasPk
                        ? partitioned
                                ? Arrays.asList("currency", "dt")
                                : Collections.singletonList("currency")
                        : Collections.emptyList(),
                timestamp,
                expected);
    }

    private int sourceParallelism(String sql) {
        DataStream<Row> stream = tEnv.toChangelogStream(tEnv.sqlQuery(sql));
        return stream.getParallelism();
    }

    private void testSinkParallelism(Integer configParallelism, int expectedParallelism)
            throws IOException {
        // 1. create a mock table sink
        Map<String, String> options = new HashMap<>();
        if (configParallelism != null) {
            options.put(SINK_PARALLELISM.key(), configParallelism.toString());
        }
        options.put(
                "path",
                new File(TEMPORARY_FOLDER.newFolder(), UUID.randomUUID().toString())
                        .toURI()
                        .toString());

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
        new TableStoreManagedFactory().onCreateTable(context, false);
        DynamicTableSink tableSink = new TableStoreManagedFactory().createDynamicTableSink(context);
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
