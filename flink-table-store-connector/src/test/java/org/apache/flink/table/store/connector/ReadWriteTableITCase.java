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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.connector.util.AbstractTestBase;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.store.connector.AbstractTableStoreFactory.buildFileStoreTable;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.SCAN_PARALLELISM;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.table.store.connector.ReadWriteTableTestBase.assertNoMoreRecords;
import static org.apache.flink.table.store.connector.TableStoreTestBase.createResolvedTable;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.bExeEnv;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.buildQuery;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.buildQueryWithTableOptions;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.buildSimpleQuery;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.checkFileStorePath;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.createTable;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.createTemporaryTable;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.init;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.insertIntoFromTable;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.insertOverwrite;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.insertOverwritePartition;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.sEnv;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.testBatchRead;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Table Store reading and writing IT cases. */
public class ReadWriteTableITCase extends AbstractTestBase {

    @BeforeEach
    public void setUp() {
        init(getTempDirPath());
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Batch Read & Write
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testBatchReadWriteWithPartitionedRecordsWithPk() throws Exception {
        List<Row> initialRecords =
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02"));

        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt String"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"));

        insertInto(
                table,
                "('US Dollar', 114, '2022-01-01')",
                "('Yen', 1, '2022-01-01')",
                "('Euro', 114, '2022-01-01')",
                "('Euro', 119, '2022-01-02')");

        checkFileStorePath(table, Arrays.asList("dt=2022-01-01", "dt=2022-01-02"));

        testBatchRead(buildSimpleQuery(table), initialRecords);

        BlockingIterator<Row, Row> streamItr =
                testStreamingRead(buildSimpleQuery(table), initialRecords);

        // test refresh after overwriting
        insertOverwritePartition(
                table, "PARTITION (dt = '2022-01-02')", "('Euro', 100)", "('Yen', 1)");

        // streaming iterator will not receive any changelog
        assertNoMoreRecords(streamItr);
        streamItr.close();

        // batch read to check partition refresh
        testBatchRead(
                buildQuery(table, "*", "WHERE dt IN ('2022-01-02')"),
                Arrays.asList(
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 100L, "2022-01-02"),
                        changelogRow("+I", "Yen", 1L, "2022-01-02")));

        // test partition filter
        List<Row> expectedPartitionRecords =
                Arrays.asList(
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"));

        testBatchRead(buildQuery(table, "*", "WHERE dt <> '2022-01-02'"), expectedPartitionRecords);

        testBatchRead(
                buildQuery(table, "*", "WHERE dt IN ('2022-01-01')"), expectedPartitionRecords);

        // test field filter
        testBatchRead(
                buildQuery(table, "*", "WHERE rate >= 100"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Euro", 100L, "2022-01-02")));

        // test partition and field filter
        testBatchRead(
                buildQuery(table, "*", "WHERE dt = '2022-01-02' AND rate >= 100"),
                Collections.singletonList(changelogRow("+I", "Euro", 100L, "2022-01-02")));

        // test projection
        testBatchRead(
                buildQuery(table, "dt", ""),
                Arrays.asList(
                        changelogRow("+I", "2022-01-01"),
                        changelogRow("+I", "2022-01-01"),
                        changelogRow("+I", "2022-01-01"),
                        changelogRow("+I", "2022-01-02"),
                        changelogRow("+I", "2022-01-02")));

        testBatchRead(
                buildQuery(table, "dt, currency, rate", ""),
                Arrays.asList(
                        changelogRow("+I", "2022-01-01", "US Dollar", 114L),
                        changelogRow("+I", "2022-01-01", "Yen", 1L),
                        changelogRow("+I", "2022-01-01", "Euro", 114L),
                        changelogRow("+I", "2022-01-02", "Euro", 100L),
                        changelogRow("+I", "2022-01-02", "Yen", 1L)));

        // test projection and filter
        testBatchRead(
                buildQuery(table, "currency, dt", "WHERE rate = 114"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", "2022-01-01"),
                        changelogRow("+I", "Euro", "2022-01-01")));
    }

    @Test
    public void testBatchReadWriteWithPartitionedRecordsWithoutPk() throws Exception {
        List<Row> initialRecords =
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02"));

        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt String"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"));

        insertInto(
                table,
                "('US Dollar', 102, '2022-01-01')",
                "('Euro', 114, '2022-01-01')",
                "('Yen', 1, '2022-01-01')",
                "('Euro', 114, '2022-01-01')",
                "('US Dollar', 114, '2022-01-01')",
                "('Euro', 119, '2022-01-02')");

        checkFileStorePath(table, Arrays.asList("dt=2022-01-01", "dt=2022-01-02"));

        testBatchRead(buildSimpleQuery(table), initialRecords);

        // overwrite
        insertOverwrite(table, "('Euro', 90, '2022-01-01')", "('Yen', 2, '2022-01-02')");

        // test streaming read
        testStreamingRead(
                        buildSimpleQuery(table),
                        Arrays.asList(
                                changelogRow("+I", "Euro", 90L, "2022-01-01"),
                                changelogRow("+I", "Yen", 2L, "2022-01-02")))
                .close();

        // overwrite with initial data
        insertOverwrite(
                table,
                "('US Dollar', 102, '2022-01-01')",
                "('Euro', 114, '2022-01-01')",
                "('Yen', 1, '2022-01-01')",
                "('Euro', 114, '2022-01-01')",
                "('US Dollar', 114, '2022-01-01')",
                "('Euro', 119, '2022-01-02')");

        // test partition filter
        testBatchRead(buildQuery(table, "*", "WHERE dt >= '2022-01-01'"), initialRecords);

        // test field filter
        testBatchRead(
                buildQuery(table, "*", "WHERE currency = 'US Dollar'"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01")));

        // test partition and field filter
        testBatchRead(
                buildQuery(table, "*", "WHERE dt = '2022-01-01' OR rate > 115"), initialRecords);

        // test projection
        testBatchRead(
                buildQuery(table, "currency", ""),
                Arrays.asList(
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro")));

        // test projection and filter
        testBatchRead(
                buildQuery(table, "currency, dt", "WHERE rate = 119"),
                Collections.singletonList(changelogRow("+I", "Euro", "2022-01-02")));
    }

    @Test
    public void testBatchReadWriteWithNonPartitionedRecordsWithPk() throws Exception {
        List<Row> initialRecords =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("+I", "Euro", 119L));

        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList());

        insertInto(table, "('US Dollar', 102)", "('Yen', 1)", "('Euro', 119)");

        checkFileStorePath(table, Collections.emptyList());

        testBatchRead(buildQuery(table, "*", ""), initialRecords);

        // overwrite
        insertOverwrite(table, "('Euro', 100)");

        testBatchRead(
                buildSimpleQuery(table),
                Collections.singletonList(changelogRow("+I", "Euro", 100L)));

        // overwrite with initial data
        insertOverwrite(table, "('US Dollar', 102)", "('Yen', 1)", "('Euro', 119)");

        // test field filter
        List<Row> expectedFieldRecords =
                Collections.singletonList(changelogRow("+I", "Euro", 119L));

        testBatchRead(buildQuery(table, "*", "WHERE currency = 'Euro'"), expectedFieldRecords);

        testBatchRead(
                buildQuery(table, "*", "WHERE rate > 102 AND rate <= 119"), expectedFieldRecords);

        // test projection
        testBatchRead(
                buildQuery(table, "currency", ""),
                Arrays.asList(
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "Euro")));

        // test projection and filter
        testBatchRead(
                buildQuery(table, "rate", "WHERE currency IN ('Yen')"),
                Collections.singletonList(changelogRow("+I", 1L)));
    }

    @Test
    public void testBatchReadWriteWithNonPartitionedRecordsWithoutPk() throws Exception {
        List<Row> initialRecords =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 119L));

        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.emptyList());

        insertInto(
                table,
                "('US Dollar', 102)",
                "('Euro', 114)",
                "('Yen', 1)",
                "('Euro', 114)",
                "('Euro', 119)");

        checkFileStorePath(table, Collections.emptyList());

        testBatchRead(buildSimpleQuery(table), initialRecords);

        // test field filter
        testBatchRead(buildQuery(table, "*", "WHERE rate >= 1"), initialRecords);

        testBatchRead(
                buildQuery(table, "*", "WHERE currency = 'Euro'"),
                Arrays.asList(
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 119L)));

        // test projection
        testBatchRead(
                buildQuery(table, "currency", ""),
                Arrays.asList(
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "US Dollar")));

        // test projection and filter
        testBatchRead(
                buildQuery(table, "currency", "WHERE rate > 100 OR currency = 'Yen'"),
                Arrays.asList(
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "US Dollar")));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Streaming Read & Write
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testStreamingReadWriteWithPartitionedRecordsWithPk() throws Exception {
        // file store continuous read
        // will not merge, at least collect two records
        List<Row> initialRecords =
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
                        changelogRow("+U", "Euro", 119L, "2022-01-02"));

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"),
                        initialRecords,
                        "dt:2022-01-01;dt:2022-01-02",
                        false,
                        "I,UA,D");

        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"));

        insertIntoFromTable(temporaryTable, table);

        sEnv.executeSql(String.format("INSERT INTO `%s` SELECT * FROM `%s`", table, temporaryTable))
                .await();

        checkFileStorePath(table, Arrays.asList("dt=2022-01-01", "dt=2022-01-02"));

        testStreamingRead(
                        buildSimpleQuery(table),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                changelogRow("+I", "Euro", 119L, "2022-01-02")))
                .close();

        // test partition filter
        testStreamingRead(
                        buildQuery(table, "*", "WHERE dt < '2022-01-02'"),
                        Collections.singletonList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01")))
                .close();

        // test field filter
        testStreamingRead(
                        buildQuery(table, "*", "WHERE rate = 102"),
                        Collections.singletonList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01")))
                .close();

        // test partition and field filter
        testStreamingRead(
                        buildQuery(table, "*", "WHERE rate = 102 OR dt < '2022-01-02'"),
                        Collections.singletonList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01")))
                .close();

        // test projection and filter
        testStreamingRead(
                        buildQuery(table, "currency", "WHERE rate = 102 OR dt < '2022-01-02'"),
                        Collections.singletonList(changelogRow("+I", "US Dollar")))
                .close();
    }

    @Test
    public void testStreamingReadWriteWithPartitionedRecordsWithoutPk() throws Exception {
        // file store bounded read with merge
        List<Row> initialRecords =
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
                        changelogRow("+I", "Euro", 115L, "2022-01-02"));

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        initialRecords,
                        "dt:2022-01-01;dt:2022-01-02",
                        false,
                        "I,UA,UB,D");

        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"));

        insertIntoFromTable(temporaryTable, table);

        checkFileStorePath(table, Arrays.asList("dt=2022-01-01", "dt=2022-01-02"));

        testStreamingRead(
                        buildSimpleQuery(table),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                changelogRow("+I", "Euro", 115L, "2022-01-02")))
                .close();

        // test partition filter
        testStreamingRead(
                        buildQuery(table, "*", "WHERE dt IS NOT NULL"),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                changelogRow("+I", "Euro", 115L, "2022-01-02")))
                .close();

        testStreamingRead(buildQuery(table, "*", "WHERE dt IS NULL"), Collections.emptyList())
                .close();

        // test field filter
        testStreamingRead(
                        buildQuery(table, "*", "WHERE currency = 'US Dollar' OR rate = 115"),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                changelogRow("+I", "Euro", 115L, "2022-01-02")))
                .close();

        // test partition and field filter
        testStreamingRead(
                        buildQuery(
                                table,
                                "*",
                                "WHERE (dt = '2022-01-02' AND currency = 'US Dollar') OR (dt = '2022-01-01' AND rate = 115)"),
                        Collections.emptyList())
                .close();

        // test projection
        testStreamingRead(
                        buildQuery(table, "rate", ""),
                        Arrays.asList(changelogRow("+I", 102L), changelogRow("+I", 115L)))
                .close();

        // test projection and filter
        testStreamingRead(
                        buildQuery(table, "rate", "WHERE dt <> '2022-01-01'"),
                        Collections.singletonList(changelogRow("+I", 115L)))
                .close();
    }

    @Test
    void testStreamingReadWriteWithNonPartitionedRecordsWithPk() throws Exception {
        // file store bounded read with merge
        List<Row> initialRecords =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("+U", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("+U", "Euro", 119L),
                        changelogRow("-D", "Yen", 1L));

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        initialRecords,
                        null,
                        false,
                        "I,UA,D");

        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList());

        insertIntoFromTable(temporaryTable, table);

        checkFileStorePath(table, Collections.emptyList());

        testStreamingRead(
                        buildSimpleQuery(table),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 119L)))
                .close();

        // test field filter
        testStreamingRead(buildQuery(table, "*", "WHERE currency = 'Yen'"), Collections.emptyList())
                .close();

        // test projection
        testStreamingRead(
                        buildQuery(table, "currency", ""),
                        Arrays.asList(changelogRow("+I", "US Dollar"), changelogRow("+I", "Euro")))
                .close();

        // test projection and filter
        testStreamingRead(
                        buildQuery(table, "currency", "WHERE rate = 102"),
                        Collections.singletonList(changelogRow("+I", "US Dollar")))
                .close();
    }

    @Test
    public void testStreamingReadWriteWithNonPartitionedRecordsWithoutPk() throws Exception {
        // default full scan mode, will merge
        List<Row> initialRecords =
                Arrays.asList(
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

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        initialRecords,
                        null,
                        false,
                        "I,UA,UB,D");

        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.emptyList());

        insertIntoFromTable(temporaryTable, table);

        checkFileStorePath(table, Collections.emptyList());

        testStreamingRead(
                        buildSimpleQuery(table),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 119L),
                                changelogRow("+I", null, 100L),
                                changelogRow("+I", "HK Dollar", null)))
                .close();

        // test field filter
        testStreamingRead(
                        buildQuery(table, "*", "WHERE currency IS NOT NULL"),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 119L),
                                changelogRow("+I", "HK Dollar", null)))
                .close();

        testStreamingRead(
                        buildQuery(table, "*", "WHERE rate IS NOT NULL"),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 119L),
                                changelogRow("+I", null, 100L)))
                .close();

        // test projection and filter
        testStreamingRead(
                        buildQuery(
                                table, "rate", "WHERE currency IS NOT NULL AND rate IS NOT NULL"),
                        Arrays.asList(changelogRow("+I", 102L), changelogRow("+I", 119L)))
                .close();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Purge data using overwrite
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testPurgeTableUsingBatchOverWrite() throws Exception {
        String table =
                createTable(
                        Arrays.asList("k0 INT", "k1 STRING", "v STRING"),
                        Collections.emptyList(),
                        Collections.emptyList());

        validateOverwriteResult(table, "", "*", Collections.emptyList());
    }

    @Test
    public void testPurgePartitionUsingBatchOverWrite() throws Exception {
        List<String> fieldsSpec = Arrays.asList("k0 INT", "k1 STRING", "v STRING");

        // single partition key
        String table =
                createTable(fieldsSpec, Collections.emptyList(), Collections.singletonList("k0"));

        validateOverwriteResult(
                table,
                "PARTITION (k0 = 0)",
                "k1, v",
                Arrays.asList(
                        changelogRow("+I", 1, "2023-01-01", "flink"),
                        changelogRow("+I", 1, "2023-01-02", "table"),
                        changelogRow("+I", 1, "2023-01-02", "store")));

        // multiple partition keys and overwrite one partition key
        table = createTable(fieldsSpec, Collections.emptyList(), Arrays.asList("k0", "k1"));

        validateOverwriteResult(
                table,
                "PARTITION (k0 = 0)",
                "k1, v",
                Arrays.asList(
                        changelogRow("+I", 1, "2023-01-01", "flink"),
                        changelogRow("+I", 1, "2023-01-02", "table"),
                        changelogRow("+I", 1, "2023-01-02", "store")));

        // multiple partition keys and overwrite all partition keys
        table = createTable(fieldsSpec, Collections.emptyList(), Arrays.asList("k0", "k1"));

        validateOverwriteResult(
                table,
                "PARTITION (k0 = 0, k1 = '2023-01-01')",
                "v",
                Arrays.asList(
                        changelogRow("+I", 0, "2023-01-02", "world"),
                        changelogRow("+I", 1, "2023-01-01", "flink"),
                        changelogRow("+I", 1, "2023-01-02", "table"),
                        changelogRow("+I", 1, "2023-01-02", "store")));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Keyword
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testLike() throws Exception {
        String table =
                createTable(
                        Arrays.asList("f0 INT", "f1 STRING"),
                        Collections.emptyList(),
                        Collections.emptyList());

        // insert multiple times
        insertInto(
                table,
                "(1, 'test_1')",
                "(2, 'test_2')",
                "(1, 'test_%')",
                "(2, 'test%2')",
                "(3, 'university')",
                "(4, 'very')",
                "(5, 'yield')");

        insertInto(
                table,
                "(7, 'villa')",
                "(8, 'tests')",
                "(20, 'test_123')",
                "(9, 'valley')",
                "(10, 'tested')",
                "(100, 'test%fff')");

        testBatchRead(
                buildQuery(table, "*", "WHERE f1 LIKE 'test%'"),
                Arrays.asList(
                        changelogRow("+I", 1, "test_1"),
                        changelogRow("+I", 2, "test_2"),
                        changelogRow("+I", 1, "test_%"),
                        changelogRow("+I", 2, "test%2"),
                        changelogRow("+I", 8, "tests"),
                        changelogRow("+I", 10, "tested"),
                        changelogRow("+I", 20, "test_123")));

        testBatchRead(
                buildQuery(table, "*", "WHERE f1 LIKE 'v%'"),
                Arrays.asList(
                        changelogRow("+I", 4, "very"),
                        changelogRow("+I", 7, "villa"),
                        changelogRow("+I", 9, "valley")));

        testBatchRead(
                buildQuery(table, "*", "WHERE f1 LIKE 'test=_%' ESCAPE '='"),
                Arrays.asList(
                        changelogRow("+I", 1, "test_1"),
                        changelogRow("+I", 2, "test_2"),
                        changelogRow("+I", 1, "test_%"),
                        changelogRow("+I", 20, "test_123")));

        testBatchRead(
                buildQuery(table, "*", "WHERE f1 LIKE 'test=__' ESCAPE '='"),
                Arrays.asList(
                        changelogRow("+I", 1, "test_1"),
                        changelogRow("+I", 2, "test_2"),
                        changelogRow("+I", 1, "test_%")));

        testBatchRead(
                buildQuery(table, "*", "WHERE f1 LIKE 'test$%%' ESCAPE '$'"),
                Arrays.asList(
                        changelogRow("+I", 2, "test%2"), changelogRow("+I", 100, "test%fff")));
    }

    @Test
    public void testIn() throws Exception {
        List<Row> initialRecords =
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

        String table =
                createTable(
                        Arrays.asList("f0 INT", "f1 STRING"),
                        Collections.emptyList(),
                        Collections.emptyList());

        insertInto(
                table,
                "(1, 'aaa')",
                "(2, 'bbb')",
                "(3, 'ccc')",
                "(4, 'ddd')",
                "(5, 'eee')",
                "(6, 'aaa')",
                "(7, 'bbb')",
                "(8, 'ccc')",
                "(9, 'ddd')",
                "(10, 'eee')",
                "(11, 'aaa')",
                "(12, 'bbb')",
                "(13, 'ccc')",
                "(14, 'ddd')",
                "(15, 'eee')",
                "(16, 'aaa')",
                "(17, 'bbb')",
                "(18, 'ccc')",
                "(19, 'ddd')",
                "(20, 'eee')",
                "(21, 'fff')");

        testBatchRead(
                buildQuery(
                        table,
                        "*",
                        "WHERE f0 IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)"),
                initialRecords);

        List<Row> expected = new ArrayList<>(initialRecords);
        expected.remove(20);
        testBatchRead(
                buildQuery(table, "*", "WHERE f1 IN ('aaa', 'bbb', 'ccc', 'ddd', 'eee')"),
                expected);
    }

    @Test
    public void testUnsupportedPredicate() throws Exception {
        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"));

        insertInto(
                table,
                "('US Dollar', 102, '2022-01-01')",
                "('Euro', 114, '2022-01-01')",
                "('Yen', 1, '2022-01-01')",
                "('Euro', 114, '2022-01-01')",
                "('US Dollar', 114, '2022-01-01')",
                "('Euro', 119, '2022-01-02')");

        // test unsupported filter
        testBatchRead(
                buildQuery(table, "*", "WHERE currency SIMILAR TO 'Euro'"),
                Arrays.asList(
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Others
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testSourceParallelism() throws Exception {
        List<Row> initialRecords =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 119L));

        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.emptyList());

        insertInto(
                table,
                "('US Dollar', 102)",
                "('Euro', 114)",
                "('Yen', 1)",
                "('Euro', 114)",
                "('Euro', 119)");

        testBatchRead(buildSimpleQuery(table), initialRecords);

        // without hint
        assertThat(sourceParallelism(buildSimpleQuery(table))).isEqualTo(bExeEnv.getParallelism());

        // with hint
        assertThat(
                        sourceParallelism(
                                buildQueryWithTableOptions(
                                        table,
                                        "*",
                                        "",
                                        new HashMap<String, String>() {
                                            {
                                                put(SCAN_PARALLELISM.key(), "66");
                                            }
                                        })))
                .isEqualTo(66);
    }

    @Test
    public void testSinkParallelism() throws Exception {
        testSinkParallelism(null, bExeEnv.getParallelism());
        testSinkParallelism(23, 23);
    }

    @Test
    public void testChangeBucketNumber() throws Exception {
        String table = "MyTable_" + UUID.randomUUID();
        bEnv.executeSql(
                String.format(
                        "CREATE TABLE `%s` (\n"
                                + "currency STRING,\n"
                                + " rate BIGINT,\n"
                                + " dt STRING\n"
                                + ") PARTITIONED BY (dt)\n"
                                + "WITH (\n"
                                + " 'bucket' = '2'\n"
                                + ")",
                        table));

        insertInto(table, "('US Dollar', 102, '2022-06-20')");

        // increase bucket num from 2 to 3
        assertChangeBucketWithoutRescale(table, 3);

        // decrease bucket num from 3 to 1
        assertChangeBucketWithoutRescale(table, 1);
    }

    @Test
    public void testStreamingInsertOverwrite() {
        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt String"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"));

        assertThatThrownBy(
                        () ->
                                sEnv.executeSql(
                                        String.format(
                                                "INSERT OVERWRITE `%s` VALUES('US Dollar', 102, '2022-06-20')",
                                                table)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Table store doesn't support streaming INSERT OVERWRITE.");
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Tools
    // ----------------------------------------------------------------------------------------------------------------

    private void validateOverwriteResult(
            String table, String partitionSpec, String projectionSpec, List<Row> expected)
            throws Exception {
        insertInto(
                table,
                "(0, '2023-01-01', 'hi')",
                "(0, '2023-01-01', 'hello')",
                "(0, '2023-01-02', 'world')",
                "(1, '2023-01-01', 'flink')",
                "(1, '2023-01-02', 'table')",
                "(1, '2023-01-02', 'store')");

        testBatchRead(
                buildSimpleQuery(table),
                Arrays.asList(
                        changelogRow("+I", 0, "2023-01-01", "hi"),
                        changelogRow("+I", 0, "2023-01-01", "hello"),
                        changelogRow("+I", 0, "2023-01-02", "world"),
                        changelogRow("+I", 1, "2023-01-01", "flink"),
                        changelogRow("+I", 1, "2023-01-02", "table"),
                        changelogRow("+I", 1, "2023-01-02", "store")));

        bEnv.executeSql(
                        String.format(
                                "INSERT OVERWRITE `%s` %s SELECT %s FROM `%s` WHERE false",
                                table, partitionSpec, projectionSpec, table))
                .await();

        testBatchRead(buildSimpleQuery(table), expected);
    }

    private int sourceParallelism(String sql) {
        DataStream<Row> stream =
                ((StreamTableEnvironment) bEnv).toChangelogStream(bEnv.sqlQuery(sql));
        return stream.getParallelism();
    }

    private void testSinkParallelism(Integer configParallelism, int expectedParallelism)
            throws Exception {
        // 1. create a mock table sink
        Map<String, String> options = new HashMap<>();
        if (configParallelism != null) {
            options.put(SINK_PARALLELISM.key(), configParallelism.toString());
        }
        options.put("path", getTempFilePath(UUID.randomUUID().toString()));

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

        // create table
        Path path = CoreOptions.path(context.getCatalogTable().getOptions());
        path.getFileSystem().mkdirs(path);
        // update schema
        new SchemaManager(path)
                .commitNewVersion(FlinkCatalog.fromCatalogTable(context.getCatalogTable()));

        DynamicTableSink tableSink =
                new TableStoreSink(
                        context.getObjectIdentifier(), buildFileStoreTable(context), context, null);
        assertThat(tableSink).isInstanceOf(TableStoreSink.class);

        // 2. get sink provider
        DynamicTableSink.SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider).isInstanceOf(DataStreamSinkProvider.class);
        DataStreamSinkProvider sinkProvider = (DataStreamSinkProvider) provider;

        // 3. assert parallelism from transformation
        DataStream<RowData> mockSource =
                bExeEnv.fromCollection(Collections.singletonList(GenericRowData.of()));
        DataStreamSink<?> sink = sinkProvider.consumeDataStream(null, mockSource);
        Transformation<?> transformation = sink.getTransformation();
        // until a PartitionTransformation, see TableStore.SinkBuilder.build()
        while (!(transformation instanceof PartitionTransformation)) {
            assertThat(transformation.getParallelism()).isIn(1, expectedParallelism);
            transformation = transformation.getInputs().get(0);
        }
    }

    private void assertChangeBucketWithoutRescale(String table, int bucketNum) throws Exception {
        bEnv.executeSql(String.format("ALTER TABLE `%s` SET ('bucket' = '%d')", table, bucketNum));
        // read is ok
        assertThat(
                        BlockingIterator.of(bEnv.executeSql(buildSimpleQuery(table)).collect())
                                .collect())
                .containsExactlyInAnyOrder(changelogRow("+I", "US Dollar", 102L, "2022-06-20"));
        assertThatThrownBy(() -> insertInto(table, "('US Dollar', 102, '2022-06-20')"))
                .rootCause()
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        String.format(
                                "Try to write partition {dt=2022-06-20} with a new bucket num %d, but the previous bucket num is 2. "
                                        + "Please switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.",
                                bucketNum));
    }
}
