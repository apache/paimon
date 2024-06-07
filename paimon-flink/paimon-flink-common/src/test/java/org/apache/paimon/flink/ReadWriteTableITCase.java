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

package org.apache.paimon.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.ChangelogProducer.LOOKUP;
import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;
import static org.apache.paimon.CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST;
import static org.apache.paimon.CoreOptions.SOURCE_SPLIT_TARGET_SIZE;
import static org.apache.paimon.flink.AbstractFlinkTableFactory.buildPaimonTable;
import static org.apache.paimon.flink.FlinkConnectorOptions.INFER_SCAN_MAX_PARALLELISM;
import static org.apache.paimon.flink.FlinkConnectorOptions.INFER_SCAN_PARALLELISM;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_PARALLELISM;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_PARALLELISM;
import static org.apache.paimon.flink.FlinkTestBase.createResolvedTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.assertNoMoreRecords;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bExeEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildQueryWithTableOptions;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildSimpleQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.checkFileStorePath;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.createTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.createTemporaryTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertIntoFromTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertOverwrite;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertOverwritePartition;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.sEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.validateStreamingReadResult;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.warehouse;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Paimon reading and writing IT cases. */
public class ReadWriteTableITCase extends AbstractTestBase {

    private final Map<String, String> streamingReadOverwrite =
            Collections.singletonMap(CoreOptions.STREAMING_READ_OVERWRITE.key(), "true");

    private final Map<String, String> staticPartitionOverwrite =
            Collections.singletonMap(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false");

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
                        Collections.emptyList(),
                        Collections.singletonList("dt"));

        insertInto(
                table,
                "('US Dollar', 114, '2022-01-01')",
                "('Yen', 1, '2022-01-01')",
                "('Euro', 114, '2022-01-01')",
                "('Euro', 119, '2022-01-02')");

        checkFileStorePath(table, Arrays.asList("dt=2022-01-01", "dt=2022-01-02"));

        testBatchRead(buildSimpleQuery(table), initialRecords);

        insertOverwritePartition(
                table, "PARTITION (dt = '2022-01-02')", "('Euro', 100)", "('Yen', 1)");

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
    public void testNaNType() throws Exception {
        bEnv.executeSql(
                "CREATE TEMPORARY TABLE S ( a DOUBLE,b DOUBLE,c STRING) WITH ( 'connector' = 'filesystem', 'format'='json' , 'path' ='"
                        + warehouse
                        + "/S' )");
        bEnv.executeSql(
                        "INSERT INTO S VALUES "
                                + "(1.0,2.0,'a'),\n"
                                + "(0.0,0.0,'b'),\n"
                                + "(1.0,1.0,'c'),\n"
                                + "(0.0,0.0,'d'),\n"
                                + "(1.0,0.0,'e'),\n"
                                + "(0.0,0.0,'f'),\n"
                                + "(-1.0,0.0,'g'),\n"
                                + "(1.0,-1.0,'h'),\n"
                                + "(1.0,-2.0,'i')")
                .await();

        bEnv.executeSql("CREATE TABLE T (d STRING, e DOUBLE)");
        bEnv.executeSql("INSERT INTO T SELECT c,a/b FROM S").await();

        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(bEnv.executeSql("SELECT * FROM  T").collect());
        assertThat(iterator.collect(9))
                .containsExactlyInAnyOrder(
                        Row.of("a", 0.5),
                        Row.of("b", Double.NaN),
                        Row.of("c", 1.0),
                        Row.of("d", Double.NaN),
                        Row.of("e", Double.POSITIVE_INFINITY),
                        Row.of("f", Double.NaN),
                        Row.of("g", Double.NEGATIVE_INFINITY),
                        Row.of("h", -1.0),
                        Row.of("i", -0.5));
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
                        Collections.singletonList("currency"),
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
                        Collections.emptyList(),
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
                        Collections.singletonList("currency"),
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
                        Collections.emptyList(),
                        Collections.singletonList("dt"));

        insertIntoFromTable(temporaryTable, table);

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
                        Collections.emptyList(),
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

    // ----------------------------------------------------------------------------------------------------------------
    // Dynamic partition overwrite (default option)
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testDynamicOverwrite() throws Exception {
        String table =
                createTable(
                        Arrays.asList("pk INT", "part0 INT", "part1 STRING", "v STRING"),
                        Arrays.asList("pk", "part0", "part1"),
                        Collections.emptyList(),
                        Arrays.asList("part0", "part1"),
                        streamingReadOverwrite);

        insertInto(
                table,
                "(1, 1, 'A', 'Hi')",
                "(2, 1, 'A', 'Hello')",
                "(3, 1, 'A', 'World')",
                "(4, 1, 'B', 'To')",
                "(5, 1, 'B', 'Apache')",
                "(6, 1, 'B', 'Paimon')",
                "(7, 2, 'A', 'Test')",
                "(8, 2, 'B', 'Case')");

        BlockingIterator<Row, Row> streamItr =
                testStreamingRead(
                        buildSimpleQuery(table),
                        Arrays.asList(
                                changelogRow("+I", 1, 1, "A", "Hi"),
                                changelogRow("+I", 2, 1, "A", "Hello"),
                                changelogRow("+I", 3, 1, "A", "World"),
                                changelogRow("+I", 4, 1, "B", "To"),
                                changelogRow("+I", 5, 1, "B", "Apache"),
                                changelogRow("+I", 6, 1, "B", "Paimon"),
                                changelogRow("+I", 7, 2, "A", "Test"),
                                changelogRow("+I", 8, 2, "B", "Case")));

        bEnv.executeSql(
                        String.format(
                                "INSERT OVERWRITE `%s` VALUES (4, 1, 'B', 'Where'), (5, 1, 'B', 'When'), (10, 2, 'A', 'Static'), (11, 2, 'A', 'Dynamic')",
                                table))
                .await();

        assertThat(streamItr.collect(8))
                .containsExactlyInAnyOrder(
                        changelogRow("-D", 4, 1, "B", "To"),
                        changelogRow("-D", 5, 1, "B", "Apache"),
                        changelogRow("-D", 6, 1, "B", "Paimon"),
                        changelogRow("-D", 7, 2, "A", "Test"),
                        changelogRow("+I", 4, 1, "B", "Where"),
                        changelogRow("+I", 5, 1, "B", "When"),
                        changelogRow("+I", 10, 2, "A", "Static"),
                        changelogRow("+I", 11, 2, "A", "Dynamic"));
        assertNoMoreRecords(streamItr);
        streamItr.close();

        testBatchRead(
                buildSimpleQuery(table),
                Arrays.asList(
                        changelogRow("+I", 1, 1, "A", "Hi"),
                        changelogRow("+I", 2, 1, "A", "Hello"),
                        changelogRow("+I", 3, 1, "A", "World"),
                        changelogRow("+I", 4, 1, "B", "Where"),
                        changelogRow("+I", 5, 1, "B", "When"),
                        changelogRow("+I", 10, 2, "A", "Static"),
                        changelogRow("+I", 11, 2, "A", "Dynamic"),
                        changelogRow("+I", 8, 2, "B", "Case")));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Purge data using overwrite (NOTE: set overwrite.dynamic-partition = false)
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testPurgeTableUsingBatchOverWrite() throws Exception {
        String table =
                createTable(
                        Arrays.asList("k0 INT", "k1 STRING", "v STRING"),
                        Collections.emptyList(),
                        Collections.singletonList("k0"),
                        Collections.emptyList(),
                        staticPartitionOverwrite);

        validatePurgingResult(table, "", "*", Collections.emptyList());
    }

    @Test
    public void testPurgePartitionUsingBatchOverWrite() throws Exception {
        List<String> fieldsSpec = Arrays.asList("k0 INT", "k1 STRING", "v STRING");

        // single partition key
        String table =
                createTable(
                        fieldsSpec,
                        Collections.emptyList(),
                        Collections.singletonList("k1"),
                        Collections.singletonList("k0"),
                        staticPartitionOverwrite);

        validatePurgingResult(
                table,
                "PARTITION (k0 = 0)",
                "k1, v",
                Arrays.asList(
                        changelogRow("+I", 1, "2023-01-01", "flink"),
                        changelogRow("+I", 1, "2023-01-02", "table"),
                        changelogRow("+I", 1, "2023-01-02", "store")));

        // multiple partition keys and overwrite one partition key
        table =
                createTable(
                        fieldsSpec,
                        Collections.emptyList(),
                        Collections.singletonList("v"),
                        Arrays.asList("k0", "k1"),
                        staticPartitionOverwrite);

        validatePurgingResult(
                table,
                "PARTITION (k0 = 0)",
                "k1, v",
                Arrays.asList(
                        changelogRow("+I", 1, "2023-01-01", "flink"),
                        changelogRow("+I", 1, "2023-01-02", "table"),
                        changelogRow("+I", 1, "2023-01-02", "store")));

        // multiple partition keys and overwrite all partition keys
        table =
                createTable(
                        fieldsSpec,
                        Collections.emptyList(),
                        Collections.singletonList("v"),
                        Arrays.asList("k0", "k1"),
                        staticPartitionOverwrite);

        validatePurgingResult(
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
    // Streaming Read of Overwrite
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testStreamingReadOverwriteWithPartitionedRecords() throws Exception {
        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt String"),
                        Arrays.asList("currency", "dt"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        streamingReadOverwrite);

        insertInto(
                table,
                "('US Dollar', 114, '2022-01-01')",
                "('Yen', 1, '2022-01-01')",
                "('Euro', 114, '2022-01-01')",
                "('Euro', 119, '2022-01-02')");

        checkFileStorePath(table, Arrays.asList("dt=2022-01-01", "dt=2022-01-02"));

        // test reading after overwriting
        insertOverwritePartition(table, "PARTITION (dt = '2022-01-01')", "('US Dollar', 120)");

        BlockingIterator<Row, Row> streamingItr =
                testStreamingRead(
                        buildSimpleQuery(table),
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 120L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test refresh after overwriting
        insertOverwritePartition(
                table, "PARTITION (dt = '2022-01-02')", "('Euro', 100)", "('Yen', 1)");

        validateStreamingReadResult(
                streamingItr,
                Arrays.asList(
                        // part = 2022-01-02
                        changelogRow("-D", "Euro", 119L, "2022-01-02"),
                        changelogRow("+I", "Euro", 100L, "2022-01-02"),
                        changelogRow("+I", "Yen", 1L, "2022-01-02")));

        streamingItr.close();
    }

    @Test
    public void testStreamingReadOverwriteWithoutPartitionedRecords() throws Exception {
        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        streamingReadOverwrite);

        insertInto(
                table,
                "('US Dollar', 102, '2022-01-01')",
                "('Yen', 1, '2022-01-02')",
                "('Euro', 119, '2022-01-02')");

        checkFileStorePath(table, Collections.emptyList());

        // test projection and filter
        BlockingIterator<Row, Row> streamingItr =
                testStreamingRead(
                        buildQuery(table, "currency, rate", "WHERE dt = '2022-01-02'"),
                        Arrays.asList(
                                changelogRow("+I", "Yen", 1L), changelogRow("+I", "Euro", 119L)));

        insertOverwrite(table, "('US Dollar', 100, '2022-01-02')", "('Yen', 10, '2022-01-01')");

        validateStreamingReadResult(
                streamingItr,
                Arrays.asList(
                        changelogRow("-D", "Yen", 1L),
                        changelogRow("-D", "Euro", 119L),
                        changelogRow("+I", "US Dollar", 100L)));

        streamingItr.close();
    }

    @Test
    public void testUnsupportStreamingReadOverwriteWithoutPk() {
        assertThatThrownBy(
                        () ->
                                createTable(
                                        Arrays.asList(
                                                "currency STRING", "rate BIGINT", "dt String"),
                                        Collections.emptyList(),
                                        Collections.singletonList("currency"),
                                        Collections.singletonList("dt"),
                                        streamingReadOverwrite))
                .satisfies(
                        anyCauseMatches(
                                RuntimeException.class,
                                "Doesn't support streaming read the changes from overwrite when the primary keys are not defined."));
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
                        Collections.singletonList("f0"),
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
                        changelogRow("+I", 20, "test_123"),
                        changelogRow("+I", 100, "test%fff")));

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
                        Collections.singletonList("f0"),
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
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        Collections.singletonMap(INFER_SCAN_PARALLELISM.key(), "false"));

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
                                                put(INFER_SCAN_PARALLELISM.key(), "false");
                                                put(SCAN_PARALLELISM.key(), "66");
                                            }
                                        })))
                .isEqualTo(66);
    }

    @Test
    void testConvertRowType2Serializer() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE CATALOG my_catalog WITH (\n"
                        + "    'type' = 'paimon',\n"
                        + "    'warehouse' = '"
                        +  getTempDirPath()
                        + "'\n"
                        + ")");
        tEnv.executeSql("USE CATALOG my_catalog");
        tEnv.executeSql(
                "CREATE TABLE tmp (\n"
                        + "execution\n"
                        + "ROW<`execution_server` STRING, `execution_insertion` ARRAY<ROW<`platform_id` BIGINT, `user_info` ROW<`user_id` STRING, `log_user_id` STRING, `is_internal_user` BOOLEAN, `ignore_usage` BOOLEAN, `anon_user_id` STRING, `retained_user_id` STRING>, `timing` ROW<`client_log_timestamp` BIGINT, `event_api_timestamp` BIGINT, `log_timestamp` BIGINT, `processing_timestamp` BIGINT>, `client_info` ROW<`client_type` STRING, `traffic_type` STRING>, `insertion_id` STRING, `request_id` STRING, `view_id` STRING, `auto_view_id` STRING, `session_id` STRING, `content_id` STRING, `position` BIGINT, `properties` ROW<`struct` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>> NOT NULL>>> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>> NOT NULL>>> NOT NULL>>> NOT NULL>>>> NOT NULL>>, `struct_json` STRING>, `feature_stage` ROW<`features` ROW<`numeric` ARRAY<ROW<`key` INT, `value` FLOAT> NOT NULL>, `categorical` ARRAY<ROW<`key` INT, `value` STRING> NOT NULL>, `sparse` ARRAY<ROW<`key` BIGINT, `value` FLOAT> NOT NULL>, `sparse_id` ARRAY<ROW<`key` BIGINT, `value` BIGINT> NOT NULL>, `embeddings` ARRAY<ROW<`key` BIGINT, `value` ROW<`embeddings` ARRAY<FLOAT>>> NOT NULL>, `feature_references` ARRAY<ROW<`type` STRING, `key` STRING, `version` STRING, `timestamp` BIGINT> NOT NULL>, `sparse_id_list` ARRAY<ROW<`key` BIGINT, `value` ROW<`ids` ARRAY<BIGINT>>> NOT NULL>, `user_events` ROW<`user_events` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>, `user_events_all` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>>, `string_features` ARRAY<ROW<`key` BIGINT, `value` STRING> NOT NULL>>>, `personalize_stage` ROW<`personalize_ranking_score` FLOAT, `score_source` STRING, `personalize_ranking_scores` ARRAY<ROW<`key` STRING, `value` ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT>> NOT NULL>, `model_scores` ARRAY<ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT> NOT NULL>, `models` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>>, `predictor_stage` ROW<`model_scores` ARRAY<ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT> NOT NULL>, `backoff_predictors` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>, `models` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>>, `blender_stage` ROW<`score` FLOAT, `steps` ARRAY<ROW<`force_step` ROW<`reason` STRING>, `boost_step` ROW<`fid` BIGINT, `delta` FLOAT>> NOT NULL>, `sort_key` ARRAY<FLOAT>, `experiments` ARRAY<ROW<`experiment_ref` INT, `score` FLOAT> NOT NULL>>, `retrieval_rank` BIGINT, `retrieval_score` FLOAT> NOT NULL>, `latency` ARRAY<ROW<`method` STRING, `start_millis` BIGINT, `duration_millis` INT> NOT NULL>, `execution_stats` ROW<`stages` ARRAY<ROW<`key` INT, `value` ROW<`stats` ARRAY<ROW<`key` INT, `value` BIGINT> NOT NULL>>> NOT NULL>>, `request_feature_stage` ROW<`features` ROW<`numeric` ARRAY<ROW<`key` INT, `value` FLOAT> NOT NULL>, `categorical` ARRAY<ROW<`key` INT, `value` STRING> NOT NULL>, `sparse` ARRAY<ROW<`key` BIGINT, `value` FLOAT> NOT NULL>, `sparse_id` ARRAY<ROW<`key` BIGINT, `value` BIGINT> NOT NULL>, `embeddings` ARRAY<ROW<`key` BIGINT, `value` ROW<`embeddings` ARRAY<FLOAT>>> NOT NULL>, `feature_references` ARRAY<ROW<`type` STRING, `key` STRING, `version` STRING, `timestamp` BIGINT> NOT NULL>, `sparse_id_list` ARRAY<ROW<`key` BIGINT, `value` ROW<`ids` ARRAY<BIGINT>>> NOT NULL>, `user_events` ROW<`user_events` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>, `user_events_all` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>>, `string_features` ARRAY<ROW<`key` BIGINT, `value` STRING> NOT NULL>>>, `user_feature_stage` ROW<`features` ROW<`numeric` ARRAY<ROW<`key` INT, `value` FLOAT> NOT NULL>, `categorical` ARRAY<ROW<`key` INT, `value` STRING> NOT NULL>, `sparse` ARRAY<ROW<`key` BIGINT, `value` FLOAT> NOT NULL>, `sparse_id` ARRAY<ROW<`key` BIGINT, `value` BIGINT> NOT NULL>, `embeddings` ARRAY<ROW<`key` BIGINT, `value` ROW<`embeddings` ARRAY<FLOAT>>> NOT NULL>, `feature_references` ARRAY<ROW<`type` STRING, `key` STRING, `version` STRING, `timestamp` BIGINT> NOT NULL>, `sparse_id_list` ARRAY<ROW<`key` BIGINT, `value` ROW<`ids` ARRAY<BIGINT>>> NOT NULL>, `user_events` ROW<`user_events` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>, `user_events_all` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>>, `string_features` ARRAY<ROW<`key` BIGINT, `value` STRING> NOT NULL>>>, `model_ref` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>, `server_version` STRING, `after_response_stage` ROW<`removed_execution_insertion_count` INT>, `personalize_stage` ROW<`personalize_ranking_score` FLOAT, `score_source` STRING, `personalize_ranking_scores` ARRAY<ROW<`key` STRING, `value` ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT>> NOT NULL>, `model_scores` ARRAY<ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT> NOT NULL>, `models` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>>, `predictor_stage` ROW<`model_scores` ARRAY<ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT> NOT NULL>, `backoff_predictors` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>, `models` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>>, `blender_config` STRING, `hyperloop_log` ROW<`parameter_logs` ARRAY<ROW<`key` BIGINT, `value` ROW<`bucket` INT, `value` FLOAT>> NOT NULL>>, `blender_session_log` ROW<`config_statements` ARRAY<STRING>, `ids` ARRAY<STRING>, `variable_logs` ARRAY<ROW<`name` STRING, `values` ARRAY<FLOAT>> NOT NULL>, `allocation_logs` ARRAY<ROW<`indexes` ARRAY<INT>, `name` STRING, `positions_considered` ARRAY<INT>, `positions_filled` ARRAY<INT>> NOT NULL>>, `experiments` ARRAY<ROW<`name` STRING, `cohort_arm` INT> NOT NULL>, `effective_user_info` ROW<`user_id` STRING, `log_user_id` STRING, `is_internal_user` BOOLEAN, `ignore_usage` BOOLEAN, `anon_user_id` STRING, `retained_user_id` STRING>>);");
        assertThatThrownBy(()->
                tEnv.executeSql(
                                "INSERT INTO tmp VALUES (CAST(NULL AS ROW<`execution_server` STRING, `execution_insertion` ARRAY<ROW<`platform_id` BIGINT, `user_info` ROW<`user_id` STRING, `log_user_id` STRING, `is_internal_user` BOOLEAN, `ignore_usage` BOOLEAN, `anon_user_id` STRING, `retained_user_id` STRING>, `timing` ROW<`client_log_timestamp` BIGINT, `event_api_timestamp` BIGINT, `log_timestamp` BIGINT, `processing_timestamp` BIGINT>, `client_info` ROW<`client_type` STRING, `traffic_type` STRING>, `insertion_id` STRING, `request_id` STRING, `view_id` STRING, `auto_view_id` STRING, `session_id` STRING, `content_id` STRING, `position` BIGINT, `properties` ROW<`struct` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>> NOT NULL>>> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN, `struct_value` ROW<`fields` ARRAY<ROW<`key` STRING, `value` ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN>> NOT NULL>>, `list_value` ROW<`values` ARRAY<ROW<`null_value` STRING, `number_value` DOUBLE, `string_value` STRING, `bool_value` BOOLEAN> NOT NULL>>> NOT NULL>>> NOT NULL>>> NOT NULL>>>> NOT NULL>>, `struct_json` STRING>, `feature_stage` ROW<`features` ROW<`numeric` ARRAY<ROW<`key` INT, `value` FLOAT> NOT NULL>, `categorical` ARRAY<ROW<`key` INT, `value` STRING> NOT NULL>, `sparse` ARRAY<ROW<`key` BIGINT, `value` FLOAT> NOT NULL>, `sparse_id` ARRAY<ROW<`key` BIGINT, `value` BIGINT> NOT NULL>, `embeddings` ARRAY<ROW<`key` BIGINT, `value` ROW<`embeddings` ARRAY<FLOAT>>> NOT NULL>, `feature_references` ARRAY<ROW<`type` STRING, `key` STRING, `version` STRING, `timestamp` BIGINT> NOT NULL>, `sparse_id_list` ARRAY<ROW<`key` BIGINT, `value` ROW<`ids` ARRAY<BIGINT>>> NOT NULL>, `user_events` ROW<`user_events` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>, `user_events_all` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>>, `string_features` ARRAY<ROW<`key` BIGINT, `value` STRING> NOT NULL>>>, `personalize_stage` ROW<`personalize_ranking_score` FLOAT, `score_source` STRING, `personalize_ranking_scores` ARRAY<ROW<`key` STRING, `value` ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT>> NOT NULL>, `model_scores` ARRAY<ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT> NOT NULL>, `models` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>>, `predictor_stage` ROW<`model_scores` ARRAY<ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT> NOT NULL>, `backoff_predictors` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>, `models` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>>, `blender_stage` ROW<`score` FLOAT, `steps` ARRAY<ROW<`force_step` ROW<`reason` STRING>, `boost_step` ROW<`fid` BIGINT, `delta` FLOAT>> NOT NULL>, `sort_key` ARRAY<FLOAT>, `experiments` ARRAY<ROW<`experiment_ref` INT, `score` FLOAT> NOT NULL>>, `retrieval_rank` BIGINT, `retrieval_score` FLOAT> NOT NULL>, `latency` ARRAY<ROW<`method` STRING, `start_millis` BIGINT, `duration_millis` INT> NOT NULL>, `execution_stats` ROW<`stages` ARRAY<ROW<`key` INT, `value` ROW<`stats` ARRAY<ROW<`key` INT, `value` BIGINT> NOT NULL>>> NOT NULL>>, `request_feature_stage` ROW<`features` ROW<`numeric` ARRAY<ROW<`key` INT, `value` FLOAT> NOT NULL>, `categorical` ARRAY<ROW<`key` INT, `value` STRING> NOT NULL>, `sparse` ARRAY<ROW<`key` BIGINT, `value` FLOAT> NOT NULL>, `sparse_id` ARRAY<ROW<`key` BIGINT, `value` BIGINT> NOT NULL>, `embeddings` ARRAY<ROW<`key` BIGINT, `value` ROW<`embeddings` ARRAY<FLOAT>>> NOT NULL>, `feature_references` ARRAY<ROW<`type` STRING, `key` STRING, `version` STRING, `timestamp` BIGINT> NOT NULL>, `sparse_id_list` ARRAY<ROW<`key` BIGINT, `value` ROW<`ids` ARRAY<BIGINT>>> NOT NULL>, `user_events` ROW<`user_events` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>, `user_events_all` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>>, `string_features` ARRAY<ROW<`key` BIGINT, `value` STRING> NOT NULL>>>, `user_feature_stage` ROW<`features` ROW<`numeric` ARRAY<ROW<`key` INT, `value` FLOAT> NOT NULL>, `categorical` ARRAY<ROW<`key` INT, `value` STRING> NOT NULL>, `sparse` ARRAY<ROW<`key` BIGINT, `value` FLOAT> NOT NULL>, `sparse_id` ARRAY<ROW<`key` BIGINT, `value` BIGINT> NOT NULL>, `embeddings` ARRAY<ROW<`key` BIGINT, `value` ROW<`embeddings` ARRAY<FLOAT>>> NOT NULL>, `feature_references` ARRAY<ROW<`type` STRING, `key` STRING, `version` STRING, `timestamp` BIGINT> NOT NULL>, `sparse_id_list` ARRAY<ROW<`key` BIGINT, `value` ROW<`ids` ARRAY<BIGINT>>> NOT NULL>, `user_events` ROW<`user_events` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>, `user_events_all` ARRAY<ROW<`event_type` BIGINT, `content_id` STRING, `timestamp` BIGINT, `custom_event_type` STRING> NOT NULL>>, `string_features` ARRAY<ROW<`key` BIGINT, `value` STRING> NOT NULL>>>, `model_ref` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>, `server_version` STRING, `after_response_stage` ROW<`removed_execution_insertion_count` INT>, `personalize_stage` ROW<`personalize_ranking_score` FLOAT, `score_source` STRING, `personalize_ranking_scores` ARRAY<ROW<`key` STRING, `value` ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT>> NOT NULL>, `model_scores` ARRAY<ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT> NOT NULL>, `models` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>>, `predictor_stage` ROW<`model_scores` ARRAY<ROW<`score` FLOAT, `score_source` STRING, `model_type` STRING, `model_id` STRING, `model_index` INT> NOT NULL>, `backoff_predictors` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>, `models` ARRAY<ROW<`model_id` STRING, `model_type` STRING, `prediction_type` STRING, `name` STRING, `feature_id` BIGINT, `is_assigned` BOOLEAN> NOT NULL>>, `blender_config` STRING, `hyperloop_log` ROW<`parameter_logs` ARRAY<ROW<`key` BIGINT, `value` ROW<`bucket` INT, `value` FLOAT>> NOT NULL>>, `blender_session_log` ROW<`config_statements` ARRAY<STRING>, `ids` ARRAY<STRING>, `variable_logs` ARRAY<ROW<`name` STRING, `values` ARRAY<FLOAT>> NOT NULL>, `allocation_logs` ARRAY<ROW<`indexes` ARRAY<INT>, `name` STRING, `positions_considered` ARRAY<INT>, `positions_filled` ARRAY<INT>> NOT NULL>>, `experiments` ARRAY<ROW<`name` STRING, `cohort_arm` INT> NOT NULL>, `effective_user_info` ROW<`user_id` STRING, `log_user_id` STRING, `is_internal_user` BOOLEAN, `ignore_usage` BOOLEAN, `anon_user_id` STRING, `retained_user_id` STRING>>))")
                        .print() ).doesNotThrowAnyException();
    }

    @Test
    public void testInferParallelism() throws Exception {
        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(SOURCE_SPLIT_OPEN_FILE_COST.key(), "1KB");
                                put(SOURCE_SPLIT_TARGET_SIZE.key(), "1KB");
                                put(BUCKET.key(), "2");
                            }
                        });
        // Empty table, infer parallelism should be at least 1
        assertThat(
                        sourceParallelism(
                                buildQueryWithTableOptions(
                                        table,
                                        "*",
                                        "",
                                        Collections.singletonMap(
                                                INFER_SCAN_PARALLELISM.key(), "true"))))
                .isEqualTo(1);

        // with scan.parallelism, respect scan.parallelism
        assertThat(
                        sourceParallelism(
                                buildQueryWithTableOptions(
                                        table,
                                        "*",
                                        "",
                                        new HashMap<String, String>() {
                                            {
                                                put(INFER_SCAN_PARALLELISM.key(), "true");
                                                put(SCAN_PARALLELISM.key(), "3");
                                            }
                                        })))
                .isEqualTo(3);

        // with illegal scan.parallelism, respect illegal scan.parallelism
        assertThatThrownBy(
                        () ->
                                sourceParallelism(
                                        buildQueryWithTableOptions(
                                                table,
                                                "*",
                                                "",
                                                new HashMap<String, String>() {
                                                    {
                                                        put(INFER_SCAN_PARALLELISM.key(), "true");
                                                        put(SCAN_PARALLELISM.key(), "-2");
                                                    }
                                                })))
                .hasMessageContaining("The parallelism of an operator must be at least 1");

        // 2 splits, the parallelism is splits num: 2
        insertInto(table, "('Euro', 119)");
        insertInto(table, "('US Dollar', 102)");
        assertThat(
                        sourceParallelism(
                                buildQueryWithTableOptions(
                                        table,
                                        "*",
                                        "",
                                        Collections.singletonMap(
                                                INFER_SCAN_PARALLELISM.key(), "true"))))
                .isEqualTo(2);
        assertThat(
                        sourceParallelism(
                                buildQueryWithTableOptions(
                                        table,
                                        "*",
                                        "WHERE currency='Euro'",
                                        Collections.singletonMap(
                                                INFER_SCAN_PARALLELISM.key(), "true"))))
                .isEqualTo(1);

        // 2 splits and limit is 1, the parallelism is the limit value : 1
        assertThat(
                        sourceParallelism(
                                buildQueryWithTableOptions(
                                        table,
                                        "*",
                                        "",
                                        1L,
                                        Collections.singletonMap(
                                                INFER_SCAN_PARALLELISM.key(), "true"))))
                .isEqualTo(1);

        // 2 splits, limit is 3, the parallelism is infer parallelism : 2
        assertThat(
                        sourceParallelism(
                                buildQueryWithTableOptions(
                                        table,
                                        "*",
                                        "",
                                        3L,
                                        Collections.singletonMap(
                                                INFER_SCAN_PARALLELISM.key(), "true"))))
                .isEqualTo(1);

        // 2 splits, infer parallelism is disabled, the parallelism is scan.parallelism
        assertThat(
                        sourceParallelism(
                                buildQueryWithTableOptions(
                                        table,
                                        "*",
                                        "",
                                        new HashMap<String, String>() {
                                            {
                                                put(INFER_SCAN_PARALLELISM.key(), "false");
                                                put(SCAN_PARALLELISM.key(), "3");
                                            }
                                        })))
                .isEqualTo(3);

        // when scan.infer-parallelism.max less than infer parallelism, the parallelism is
        // scan.infer-parallelism.max
        assertThat(
                        sourceParallelism(
                                buildQueryWithTableOptions(
                                        table,
                                        "*",
                                        "",
                                        new HashMap<String, String>() {
                                            {
                                                put(INFER_SCAN_PARALLELISM.key(), "true");
                                                put(INFER_SCAN_MAX_PARALLELISM.key(), "1");
                                            }
                                        })))
                .isEqualTo(1);

        // for streaming mode
        assertThat(
                        sourceParallelismStreaming(
                                buildQueryWithTableOptions(table, "*", "", new HashMap<>())))
                .isEqualTo(2);
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
                                + " 'bucket' = '2',\n"
                                + " 'bucket-key' = 'currency'\n"
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
                        Collections.singletonList("currency"),
                        Collections.singletonList("dt"));

        assertThatThrownBy(
                        () ->
                                sEnv.executeSql(
                                        String.format(
                                                "INSERT OVERWRITE `%s` VALUES('US Dollar', 102, '2022-06-20')",
                                                table)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon doesn't support streaming INSERT OVERWRITE.");
    }

    @Test
    public void testPhysicalColumnComments() {
        String ddl = "CREATE TABLE T(a INT COMMENT 'comment of a', b INT);";
        bEnv.executeSql(ddl);

        List<String> result =
                CollectionUtil.iteratorToList(bEnv.executeSql("DESC T").collect()).stream()
                        .map(Objects::toString)
                        .collect(Collectors.toList());

        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, INT, true, null, null, null, comment of a]",
                        "+I[b, INT, true, null, null, null, null]");
    }

    @Test
    public void testComputedColumnComments() {
        String ddl = "CREATE TABLE T(a INT , b INT, c AS a + b COMMENT 'computed');";
        bEnv.executeSql(ddl);

        List<String> result =
                CollectionUtil.iteratorToList(bEnv.executeSql("DESC T").collect()).stream()
                        .map(Objects::toString)
                        .collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, INT, true, null, null, null, null]",
                        "+I[b, INT, true, null, null, null, null]",
                        "+I[c, INT, true, null, AS `a` + `b`, null, computed]");
    }

    @Test
    public void testCleanedSchemaOptions() {
        String ddl =
                "CREATE TABLE T (\n"
                        + "id INT,\n"
                        + "price INT,\n"
                        + "record_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,\n"
                        + "comp AS price * 2,\n"
                        + "order_time TIMESTAMP(3),\n"
                        + "WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,\n"
                        + "PRIMARY KEY (id) NOT ENFORCED\n"
                        + ");";
        bEnv.executeSql(ddl);

        // validate schema options
        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(warehouse, "default.db/T"));
        TableSchema schema = schemaManager.latest().get();
        Map<String, String> expected = new HashMap<>();
        // metadata column
        expected.put("schema.2.name", "record_time");
        expected.put("schema.2.data-type", "TIMESTAMP(3) WITH LOCAL TIME ZONE");
        expected.put("schema.2.metadata", "timestamp");
        expected.put("schema.2.virtual", "true");
        // computed column
        expected.put("schema.3.name", "comp");
        expected.put("schema.3.data-type", "INT");
        expected.put("schema.3.expr", "`price` * 2");
        // watermark
        expected.put("schema.watermark.0.rowtime", "order_time");
        expected.put("schema.watermark.0.strategy.expr", "`order_time` - INTERVAL '5' SECOND");
        expected.put("schema.watermark.0.strategy.data-type", "TIMESTAMP(3)");

        assertThat(schema.options()).containsExactlyInAnyOrderEntriesOf(expected);

        validateSchemaOptionResult();
    }

    @Test
    public void testReadFromOldStyleSchemaOptions() throws Exception {
        Map<String, String> oldStyleOptions = new HashMap<>();
        oldStyleOptions.put("schema.0.name", "id");
        oldStyleOptions.put("schema.0.data-type", "INT NOT NULL");

        oldStyleOptions.put("schema.1.name", "price");
        oldStyleOptions.put("schema.1.data-type", "INT");

        oldStyleOptions.put("schema.2.name", "record_time");
        oldStyleOptions.put("schema.2.data-type", "TIMESTAMP(3) WITH LOCAL TIME ZONE");
        oldStyleOptions.put("schema.2.metadata", "timestamp");
        oldStyleOptions.put("schema.2.virtual", "true");

        oldStyleOptions.put("schema.3.name", "comp");
        oldStyleOptions.put("schema.3.data-type", "INT");
        oldStyleOptions.put("schema.3.expr", "`price` * 2");

        oldStyleOptions.put("schema.4.name", "order_time");
        oldStyleOptions.put("schema.4.data-type", "TIMESTAMP(3)");

        oldStyleOptions.put("schema.watermark.0.rowtime", "order_time");
        oldStyleOptions.put(
                "schema.watermark.0.strategy.expr", "`order_time` - INTERVAL '5' SECOND");
        oldStyleOptions.put("schema.watermark.0.strategy.data-type", "TIMESTAMP(3)");

        oldStyleOptions.put("schema.primary-key.name", "constrain_pk");
        oldStyleOptions.put("schema.primary-key.columns", "id");

        // create corresponding table
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("price", DataTypes.INT())
                        .column("order_time", DataTypes.TIMESTAMP(3))
                        .options(oldStyleOptions)
                        .primaryKey("id")
                        .build();

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(warehouse, "default.db/T"));
        schemaManager.createTable(schema);

        validateSchemaOptionResult();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Update statement
    // ----------------------------------------------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"deduplicate", "partial-update"})
    public void testUpdateWithPrimaryKey(String mergeEngine) throws Exception {
        // Step1: define table schema
        Map<String, String> options = new HashMap<>();
        options.put(MERGE_ENGINE.key(), mergeEngine);
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "currency STRING",
                                "rate BIGINT",
                                "dt String"),
                        Arrays.asList("id", "dt"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        options);

        // Step2: batch write some historical data
        insertInto(
                table,
                "(1, 'US Dollar', 114, '2022-01-01')",
                "(2, 'UNKNOWN', -1, '2022-01-01')",
                "(3, 'Euro', 114, '2022-01-01')",
                "(3, 'Euro', 119, '2022-01-02')");

        // Step3: prepare update statement
        String updateStatement =
                String.format(
                        "UPDATE %s "
                                + "SET currency = 'Yen', "
                                + "rate = 1 "
                                + "WHERE currency = 'UNKNOWN' and dt = '2022-01-01'",
                        table);

        // Step4: execute update statement and verify result
        bEnv.executeSql(updateStatement).await();
        String querySql = String.format("SELECT * FROM %s", table);
        String rowKind = mergeEngine.equals("deduplicate") ? "+U" : "+I";
        testBatchRead(
                querySql,
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", 1L, "US Dollar", 114L, "2022-01-01"),
                        changelogRow(rowKind, 2L, "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", 3L, "Euro", 114L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", 3L, "Euro", 119L, "2022-01-02")));
    }

    @Test
    public void testDefaultValueWithoutPrimaryKey() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(
                CoreOptions.FIELDS_PREFIX + ".rate." + CoreOptions.DEFAULT_VALUE_SUFFIX, "1000");

        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "currency STRING",
                                "rate BIGINT",
                                "dt String"),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        options);
        insertInto(
                table,
                "(1, 'US Dollar', 114, '2022-01-01')",
                "(2, 'Yen', cast(null as int), '2022-01-01')",
                "(3, 'Euro', cast(null as int), '2022-01-01')",
                "(3, 'Euro', 119, '2022-01-02')");

        List<Row> expectedRecords =
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", 2L, "Yen", 1000L, "2022-01-01"),
                        changelogRow("+I", 3L, "Euro", 1000L, "2022-01-01"));

        String querySql = String.format("SELECT * FROM %s where rate = 1000", table);
        testBatchRead(querySql, expectedRecords);
    }

    @ParameterizedTest
    @EnumSource(CoreOptions.MergeEngine.class)
    public void testDefaultValueWithPrimaryKey(CoreOptions.MergeEngine mergeEngine)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(
                CoreOptions.FIELDS_PREFIX + ".rate." + CoreOptions.DEFAULT_VALUE_SUFFIX, "1000");
        options.put(MERGE_ENGINE.key(), mergeEngine.toString());
        if (mergeEngine == FIRST_ROW) {
            options.put(CHANGELOG_PRODUCER.key(), LOOKUP.toString());
        }
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "currency STRING",
                                "rate BIGINT",
                                "dt String"),
                        Lists.newArrayList("id", "dt"),
                        Collections.emptyList(),
                        Lists.newArrayList("dt"),
                        options);
        insertInto(
                table,
                "(1, 'US Dollar', 114, '2022-01-01')",
                "(2, 'Yen', cast(null as int), '2022-01-01')",
                "(2, 'Yen', cast(null as int), '2022-01-01')",
                "(3, 'Euro', cast(null as int) , '2022-01-02')");

        List<Row> expectedRecords =
                Arrays.asList(changelogRow("+I", 3L, "Euro", 1000L, "2022-01-02"));

        String querySql =
                String.format("SELECT * FROM %s where rate = 1000 and currency ='Euro'", table);
        testBatchRead(querySql, expectedRecords);
    }

    @Test
    public void testUpdateWithoutPrimaryKey() throws Exception {
        // Step1: define table schema
        Map<String, String> options = new HashMap<>();
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "currency STRING",
                                "rate BIGINT",
                                "dt String"),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.singletonList("dt"),
                        options);

        // Step2: batch write some historical data
        insertInto(
                table,
                "(1, 'US Dollar', 114, '2022-01-01')",
                "(2, 'UNKNOWN', -1, '2022-01-01')",
                "(3, 'Euro', 114, '2022-01-01')",
                "(3, 'Euro', 119, '2022-01-02')");

        // Step3: prepare update statement
        String updateStatement =
                String.format(
                        ""
                                + "UPDATE %s "
                                + "SET currency = 'Yen', "
                                + "rate = 1 "
                                + "WHERE currency = 'UNKNOWN' and dt = '2022-01-01'",
                        table);

        // Step4: execute update statement
        assertThatThrownBy(() -> bEnv.executeSql(updateStatement).await())
                .satisfies(anyCauseMatches(UnsupportedOperationException.class));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Delete statement
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testDeleteWithPrimaryKey() throws Exception {
        // Step1: define table schema
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "currency STRING",
                                "rate BIGINT",
                                "dt String"),
                        Arrays.asList("id", "dt"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        Collections.emptyMap());

        // Step2: batch write some historical data
        insertInto(
                table,
                "(1, 'US Dollar', 114, '2022-01-01')",
                "(2, 'UNKNOWN', -1, '2022-01-01')",
                "(3, 'Euro', 119, '2022-01-02')");

        // Step3: prepare delete statement
        String deleteStatement = String.format("DELETE FROM %s WHERE currency = 'UNKNOWN'", table);

        // Step4: execute delete statement and verify result
        bEnv.executeSql(deleteStatement).await();
        String querySql = String.format("SELECT * FROM %s", table);
        testBatchRead(
                querySql,
                Arrays.asList(
                        changelogRow("+I", 1L, "US Dollar", 114L, "2022-01-01"),
                        changelogRow("+I", 3L, "Euro", 119L, "2022-01-02")));
    }

    @Test
    public void testDeleteWithoutPrimaryKey() throws Exception {
        // Step1: define table schema
        Map<String, String> options = new HashMap<>();
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "currency STRING",
                                "rate BIGINT",
                                "dt String"),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.singletonList("dt"),
                        options);

        // Step2: batch write some historical data
        insertInto(
                table,
                "(1, 'US Dollar', 114, '2022-01-01')",
                "(2, 'UNKNOWN', -1, '2022-01-01')",
                "(3, 'Euro', 119, '2022-01-02')");

        // Step3: prepare delete statement
        String deleteStatement = String.format("DELETE FROM %s WHERE currency = 'UNKNOWN'", table);

        // Step4: execute delete statement and verify result
        assertThatThrownBy(() -> bEnv.executeSql(deleteStatement).await())
                .satisfies(anyCauseMatches(UnsupportedOperationException.class));
    }

    @Test
    public void testDeleteWithPrimaryKeyFilter() throws Exception {
        // Step1: define table schema
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "currency STRING",
                                "rate BIGINT",
                                "dt String"),
                        Arrays.asList("id", "dt"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        Collections.emptyMap());

        // Step2: batch write some historical data
        insertInto(
                table,
                "(1, 'US Dollar', 114, '2022-01-01')",
                "(2, 'UNKNOWN', -1, '2022-01-01')",
                "(3, 'Euro', 119, '2022-01-02')",
                "(4, 'CNY', 119, '2022-01-02')",
                "(5, 'HKD', 119, '2022-01-03')",
                "(6, 'CAD', 119, '2022-01-03')",
                "(7, 'INR', 119, '2022-01-03')",
                "(8, 'MOP', 119, '2022-01-03')");

        // Test1 delete statement 'where pk = x'
        String deleteStatement =
                String.format("DELETE FROM %s WHERE id = 2 and dt = '2022-01-01'", table);
        List<Row> expectedRecords =
                Arrays.asList(
                        changelogRow("+I", 1L, "US Dollar", 114L, "2022-01-01"),
                        changelogRow("+I", 3L, "Euro", 119L, "2022-01-02"),
                        changelogRow("+I", 4L, "CNY", 119L, "2022-01-02"),
                        changelogRow("+I", 5L, "HKD", 119L, "2022-01-03"),
                        changelogRow("+I", 6L, "CAD", 119L, "2022-01-03"),
                        changelogRow("+I", 7L, "INR", 119L, "2022-01-03"),
                        changelogRow("+I", 8L, "MOP", 119L, "2022-01-03"));
        bEnv.executeSql(deleteStatement).await();
        String querySql = String.format("SELECT * FROM %s", table);
        testBatchRead(querySql, expectedRecords);

        // Test2 delete statement no where
        String deleteStatement2 = String.format("DELETE FROM %s", table);
        bEnv.executeSql(deleteStatement2).await();
        testBatchRead(String.format("SELECT * FROM %s", table), Collections.emptyList());

        // Test3 delete statement where pt
        String deleteStatement3 = String.format("DELETE FROM %s WHERE dt = '2022-01-03'", table);
        bEnv.executeSql(deleteStatement3).await();
        testBatchRead(String.format("SELECT * FROM %s", table), Collections.emptyList());
    }

    @Test
    public void testDeletePushDownWithPartitionKey() throws Exception {
        // Step1: define table schema
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "currency STRING",
                                "rate BIGINT",
                                "dt String",
                                "hh String"),
                        Arrays.asList("id", "dt", "hh"),
                        Collections.emptyList(),
                        Arrays.asList("dt", "hh"),
                        Collections.emptyMap());

        // Step2: batch write some historical data
        insertInto(
                table,
                "(1, 'US Dollar', 114, '2022-01-01', '11')",
                "(2, 'UNKNOWN', -1, '2022-01-01', '12')",
                "(3, 'Euro', 119, '2022-01-02', '13')",
                "(4, 'CNY', 119, '2022-01-03', '14')",
                "(5, 'HKD', 119, '2022-01-03', '15')",
                "(6, 'CAD', 119, '2022-01-03', '16')",
                "(7, 'INR', 119, '2022-01-03', '17')",
                "(8, 'MOP', 119, '2022-01-03', '18')");

        // Step3: partition key not delete push down
        String deleteStatement =
                String.format("DELETE FROM %s WHERE dt = '2022-01-03' AND currency = 'CNY'", table);

        // Step4: execute delete statement and verify result
        List<Row> expectedRecords =
                Arrays.asList(
                        changelogRow("+I", 1L, "US Dollar", 114L, "2022-01-01", "11"),
                        changelogRow("+I", 2L, "UNKNOWN", -1L, "2022-01-01", "12"),
                        changelogRow("+I", 3L, "Euro", 119L, "2022-01-02", "13"),
                        changelogRow("+I", 5L, "HKD", 119L, "2022-01-03", "15"),
                        changelogRow("+I", 6L, "CAD", 119L, "2022-01-03", "16"),
                        changelogRow("+I", 7L, "INR", 119L, "2022-01-03", "17"),
                        changelogRow("+I", 8L, "MOP", 119L, "2022-01-03", "18"));
        bEnv.executeSql(deleteStatement).await();
        String querySql = String.format("SELECT * FROM %s", table);
        testBatchRead(querySql, expectedRecords);

        // Step5: partition key not push down
        String deleteStatement1 =
                String.format("DELETE FROM %s WHERE dt = '2022-01-02' or hh = '15'", table);
        List<Row> expectedRecords1 =
                Arrays.asList(
                        changelogRow("+I", 1L, "US Dollar", 114L, "2022-01-01", "11"),
                        changelogRow("+I", 2L, "UNKNOWN", -1L, "2022-01-01", "12"),
                        changelogRow("+I", 6L, "CAD", 119L, "2022-01-03", "16"),
                        changelogRow("+I", 7L, "INR", 119L, "2022-01-03", "17"),
                        changelogRow("+I", 8L, "MOP", 119L, "2022-01-03", "18"));
        bEnv.executeSql(deleteStatement1).await();
        testBatchRead(String.format("SELECT * FROM %s", table), expectedRecords1);

        // Step6: partition key delete push down
        String deleteStatement2 =
                String.format("DELETE FROM %s WHERE dt = '2022-01-03' and hh = '16'", table);

        // Step7: execute delete statement and verify result
        List<Row> expectedRecords2 =
                Arrays.asList(
                        changelogRow("+I", 1L, "US Dollar", 114L, "2022-01-01", "11"),
                        changelogRow("+I", 2L, "UNKNOWN", -1L, "2022-01-01", "12"),
                        changelogRow("+I", 7L, "INR", 119L, "2022-01-03", "17"),
                        changelogRow("+I", 8L, "MOP", 119L, "2022-01-03", "18"));
        bEnv.executeSql(deleteStatement2).await();
        testBatchRead(String.format("SELECT * FROM %s", table), expectedRecords2);

        // Step8: partition key delete push down
        String deleteStatement3 = String.format("DELETE FROM %s WHERE dt = '2022-01-03'", table);

        // Step9: execute delete statement and verify result
        List<Row> expectedRecords3 =
                Arrays.asList(
                        changelogRow("+I", 1L, "US Dollar", 114L, "2022-01-01", "11"),
                        changelogRow("+I", 2L, "UNKNOWN", -1L, "2022-01-01", "12"));
        bEnv.executeSql(deleteStatement3).await();
        testBatchRead(String.format("SELECT * FROM %s", table), expectedRecords3);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Tools
    // ----------------------------------------------------------------------------------------------------------------

    private void validatePurgingResult(
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

    private int sourceParallelismStreaming(String sql) {
        DataStream<Row> stream =
                ((StreamTableEnvironment) sEnv).toChangelogStream(sEnv.sqlQuery(sql));
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
        options.put("bucket", "1");
        options.put("bucket-key", "a");

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
        LocalFileIO.create().mkdirs(path);
        // update schema
        new SchemaManager(LocalFileIO.create(), path)
                .createTable(FlinkCatalog.fromCatalogTable(context.getCatalogTable()));

        DynamicTableSink tableSink =
                new FlinkTableSink(
                        context.getObjectIdentifier(), buildPaimonTable(context), context, null);
        assertThat(tableSink).isInstanceOf(FlinkTableSink.class);

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
        // until a PartitionTransformation, see FlinkSinkBuilder.build()
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

    private void validateSchemaOptionResult() {
        // validate columns
        List<String> descResults =
                CollectionUtil.iteratorToList(bEnv.executeSql("DESC T").collect()).stream()
                        .map(Object::toString)
                        .collect(Collectors.toList());
        assertThat(descResults)
                .isEqualTo(
                        Arrays.asList(
                                "+I[id, INT, false, PRI(id), null, null]",
                                "+I[price, INT, true, null, null, null]",
                                "+I[record_time, TIMESTAMP_LTZ(3), true, null, METADATA FROM 'timestamp' VIRTUAL, null]",
                                "+I[comp, INT, true, null, AS `price` * 2, null]",
                                "+I[order_time, TIMESTAMP(3), true, null, null, `order_time` - INTERVAL '5' SECOND]"));

        // validate WITH options doesn't contains 'schema.'
        String showResult =
                CollectionUtil.iteratorToList(bEnv.executeSql("SHOW CREATE TABLE T").collect())
                        .get(0)
                        .toString();
        assertThat(showResult.contains("schema.")).isFalse();
    }
}
