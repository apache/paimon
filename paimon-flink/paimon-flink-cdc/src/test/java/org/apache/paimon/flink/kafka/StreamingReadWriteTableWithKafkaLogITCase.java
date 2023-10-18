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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.flink.kafka.KafkaLogTestUtils.createTableWithKafkaLog;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.SCAN_LATEST;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.assertNoMoreRecords;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildQueryWithTableOptions;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildSimpleQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.checkFileStorePath;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.createTemporaryTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertIntoFromTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertIntoPartition;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertOverwritePartition;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingReadWithReadFirst;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.validateStreamingReadResult;

/** Streaming reading and writing with Kafka log IT cases. */
public class StreamingReadWriteTableWithKafkaLogITCase extends KafkaTableTestBase {

    @BeforeEach
    public void setUp() {
        init(createAndRegisterTempFile("").toString());
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Write First/Kafka log table auto created/scan.mode = latest-full (default setting)
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testReadWriteWithPartitionedRecordsWithPk() throws Exception {
        // test hybrid read
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
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"),
                        false);

        insertIntoFromTable(temporaryTable, table);

        checkFileStorePath(table, Arrays.asList("dt=2022-01-01", "dt=2022-01-02"));

        BlockingIterator<Row, Row> streamItr =
                testStreamingRead(
                        buildQuery(
                                table,
                                "*",
                                "WHERE dt >= '2022-01-01' AND dt <= '2022-01-03' OR currency = 'HK Dollar'"),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test log store in hybrid mode accepts all filters
        insertIntoPartition(
                table, "PARTITION (dt = '2022-01-03')", "('HK Dollar', 100)", "('Yen', 20)");

        insertIntoPartition(table, "PARTITION (dt = '2022-01-04')", "('Yen', 20)");

        validateStreamingReadResult(
                streamItr,
                Arrays.asList(
                        changelogRow("+I", "HK Dollar", 100L, "2022-01-03"),
                        changelogRow("+I", "Yen", 20L, "2022-01-03")));

        // overwrite partition 2022-01-02
        insertOverwritePartition(
                table, "PARTITION (dt = '2022-01-02')", "('Euro', 100)", "('Yen', 1)");

        // check no changelog generated for streaming read
        assertNoMoreRecords(streamItr);
        streamItr.close();

        // batch read to check data refresh
        testBatchRead(
                buildSimpleQuery(table),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 100L, "2022-01-02"),
                        changelogRow("+I", "Yen", 1L, "2022-01-02"),
                        changelogRow("+I", "HK Dollar", 100L, "2022-01-03"),
                        changelogRow("+I", "Yen", 20L, "2022-01-03"),
                        changelogRow("+I", "Yen", 20L, "2022-01-04")));

        // filter on partition
        testStreamingRead(
                        buildQuery(table, "*", "WHERE dt = '2022-01-01'"),
                        Collections.singletonList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01")))
                .close();

        // test field filter
        testStreamingRead(
                        buildQuery(table, "*", "WHERE currency = 'US Dollar'"),
                        Collections.singletonList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01")))
                .close();

        // test partition and field filter
        testStreamingRead(
                        buildQuery(table, "*", "WHERE dt = '2022-01-01' AND rate = 1"),
                        Collections.emptyList())
                .close();

        // test projection and filter
        testStreamingRead(
                        buildQuery(
                                table,
                                "rate, dt, currency",
                                "WHERE dt = '2022-01-02' AND currency = 'Euro'"),
                        Collections.singletonList(changelogRow("+I", 100L, "2022-01-02", "Euro")))
                .close();
    }

    @Test
    public void testSReadWriteWithNonPartitionedRecordsWithPk() throws Exception {
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
                        "I, UA, D");

        String table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        false);

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
    // Read First/Manually create Kafka log table/scan.mode = latest
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testReadLatestChangelogOfPartitionedRecordsWithPk() throws Exception {
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
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        BlockingIterator<Row, Row> streamItr =
                testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "*", "", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                                changelogRow("-U", "Euro", 114L, "2022-01-01"),
                                changelogRow("+U", "Euro", 116L, "2022-01-01"),
                                changelogRow("-D", "Yen", 1L, "2022-01-01"),
                                changelogRow("-D", "Euro", 116L, "2022-01-01"),
                                changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test only read the latest log
        insertInto(table, "('US Dollar', 104, '2022-01-01')", "('Euro', 100, '2022-01-02')");
        validateStreamingReadResult(
                streamItr,
                Arrays.asList(
                        changelogRow("-U", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+U", "US Dollar", 104L, "2022-01-01"),
                        changelogRow("-U", "Euro", 119L, "2022-01-02"),
                        changelogRow("+U", "Euro", 100L, "2022-01-02")));

        assertNoMoreRecords(streamItr);
        streamItr.close();

        // test partition filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "*", "WHERE dt = '2022-01-01'", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                                changelogRow("-U", "Euro", 114L, "2022-01-01"),
                                changelogRow("+U", "Euro", 116L, "2022-01-01"),
                                changelogRow("-D", "Yen", 1L, "2022-01-01"),
                                changelogRow("-D", "Euro", 116L, "2022-01-01")))
                .close();

        // test field filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "*", "WHERE currency = 'Yen'", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                                changelogRow("-D", "Yen", 1L, "2022-01-01")))
                .close();

        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "*", "WHERE rate = 114", SCAN_LATEST),
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                                changelogRow("-U", "Euro", 114L, "2022-01-01")))
                .close();

        // test partition and field filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "*", "WHERE rate = 114 AND dt = '2022-01-02'", SCAN_LATEST),
                        Collections.emptyList())
                .close();

        // test projection
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "rate", "", SCAN_LATEST),
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
                                ))
                .close();

        // test projection and filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "rate", "WHERE dt = '2022-01-02'", SCAN_LATEST),
                        Collections.singletonList(changelogRow("+I", 119L)) // Euro
                        )
                .close();
    }

    @Test
    public void testReadLatestChangelogOfNonPartitionedRecordsWithPk() throws Exception {
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
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "*", "", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 114L),
                                changelogRow("+I", "Yen", 1L),
                                changelogRow("-U", "Euro", 114L),
                                changelogRow("+U", "Euro", 116L),
                                changelogRow("-D", "Euro", 116L),
                                changelogRow("+I", "Euro", 119L),
                                changelogRow("-D", "Yen", 1L)))
                .close();

        // test field filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "*", "WHERE currency = 'Euro'", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", "Euro", 114L),
                                changelogRow("-U", "Euro", 114L),
                                changelogRow("+U", "Euro", 116L),
                                changelogRow("-D", "Euro", 116L),
                                changelogRow("+I", "Euro", 119L)))
                .close();

        // test projection
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "currency", "", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar"),
                                changelogRow("+I", "Euro"),
                                changelogRow("+I", "Yen"),
                                changelogRow("-D", "Euro"),
                                changelogRow("+I", "Euro"),
                                changelogRow("-D", "Yen")))
                .close();

        // test projection and filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "rate", "WHERE currency = 'Euro'", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", 114L),
                                changelogRow("-U", 114L),
                                changelogRow("+U", 116L),
                                changelogRow("-D", 116L),
                                changelogRow("+I", 119L)))
                .close();
    }

    @Test
    public void testReadLatestChangelogOfInsertOnlyRecords() throws Exception {
        List<Row> initialRecords =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 119L));

        // without pk
        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        initialRecords,
                        null,
                        true,
                        "I");

        String table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "*", "", SCAN_LATEST),
                        initialRecords)
                .close();

        // currency as pk in the next tests
        temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        initialRecords,
                        null,
                        true,
                        "I");

        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "*", "", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 114L),
                                changelogRow("+I", "Yen", 1L),
                                changelogRow("-U", "Euro", 114L),
                                changelogRow("+U", "Euro", 119L)))
                .close();

        // test field filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "*", "WHERE rate = 114", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", "Euro", 114L), changelogRow("-U", "Euro", 114L)))
                .close();

        // test projection
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "rate", "", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", 102L),
                                changelogRow("+I", 114L),
                                changelogRow("+I", 1L),
                                changelogRow("-U", 114L),
                                changelogRow("+U", 119L)))
                .close();

        // test projection and filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "currency", "WHERE rate = 114", SCAN_LATEST),
                        Arrays.asList(changelogRow("+I", "Euro"), changelogRow("-U", "Euro")))
                .close();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Write First/Kafka log table auto created/scan.mode = from-timestamp
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testReadInsertOnlyChangelogFromTimestamp() throws Exception {
        // test records 0
        List<Row> initialRecords0 =
                Arrays.asList(
                        // dt = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                        // dt = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02"));

        // partitioned without pk, scan from timestamp 0
        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        initialRecords0,
                        "dt:2022-01-01;dt:2022-01-02",
                        true,
                        "I");

        String table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        false);

        insertIntoFromTable(temporaryTable, table);

        testStreamingRead(
                        buildQueryWithTableOptions(table, "*", "", scanFromTimeStampMillis(0L)),
                        initialRecords0)
                .close();

        // partitioned with pk, scan from timestamp 0
        temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"),
                        initialRecords0,
                        "dt:2022-01-01;dt:2022-01-02",
                        true,
                        "I");

        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING"),
                        Arrays.asList("currency", "dt"),
                        Collections.singletonList("dt"),
                        false);

        insertIntoFromTable(temporaryTable, table);

        testStreamingRead(
                        buildQueryWithTableOptions(table, "*", "", scanFromTimeStampMillis(0L)),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                                changelogRow("-U", "US Dollar", 102L, "2022-01-01"),
                                changelogRow("+U", "US Dollar", 114L, "2022-01-01"),
                                changelogRow("+I", "Euro", 119L, "2022-01-02")))
                .close();

        // test records 1
        List<Row> initialRecords1 =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 119L));

        // non-partitioned with pk, scan from timestamp 0
        temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        initialRecords1,
                        null,
                        true,
                        "I");

        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.singletonList("currency"),
                        Collections.emptyList(),
                        false);

        insertIntoFromTable(temporaryTable, table);

        testStreamingRead(
                        buildQueryWithTableOptions(table, "*", "", scanFromTimeStampMillis(0L)),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 114L),
                                changelogRow("+I", "Yen", 1L),
                                changelogRow("-U", "Euro", 114L),
                                changelogRow("+U", "Euro", 119L)))
                .close();

        // non-partitioned without pk, scan from timestamp 0
        temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        initialRecords1,
                        null,
                        true,
                        "I");

        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        false);

        insertIntoFromTable(temporaryTable, table);

        testStreamingRead(
                        buildQueryWithTableOptions(table, "*", "", scanFromTimeStampMillis(0L)),
                        initialRecords1)
                .close();
    }

    @Test
    public void testReadInsertOnlyChangelogFromEnormousTimestamp() throws Exception {
        List<Row> initialRecords =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 119L));

        // non-partitioned without pk, scan from timestamp Long.MAX_VALUE - 1
        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        initialRecords,
                        null,
                        true,
                        "I");

        String table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        false);

        insertIntoFromTable(temporaryTable, table);

        testStreamingRead(
                        buildQueryWithTableOptions(
                                table, "*", "", scanFromTimeStampMillis(Long.MAX_VALUE - 1)),
                        Collections.emptyList())
                .close();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Tools
    // ----------------------------------------------------------------------------------------------------------------

    private Map<String, String> scanFromTimeStampMillis(Long timeStampMillis) {
        return new HashMap<String, String>() {
            {
                put(SCAN_MODE.key(), CoreOptions.StartupMode.FROM_TIMESTAMP.toString());
                put(SCAN_TIMESTAMP_MILLIS.key(), String.valueOf(timeStampMillis));
            }
        };
    }
}
