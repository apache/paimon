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

import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.flink.kafka.KafkaLogTestUtils.createTableWithKafkaLog;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.SCAN_LATEST;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.assertNoMoreRecords;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildQueryWithTableOptions;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildSimpleQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.checkFileStorePath;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.createTemporaryTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertIntoFromTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertIntoPartition;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingReadWithReadFirst;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.validateStreamingReadResult;

/**
 * IT cases of streaming reading and writing tables which have composite primary keys and multiple
 * partition fields with Kafka log.
 */
public class CompositePkAndMultiPartitionedTableWithKafkaLogITCase extends KafkaTableTestBase {

    @BeforeEach
    public void setUp() {
        init(getTempDirPath());
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Non latest
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testStreamingReadWriteMultiPartitionedRecordsWithMultiPk() throws Exception {
        List<Row> initialRecords =
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
                        changelogRow("+U", "Singapore Dollar", "Yen", 90.1d, "2022-01-02", "20"));

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "hh STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        initialRecords,
                        "dt:2022-01-02,hh:20;dt:2022-01-02,hh:21",
                        false,
                        "I,UA,D");

        String table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "hh STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        false);

        insertIntoFromTable(temporaryTable, table);

        checkFileStorePath(table, Arrays.asList("dt=2022-01-02,hh=20", "dt=2022-01-02,hh=21"));

        BlockingIterator<Row, Row> streamingItr =
                testStreamingRead(
                        buildSimpleQuery(table),
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

        // test streaming consume changelog
        insertInto(table, "('Chinese Yuan', 'HK Dollar', 1.231, '2022-01-03', '15')");

        validateStreamingReadResult(
                streamingItr,
                Collections.singletonList(
                        changelogRow(
                                "+I", "Chinese Yuan", "HK Dollar", 1.231d, "2022-01-03", "15")));

        // dynamic overwrite the whole table
        bEnv.executeSql(
                        String.format(
                                "INSERT OVERWRITE `%s` SELECT 'US Dollar', 'US Dollar', 1, '2022-04-02', '10' FROM `%s`",
                                table, table))
                .await();

        checkFileStorePath(table, Collections.singletonList("dt=2022-04-02,hh=10"));

        // batch read to check data refresh
        testBatchRead(
                buildSimpleQuery(table),
                Collections.singletonList(
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-04-02", "10")));

        // check no changelog generated for streaming read
        assertNoMoreRecords(streamingItr);
        streamingItr.close();

        // reset table
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "hh STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        false);

        insertIntoFromTable(temporaryTable, table);

        // filter on partition and field filter
        testStreamingRead(
                        buildQuery(
                                table,
                                "*",
                                "WHERE dt = '2022-01-02' AND from_currency = 'US Dollar'"),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02", "20"),
                                changelogRow(
                                        "+I", "US Dollar", "Yen", 122.46d, "2022-01-02", "21")))
                .close();

        // test projection and filter
        testStreamingRead(
                        buildQuery(
                                table,
                                "from_currency, to_currency",
                                "WHERE dt = '2022-01-02' AND from_currency = 'US Dollar'"),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", "Euro"),
                                changelogRow("+I", "US Dollar", "Yen")))
                .close();
    }

    @Test
    public void testStreamingReadWriteSinglePartitionedRecordsWithMultiPk() throws Exception {
        List<Row> initialRecords =
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
                        changelogRow("+U", "Singapore Dollar", "Yen", 122.46d, "2022-01-02"));

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.singletonList("dt"),
                        initialRecords,
                        "dt:2022-01-01;dt:2022-01-02",
                        false,
                        "I,UA,D");

        String table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.singletonList("dt"),
                        false);

        insertIntoFromTable(temporaryTable, table);

        checkFileStorePath(table, Arrays.asList("dt=2022-01-01", "dt=2022-01-02"));

        BlockingIterator<Row, Row> streamingItr =
                testStreamingRead(
                        buildSimpleQuery(table),
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

        // test streaming consume changelog
        insertIntoPartition(
                table, "PARTITION (dt = '2022-01-03')", "('Chinese Yuan', 'HK Dollar', 1.231)");

        validateStreamingReadResult(
                streamingItr,
                Collections.singletonList(
                        changelogRow("+I", "Chinese Yuan", "HK Dollar", 1.231d, "2022-01-03")));
        streamingItr.close();

        // filter on partition and field filter
        testStreamingRead(
                        buildQuery(
                                table,
                                "*",
                                "WHERE dt = '2022-01-02' AND from_currency = 'US Dollar'"),
                        Collections.singletonList(
                                changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02")))
                .close();

        // test projection and filter
        testStreamingRead(
                        buildQuery(
                                table,
                                "from_currency, to_currency",
                                "WHERE dt = '2022-01-01' AND rate_by_to_currency IS NULL"),
                        Collections.emptyList())
                .close();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Latest
    // ----------------------------------------------------------------------------------------------------------------
    @Test
    public void testReadLatestChangelogOfMultiPartitionedRecordsWithMultiPk() throws Exception {
        List<Row> initialRecords =
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
                        changelogRow("+U", "Singapore Dollar", "Yen", 90.1d, "2022-01-02", "20"));

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "hh STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        initialRecords,
                        "dt:2022-01-02,hh:20;dt:2022-01-02,hh:21",
                        false,
                        "I,UA,D");

        String table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "hh STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        true);

        List<Row> expectedRecords = new ArrayList<>(initialRecords);
        expectedRecords.add(changelogRow("-U", "Yen", "US Dollar", 0.0082d, "2022-01-02", "20"));
        expectedRecords.add(
                changelogRow("-U", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-02", "20"));
        expectedRecords.add(
                changelogRow("-U", "Singapore Dollar", "Euro", 0.67d, "2022-01-02", "20"));
        expectedRecords.add(changelogRow("-U", "Chinese Yuan", "Yen", 19.25d, "2022-01-02", "20"));
        expectedRecords.add(
                changelogRow("-U", "Singapore Dollar", "Yen", 90.32d, "2022-01-02", "20"));

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "*", "", SCAN_LATEST),
                        expectedRecords)
                .close();

        // test partition filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "hh STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "*", "WHERE dt = '2022-01-02' AND hh = '21'", SCAN_LATEST),
                        Collections.singletonList(
                                changelogRow(
                                        "+I", "US Dollar", "Yen", 122.46d, "2022-01-02", "21")))
                .close();

        // test field filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "hh STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table,
                                "*",
                                "WHERE rate_by_to_currency IS NOT NULL AND from_currency = 'US Dollar'",
                                SCAN_LATEST),
                        Arrays.asList(
                                // to_currency is USD
                                changelogRow(
                                        "+I", "US Dollar", "US Dollar", 1.0d, "2022-01-02", "20"),
                                changelogRow(
                                        "-D", "US Dollar", "US Dollar", 1.0d, "2022-01-02", "20"),
                                // to_currency is Euro
                                changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02", "20"),
                                // to_currency is Yen
                                changelogRow(
                                        "+I", "US Dollar", "Yen", 122.46d, "2022-01-02", "21")))
                .close();

        // test partition and field filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "hh STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table,
                                "*",
                                "WHERE hh = '21' AND from_currency = 'US Dollar'",
                                SCAN_LATEST),
                        Collections.singletonList(
                                changelogRow(
                                        "+I", "US Dollar", "Yen", 122.46d, "2022-01-02", "21")))
                .close();

        // test projection
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "hh STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "from_currency, to_currency", "", SCAN_LATEST),
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
                                changelogRow("+I", "US Dollar", "Yen")))
                .close();

        // test projection and filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "hh STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table,
                                "from_currency, to_currency",
                                "WHERE rate_by_to_currency > 100",
                                SCAN_LATEST),
                        Collections.singletonList(changelogRow("+I", "US Dollar", "Yen")))
                .close();
    }

    @Test
    public void testReadLatestChangelogOfSinglePartitionedRecordsWithMultiPk() throws Exception {
        List<Row> initialRecords =
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
                        changelogRow("+U", "Singapore Dollar", "Yen", 122.46d, "2022-01-02"));

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.singletonList("dt"),
                        initialRecords,
                        "dt:2022-01-01;dt:2022-01-02",
                        false,
                        "I,UA,D");

        String table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        List<Row> expectedRecords = new ArrayList<>(initialRecords);
        expectedRecords.add(changelogRow("-U", "Euro", "US Dollar", null, "2022-01-01"));
        expectedRecords.add(changelogRow("-U", "US Dollar", "US Dollar", null, "2022-01-01"));
        expectedRecords.add(changelogRow("-U", "Singapore Dollar", "Yen", 90.32d, "2022-01-02"));

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "*", "", SCAN_LATEST),
                        expectedRecords)
                .close();

        // test partition filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "*", "WHERE dt = '2022-01-02'", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", "Euro", 0.9d, "2022-01-02"),
                                changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-02"),
                                changelogRow("-D", "Singapore Dollar", "Euro", 0.67d, "2022-01-02"),
                                changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-02"),
                                changelogRow("+I", "Chinese Yuan", "Yen", 19.25d, "2022-01-02"),
                                changelogRow("+I", "Singapore Dollar", "Yen", 90.32d, "2022-01-02"),
                                changelogRow(
                                        "+U", "Singapore Dollar", "Yen", 122.46d, "2022-01-02"),
                                changelogRow(
                                        "-U", "Singapore Dollar", "Yen", 90.32d, "2022-01-02")))
                .close();

        // test field filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "*", "WHERE rate_by_to_currency IS NULL", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", "US Dollar", null, "2022-01-01"),
                                changelogRow("+I", "Euro", "US Dollar", null, "2022-01-01"),
                                changelogRow("-U", "Euro", "US Dollar", null, "2022-01-01"),
                                changelogRow("-U", "US Dollar", "US Dollar", null, "2022-01-01")))
                .close();

        // test partition and field filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table,
                                "*",
                                "WHERE dt = '2022-01-02' AND from_currency = 'Yen'",
                                SCAN_LATEST),
                        Collections.singletonList(
                                changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-02")))
                .close();

        // test projection and filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table,
                                "from_currency, to_currency",
                                "WHERE rate_by_to_currency > 100",
                                SCAN_LATEST),
                        Collections.singletonList(changelogRow("+U", "Singapore Dollar", "Yen")))
                .close();
    }

    @Test
    public void testReadLatestChangelogOfNonPartitionedRecordsWithMultiPk() throws Exception {
        List<Row> initialRecords =
                Arrays.asList(
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

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE"),
                        Arrays.asList("from_currency", "to_currency"),
                        Collections.emptyList(),
                        initialRecords,
                        null,
                        false,
                        "I,UA,D");

        String table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE"),
                        Arrays.asList("from_currency", "to_currency"),
                        Collections.emptyList(),
                        true);

        List<Row> expectedRecords = new ArrayList<>(initialRecords);
        expectedRecords.add(changelogRow("-U", "Yen", "US Dollar", 0.0082d));
        expectedRecords.add(changelogRow("-U", "Euro", "US Dollar", 1.11d));
        expectedRecords.add(changelogRow("-U", "Singapore Dollar", "Euro", 0.67d));
        expectedRecords.add(changelogRow("-U", "Singapore Dollar", "Yen", 90.32d));
        expectedRecords.add(changelogRow("-U", "Singapore Dollar", "Yen", 122.46d));

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "*", "", SCAN_LATEST),
                        expectedRecords)
                .close();

        // test field filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE"),
                        Arrays.asList("from_currency", "to_currency"),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table,
                                "*",
                                "WHERE rate_by_to_currency < 1 OR rate_by_to_currency > 100",
                                SCAN_LATEST),
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
                                changelogRow("+U", "Singapore Dollar", "Yen", 122d)))
                .close();

        // test projection and filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE"),
                        Arrays.asList("from_currency", "to_currency"),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table,
                                "from_currency, to_currency",
                                "WHERE rate_by_to_currency < 1 OR rate_by_to_currency > 100",
                                SCAN_LATEST),
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
                                changelogRow("+U", "Singapore Dollar", "Yen")))
                .close();
    }

    @Test
    public void testReadLatestChangelogOfMultiPartitionedRecordsWithOnePk() throws Exception {
        List<Row> initialRecords =
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
                        changelogRow("+U", "Euro", 119L, "2022-01-02", "23"));

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING", "hh STRING"),
                        Arrays.asList("currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        initialRecords,
                        "dt:2022-01-01,hh:15;dt:2022-01-02,hh:23",
                        false,
                        "I,UA,D");

        String table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING", "hh STRING"),
                        Arrays.asList("currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        true);

        List<Row> expectedRecords = new ArrayList<>(initialRecords);
        expectedRecords.remove(changelogRow("+U", "Euro", 119L, "2022-01-02", "23"));
        expectedRecords.add(changelogRow("-U", "Euro", 114L, "2022-01-01", "15"));

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "*", "", SCAN_LATEST),
                        expectedRecords)
                .close();

        // test partition filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING", "hh STRING"),
                        Arrays.asList("currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "*", "WHERE dt >= '2022-01-02'", SCAN_LATEST),
                        Collections.singletonList(
                                changelogRow("+I", "Euro", 119L, "2022-01-02", "23")))
                .close();

        // test field filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING", "hh STRING"),
                        Arrays.asList("currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(table, "*", "WHERE rate = 1", SCAN_LATEST),
                        Arrays.asList(
                                changelogRow("+I", "Yen", 1L, "2022-01-01", "15"),
                                changelogRow("-D", "Yen", 1L, "2022-01-01", "15")))
                .close();

        // test projection and filter
        table =
                createTableWithKafkaLog(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING", "hh STRING"),
                        Arrays.asList("currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table, "currency", "WHERE rate = 1", SCAN_LATEST),
                        Arrays.asList(changelogRow("+I", "Yen"), changelogRow("-D", "Yen")))
                .close();
    }
}
