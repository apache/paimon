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

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.flink.kafka.KafkaLogTestUtils.createTableWithKafkaLog;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.SCAN_LATEST;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildQueryWithTableOptions;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildSimpleQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.createTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.createTemporaryTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertIntoFromTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingReadWithReadFirst;

/** Paimon IT case when the table has computed column and watermark spec. */
public class ComputedColumnAndWatermarkTableITCase extends KafkaTableTestBase {

    @BeforeEach
    public void setUp() {
        init(createAndRegisterTempFile("").toString());
    }

    @Test
    public void testBatchSelectComputedColumn() throws Exception {
        // test 1
        List<Row> initialRecords =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 119L));

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        initialRecords,
                        null,
                        true,
                        "I");

        // write computed column to fieldSpec
        String table =
                createTable(
                        Arrays.asList(
                                "currency STRING",
                                "rate BIGINT",
                                "capital_currency AS UPPER(currency)"),
                        Collections.emptyList(),
                        Collections.emptyList());

        insertIntoFromTable(temporaryTable, table);

        testBatchRead(
                buildQuery(table, "capital_currency", ""),
                initialRecords.stream()
                        .map(
                                row ->
                                        changelogRow(
                                                row.getKind().shortString(),
                                                ((String) row.getField(0)).toUpperCase()))
                        .collect(Collectors.toList()));

        // test 2
        table =
                createTable(
                        Arrays.asList(
                                "currency STRING",
                                "rate BIGINT",
                                "capital_currency AS LOWER(currency)"),
                        Collections.singletonList("currency"),
                        Collections.emptyList());

        insertIntoFromTable(temporaryTable, table);

        testBatchRead(
                buildQuery(table, "capital_currency", ""),
                Arrays.asList(
                        changelogRow("+I", "us dollar"),
                        changelogRow("+I", "yen"),
                        changelogRow("+I", "euro")));

        // test 3
        initialRecords =
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
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "12"));

        temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING", "hh STRING"),
                        Arrays.asList("currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        initialRecords,
                        "dt:2022-01-01,hh:00;dt:2022-01-01,hh:20;dt:2022-01-02,hh:12",
                        true,
                        "I");

        table =
                createTable(
                        Arrays.asList(
                                "currency STRING",
                                "rate BIGINT",
                                "dt STRING",
                                "hh STRING",
                                "dth AS dt || ' ' || hh"),
                        Arrays.asList("currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"));

        insertIntoFromTable(temporaryTable, table);

        testBatchRead(
                buildQuery(table, "dth", "WHERE dth = '2022-01-02 12'"),
                Collections.singletonList(changelogRow("+I", "2022-01-02 12")));

        // test 4 (proctime)
        table =
                createTable(
                        Arrays.asList(
                                "currency STRING",
                                "rate BIGINT",
                                "dt STRING",
                                "hh STRING",
                                "ptime AS PROCTIME()"),
                        Collections.singletonList("currency"),
                        Collections.emptyList());

        insertIntoFromTable(temporaryTable, table);

        testBatchRead(
                buildQuery(
                        table,
                        "CHAR_LENGTH(DATE_FORMAT(ptime, 'yyyy-MM-dd HH:mm'))",
                        "WHERE currency = 'US Dollar'"),
                Collections.singletonList(changelogRow("+I", 16)));
    }

    @Test
    public void testStreamingSelectComputedColumn() throws Exception {
        // test 1
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
                createTableWithKafkaLog(
                        Arrays.asList(
                                "currency STRING",
                                "rate BIGINT",
                                "capital_currency AS UPPER(currency)",
                                "ptime AS PROCTIME()"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        false);

        insertIntoFromTable(temporaryTable, table);

        testStreamingRead(
                        buildQuery(
                                table,
                                "capital_currency, CHAR_LENGTH(DATE_FORMAT(ptime, 'yyyy-MM-dd HH:mm'))",
                                "WHERE currency IS NULL"),
                        Collections.singletonList(changelogRow("+I", null, 16)))
                .close();

        // test 2
        initialRecords =
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

        temporaryTable =
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

        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "corrected_rate_by_to_currency AS COALESCE(rate_by_to_currency, 1)",
                                "ptime AS PROCTIME()"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.singletonList("dt"),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table,
                                "corrected_rate_by_to_currency, CHAR_LENGTH(DATE_FORMAT(ptime, 'yyyy-MM-dd HH:mm'))",
                                "WHERE rate_by_to_currency IS NULL",
                                SCAN_LATEST),
                        Collections.singletonList(changelogRow("+I", 1d, 16)))
                .close();
    }

    @Test
    public void testBatchSelectWithWatermark() throws Exception {
        List<Row> initialRecords =
                Arrays.asList(
                        changelogRow(
                                "+I",
                                "US Dollar",
                                102L,
                                LocalDateTime.parse("1990-04-07T10:00:11.120")),
                        changelogRow(
                                "+I", "Euro", 119L, LocalDateTime.parse("2020-04-07T10:10:11.120")),
                        changelogRow(
                                "+I", "Yen", 1L, LocalDateTime.parse("2022-04-07T09:54:11.120")));

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "ts TIMESTAMP(3)"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        initialRecords,
                        null,
                        true,
                        "I");

        String table =
                createTable(
                        Arrays.asList(
                                "currency STRING",
                                "rate BIGINT",
                                "ts TIMESTAMP(3)",
                                "WATERMARK FOR ts AS ts - INTERVAL '3' YEAR"),
                        Collections.emptyList(),
                        Collections.emptyList());

        insertIntoFromTable(temporaryTable, table);

        testBatchRead(buildSimpleQuery(table), initialRecords);
    }

    @Test
    public void testStreamingSelectWithWatermark() throws Exception {
        // physical column as watermark
        List<Row> initialRecords =
                Arrays.asList(
                        changelogRow(
                                "+I",
                                "US Dollar",
                                102L,
                                LocalDateTime.parse("1990-04-07T10:00:11.120")),
                        changelogRow(
                                "+I", "Euro", 119L, LocalDateTime.parse("2020-04-07T10:10:11.120")),
                        changelogRow(
                                "+I", "Yen", 1L, LocalDateTime.parse("2022-04-07T09:54:11.120")));

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "ts TIMESTAMP(3)"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        initialRecords,
                        null,
                        true,
                        "I");

        String table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "currency STRING",
                                "rate BIGINT",
                                "ts TIMESTAMP(3)",
                                "WATERMARK FOR ts AS ts - INTERVAL '3' YEAR"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table,
                                "*",
                                "WHERE CURRENT_WATERMARK(ts) IS NULL OR ts > CURRENT_WATERMARK(ts)",
                                SCAN_LATEST),
                        Collections.singletonList(
                                changelogRow(
                                        "+I",
                                        "US Dollar",
                                        102L,
                                        LocalDateTime.parse("1990-04-07T10:00:11.120"))))
                .close();

        // computed column as watermark
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "currency STRING",
                                "rate BIGINT",
                                "ts TIMESTAMP(3)",
                                "ts1 AS ts",
                                "WATERMARK FOR ts1 AS ts1 - INTERVAL '3' YEAR"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table,
                                "currency, rate, ts1",
                                "WHERE CURRENT_WATERMARK(ts1) IS NULL OR ts1 > CURRENT_WATERMARK(ts1)",
                                SCAN_LATEST),
                        Collections.singletonList(
                                changelogRow(
                                        "+I",
                                        "US Dollar",
                                        102L,
                                        LocalDateTime.parse("1990-04-07T10:00:11.120"))))
                .close();

        // query both event time and processing time
        table =
                createTableWithKafkaLog(
                        Arrays.asList(
                                "currency STRING",
                                "rate BIGINT",
                                "ts TIMESTAMP(3)",
                                "ptime AS PROCTIME()",
                                "WATERMARK FOR ts AS ts - INTERVAL '3' YEAR"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        true);

        testStreamingReadWithReadFirst(
                        temporaryTable,
                        table,
                        buildQueryWithTableOptions(
                                table,
                                "currency, rate, ts, CHAR_LENGTH(DATE_FORMAT(ptime, 'yyyy-MM-dd HH:mm'))",
                                "WHERE CURRENT_WATERMARK(ts) IS NULL OR ts > CURRENT_WATERMARK(ts)",
                                SCAN_LATEST),
                        Collections.singletonList(
                                changelogRow(
                                        "+I",
                                        "US Dollar",
                                        102L,
                                        LocalDateTime.parse("1990-04-07T10:00:11.120"),
                                        16)))
                .close();
    }
}
