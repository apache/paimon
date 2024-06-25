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
                        Collections.singletonList("currency"),
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
                        Collections.emptyList(),
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
                        Collections.emptyList(),
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
                        Collections.emptyList(),
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
                        Collections.singletonList("currency"),
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
