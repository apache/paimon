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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.util.AbstractTestBase;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildSimpleQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.checkFileStorePath;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.createTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.createTemporaryTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertIntoFromTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertOverwrite;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertOverwritePartition;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;

/** Paimon IT case when the table has composite primary keys and multiple partition fields. */
public class CompositePkAndMultiPartitionedTableITCase extends AbstractTestBase {

    private final Map<String, String> staticPartitionOverwrite =
            Collections.singletonMap(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false");

    @BeforeEach
    public void setUp() {
        init(getTempDirPath());
    }

    @Test
    public void testBatchWriteWithMultiPartitionedRecordsWithMultiPk() throws Exception {
        List<Row> initialRecords =
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
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-02", "23"));

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
                        "dt:2022-01-01,hh:11;dt:2022-01-01,hh:12;dt:2022-01-02,hh:23",
                        true,
                        "I");

        String table =
                createTable(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING",
                                "hh STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt", "hh"),
                        Collections.emptyList(),
                        Arrays.asList("dt", "hh"));

        insertIntoFromTable(temporaryTable, table);

        checkFileStorePath(
                table,
                Arrays.asList("dt=2022-01-01,hh=11", "dt=2022-01-01,hh=12", "dt=2022-01-02,hh=23"));

        // test batch read
        testBatchRead(
                buildSimpleQuery(table),
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
                        changelogRow("+I", "Singapore Dollar", "Euro", 0.67d, "2022-01-02", "23")));

        // overwrite static partition dt = 2022-01-02 and hh = 23
        insertOverwritePartition(
                table,
                "PARTITION (dt = '2022-01-02', hh = '23')",
                "('US Dollar', 'Thai Baht', 33.51)");

        // batch read to check partition refresh
        testBatchRead(
                buildQuery(table, "*", "WHERE dt = '2022-01-02' AND hh = '23'"),
                Collections.singletonList(
                        changelogRow("+I", "US Dollar", "Thai Baht", 33.51d, "2022-01-02", "23")));

        // test partition filter
        testBatchRead(
                buildQuery(table, "*", "WHERE dt = '2022-01-01'"),
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

        testBatchRead(
                buildQuery(table, "*", "WHERE dt = '2022-01-01' AND hh = '12'"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "12"),
                        changelogRow("+I", "Euro", "US Dollar", 1.12d, "2022-01-01", "12"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.129d, "2022-01-01", "12"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.741d, "2022-01-01", "12"),
                        changelogRow("+I", "Yen", "US Dollar", 0.00812d, "2022-01-01", "12")));

        // test field filter
        testBatchRead(
                buildQuery(table, "*", "WHERE to_currency = 'Euro'"), Collections.emptyList());

        testBatchRead(
                buildQuery(table, "*", "WHERE from_currency = 'HK Dollar'"),
                Arrays.asList(
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.13d, "2022-01-01", "11"),
                        changelogRow("+I", "HK Dollar", "US Dollar", 0.129d, "2022-01-01", "12")));

        testBatchRead(
                buildQuery(table, "*", "WHERE rate_by_to_currency > 0.5"),
                Arrays.asList(
                        changelogRow("+I", "Euro", "US Dollar", 1.11d, "2022-01-01", "11"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.74d, "2022-01-01", "11"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "11"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01", "12"),
                        changelogRow("+I", "Euro", "US Dollar", 1.12d, "2022-01-01", "12"),
                        changelogRow(
                                "+I", "Singapore Dollar", "US Dollar", 0.741d, "2022-01-01", "12"),
                        changelogRow("+I", "US Dollar", "Thai Baht", 33.51d, "2022-01-02", "23")));

        // test partition and field filter
        testBatchRead(
                buildQuery(table, "*", "WHERE rate_by_to_currency > 1 AND hh = '12'"),
                Collections.singletonList(
                        changelogRow("+I", "Euro", "US Dollar", 1.12, "2022-01-01", "12")));

        // test projection
        testBatchRead(
                buildQuery(table, "from_currency, dt, hh", ""),
                Arrays.asList(
                        // dt = 2022-01-01, hh = 11
                        changelogRow("+I", "Euro", "2022-01-01", "11"),
                        changelogRow("+I", "HK Dollar", "2022-01-01", "11"),
                        changelogRow("+I", "Singapore Dollar", "2022-01-01", "11"),
                        changelogRow("+I", "Yen", "2022-01-01", "11"),
                        changelogRow("+I", "US Dollar", "2022-01-01", "11"),
                        // dt = 2022-01-01, hh = 12
                        changelogRow("+I", "US Dollar", "2022-01-01", "12"),
                        changelogRow("+I", "Euro", "2022-01-01", "12"),
                        changelogRow("+I", "HK Dollar", "2022-01-01", "12"),
                        changelogRow("+I", "Singapore Dollar", "2022-01-01", "12"),
                        changelogRow("+I", "Yen", "2022-01-01", "12"),
                        // dt = 2022-01-02, hh = 23
                        changelogRow("+I", "US Dollar", "2022-01-02", "23")));

        // test projection and filter
        testBatchRead(
                buildQuery(
                        table,
                        "from_currency, to_currency",
                        "WHERE dt = '2022-01-01' AND hh >= '12' OR rate_by_to_currency > 2"),
                Arrays.asList(
                        // dt = 2022-01-01, hh = 12
                        changelogRow("+I", "US Dollar", "US Dollar"),
                        changelogRow("+I", "Euro", "US Dollar"),
                        changelogRow("+I", "HK Dollar", "US Dollar"),
                        changelogRow("+I", "Singapore Dollar", "US Dollar"),
                        changelogRow("+I", "Yen", "US Dollar"),
                        // dt = 2022-01-02, hh = 23
                        changelogRow("+I", "US Dollar", "Thai Baht")));
    }

    @Test
    public void testBatchWriteWithSinglePartitionedRecordsWithMultiPk() throws Exception {
        List<Row> initialRecords =
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
                        changelogRow("+I", "Singapore Dollar", "Yen", 122.46d, "2022-01-01"));

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
                        true,
                        "I");

        String table =
                createTable(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        staticPartitionOverwrite);

        insertIntoFromTable(temporaryTable, table);

        checkFileStorePath(table, Arrays.asList("dt=2022-01-01", "dt=2022-01-02"));

        // test batch read
        testBatchRead(
                buildSimpleQuery(table),
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
                        changelogRow("+I", "Singapore Dollar", "Yen", 122.46d, "2022-01-01")));

        // overwrite dynamic partition
        insertOverwrite(table, "('US Dollar', 'Thai Baht', 33.51, '2022-01-01')");

        // batch read to check partition refresh
        testBatchRead(
                buildSimpleQuery(table),
                Collections.singletonList(
                        changelogRow("+I", "US Dollar", "Thai Baht", 33.51d, "2022-01-01")));

        // reset table
        table =
                createTable(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE",
                                "dt STRING"),
                        Arrays.asList("from_currency", "to_currency", "dt"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"));

        insertIntoFromTable(temporaryTable, table);

        // test partition filter
        testBatchRead(
                buildQuery(table, "*", "WHERE dt = '2022-01-02'"),
                Arrays.asList(
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-02"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-02")));

        // test field filter
        testBatchRead(
                buildQuery(table, "*", "WHERE rate_by_to_currency < 0.1"),
                Arrays.asList(
                        changelogRow("+I", "Yen", "US Dollar", 0.0082d, "2022-01-01"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-02")));

        // test partition and field filter
        testBatchRead(
                buildQuery(table, "*", "WHERE rate_by_to_currency > 0.9 OR dt = '2022-01-02'"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-01"),
                        changelogRow("+I", "Euro", "US Dollar", 1.11d, "2022-01-01"),
                        changelogRow("+I", "Yen", "Yen", 1.0d, "2022-01-01"),
                        changelogRow("+I", "Chinese Yuan", "Yen", 19.25d, "2022-01-01"),
                        changelogRow("+I", "Singapore Dollar", "Yen", 122.46d, "2022-01-01"),
                        changelogRow("+I", "Yen", "US Dollar", 0.0081d, "2022-01-02"),
                        changelogRow("+I", "US Dollar", "US Dollar", 1.0d, "2022-01-02")));

        // test projection
        testBatchRead(
                buildQuery(table, "rate_by_to_currency, dt", ""),
                Arrays.asList(
                        changelogRow("+I", 1.0d, "2022-01-01"), // US Dollar，US Dollar
                        changelogRow("+I", 1.11d, "2022-01-01"), // Euro，US Dollar
                        changelogRow("+I", 0.13d, "2022-01-01"), // HK Dollar，US Dollar
                        changelogRow("+I", 0.0082d, "2022-01-01"), // Yen，US Dollar
                        changelogRow("+I", 0.74d, "2022-01-01"), // Singapore Dollar，USDollar
                        changelogRow("+I", 0.9d, "2022-01-01"), // US Dollar，Euro
                        changelogRow("+I", 0.67d, "2022-01-01"), // Singapore Dollar，Euro
                        changelogRow("+I", 1.0d, "2022-01-01"), // Yen，Yen
                        changelogRow("+I", 19.25d, "2022-01-01"), // Chinese Yuan，Yen
                        changelogRow("+I", 122.46d, "2022-01-01"), // Singapore Dollar，Yen
                        changelogRow("+I", 0.0081d, "2022-01-02"), // Yen，US Dollar
                        changelogRow("+I", 1.0d, "2022-01-02") // US Dollar，US Dollar
                        ));

        // test projection and filter
        testBatchRead(
                buildQuery(
                        table,
                        "from_currency, to_currency",
                        "WHERE dt = '2022-01-02' OR rate_by_to_currency > 100"),
                Arrays.asList(
                        changelogRow("+I", "Yen", "US Dollar"),
                        changelogRow("+I", "US Dollar", "US Dollar"),
                        changelogRow("+I", "Singapore Dollar", "Yen")));
    }

    @Test
    public void testBatchWriteWithNonPartitionedRecordsWithMultiPk() throws Exception {
        List<Row> initialRecords =
                Arrays.asList(
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
                        true,
                        "I");

        String table =
                createTable(
                        Arrays.asList(
                                "from_currency STRING",
                                "to_currency STRING",
                                "rate_by_to_currency DOUBLE"),
                        Arrays.asList("from_currency", "to_currency"),
                        Collections.emptyList(),
                        Collections.emptyList());

        insertIntoFromTable(temporaryTable, table);

        checkFileStorePath(table, Collections.emptyList());

        // test batch read
        testBatchRead(
                buildSimpleQuery(table),
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
                        changelogRow("+I", "Singapore Dollar", "Yen", 122.46d)));

        // test field filter
        testBatchRead(
                buildQuery(table, "*", "WHERE rate_by_to_currency < 0.1"),
                Collections.singletonList(changelogRow("+I", "Yen", "US Dollar", 0.0081d)));

        // test projection
        testBatchRead(
                buildQuery(table, "rate_by_to_currency", ""),
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
        testBatchRead(
                buildQuery(table, "from_currency, to_currency", "WHERE rate_by_to_currency > 100"),
                Collections.singletonList(changelogRow("+I", "Singapore Dollar", "Yen")));
    }

    @Test
    public void testBatchWriteMultiPartitionedRecordsWithOnePk() throws Exception {
        List<Row> initialRecords =
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

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING", "hh STRING"),
                        Arrays.asList("currency", "dt", "hh"),
                        Arrays.asList("dt", "hh"),
                        initialRecords,
                        "dt:2022-01-01,hh:00;dt:2022-01-01,hh:20;dt:2022-01-02,hh:12",
                        true,
                        "I");

        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING", "hh STRING"),
                        Arrays.asList("currency", "dt", "hh"),
                        Collections.emptyList(),
                        Arrays.asList("dt", "hh"));

        insertIntoFromTable(temporaryTable, table);

        checkFileStorePath(
                table,
                Arrays.asList("dt=2022-01-01,hh=00", "dt=2022-01-01,hh=20", "dt=2022-01-02,hh=12"));

        // test batch read
        testBatchRead(
                buildSimpleQuery(table),
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
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "12")));

        // test partition filter
        testBatchRead(
                buildQuery(table, "*", "WHERE hh >= '10'"),
                Arrays.asList(
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "20"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "20"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01", "20"),
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "12")));

        // test field filter
        testBatchRead(
                buildQuery(table, "*", "WHERE rate >= 119"),
                Collections.singletonList(changelogRow("+I", "Euro", 119L, "2022-01-02", "12")));

        // test projection
        testBatchRead(
                buildQuery(table, "hh", ""),
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
        testBatchRead(
                buildQuery(table, "rate", "WHERE rate > 100 AND hh >= '20'"),
                Collections.nCopies(2, changelogRow("+I", 114L)));
    }

    @Test
    public void testBatchWriteMultiPartitionedRecordsWithoutPk() throws Exception {
        List<Row> initialRecords =
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

        String temporaryTable =
                createTemporaryTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING", "hh STRING"),
                        Collections.emptyList(),
                        Arrays.asList("dt", "hh"),
                        initialRecords,
                        "dt:2022-01-01,hh:00;dt:2022-01-01,hh:20;dt:2022-01-02,hh:12",
                        true,
                        "I");

        String table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING", "hh STRING"),
                        Collections.emptyList(),
                        Collections.singletonList("currency"),
                        Arrays.asList("dt", "hh"),
                        staticPartitionOverwrite);

        insertIntoFromTable(temporaryTable, table);

        checkFileStorePath(
                table,
                Arrays.asList("dt=2022-01-01,hh=00", "dt=2022-01-01,hh=20", "dt=2022-01-02,hh=12"));

        // test batch read
        testBatchRead(buildSimpleQuery(table), initialRecords);

        // dynamic overwrite the whole table with null partitions
        bEnv.executeSql(
                        String.format(
                                "INSERT OVERWRITE `%s` SELECT 'Yen', 1, CAST(NULL AS STRING), CAST(NULL AS STRING) FROM `%s`",
                                table, table))
                .await();

        // check new partition path
        checkFileStorePath(table, Collections.singletonList("dt=null,hh=null"));

        // batch read to check data refresh
        testBatchRead(
                buildSimpleQuery(table),
                Collections.nCopies(11, changelogRow("+I", "Yen", 1L, null, null)));

        // reset table
        table =
                createTable(
                        Arrays.asList("currency STRING", "rate BIGINT", "dt STRING", "hh STRING"),
                        Collections.emptyList(),
                        Collections.singletonList("currency"),
                        Arrays.asList("dt", "hh"));

        insertIntoFromTable(temporaryTable, table);

        // test partition filter
        testBatchRead(
                buildQuery(table, "*", "WHERE hh >= '10'"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01", "20"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "20"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01", "20"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01", "20"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01", "20"),
                        changelogRow("+I", "Euro", 119L, "2022-01-02", "12")));

        // test field filter
        testBatchRead(
                buildQuery(table, "*", "WHERE rate >= 119"),
                Collections.singletonList(changelogRow("+I", "Euro", 119L, "2022-01-02", "12")));

        // test projection
        testBatchRead(
                buildQuery(table, "currency", ""),
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
        testBatchRead(
                buildQuery(table, "rate", "WHERE rate > 110 AND hh >= '20'"),
                Collections.nCopies(3, changelogRow("+I", 114L)));
    }
}
