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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.store.kafka.KafkaTableTestBase;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.planner.factories.TestValuesTableFactory.registerData;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.LOG_SYSTEM;
import static org.apache.flink.table.store.kafka.KafkaLogOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for managed table dml. */
public class ReadWriteTableITCase extends KafkaTableTestBase {

    private String rootPath;

    @Test
    public void testBatchWriteWithPartitionedRecordsWithPk() throws Exception {
        // input is dailyRates()
        List<Row> expectedRecords =
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02"));
        // test batch read
        String managedTable = collectAndCheckBatchReadWrite(true, true, expectedRecords);
        checkFileStorePath(tEnv, managedTable);

        // test streaming read
        final StreamTableEnvironment streamTableEnv =
                StreamTableEnvironment.create(buildStreamEnv());
        registerTable(streamTableEnv, managedTable);
        BlockingIterator<Row, Row> streamIter =
                collectAndCheck(
                        streamTableEnv, managedTable, Collections.emptyMap(), expectedRecords);

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
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Euro", 100L, "2022-01-02"),
                                changelogRow("+I", "Yen", 1L, "2022-01-02")))
                .close();
    }

    @Test
    public void testBatchWriteWithPartitionedRecordsWithoutPk() throws Exception {
        // input is dailyRates()

        // test batch read
        String managedTable = collectAndCheckBatchReadWrite(true, false, dailyRates());
        checkFileStorePath(tEnv, managedTable);

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
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "Euro", 90L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Yen", 2L, "2022-01-02")))
                .close();
    }

    @Test
    public void testBatchWriteWithNonPartitionedRecordsWithPk() throws Exception {
        // input is rates()
        String managedTable =
                collectAndCheckBatchReadWrite(
                        false,
                        true,
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Yen", 1L),
                                changelogRow("+I", "Euro", 119L)));
        checkFileStorePath(tEnv, managedTable);

        // overwrite the whole table
        prepareEnvAndOverwrite(
                managedTable,
                Collections.emptyMap(),
                Collections.singletonList(new String[] {"'Euro'", "100"}));
        collectAndCheck(
                tEnv,
                managedTable,
                Collections.emptyMap(),
                Collections.singletonList(changelogRow("+I", "Euro", 100L)));
    }

    @Test
    public void testBatchWriteNonPartitionedRecordsWithoutPk() throws Exception {
        // input is rates()
        String managedTable = collectAndCheckBatchReadWrite(false, false, rates());
        checkFileStorePath(tEnv, managedTable);
    }

    @Test
    public void testEnableLogAndStreamingReadWritePartitionedRecordsWithPk() throws Exception {
        // input is dailyRatesChangelogWithoutUB()
        // test hybrid read
        Tuple2<String, BlockingIterator<Row, Row>> tuple =
                collectAndCheckStreamingReadWriteWithoutClose(
                        Collections.emptyMap(),
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Euro", 119L, "2022-01-02")));
        String managedTable = tuple.f0;
        checkFileStorePath(tEnv, managedTable);
        BlockingIterator<Row, Row> streamIter = tuple.f1;

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
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 100L, "2022-01-02"),
                        changelogRow("+I", "Yen", 1L, "2022-01-02")));

        // check no changelog generated for streaming read
        assertNoMoreRecords(streamIter);
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
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Euro", 119L, "2022-01-02"))));
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
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Euro", 115L, "2022-01-02"))));
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
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 119L))));
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
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 119L))));
    }

    @Test
    public void testReadLatestChangelogOfPartitionedRecordsWithPk() throws Exception {
        // input is dailyRatesChangelogWithoutUB()
        collectLatestLogAndCheck(
                false,
                true,
                true,
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
    public void testReadLatestChangelogOfPartitionedRecordsWithoutPk() throws Exception {
        // input is dailyRatesChangelogWithUB()
        collectLatestLogAndCheck(
                false,
                true,
                false,
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
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-U", "Euro", 114L),
                        changelogRow("+U", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Yen", 1L)));
    }

    @Test
    public void testReadLatestChangelogOfNonPartitionedRecordsWithoutPk() throws Exception {
        // input is ratesChangelogWithUB()
        collectLatestLogAndCheck(
                false,
                false,
                false,
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
                        changelogRow("-D", "Yen", 1L)));
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
        collectLatestLogAndCheck(true, false, true, expected);

        // without pk
        collectLatestLogAndCheck(true, false, true, expected);
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
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));

        collectChangelogFromTimestampAndCheck(true, true, false, 0, dailyRates());

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

    // ------------------------ Tools ----------------------------------

    private String collectAndCheckBatchReadWrite(
            boolean partitioned, boolean hasPk, List<Row> expected) throws Exception {
        return collectAndCheckUnderSameEnv(
                        false,
                        false,
                        true,
                        partitioned,
                        hasPk,
                        true,
                        Collections.emptyMap(),
                        expected)
                .f0;
    }

    private String collectAndCheckStreamingReadWriteWithClose(
            boolean enableLogStore,
            boolean partitioned,
            boolean hasPk,
            Map<String, String> readHints,
            List<Row> expected)
            throws Exception {
        Tuple2<String, BlockingIterator<Row, Row>> tuple =
                collectAndCheckUnderSameEnv(
                        true, enableLogStore, false, partitioned, hasPk, true, readHints, expected);
        tuple.f1.close();
        return tuple.f0;
    }

    private Tuple2<String, BlockingIterator<Row, Row>>
            collectAndCheckStreamingReadWriteWithoutClose(
                    Map<String, String> readHints, List<Row> expected) throws Exception {
        return collectAndCheckUnderSameEnv(
                true, true, false, true, true, true, readHints, expected);
    }

    private void collectLatestLogAndCheck(
            boolean insertOnly, boolean partitioned, boolean hasPk, List<Row> expected)
            throws Exception {
        Map<String, String> hints = new HashMap<>();
        hints.put(
                LOG_PREFIX + LogOptions.SCAN.key(),
                LogOptions.LogStartupMode.LATEST.name().toLowerCase());
        collectAndCheckUnderSameEnv(
                        true, true, insertOnly, partitioned, hasPk, false, hints, expected)
                .f1
                .close();
    }

    private void collectChangelogFromTimestampAndCheck(
            boolean insertOnly,
            boolean partitioned,
            boolean hasPk,
            long timestamp,
            List<Row> expected)
            throws Exception {
        Map<String, String> hints = new HashMap<>();
        hints.put(LOG_PREFIX + LogOptions.SCAN.key(), "from-timestamp");
        hints.put(LOG_PREFIX + LogOptions.SCAN_TIMESTAMP_MILLS.key(), String.valueOf(timestamp));
        collectAndCheckUnderSameEnv(
                        true, true, insertOnly, partitioned, hasPk, true, hints, expected)
                .f1
                .close();
    }

    private Tuple2<String, BlockingIterator<Row, Row>> collectAndCheckUnderSameEnv(
            boolean streaming,
            boolean enableLogStore,
            boolean insertOnly,
            boolean partitioned,
            boolean hasPk,
            boolean writeFirst,
            Map<String, String> readHints,
            List<Row> expected)
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
                                    sourceTable, partitioned, hasPk)
                            : prepareHelperSourceWithChangelogRecords(
                                    sourceTable, partitioned, hasPk);
            env = buildStreamEnv();
            builder.inStreamingMode();
        } else {
            helperTableDdl =
                    prepareHelperSourceWithInsertOnlyRecords(sourceTable, partitioned, hasPk);
            env = buildBatchEnv();
            builder.inBatchMode();
        }
        String managedTableDdl = prepareManagedTableDdl(sourceTable, managedTable, tableOptions);

        tEnv = StreamTableEnvironment.create(env, builder.build());
        tEnv.executeSql(helperTableDdl);
        tEnv.executeSql(managedTableDdl);

        String insertQuery = prepareInsertIntoQuery(sourceTable, managedTable);
        String selectQuery = prepareSimpleSelectQuery(managedTable, readHints);

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
        final StreamTableEnvironment batchEnv =
                StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        registerTable(batchEnv, managedTable);
        String insertQuery =
                prepareInsertOverwriteQuery(managedTable, staticPartitions, overwriteRecords);
        batchEnv.executeSql(insertQuery).await();
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
            List<Row> expectedRecords)
            throws Exception {
        String selectQuery = prepareSimpleSelectQuery(managedTable, hints);
        List<Row> actual = new ArrayList<>();
        BlockingIterator<Row, Row> iterator =
                collect(tEnv, selectQuery, expectedRecords.size(), actual);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRecords);
        return iterator;
    }

    private void checkFileStorePath(StreamTableEnvironment tEnv, String managedTable) {
        String relativeFilePath =
                FileStoreOptions.relativeTablePath(
                        ObjectIdentifier.of(
                                tEnv.getCurrentCatalog(), tEnv.getCurrentDatabase(), managedTable));
        // check snapshot file path
        assertThat(Paths.get(rootPath, relativeFilePath, "snapshot")).exists();
        // check manifest file path
        assertThat(Paths.get(rootPath, relativeFilePath, "manifest")).exists();
    }

    private static void assertNoMoreRecords(BlockingIterator<Row, Row> iterator) {
        List<Row> expectedRecords = Collections.emptyList();
        try {
            // set expectation size to 1 to let time pass by until timeout
            // just wait 5s to avoid too long time
            expectedRecords = iterator.collect(1, 5L, TimeUnit.SECONDS);
            iterator.close();
        } catch (Exception ignored) {
            // don't throw exception
        }
        assertThat(expectedRecords).isEmpty();
    }

    private static String prepareManagedTableDdl(
            String sourceTableName, String managedTableName, Map<String, String> tableOptions) {
        StringBuilder ddl =
                new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                        .append(String.format("`%s`", managedTableName));
        if (tableOptions.size() > 0) {
            ddl.append(" WITH (\n");
            tableOptions.forEach(
                    (k, v) ->
                            ddl.append("  ")
                                    .append(String.format("'%s'", k))
                                    .append(" = ")
                                    .append(String.format("'%s',\n", v)));
            int len = ddl.length();
            ddl.delete(len - 2, len);
            ddl.append(")");
        }
        ddl.append(String.format(" LIKE `%s` (EXCLUDING OPTIONS)\n", sourceTableName));
        return ddl.toString();
    }

    private static String prepareInsertIntoQuery(String sourceTableName, String managedTableName) {
        return prepareInsertIntoQuery(
                sourceTableName, managedTableName, Collections.emptyMap(), Collections.emptyMap());
    }

    private static String prepareInsertIntoQuery(
            String sourceTableName,
            String managedTableName,
            Map<String, String> partitions,
            Map<String, String> hints) {
        StringBuilder insertDmlBuilder =
                new StringBuilder(String.format("INSERT INTO `%s`", managedTableName));
        if (partitions.size() > 0) {
            insertDmlBuilder.append(" PARTITION (");
            partitions.forEach(
                    (k, v) -> {
                        insertDmlBuilder.append(String.format("'%s'", k));
                        insertDmlBuilder.append(" = ");
                        insertDmlBuilder.append(String.format("'%s', ", v));
                    });
            int len = insertDmlBuilder.length();
            insertDmlBuilder.deleteCharAt(len - 1);
            insertDmlBuilder.append(")");
        }
        insertDmlBuilder.append(String.format("\n SELECT * FROM `%s`", sourceTableName));
        insertDmlBuilder.append(buildHints(hints));

        return insertDmlBuilder.toString();
    }

    private static String prepareInsertOverwriteQuery(
            String managedTableName,
            Map<String, String> staticPartitions,
            List<String[]> overwriteRecords) {
        StringBuilder insertDmlBuilder =
                new StringBuilder(String.format("INSERT OVERWRITE `%s`", managedTableName));
        if (staticPartitions.size() > 0) {
            insertDmlBuilder.append(" PARTITION (");
            staticPartitions.forEach(
                    (k, v) -> {
                        insertDmlBuilder.append(String.format("%s", k));
                        insertDmlBuilder.append(" = ");
                        insertDmlBuilder.append(String.format("%s, ", v));
                    });
            int len = insertDmlBuilder.length();
            insertDmlBuilder.delete(len - 2, len);
            insertDmlBuilder.append(")");
        }
        insertDmlBuilder.append("\n VALUES ");
        overwriteRecords.forEach(
                record -> {
                    int arity = record.length;
                    insertDmlBuilder.append("(");
                    IntStream.range(0, arity)
                            .forEach(i -> insertDmlBuilder.append(record[i]).append(", "));

                    if (arity > 0) {
                        int len = insertDmlBuilder.length();
                        insertDmlBuilder.delete(len - 2, len);
                    }
                    insertDmlBuilder.append("), ");
                });
        int len = insertDmlBuilder.length();
        insertDmlBuilder.delete(len - 2, len);
        return insertDmlBuilder.toString();
    }

    private static String prepareSimpleSelectQuery(String tableName, Map<String, String> hints) {
        return String.format("SELECT * FROM `%s` %s", tableName, buildHints(hints));
    }

    private static String buildHints(Map<String, String> hints) {
        if (hints.size() > 0) {
            StringBuilder hintsBuilder = new StringBuilder("/*+ OPTIONS (");
            hints.forEach(
                    (k, v) -> {
                        hintsBuilder.append(String.format("'%s'", k));
                        hintsBuilder.append(" = ");
                        hintsBuilder.append(String.format("'%s', ", v));
                    });
            int len = hintsBuilder.length();
            hintsBuilder.deleteCharAt(len - 2);
            hintsBuilder.append(") */");
            return hintsBuilder.toString();
        }
        return "";
    }

    private static String prepareHelperSourceWithInsertOnlyRecords(
            String sourceTable, boolean partitioned, boolean hasPk) {
        return prepareHelperSourceRecords(
                RuntimeExecutionMode.BATCH, sourceTable, partitioned, hasPk);
    }

    private static String prepareHelperSourceWithChangelogRecords(
            String sourceTable, boolean partitioned, boolean hasPk) {
        return prepareHelperSourceRecords(
                RuntimeExecutionMode.STREAMING, sourceTable, partitioned, hasPk);
    }

    /**
     * Prepare helper source table ddl according to different input parameter.
     *
     * <pre> E.g. pk with partition
     *   {@code
     *   CREATE TABLE source_table (
     *     currency STRING,
     *     rate BIGINT,
     *     dt STRING) PARTITIONED BY (dt)
     *    WITH (
     *      'connector' = 'values',
     *      'bounded' = executionMode == RuntimeExecutionMode.BATCH,
     *      'partition-list' = '...'
     *     )
     *   }
     * </pre>
     *
     * @param executionMode is used to calculate {@code bounded}
     * @param sourceTable source table name
     * @param partitioned is used to calculate {@code partition-list}
     * @param hasPk
     * @return helper source ddl
     */
    private static String prepareHelperSourceRecords(
            RuntimeExecutionMode executionMode,
            String sourceTable,
            boolean partitioned,
            boolean hasPk) {
        boolean bounded = executionMode == RuntimeExecutionMode.BATCH;
        String changelogMode = bounded ? "I" : hasPk ? "I,UA,D" : "I,UA,UB,D";
        StringBuilder ddlBuilder =
                new StringBuilder(String.format("CREATE TABLE `%s` (\n", sourceTable))
                        .append("  currency STRING,\n")
                        .append("  rate BIGINT");
        if (partitioned) {
            ddlBuilder.append(",\n dt STRING");
        }
        if (hasPk) {
            ddlBuilder.append(", \n PRIMARY KEY (currency");
            if (partitioned) {
                ddlBuilder.append(", dt");
            }
            ddlBuilder.append(") NOT ENFORCED\n");
        } else {
            ddlBuilder.append("\n");
        }
        ddlBuilder.append(")");
        if (partitioned) {
            ddlBuilder.append(" PARTITIONED BY (dt)\n");
        }
        List<Row> input;
        if (bounded) {
            input = partitioned ? dailyRates() : rates();
        } else {
            if (hasPk) {
                input = partitioned ? dailyRatesChangelogWithoutUB() : ratesChangelogWithoutUB();
            } else {
                input = partitioned ? dailyRatesChangelogWithUB() : ratesChangelogWithUB();
            }
        }
        ddlBuilder.append(
                String.format(
                        "  WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = '%s',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'changelog-mode' = '%s',\n"
                                + "  'disable-lookup' = 'true',\n"
                                + "  'partition-list' = '%s'\n"
                                + ")",
                        bounded,
                        registerData(input),
                        changelogMode,
                        partitioned ? "dt:2022-01-01;dt:2022-01-02" : ""));
        return ddlBuilder.toString();
    }

    private static List<Row> rates() {
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Euro", 119L));
    }

    private static List<Row> dailyRates() {
        return Arrays.asList(
                // part = 2022-01-01
                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                // part = 2022-01-02
                changelogRow("+I", "Euro", 119L, "2022-01-02"));
    }

    static List<Row> ratesChangelogWithoutUB() {
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("+U", "Euro", 116L),
                changelogRow("-D", "Euro", 116L),
                changelogRow("+I", "Euro", 119L),
                changelogRow("+U", "Euro", 119L),
                changelogRow("-D", "Yen", 1L));
    }

    static List<Row> dailyRatesChangelogWithoutUB() {
        return Arrays.asList(
                // part = 2022-01-01
                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                changelogRow("+U", "Euro", 116L, "2022-01-01"),
                changelogRow("-D", "Yen", 1L, "2022-01-01"),
                changelogRow("-D", "Euro", 116L, "2022-01-01"),
                // part = 2022-01-02
                changelogRow("+I", "Euro", 119L, "2022-01-02"),
                changelogRow("+U", "Euro", 119L, "2022-01-02"));
    }

    private static List<Row> ratesChangelogWithUB() {
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("-U", "Euro", 114L),
                changelogRow("+U", "Euro", 116L),
                changelogRow("-D", "Euro", 116L),
                changelogRow("+I", "Euro", 119L),
                changelogRow("-U", "Euro", 119L),
                changelogRow("+U", "Euro", 119L),
                changelogRow("-D", "Yen", 1L));
    }

    private static List<Row> dailyRatesChangelogWithUB() {
        return Arrays.asList(
                // part = 2022-01-01
                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                changelogRow("+I", "Euro", 116L, "2022-01-01"),
                changelogRow("-D", "Euro", 116L, "2022-01-01"),
                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                changelogRow("-D", "Yen", 1L, "2022-01-01"),
                // part = 2022-01-02
                changelogRow("+I", "Euro", 114L, "2022-01-02"),
                changelogRow("-U", "Euro", 114L, "2022-01-02"),
                changelogRow("+U", "Euro", 119L, "2022-01-02"),
                changelogRow("-D", "Euro", 119L, "2022-01-02"),
                changelogRow("+I", "Euro", 115L, "2022-01-02"));
    }

    private static StreamExecutionEnvironment buildStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100);
        env.setParallelism(2);
        return env;
    }

    private static StreamExecutionEnvironment buildBatchEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);
        return env;
    }
}
