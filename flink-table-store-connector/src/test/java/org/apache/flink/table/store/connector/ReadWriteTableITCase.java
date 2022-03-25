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
        String managedTable = prepareEnvAndWrite(false, false, true, true);

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
        collectAndCheck(tEnv, managedTable, Collections.emptyMap(), expectedRecords).close();
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
        expectedRecords = new ArrayList<>(expectedRecords);
        expectedRecords.remove(3);
        expectedRecords.add(changelogRow("+I", "Euro", 100L, "2022-01-02"));
        expectedRecords.add(changelogRow("+I", "Yen", 1L, "2022-01-02"));
        collectAndCheck(tEnv, managedTable, Collections.emptyMap(), expectedRecords).close();
    }

    @Test
    public void testBatchWriteWithPartitionedRecordsWithoutPk() throws Exception {
        String managedTable = prepareEnvAndWrite(false, false, true, false);

        // input is dailyRates()
        List<Row> expectedRecords = dailyRates();

        // test batch read
        collectAndCheck(tEnv, managedTable, Collections.emptyMap(), expectedRecords).close();
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
        String managedTable = prepareEnvAndWrite(false, false, false, true);

        // input is rates()
        List<Row> expectedRecords =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("+I", "Euro", 119L));
        collectAndCheck(tEnv, managedTable, Collections.emptyMap(), expectedRecords).close();
        checkFileStorePath(tEnv, managedTable);

        // overwrite the whole table
        prepareEnvAndOverwrite(
                managedTable,
                Collections.emptyMap(),
                Collections.singletonList(new String[] {"'Euro'", "100"}));
        expectedRecords = new ArrayList<>(expectedRecords);
        expectedRecords.clear();
        expectedRecords.add(changelogRow("+I", "Euro", 100L));
        collectAndCheck(tEnv, managedTable, Collections.emptyMap(), expectedRecords).close();
    }

    @Test
    public void testBatchWriteNonPartitionedRecordsWithoutPk() throws Exception {
        String managedTable = prepareEnvAndWrite(false, false, false, false);

        // input is rates()
        List<Row> expectedRecords = rates();
        collectAndCheck(tEnv, managedTable, Collections.emptyMap(), expectedRecords).close();
        checkFileStorePath(tEnv, managedTable);
    }

    @Test
    public void testEnableLogAndStreamingReadWritePartitionedRecordsWithPk() throws Exception {
        String managedTable = prepareEnvAndWrite(true, true, true, true);

        // input is dailyRatesChangelogWithoutUB()
        // test hybrid read
        List<Row> expectedRecords =
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02"));
        BlockingIterator<Row, Row> streamIter =
                collectAndCheck(tEnv, managedTable, Collections.emptyMap(), expectedRecords);
        checkFileStorePath(tEnv, managedTable);

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
        String managedTable = prepareEnvAndWrite(true, false, true, true);

        // input is dailyRatesChangelogWithoutUB()
        // file store continuous read
        // will not merge, at least collect two records
        List<Row> expectedRecords =
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02"));
        collectAndCheck(tEnv, managedTable, Collections.emptyMap(), expectedRecords).close();
    }

    @Test
    public void testStreamingReadWritePartitionedRecordsWithoutPk() throws Exception {
        String managedTable = prepareEnvAndWrite(true, true, true, false);

        // input is dailyRatesChangelogWithUB()
        // enable log store, file store bounded read with merge
        List<Row> expectedRecords =
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 115L, "2022-01-02"));
        collectAndCheck(tEnv, managedTable, Collections.emptyMap(), expectedRecords).close();
    }

    @Test
    public void testStreamingReadWriteNonPartitionedRecordsWithPk() throws Exception {
        String managedTable = prepareEnvAndWrite(true, true, false, true);

        // input is ratesChangelogWithoutUB()
        // enable log store, file store bounded read with merge
        List<Row> expectedRecords =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L), changelogRow("+I", "Euro", 119L));
        collectAndCheck(tEnv, managedTable, Collections.emptyMap(), expectedRecords).close();
    }

    @Test
    public void testStreamingReadWriteNonPartitionedRecordsWithoutPk() throws Exception {
        String managedTable = prepareEnvAndWrite(true, true, false, false);

        // input is ratesChangelogWithUB()
        // enable log store, with default full scan mode, will merge
        List<Row> expectedRecords =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L), changelogRow("+I", "Euro", 119L));

        collectAndCheck(tEnv, managedTable, Collections.emptyMap(), expectedRecords).close();
    }

    // ------------------------ Tools ----------------------------------

    private String prepareEnvAndWrite(
            boolean streaming, boolean enableLogStore, boolean partitioned, boolean hasPk)
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
                    prepareHelperSourceWithChangelogRecords(sourceTable, partitioned, hasPk);
            env = buildStreamEnv();
            builder.inStreamingMode();
        } else {
            helperTableDdl =
                    prepareHelperSourceWithInsertOnlyRecords(sourceTable, partitioned, hasPk);
            env = buildBatchEnv();
            builder.inBatchMode();
        }
        String managedTableDdl = prepareManagedTableDdl(sourceTable, managedTable, tableOptions);
        String insertQuery = prepareInsertIntoQuery(sourceTable, managedTable);

        tEnv = StreamTableEnvironment.create(env, builder.build());
        tEnv.executeSql(helperTableDdl);
        tEnv.executeSql(managedTableDdl);
        tEnv.executeSql(insertQuery).await();
        return managedTable;
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
            StringBuilder hintsBuilder = new StringBuilder("/* + OPTIONS (");
            hints.forEach(
                    (k, v) -> {
                        hintsBuilder.append(String.format("'%s'", k));
                        hintsBuilder.append(" = ");
                        hintsBuilder.append(String.format("'%s', ", v));
                    });
            int len = hintsBuilder.length();
            hintsBuilder.deleteCharAt(len - 1);
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
