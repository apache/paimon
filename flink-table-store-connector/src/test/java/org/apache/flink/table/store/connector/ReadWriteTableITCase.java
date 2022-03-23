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
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.function.TriFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.planner.factories.TestValuesTableFactory.registerData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for managed table dml. */
@RunWith(Parameterized.class)
public class ReadWriteTableITCase extends TableStoreTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(ReadWriteTableITCase.class);

    private static final Map<Row, Pair<RowKind, Row>> PROCESSED_RECORDS = new LinkedHashMap<>();

    private static final TriFunction<Row, Boolean, Boolean, Pair<Row, Pair<RowKind, Row>>>
            KEY_VALUE_ASSIGNER =
                    (record, hasPk, partitioned) -> {
                        boolean retract =
                                record.getKind() == RowKind.DELETE
                                        || record.getKind() == RowKind.UPDATE_BEFORE;
                        Row key;
                        Row value;
                        RowKind rowKind = record.getKind();
                        if (hasPk) {
                            key =
                                    partitioned
                                            ? Row.of(record.getField(0), record.getField(2))
                                            : Row.of(record.getField(0));
                            value = record;
                        } else {
                            key = record;
                            value = Row.of(retract ? -1 : 1);
                        }
                        key.setKind(RowKind.INSERT);
                        value.setKind(RowKind.INSERT);
                        return Pair.of(key, Pair.of(rowKind, value));
                    };

    private static final TriFunction<List<Row>, Boolean, List<Boolean>, List<Row>> COMBINER =
            (records, insertOnly, schema) -> {
                boolean hasPk = schema.get(0);
                boolean partitioned = schema.get(1);
                records.forEach(
                        record -> {
                            Pair<Row, Pair<RowKind, Row>> kvPair =
                                    KEY_VALUE_ASSIGNER.apply(record, hasPk, partitioned);
                            Row key = kvPair.getLeft();
                            Pair<RowKind, Row> valuePair = kvPair.getRight();
                            if (insertOnly || !PROCESSED_RECORDS.containsKey(key)) {
                                update(hasPk, key, valuePair);
                            } else {
                                Pair<RowKind, Row> existingValuePair = PROCESSED_RECORDS.get(key);
                                RowKind existingKind = existingValuePair.getLeft();
                                Row existingValue = existingValuePair.getRight();
                                RowKind newKind = valuePair.getLeft();
                                Row newValue = valuePair.getRight();

                                if (hasPk) {
                                    if (existingKind == newKind && existingKind == RowKind.INSERT) {
                                        throw new IllegalStateException(
                                                "primary key "
                                                        + key
                                                        + " already exists for record: "
                                                        + record);
                                    } else if (existingKind == RowKind.INSERT
                                            && newKind == RowKind.UPDATE_AFTER) {
                                        PROCESSED_RECORDS.replace(key, valuePair);
                                    } else if (newKind == RowKind.DELETE
                                            || newKind == RowKind.UPDATE_BEFORE) {
                                        if (existingValue.equals(newValue)) {
                                            PROCESSED_RECORDS.remove(key);
                                        } else {
                                            throw new IllegalStateException(
                                                    "Try to retract an non-existing record: "
                                                            + record);
                                        }
                                    }
                                } else {
                                    update(false, key, valuePair);
                                }
                            }
                        });
                List<Row> results =
                        PROCESSED_RECORDS.entrySet().stream()
                                .flatMap(
                                        entry -> {
                                            if (hasPk) {
                                                Row row = entry.getValue().getRight();
                                                row.setKind(RowKind.INSERT);
                                                return Stream.of(row);
                                            }
                                            Row row = entry.getKey();
                                            row.setKind(RowKind.INSERT);
                                            int count =
                                                    (int) entry.getValue().getRight().getField(0);
                                            List<Row> rows = new ArrayList<>();
                                            while (count > 0) {
                                                rows.add(row);
                                                count--;
                                            }
                                            return rows.stream();
                                        })
                                .collect(Collectors.toList());
                PROCESSED_RECORDS.clear();
                return results;
            };

    private final String helperTableDdl;
    private final String managedTableDdl;
    private final String insertQuery;
    private final String selectQuery;

    private static int testId = 0;

    private static void update(boolean hasPk, Row key, Pair<RowKind, Row> value) {
        if (hasPk) {
            PROCESSED_RECORDS.put(key, value);
        } else {
            PROCESSED_RECORDS.compute(
                    key,
                    (k, v) -> {
                        if (v == null) {
                            return value;
                        } else {
                            return Pair.of(
                                    v.getLeft(),
                                    Row.of(
                                            (int) v.getRight().getField(0)
                                                    + (int) value.getRight().getField(0)));
                        }
                    });
            if ((int) PROCESSED_RECORDS.get(key).getRight().getField(0) == 0) {
                PROCESSED_RECORDS.remove(key);
            }
        }
    }

    public ReadWriteTableITCase(
            RuntimeExecutionMode executionMode,
            String tableName,
            boolean enableLogStore,
            String helperTableDdl,
            String managedTableDdl,
            String insertQuery,
            String selectQuery,
            ExpectedResult expectedResult) {
        super(executionMode, tableName, enableLogStore, expectedResult);
        this.helperTableDdl = helperTableDdl;
        this.managedTableDdl = managedTableDdl;
        this.insertQuery = insertQuery;
        this.selectQuery = selectQuery;
    }

    @Parameterized.Parameters(
            name =
                    "executionMode-{0}, tableName-{1}, "
                            + "enableLogStore-{2}, helperTableDdl-{3}, managedTableDdl-{4}, "
                            + "insertQuery-{5}, selectQuery-{6}, expectedResult-{7}")
    public static List<Object[]> data() {
        List<Object[]> specs = prepareReadWriteTestSpecs();
        specs.addAll(prepareBatchWriteStreamingReadTestSpecs());
        specs.addAll(prepareStreamingWriteBatchReadTestSpecs());
        specs.addAll(prepareOverwriteTestSpecs());
        return specs;
    }

    @Override
    protected void prepareEnv() {
        tEnv.executeSql(helperTableDdl);
        tEnv.executeSql(managedTableDdl);
    }

    @Test
    public void testSequentialWriteRead() throws Exception {
        logTestSpec(
                executionMode,
                tableIdentifier,
                enableLogStore,
                helperTableDdl,
                managedTableDdl,
                insertQuery,
                selectQuery,
                expectedResult);
        if (expectedResult.success) {
            tEnv.executeSql(insertQuery).await();
            TableResult result = tEnv.executeSql(selectQuery);
            List<Row> actual = new ArrayList<>();

            try (CloseableIterator<Row> iterator = result.collect()) {
                if (env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING) {
                    ExecutorService executorService = Executors.newSingleThreadExecutor();
                    Callable<List<Row>> callable =
                            () ->
                                    collectResult(
                                            false, iterator, expectedResult.expectedRecords.size());
                    Future<List<Row>> future = executorService.submit(callable);
                    executorService.shutdown();
                    actual.addAll(future.get(10, TimeUnit.SECONDS));
                } else {
                    actual.addAll(collectResult(true, iterator, -1));
                }
            }

            assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedResult.expectedRecords);
            String relativeFilePath = FileStoreOptions.relativeTablePath(tableIdentifier);
            // check snapshot file path
            assertThat(Paths.get(rootPath, relativeFilePath, "snapshot")).exists();
            // check manifest file path
            assertThat(Paths.get(rootPath, relativeFilePath, "manifest")).exists();

            if (enableLogStore) {
                assertThat(topicExists(tableIdentifier.asSummaryString())).isTrue();
            }
        } else {
            AbstractThrowableAssert<?, ? extends Throwable> throwableAssert =
                    assertThatThrownBy(
                            () -> {
                                tEnv.executeSql(insertQuery).await();
                                tEnv.executeSql(selectQuery).collect();
                            });
            if (expectedResult.failureHasCause) {
                throwableAssert
                        .getCause()
                        .isInstanceOf(expectedResult.expectedType)
                        .hasMessageContaining(expectedResult.expectedMessage);
            } else {
                throwableAssert
                        .isInstanceOf(expectedResult.expectedType)
                        .hasMessageContaining(expectedResult.expectedMessage);
            }
        }
    }

    private static List<Object[]> prepareReadWriteTestSpecs() {
        List<Object[]> specs = new ArrayList<>();
        RuntimeExecutionMode[] executionModes =
                new RuntimeExecutionMode[] {
                    RuntimeExecutionMode.BATCH, RuntimeExecutionMode.STREAMING
                };
        boolean[] partitionStatus = new boolean[] {true, false};
        boolean[] pkStatus = new boolean[] {true, false};
        boolean[] enableLogStoreStatus = new boolean[] {true, false};
        for (RuntimeExecutionMode executionMode : executionModes) {
            for (boolean partitioned : partitionStatus) {
                for (boolean hasPk : pkStatus) {
                    for (boolean enableLogStore : enableLogStoreStatus) {
                        boolean batchMode = executionMode == RuntimeExecutionMode.BATCH;
                        List<String> tableNameAndDdl =
                                batchMode
                                        ? prepareDdlWithInsertOnlySource(partitioned, hasPk)
                                        : prepareDdlWithChangelogSource(partitioned, hasPk);
                        String sourceTableName = tableNameAndDdl.get(0);
                        String managedTableName = tableNameAndDdl.get(1);
                        String helperTableDdl = tableNameAndDdl.get(2);
                        String managedTableDdl = tableNameAndDdl.get(3);
                        String insertQuery =
                                prepareInsertIntoQuery(sourceTableName, managedTableName);
                        // TODO: prepare read with hints test
                        String selectQuery = prepareSimpleSelectQuery(managedTableName);
                        ExpectedResult expectedResult = new ExpectedResult();
                        expectedResult.success(true);
                        List<Row> expectedRecords;
                        if (batchMode) {
                            expectedRecords = partitioned ? dailyRates() : rates();
                        } else {
                            if (partitioned) {
                                expectedRecords =
                                        hasPk
                                                ? dailyRatesChangelogWithoutUB()
                                                : dailyRatesChangelogWithUB();
                            } else {
                                expectedRecords =
                                        hasPk ? ratesChangelogWithoutUB() : ratesChangelogWithUB();
                            }
                        }
                        expectedResult.expectedRecords(
                                COMBINER.apply(
                                        expectedRecords,
                                        batchMode,
                                        Arrays.asList(hasPk, partitioned)));
                        Object[] spec =
                                new Object[] {
                                    executionMode,
                                    managedTableName,
                                    enableLogStore,
                                    helperTableDdl,
                                    managedTableDdl,
                                    insertQuery,
                                    selectQuery,
                                    expectedResult
                                };
                        specs.add(spec);
                    }
                }
            }
        }
        return specs;
    }

    private static List<Object[]> prepareOverwriteTestSpecs() {
        // TODO: add overwrite case
        return Collections.emptyList();
    }

    private static List<Object[]> prepareBatchWriteStreamingReadTestSpecs() {
        // TODO: add batch write & streaming read case
        return new ArrayList<>();
    }

    private static List<Object[]> prepareStreamingWriteBatchReadTestSpecs() {
        // TODO: add streaming write and batch read case
        return new ArrayList<>();
    }

    private static List<String> prepareDdlWithInsertOnlySource(boolean partitioned, boolean hasPk) {
        String sourceTableName = "source_table_" + UUID.randomUUID();
        String managedTableName = "table_" + UUID.randomUUID();
        String sourceDdl =
                prepareHelperSourceWithInsertOnlyRecords(sourceTableName, partitioned, hasPk);
        String managedDdl = prepareManagedTableDdl(sourceTableName, managedTableName);
        return Arrays.asList(sourceTableName, managedTableName, sourceDdl, managedDdl);
    }

    private static List<String> prepareDdlWithChangelogSource(boolean partitioned, boolean hasPk) {
        String sourceTableName = "source_table_" + UUID.randomUUID();
        String managedTableName = "table_" + UUID.randomUUID();
        String sourceDdl =
                prepareHelperSourceWithChangelogRecords(sourceTableName, partitioned, hasPk);
        String managedDdl = prepareManagedTableDdl(sourceTableName, managedTableName);
        return Arrays.asList(sourceTableName, managedTableName, sourceDdl, managedDdl);
    }

    private static String prepareManagedTableDdl(String sourceTableName, String managedTableName) {
        return prepareManagedTableDdl(sourceTableName, managedTableName, Collections.emptyMap());
    }

    private static String prepareManagedTableDdl(
            String sourceTableName, String managedTableName, Map<String, String> tableOptions) {
        StringBuilder ddl =
                new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                        .append(
                                ObjectIdentifier.of(
                                                CURRENT_CATALOG, CURRENT_DATABASE, managedTableName)
                                        .asSerializableString());
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
        return prepareInsertIntoQuery(sourceTableName, managedTableName, Collections.emptyMap());
    }

    private static String prepareInsertIntoQuery(
            String sourceTableName, String managedTableName, Map<String, String> hints) {
        return prepareInsertQuery(
                sourceTableName, managedTableName, false, Collections.emptyMap(), hints);
    }

    private static String prepareInsertQuery(
            String sourceTableName,
            String managedTableName,
            boolean overwrite,
            Map<String, String> partitions,
            Map<String, String> hints) {
        StringBuilder insertDmlBuilder =
                new StringBuilder(
                        String.format(
                                "INSERT %s `%s`",
                                overwrite ? "OVERWRITE" : "INTO", managedTableName));
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

    private static String prepareSimpleSelectQuery(String tableName) {
        return prepareSimpleSelectQuery(tableName, Collections.emptyMap());
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
     *      'bounded' = executionMode,
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

    private static void logTestSpec(
            RuntimeExecutionMode executionMode,
            ObjectIdentifier tableIdentifier,
            boolean enableLogStore,
            String helperTableDdl,
            String managedTableDdl,
            String insertQuery,
            String selectQuery,
            ExpectedResult expectedResult) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Test id => {}", testId);
            LOG.debug("Execution mode => {}", executionMode);
            LOG.debug("Table name => {}", tableIdentifier);
            LOG.debug("Enable logStore => {}", enableLogStore);
            LOG.debug("Helper table ddl => {}", helperTableDdl);
            LOG.debug("Managed table ddl => {}", managedTableDdl);
            LOG.debug("Insert query => {}", insertQuery);
            LOG.debug("Select query => {}", selectQuery);
            LOG.debug("Expected => {}", expectedResult);
            testId++;
        }
    }
}
