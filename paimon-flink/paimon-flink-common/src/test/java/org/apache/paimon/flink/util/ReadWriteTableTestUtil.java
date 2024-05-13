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

package org.apache.paimon.flink.util;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.ReadWriteTableITCase;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.registerData;
import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test util for {@link ReadWriteTableITCase}. */
public class ReadWriteTableTestUtil {

    private static final Time TIME_OUT = Time.seconds(10);

    public static final int DEFAULT_PARALLELISM = 2;

    public static final Map<String, String> SCAN_LATEST =
            new HashMap<String, String>() {
                {
                    put(SCAN_MODE.key(), CoreOptions.StartupMode.LATEST.toString());
                }
            };

    public static TableEnvironment sEnv;

    public static StreamExecutionEnvironment bExeEnv;
    public static TableEnvironment bEnv;

    public static String warehouse;

    public static void init(String warehouse) {
        init(warehouse, DEFAULT_PARALLELISM);
    }

    public static void init(String warehouse, int parallelism) {
        StreamExecutionEnvironment sExeEnv = buildStreamEnv(parallelism);
        sExeEnv.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        sEnv = StreamTableEnvironment.create(sExeEnv);

        bExeEnv = buildBatchEnv(parallelism);
        bExeEnv.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        bEnv = StreamTableEnvironment.create(bExeEnv, EnvironmentSettings.inBatchMode());

        ReadWriteTableTestUtil.warehouse = warehouse;
        String catalog = "PAIMON";
        sEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH ('type'='paimon', 'warehouse'='%s');",
                        catalog, warehouse));
        sEnv.useCatalog(catalog);

        bEnv.registerCatalog(catalog, sEnv.getCatalog(catalog).get());
        bEnv.useCatalog(catalog);
    }

    public static StreamExecutionEnvironment buildStreamEnv(int parallelism) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100);
        env.setParallelism(parallelism);
        return env;
    }

    public static StreamExecutionEnvironment buildBatchEnv(int parallelism) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(parallelism);
        return env;
    }

    public static String createTable(
            List<String> fieldsSpec,
            List<String> primaryKeys,
            List<String> bucketKeys,
            List<String> partitionKeys) {
        return createTable(fieldsSpec, primaryKeys, bucketKeys, partitionKeys, new HashMap<>());
    }

    public static String createTable(
            List<String> fieldsSpec,
            List<String> primaryKeys,
            List<String> bucketKeys,
            List<String> partitionKeys,
            Map<String, String> options) {
        // "-" is not allowed in the table name.
        String table = ("MyTable_" + UUID.randomUUID()).replace("-", "_");
        Map<String, String> newOptions = new HashMap<>(options);
        if (!newOptions.containsKey("bucket")) {
            newOptions.put("bucket", "1");
        }
        if (!bucketKeys.isEmpty()) {
            newOptions.put("bucket-key", String.join(",", bucketKeys));
        }
        sEnv.executeSql(buildDdl(table, fieldsSpec, primaryKeys, partitionKeys, newOptions));
        return table;
    }

    public static String createTemporaryTable(
            List<String> fieldsSpec,
            List<String> primaryKeys,
            List<String> partitionKeys,
            List<Row> initialRecords,
            @Nullable String partitionList,
            boolean bounded,
            String changelogMode) {
        String temporaryTableDdlFormat =
                "CREATE TEMPORARY TABLE `%s`( %s %s) %s WITH (\n"
                        + "'connector' = 'values',\n"
                        + "'disable-lookup' = 'true',\n"
                        + "'data-id' = '%s',\n"
                        + "%s"
                        + "'bounded' = '%s',\n"
                        + "'changelog-mode' = '%s'\n"
                        + ");";

        String temporaryTable = "temp_" + UUID.randomUUID();

        sEnv.executeSql(
                String.format(
                        temporaryTableDdlFormat,
                        temporaryTable,
                        String.join(",", fieldsSpec),
                        buildPkConstraint(primaryKeys),
                        buildPartitionSpec(partitionKeys),
                        registerData(initialRecords),
                        partitionList == null
                                ? ""
                                : String.format("'partition-list' = '%s',\n", partitionList),
                        bounded,
                        changelogMode));

        return temporaryTable;
    }

    public static void insertInto(String table, String... records) throws Exception {
        insertIntoPartition(table, "", records);
    }

    public static void insertIntoPartition(String table, String partitionSpec, String... records)
            throws Exception {
        sEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s` %s VALUES %s;",
                                table, partitionSpec, String.join(",", records)))
                .await();
    }

    public static void insertIntoFromTable(String source, String sink) throws Exception {
        sEnv.executeSql(String.format("INSERT INTO `%s` SELECT * FROM `%s`;", sink, source))
                .await();
    }

    public static void insertOverwrite(String table, String... records) throws Exception {
        insertOverwritePartition(table, "", records);
    }

    public static void insertOverwritePartition(
            String table, String partitionSpe, String... records) throws Exception {
        String insert =
                String.format(
                        "INSERT OVERWRITE `%s` %s VALUES %s;",
                        table, partitionSpe, String.join(",", records));
        bEnv.executeSql(insert).await();
    }

    public static String buildSimpleQuery(String table) {
        return buildQuery(table, "*", "");
    }

    public static String buildQuery(String table, String projection, String filter) {
        return buildQueryWithTableOptions(table, projection, filter, new HashMap<>());
    }

    public static String buildQueryWithTableOptions(
            String table,
            String projection,
            String filter,
            Long limit,
            Map<String, String> options) {
        List<Object> params = new ArrayList<>();
        params.add(projection);
        params.add(table);
        params.add(buildTableOptionsSpec(options));
        params.add(filter);
        StringBuilder queryFormat = new StringBuilder("SELECT %s FROM `%s` %s %s");
        if (null != limit) {
            queryFormat.append(" limit %s");
            params.add(limit);
        }

        return String.format(queryFormat.toString(), params.toArray());
    }

    public static String buildQueryWithTableOptions(
            String table, String projection, String filter, Map<String, String> options) {
        return buildQueryWithTableOptions(table, projection, filter, null, options);
    }

    public static void checkFileStorePath(String table, List<String> partitionSpec) {
        String relativeFilePath = String.format("/%s.db/%s", sEnv.getCurrentDatabase(), table);
        // check snapshot file path
        assertThat(Paths.get(warehouse, relativeFilePath, "snapshot")).exists();
        // check manifest file path
        assertThat(Paths.get(warehouse, relativeFilePath, "manifest")).exists();
        // check data file path
        if (partitionSpec.isEmpty()) {
            partitionSpec = Collections.singletonList("");
        }
        partitionSpec.stream()
                .map(str -> str.replaceAll(",", "/"))
                .map(str -> str.replaceAll("null", "__DEFAULT_PARTITION__"))
                .forEach(
                        partition -> {
                            assertThat(Paths.get(warehouse, relativeFilePath, partition)).exists();
                            // at least exists bucket-0
                            assertThat(
                                            Paths.get(
                                                    warehouse,
                                                    relativeFilePath,
                                                    partition,
                                                    "bucket-0"))
                                    .exists();
                        });
    }

    public static void testBatchRead(String query, List<Row> expected) throws Exception {
        CloseableIterator<Row> resultItr = bEnv.executeSql(query).collect();
        try (BlockingIterator<Row, Row> iterator = BlockingIterator.of(resultItr)) {
            if (!expected.isEmpty()) {
                List<Row> result =
                        iterator.collect(expected.size(), TIME_OUT.getSize(), TIME_OUT.getUnit());
                assertThat(toInsertOnlyRows(result))
                        .containsExactlyInAnyOrderElementsOf(toInsertOnlyRows(expected));
            }
            assertThat(resultItr.hasNext()).isFalse();
        }
    }

    private static List<Row> toInsertOnlyRows(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        for (Row row : rows) {
            assertThat(row.getKind()).isIn(RowKind.INSERT, RowKind.UPDATE_AFTER);
            Row newRow = new Row(row.getArity());
            for (int i = 0; i < row.getArity(); i++) {
                newRow.setField(i, row.getField(i));
            }
            result.add(newRow);
        }
        return result;
    }

    public static BlockingIterator<Row, Row> testStreamingRead(String query, List<Row> expected)
            throws Exception {
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());
        validateStreamingReadResult(iterator, expected);
        return iterator;
    }

    public static BlockingIterator<Row, Row> testStreamingReadWithReadFirst(
            String source, String sink, String query, List<Row> expected) throws Exception {
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());
        insertIntoFromTable(source, sink);
        validateStreamingReadResult(iterator, expected);
        return iterator;
    }

    public static void validateStreamingReadResult(
            BlockingIterator<Row, Row> streamingItr, List<Row> expected) throws Exception {
        if (expected.isEmpty()) {
            assertNoMoreRecords(streamingItr);
        } else {
            assertThat(streamingItr.collect(expected.size()))
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    public static void assertNoMoreRecords(BlockingIterator<Row, Row> iterator) {
        List<Row> expectedRecords = Collections.emptyList();
        try {
            // set expectation size to 1 to let time pass by until timeout
            // just wait 5s to avoid too long time
            expectedRecords = iterator.collect(1, 5L, TimeUnit.SECONDS);
        } catch (TimeoutException ignored) {
            // don't throw exception
        }
        assertThat(expectedRecords).isEmpty();
    }

    public static String buildDdl(
            String table,
            List<String> fieldsSpec,
            List<String> primaryKeys,
            List<String> partitionKeys,
            Map<String, String> options) {
        return String.format(
                "CREATE TABLE `%s`(%s %s) %s %s;",
                table,
                String.join(",", fieldsSpec),
                buildPkConstraint(primaryKeys),
                buildPartitionSpec(partitionKeys),
                buildOptionsSpec(options));
    }

    private static String buildPkConstraint(List<String> primaryKeys) {
        if (!primaryKeys.isEmpty()) {
            return String.format(",PRIMARY KEY (%s) NOT ENFORCED", String.join(",", primaryKeys));
        }
        return "";
    }

    private static String buildPartitionSpec(List<String> partitionKeys) {
        if (!partitionKeys.isEmpty()) {
            return String.format("PARTITIONED BY (%s)", String.join(",", partitionKeys));
        }
        return "";
    }

    private static String buildOptionsSpec(Map<String, String> options) {
        if (!options.isEmpty()) {
            return String.format("WITH ( %s )", optionsToString(options));
        }
        return "";
    }

    private static String buildTableOptionsSpec(Map<String, String> hints) {
        if (!hints.isEmpty()) {
            return String.format("/*+ OPTIONS ( %s ) */", optionsToString(hints));
        }
        return "";
    }

    private static String optionsToString(Map<String, String> options) {
        List<String> pairs = new ArrayList<>();
        options.forEach((k, v) -> pairs.add(String.format("'%s' = '%s'", k, v)));
        return String.join(",", pairs);
    }
}
