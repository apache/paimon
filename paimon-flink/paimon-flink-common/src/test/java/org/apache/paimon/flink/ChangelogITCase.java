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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.paimon.flink.action.CompactAction;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.FailingFileIO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for changelog table with primary keys. */
public class ChangelogITCase extends AbstractTestBase {

    // ------------------------------------------------------------------------
    //  Test Utilities
    // ------------------------------------------------------------------------
    private String path;

    @BeforeEach
    public void before() throws IOException {
        path = getTempDirPath();
    }

    private TableEnvironment createBatchTableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    private TableEnvironment createStreamingTableEnvironment(int checkpointIntervalMs) {
        TableEnvironment sEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        // set checkpoint interval to a random number to emulate different speed of commit
        sEnv.getConfig()
                .getConfiguration()
                .set(CHECKPOINTING_INTERVAL, Duration.ofMillis(checkpointIntervalMs));
        return sEnv;
    }

    private StreamExecutionEnvironment createStreamExecutionEnvironment(int checkpointIntervalMs) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(checkpointIntervalMs);
        return env;
    }


    @Test
    @Timeout(1200)
    public void testStandAloneInputJobRandom() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        TableEnvironment sEnv = createStreamingTableEnvironment(3000);
        testStandAloneInputJobRandom(sEnv, 1, random.nextBoolean());
    }



    private static final int NUM_PARTS = 1;
    private static final int NUM_KEYS = 64;
    private static final int NUM_VALUES = 1024;
    private static final int LIMIT = 20;


    private void testStandAloneInputJobRandom(
            TableEnvironment tEnv, int numProducers, boolean enableConflicts) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        testRandom(
                tEnv,
                numProducers,
                false,
                "'bucket' = '1',"
                        + String.format(
                                "'write-buffer-size' = '%s',",
                                random.nextBoolean() ? "512kb" : "1mb")
                        + "'changelog-producer' = 'input',"
                        + "'write-only' = 'true'");

        // sleep for a random amount of time to check
        // if dedicated compactor job can find first snapshot to compact correctly
        //Thread.sleep(random.nextInt(2500));

        for (int i = enableConflicts ? 2 : 1; i > 0; i--) {
            StreamExecutionEnvironment env =
                    createStreamExecutionEnvironment(5000);
            env.setParallelism(2);
            new CompactAction(path, "default", "T").build(env);
            env.executeAsync();
        }

        // sleep for a random amount of time to check
        // if we can first read complete records then read incremental records correctly
        //Thread.sleep(random.nextInt(2500));

        checkChangelogTestResult(numProducers);
    }

    private void checkChangelogTestResult(int numProducers) throws Exception {
        TableEnvironment sEnv = createStreamingTableEnvironment(5000);
        sEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        sEnv.executeSql(
                String.format(
                        "CREATE CATALOG testCatalog WITH ('type'='paimon', 'warehouse'='%s')",
                        path));
        sEnv.executeSql("USE CATALOG testCatalog");

        ResultChecker checker = new ResultChecker();
        int endCnt = 0;
        try (CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM T").collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                checker.addChangelog(row);
                if (((long) row.getField(2)) >= LIMIT) {
                    endCnt++;
                    if (endCnt == numProducers * NUM_PARTS * NUM_KEYS) {
                        break;
                    }
                }
            }
        }
        checker.assertResult(numProducers);

        checkBatchResult(numProducers);
    }

    /**
     * Run {@code numProducers} jobs at the same time. Each job randomly update {@code NUM_PARTS}
     * partitions and {@code NUM_KEYS} keys for about {@code LIMIT} records. For the final {@code
     * NUM_PARTS * NUM_KEYS} records, keys are updated to some specific values for result checking.
     *
     * <p>All jobs will modify the same set of partitions to emulate conflicting writes. Each job
     * will write its own set of keys for easy result checking.
     */
    private List<TableResult> testRandom(
            TableEnvironment tEnv, int numProducers, boolean enableFailure, String tableProperties)
            throws Exception {
        String failingName = UUID.randomUUID().toString();
        String failingPath = FailingFileIO.getFailingPath(failingName, path);

        // no failure when creating catalog and table
        FailingFileIO.reset(failingName, 0, 1);
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG testCatalog WITH ('type'='paimon', 'warehouse'='%s')",
                        failingPath));
        tEnv.executeSql("USE CATALOG testCatalog");
        tEnv.executeSql(
                "CREATE TABLE T("
                        + "  pt STRING,"
                        + "  k INT,"
                        + "  v1 BIGINT,"
                        + "  v2 STRING,"
                        + "  PRIMARY KEY (pt, k) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + tableProperties
                        + ")");

        // input data must be strictly ordered
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.executeSql(
                        "CREATE TABLE `default_catalog`.`default_database`.`S` ("
                                + "  i INT"
                                + ") WITH ("
                                + "  'connector' = 'datagen',"
                                + "  'fields.i.kind' = 'sequence',"
                                + "  'fields.i.start' = '0',"
                                + "  'fields.i.end' = '"
                                + (LIMIT + NUM_PARTS * NUM_KEYS - 1)
                                + "',"
                                + "  'number-of-rows' = '"
                                //+ (LIMIT + NUM_PARTS * NUM_KEYS)
                                + 10
                                + "',"
                                + "  'rows-per-second' = '"
                                //+ (LIMIT / 20 + ThreadLocalRandom.current().nextInt(LIMIT / 20))
                                + 1
                                + "'"
                                + ")")
                .await();

        List<TableResult> results = new ArrayList<>();

        if (enableFailure) {
            FailingFileIO.reset(failingName, 2, 10000);
        }
        for (int i = 0; i < numProducers; i++) {
            // for the last `NUM_PARTS * NUM_KEYS` records, we update every key to a specific value
            String ptSql =
                    String.format(
                            "IF(i >= %d, CAST((i - %d) / %d AS STRING), CAST(CAST(FLOOR(RAND() * %d) AS INT) AS STRING)) AS pt",
                            LIMIT, LIMIT, NUM_KEYS, NUM_PARTS);
            String kSql =
                    String.format(
                            "IF(i >= %d, MOD(i - %d, %d), CAST(FLOOR(RAND() * %d) AS INT)) + %d AS k",
                            LIMIT, LIMIT, NUM_KEYS, NUM_KEYS, i * NUM_KEYS);
            String v1Sql =
                    String.format(
                            "IF(i >= %d, i, CAST(FLOOR(RAND() * %d) AS BIGINT)) AS v1",
                            LIMIT, NUM_VALUES);
            String v2Sql = "CAST(i AS STRING) || '.str' AS v2";
            tEnv.executeSql(
                    String.format(
                            "CREATE TEMPORARY VIEW myView%d AS SELECT %s, %s, %s, %s FROM `default_catalog`.`default_database`.`S`",
                            i, ptSql, kSql, v1Sql, v2Sql));

            // run test SQL
            int idx = i;
            TableResult result =
                    FailingFileIO.retryArtificialException(
                            () ->
                                    tEnv.executeSql(
                                            "INSERT INTO T /*+ OPTIONS('sink.parallelism' = '1') */ SELECT * FROM myView"
                                                    + idx));
            results.add(result);
        }

        return results;
    }

    private void checkBatchResult(int numProducers) throws Exception {
        TableEnvironment bEnv = createBatchTableEnvironment();
        bEnv.executeSql(
                String.format(
                        "CREATE CATALOG testCatalog WITH ('type'='paimon', 'warehouse'='%s')",
                        path));
        bEnv.executeSql("USE CATALOG testCatalog");

        ResultChecker checker = new ResultChecker();
        try (CloseableIterator<Row> it = bEnv.executeSql("SELECT * FROM T").collect()) {
            while (it.hasNext()) {
                checker.addChangelog(it.next());
            }
        }
        checker.assertResult(numProducers);
    }

    private static class ResultChecker {

        private final Map<String, String> valueMap;
        private final Map<String, RowKind> kindMap;

        private ResultChecker() {
            this.valueMap = new HashMap<>();
            this.kindMap = new HashMap<>();
        }

        private void addChangelog(Row row) {
            String key = row.getField(0) + "|" + row.getField(1);
            String value = row.getField(2) + "|" + row.getField(3);
            switch (row.getKind()) {
                case INSERT:
                    assertThat(valueMap.containsKey(key)).isFalse();
                    assertThat(!kindMap.containsKey(key) || kindMap.get(key) == RowKind.DELETE)
                            .isTrue();
                    valueMap.put(key, value);
                    break;
                case UPDATE_AFTER:
                    assertThat(valueMap.containsKey(key)).isFalse();
                    assertThat(kindMap.get(key)).isEqualTo(RowKind.UPDATE_BEFORE);
                    valueMap.put(key, value);
                    break;
                case UPDATE_BEFORE:
                case DELETE:
                    assertThat(valueMap.get(key)).isEqualTo(value);
                    assertThat(
                                    kindMap.get(key) == RowKind.INSERT
                                            || kindMap.get(key) == RowKind.UPDATE_AFTER)
                            .isTrue();
                    valueMap.remove(key);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown row kind " + row.getKind());
            }
            kindMap.put(key, row.getKind());
        }

        private void assertResult(int numProducers) {
            assertThat(valueMap.size()).isEqualTo(NUM_PARTS * NUM_KEYS * numProducers);
            for (int i = 0; i < NUM_PARTS; i++) {
                for (int j = 0; j < NUM_KEYS * numProducers; j++) {
                    String key = i + "|" + j;
                    int x = LIMIT + i * NUM_KEYS + j % NUM_KEYS;
                    String expectedValue = x + "|" + x + ".str";
                    assertThat(valueMap.get(key)).isEqualTo(expectedValue);
                }
            }
        }
    }
}
