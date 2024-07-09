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
import org.apache.paimon.flink.action.CompactAction;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.FailingFileIO;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for changelog table with primary keys. */
public class PrimaryKeyFileStoreTableITCase extends AbstractTestBase {

    // ------------------------------------------------------------------------
    //  Test Utilities
    // ------------------------------------------------------------------------
    private String path;
    private Map<String, String> tableDefaultProperties;

    @BeforeEach
    public void before() throws IOException {
        path = getTempDirPath();

        ThreadLocalRandom random = ThreadLocalRandom.current();
        tableDefaultProperties = new HashMap<>();
        if (random.nextBoolean()) {
            tableDefaultProperties.put(CoreOptions.LOCAL_MERGE_BUFFER_SIZE.key(), "256 kb");
        }
    }

    private String createCatalogSql(String catalogName, String warehouse) {
        String defaultPropertyString = "";
        if (tableDefaultProperties.size() > 0) {
            defaultPropertyString = ", ";
            defaultPropertyString +=
                    tableDefaultProperties.entrySet().stream()
                            .map(
                                    e ->
                                            String.format(
                                                    "'table-default.%s' = '%s'",
                                                    e.getKey(), e.getValue()))
                            .collect(Collectors.joining(", "));
        }

        return String.format(
                "CREATE CATALOG `%s` WITH ( 'type' = 'paimon', 'warehouse' = '%s' %s )",
                catalogName, warehouse, defaultPropertyString);
    }

    // ------------------------------------------------------------------------
    //  Constructed Tests
    // ------------------------------------------------------------------------

    @Test
    @Timeout(1200)
    public void testFullCompactionTriggerInterval() throws Exception {
        innerTestChangelogProducing(
                Arrays.asList(
                        "'changelog-producer' = 'full-compaction'",
                        "'full-compaction.delta-commits' = '3'"));
    }

    @Test
    @Timeout(1200)
    public void testFullCompactionWithLongCheckpointInterval() throws Exception {
        // create table
        TableEnvironment bEnv = tableEnvironmentBuilder().batchMode().parallelism(1).build();
        bEnv.executeSql(createCatalogSql("testCatalog", path));
        bEnv.executeSql("USE CATALOG testCatalog");
        bEnv.executeSql(
                "CREATE TABLE T ("
                        + "  k INT,"
                        + "  v INT,"
                        + "  PRIMARY KEY (k) NOT ENFORCED"
                        + ") WITH ("
                        + "  'bucket' = '1',"
                        + "  'changelog-producer' = 'full-compaction',"
                        + "  'write-only' = 'true'"
                        + ")");

        // run select job
        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(100)
                        .parallelism(1)
                        .build();
        sEnv.executeSql(createCatalogSql("testCatalog", path));
        sEnv.executeSql("USE CATALOG testCatalog");
        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM T").collect();

        // run compact job
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(2000)
                        .build();
        env.setParallelism(1);
        new CompactAction(path, "default", "T").withStreamExecutionEnvironment(env).build();
        JobClient client = env.executeAsync();

        // write records for a while
        long startMs = System.currentTimeMillis();
        int currentKey = 0;
        while (System.currentTimeMillis() - startMs <= 10000) {
            currentKey++;
            bEnv.executeSql(
                            String.format(
                                    "INSERT INTO T VALUES (%d, %d)", currentKey, currentKey * 100))
                    .await();
        }

        assertThat(client.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);

        for (int i = 1; i <= currentKey; i++) {
            assertThat(it.hasNext()).isTrue();
            assertThat(it.next().toString()).isEqualTo(String.format("+I[%d, %d]", i, i * 100));
        }
        it.close();
    }

    @Test
    @Timeout(1200)
    public void testLookupChangelog() throws Exception {
        innerTestChangelogProducing(Collections.singletonList("'changelog-producer' = 'lookup'"));
    }

    private void innerTestChangelogProducing(List<String> options) throws Exception {
        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(ThreadLocalRandom.current().nextInt(900) + 100)
                        .parallelism(1)
                        .build();

        sEnv.executeSql(createCatalogSql("testCatalog", path + "/warehouse"));
        sEnv.executeSql("USE CATALOG testCatalog");
        sEnv.executeSql(
                "CREATE TABLE T ( k INT, v STRING, PRIMARY KEY (k) NOT ENFORCED ) "
                        + "WITH ( "
                        + "'bucket' = '2', "
                        + String.join(",", options)
                        + ")");

        Path inputPath = new Path(path, "input");
        LocalFileIO.create().mkdirs(inputPath);
        sEnv.executeSql(
                "CREATE TABLE `default_catalog`.`default_database`.`S` ( i INT, g STRING ) "
                        + "WITH ( 'connector' = 'filesystem', 'format' = 'testcsv', 'path' = '"
                        + inputPath
                        + "', 'source.monitor-interval' = '500ms' )");

        sEnv.executeSql(
                "INSERT INTO T SELECT SUM(i) AS k, g AS v FROM `default_catalog`.`default_database`.`S` GROUP BY g");
        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM T").collect();

        // write initial data
        sEnv.executeSql(
                        "INSERT INTO `default_catalog`.`default_database`.`S` "
                                + "VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')")
                .await();

        // read initial data
        List<String> actual = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            actual.add(it.next().toString());
        }

        assertThat(actual)
                .containsExactlyInAnyOrder("+I[1, A]", "+I[2, B]", "+I[3, C]", "+I[4, D]");

        // write update data
        sEnv.executeSql(
                        "INSERT INTO `default_catalog`.`default_database`.`S` "
                                + "VALUES (1, 'D'), (1, 'C'), (1, 'B'), (1, 'A')")
                .await();

        // read update data
        actual.clear();
        for (int i = 0; i < 8; i++) {
            actual.add(it.next().toString());
        }

        assertThat(actual)
                .containsExactlyInAnyOrder(
                        "-D[1, A]",
                        "-U[2, B]",
                        "+U[2, A]",
                        "-U[3, C]",
                        "+U[3, B]",
                        "-U[4, D]",
                        "+U[4, C]",
                        "+I[5, D]");

        it.close();
    }

    @Test
    public void testBatchJobWithConflictAndRestart() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().allowRestart(10).build();
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + getTempDirPath()
                        + "' )");
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql(
                "CREATE TABLE t ( k INT, v INT, PRIMARY KEY (k) NOT ENFORCED ) "
                        // force compaction for each commit
                        + "WITH ( 'bucket' = '2', 'full-compaction.delta-commits' = '1' )");
        // write some basic records
        tEnv.executeSql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").await();

        // two batch jobs compact at the same time
        // let writer's parallelism > 1, so it cannot be chained with committer
        TableResult result1 =
                tEnv.executeSql(
                        "INSERT INTO t /*+ OPTIONS('sink.parallelism' = '2') */ VALUES (1, 11), (2, 21), (3, 31)");
        TableResult result2 =
                tEnv.executeSql(
                        "INSERT INTO t /*+ OPTIONS('sink.parallelism' = '2') */ VALUES (1, 12), (2, 22), (3, 32)");

        result1.await();
        result2.await();

        try (CloseableIterator<Row> it = tEnv.executeSql("SELECT * FROM t").collect()) {
            for (int i = 0; i < 3; i++) {
                assertThat(it).hasNext();
                Row row = it.next();
                assertThat(row.getField(1)).isNotEqualTo((int) row.getField(0) * 10);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Random Tests
    // ------------------------------------------------------------------------

    @Test
    @Timeout(1200)
    public void testNoChangelogProducerBatchRandom() throws Exception {
        TableEnvironment bEnv = tableEnvironmentBuilder().batchMode().build();
        testNoChangelogProducerRandom(bEnv, 1, false);
    }

    @Test
    @Timeout(1200)
    public void testNoChangelogProducerStreamingRandom() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(random.nextInt(900) + 100)
                        .allowRestart()
                        .build();
        testNoChangelogProducerRandom(sEnv, random.nextInt(1, 3), random.nextBoolean());
    }

    @Test
    @Timeout(1200)
    public void testFullCompactionChangelogProducerBatchRandom() throws Exception {
        TableEnvironment bEnv = tableEnvironmentBuilder().batchMode().build();
        testFullCompactionChangelogProducerRandom(bEnv, 1, false);
    }

    @Test
    @Timeout(1200)
    public void testFullCompactionChangelogProducerStreamingRandom() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(random.nextInt(900) + 100)
                        .allowRestart()
                        .build();
        testFullCompactionChangelogProducerRandom(sEnv, random.nextInt(1, 3), random.nextBoolean());
    }

    @Test
    @Timeout(1200)
    public void testStandAloneFullCompactJobRandom() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(random.nextInt(900) + 100)
                        .allowRestart()
                        .build();
        testStandAloneFullCompactJobRandom(sEnv, random.nextInt(1, 3), random.nextBoolean());
    }

    @Test
    @Timeout(1200)
    public void testLookupChangelogProducerBatchRandom() throws Exception {
        TableEnvironment bEnv = tableEnvironmentBuilder().batchMode().build();
        testLookupChangelogProducerRandom(bEnv, 1, false);
    }

    @Test
    @Timeout(1200)
    public void testLookupChangelogProducerStreamingRandom() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(random.nextInt(900) + 100)
                        .allowRestart()
                        .build();
        testLookupChangelogProducerRandom(sEnv, random.nextInt(1, 3), random.nextBoolean());
    }

    @Test
    @Timeout(1200)
    public void testStandAloneLookupJobRandom() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(random.nextInt(900) + 100)
                        .allowRestart()
                        .build();
        testStandAloneLookupJobRandom(sEnv, random.nextInt(1, 3), random.nextBoolean());
    }

    private static final int NUM_PARTS = 4;
    private static final int NUM_KEYS = 64;
    private static final int NUM_VALUES = 1024;
    private static final int LIMIT = 10000;

    private void testNoChangelogProducerRandom(
            TableEnvironment tEnv, int numProducers, boolean enableFailure) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        boolean enableDeletionVectors = random.nextBoolean();
        if (enableDeletionVectors) {
            // Deletion vectors mode not support concurrent write
            numProducers = 1;
        }
        List<TableResult> results =
                testRandom(
                        tEnv,
                        numProducers,
                        enableFailure,
                        "'bucket' = '4',"
                                + String.format(
                                        "'deletion-vectors.enabled' = '%s'",
                                        enableDeletionVectors));

        for (TableResult result : results) {
            result.await();
        }
        checkBatchResult(numProducers);
    }

    private void testFullCompactionChangelogProducerRandom(
            TableEnvironment tEnv, int numProducers, boolean enableFailure) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        testRandom(
                tEnv,
                numProducers,
                enableFailure,
                "'bucket' = '4',"
                        + String.format(
                                "'write-buffer-size' = '%s',",
                                random.nextBoolean() ? "512kb" : "1mb")
                        + "'changelog-producer' = 'full-compaction',"
                        + "'full-compaction.delta-commits' = '3'");

        // sleep for a random amount of time to check
        // if we can first read complete records then read incremental records correctly
        Thread.sleep(random.nextInt(5000));

        checkChangelogTestResult(numProducers);
    }

    private void testLookupChangelogProducerRandom(
            TableEnvironment tEnv, int numProducers, boolean enableFailure) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        boolean enableDeletionVectors = random.nextBoolean();
        if (enableDeletionVectors) {
            // Deletion vectors mode not support concurrent write
            numProducers = 1;
        }
        testRandom(
                tEnv,
                numProducers,
                enableFailure,
                "'bucket' = '4',"
                        + String.format(
                                "'write-buffer-size' = '%s',",
                                random.nextBoolean() ? "512kb" : "1mb")
                        + "'changelog-producer' = 'lookup',"
                        + String.format(
                                "'changelog-producer.lookup-wait' = '%s',", random.nextBoolean())
                        + String.format(
                                "'deletion-vectors.enabled' = '%s'", enableDeletionVectors));

        // sleep for a random amount of time to check
        // if we can first read complete records then read incremental records correctly
        Thread.sleep(random.nextInt(5000));

        checkChangelogTestResult(numProducers);
    }

    private void testStandAloneFullCompactJobRandom(
            TableEnvironment tEnv, int numProducers, boolean enableConflicts) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        testRandom(
                tEnv,
                numProducers,
                false,
                "'bucket' = '4',"
                        + String.format(
                                "'write-buffer-size' = '%s',",
                                random.nextBoolean() ? "512kb" : "1mb")
                        + "'changelog-producer' = 'full-compaction',"
                        + "'full-compaction.delta-commits' = '3',"
                        + "'write-only' = 'true'");

        // sleep for a random amount of time to check
        // if dedicated compactor job can find first snapshot to compact correctly
        Thread.sleep(random.nextInt(2500));

        for (int i = enableConflicts ? 2 : 1; i > 0; i--) {
            StreamExecutionEnvironment env =
                    streamExecutionEnvironmentBuilder()
                            .streamingMode()
                            .checkpointIntervalMs(random.nextInt(1900) + 100)
                            .parallelism(2)
                            .allowRestart()
                            .build();
            new CompactAction(path, "default", "T").withStreamExecutionEnvironment(env).build();
            env.executeAsync();
        }

        // sleep for a random amount of time to check
        // if we can first read complete records then read incremental records correctly
        Thread.sleep(random.nextInt(2500));

        checkChangelogTestResult(numProducers);
    }

    private void testStandAloneLookupJobRandom(
            TableEnvironment tEnv, int numProducers, boolean enableConflicts) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        testRandom(
                tEnv,
                numProducers,
                false,
                "'bucket' = '4',"
                        + String.format(
                                "'write-buffer-size' = '%s',",
                                random.nextBoolean() ? "512kb" : "1mb")
                        + "'changelog-producer' = 'lookup',"
                        + String.format(
                                "'changelog-producer.lookup-wait' = '%s',", random.nextBoolean())
                        + "'write-only' = 'true'");

        // sleep for a random amount of time to check
        // if dedicated compactor job can find first snapshot to compact correctly
        Thread.sleep(random.nextInt(2500));

        for (int i = enableConflicts ? 2 : 1; i > 0; i--) {
            StreamExecutionEnvironment env =
                    streamExecutionEnvironmentBuilder()
                            .streamingMode()
                            .checkpointIntervalMs(random.nextInt(1900) + 100)
                            .allowRestart()
                            .build();
            env.setParallelism(2);
            new CompactAction(path, "default", "T").withStreamExecutionEnvironment(env).build();
            env.executeAsync();
        }

        // sleep for a random amount of time to check
        // if we can first read complete records then read incremental records correctly
        Thread.sleep(random.nextInt(2500));

        checkChangelogTestResult(numProducers);
    }

    private void checkChangelogTestResult(int numProducers) throws Exception {
        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(100)
                        .parallelism(1)
                        .build();
        sEnv.executeSql(createCatalogSql("testCatalog", path));
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
        tEnv.executeSql(createCatalogSql("testCatalog", failingPath));
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
                                + (LIMIT + NUM_PARTS * NUM_KEYS)
                                + "',"
                                + "  'rows-per-second' = '"
                                + (LIMIT / 20 + ThreadLocalRandom.current().nextInt(LIMIT / 20))
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
                                            "INSERT INTO T /*+ OPTIONS('sink.parallelism' = '2') */ SELECT * FROM myView"
                                                    + idx));
            results.add(result);
        }

        return results;
    }

    private void checkBatchResult(int numProducers) throws Exception {
        TableEnvironment bEnv = tableEnvironmentBuilder().batchMode().build();
        bEnv.executeSql(createCatalogSql("testCatalog", path));
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
