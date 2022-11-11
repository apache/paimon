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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

/** Tests for changelog table with primary keys. */
public class ChangelogWithKeyFileStoreTableITCase extends TestBaseUtils {

    // ------------------------------------------------------------------------
    //  Test Utilities
    // ------------------------------------------------------------------------

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    (new MiniClusterResourceConfiguration.Builder())
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private String path;

    @Before
    public void before() throws IOException {
        path = TEMPORARY_FOLDER.newFolder().toPath().toString();
    }

    /**
     * Copied from {@link AbstractTestBase}. This test class does not directly extend from {@link
     * AbstractTestBase} because we need more task managers to run multiple jobs.
     */
    @After
    public final void cleanupRunningJobs() throws Exception {
        if (!MINI_CLUSTER_RESOURCE.getMiniCluster().isRunning()) {
            // do nothing if the MiniCluster is not running
            return;
        }

        for (JobStatusMessage path : MINI_CLUSTER_RESOURCE.getClusterClient().listJobs().get()) {
            if (!path.getJobState().isTerminalState()) {
                try {
                    MINI_CLUSTER_RESOURCE.getClusterClient().cancel(path.getJobId()).get();
                } catch (Exception ignored) {
                    // ignore exceptions when cancelling dangling jobs
                }
            }
        }
    }

    private TableEnvironment createBatchTableEnvironment() {
        return TableEnvironmentTestUtils.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
    }

    private TableEnvironment createStreamingTableEnvironment() {
        TableEnvironment sEnv =
                TableEnvironmentTestUtils.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        // set checkpoint interval to a random number to emulate different speed of commit
        sEnv.getConfig()
                .getConfiguration()
                .set(
                        CHECKPOINTING_INTERVAL,
                        Duration.ofMillis(ThreadLocalRandom.current().nextInt(900) + 100));
        return sEnv;
    }

    // ------------------------------------------------------------------------
    //  Constructed Tests
    // ------------------------------------------------------------------------

    @Test
    public void testFullCompactionTriggerInterval() throws Exception {
        TableEnvironment sEnv = createStreamingTableEnvironment();
        sEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        sEnv.executeSql(
                String.format(
                        "CREATE CATALOG testCatalog WITH ('type'='table-store', 'warehouse'='%s/warehouse')",
                        path));
        sEnv.executeSql("USE CATALOG testCatalog");
        sEnv.executeSql(
                "CREATE TABLE T ( k INT, v STRING, PRIMARY KEY (k) NOT ENFORCED ) "
                        + "WITH ( "
                        + "'bucket' = '2', "
                        + "'changelog-producer' = 'full-compaction', "
                        + "'changelog-producer.compaction-interval' = '1s' )");

        Path inputPath = new Path(path, "input");
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
        actual.sort(String::compareTo);

        List<String> expected =
                new ArrayList<>(Arrays.asList("+I[1, A]", "+I[2, B]", "+I[3, C]", "+I[4, D]"));
        expected.sort(String::compareTo);
        Assert.assertEquals(expected, actual);

        // write update data
        sEnv.executeSql(
                        "INSERT INTO `default_catalog`.`default_database`.`S` "
                                + "VALUES (1, 'A'), (1, 'B'), (1, 'C'), (1, 'D')")
                .await();

        // read update data
        actual.clear();
        for (int i = 0; i < 8; i++) {
            actual.add(it.next().toString());
        }
        actual.sort(String::compareTo);

        expected =
                new ArrayList<>(
                        Arrays.asList(
                                "-D[1, A]",
                                "-U[2, B]",
                                "+U[2, A]",
                                "-U[3, C]",
                                "+U[3, B]",
                                "-U[4, D]",
                                "+U[4, C]",
                                "+I[5, D]"));
        expected.sort(String::compareTo);
        Assert.assertEquals(expected, actual);
    }

    // ------------------------------------------------------------------------
    //  Random Tests
    // ------------------------------------------------------------------------

    @Test
    public void testNoChangelogProducerBatchRandom() throws Exception {
        TableEnvironment bEnv = createBatchTableEnvironment();
        testNoChangelogProducerRandom(bEnv, 1, false);
    }

    @Test
    public void testNoChangelogProducerStreamingRandom() throws Exception {
        TableEnvironment sEnv = createStreamingTableEnvironment();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        testNoChangelogProducerRandom(sEnv, random.nextInt(1, 3), random.nextBoolean());
    }

    @Test
    public void testFullCompactionChangelogProducerBatchRandom() throws Exception {
        TableEnvironment bEnv = createBatchTableEnvironment();
        testFullCompactionChangelogProducerRandom(bEnv, 1, false);
    }

    @Test
    public void testFullCompactionChangelogProducerStreamingRandom() throws Exception {
        TableEnvironment sEnv = createStreamingTableEnvironment();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        testFullCompactionChangelogProducerRandom(sEnv, random.nextInt(1, 3), random.nextBoolean());
    }

    private static final int NUM_PARTS = 4;
    private static final int NUM_KEYS = 64;
    private static final int NUM_VALUES = 1024;
    private static final int LIMIT = 10000;

    private void testNoChangelogProducerRandom(
            TableEnvironment tEnv, int numProducers, boolean enableFailure) throws Exception {
        List<TableResult> results = testRandom(tEnv, numProducers, enableFailure, "'bucket' = '4'");
        for (TableResult result : results) {
            result.await();
        }
        checkBatchResult(numProducers);
    }

    private void testFullCompactionChangelogProducerRandom(
            TableEnvironment tEnv, int numProducers, boolean enableFailure) throws Exception {
        testRandom(
                tEnv,
                numProducers,
                enableFailure,
                "'bucket' = '4',"
                        + "'changelog-producer' = 'full-compaction',"
                        + "'changelog-producer.compaction-interval' = '1s'");

        // sleep for a random amount of time to check
        // if we can first read complete records then read incremental records correctly
        Thread.sleep(ThreadLocalRandom.current().nextInt(5000));

        TableEnvironment sEnv = createStreamingTableEnvironment();
        sEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        sEnv.executeSql(
                String.format(
                        "CREATE CATALOG testCatalog WITH ('type'='table-store', 'warehouse'='%s')",
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
        assert LIMIT > NUM_VALUES;

        String failingName = UUID.randomUUID().toString();
        String failingPath = FailingAtomicRenameFileSystem.getFailingPath(failingName, path);

        // no failure when creating catalog and table
        FailingAtomicRenameFileSystem.reset(failingName, 0, 1);
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG testCatalog WITH ('type'='table-store', 'warehouse'='%s')",
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
                                + (LIMIT + NUM_PARTS * NUM_KEYS)
                                + "',"
                                + "  'rows-per-second' = '"
                                + (LIMIT / 5 + ThreadLocalRandom.current().nextInt(LIMIT / 10))
                                + "'"
                                + ")")
                .await();

        List<TableResult> results = new ArrayList<>();

        if (enableFailure) {
            FailingAtomicRenameFileSystem.reset(failingName, 100, 1000);
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
                    FailingAtomicRenameFileSystem.retryArtificialException(
                            () ->
                                    tEnv.executeSql(
                                            "INSERT INTO T /*+ OPTIONS('sink.parallelism' = '2') */ SELECT * FROM myView"
                                                    + idx));
            results.add(result);
        }

        return results;
    }

    private void checkBatchResult(int numProducers) throws Exception {
        TableEnvironment bEnv = createBatchTableEnvironment();
        bEnv.executeSql(
                String.format(
                        "CREATE CATALOG testCatalog WITH ('type'='table-store', 'warehouse'='%s')",
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
                    Assert.assertFalse(valueMap.containsKey(key));
                    Assert.assertTrue(
                            !kindMap.containsKey(key) || kindMap.get(key) == RowKind.DELETE);
                    valueMap.put(key, value);
                    break;
                case UPDATE_AFTER:
                    Assert.assertFalse(valueMap.containsKey(key));
                    Assert.assertEquals(RowKind.UPDATE_BEFORE, kindMap.get(key));
                    valueMap.put(key, value);
                    break;
                case UPDATE_BEFORE:
                case DELETE:
                    Assert.assertEquals(value, valueMap.get(key));
                    Assert.assertTrue(
                            kindMap.get(key) == RowKind.INSERT
                                    || kindMap.get(key) == RowKind.UPDATE_AFTER);
                    valueMap.remove(key);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown row kind " + row.getKind());
            }
            kindMap.put(key, row.getKind());
        }

        private void assertResult(int numProducers) {
            Assert.assertEquals(NUM_PARTS * NUM_KEYS * numProducers, valueMap.size());
            for (int i = 0; i < NUM_PARTS; i++) {
                for (int j = 0; j < NUM_KEYS * numProducers; j++) {
                    String key = i + "|" + j;
                    int x = LIMIT + i * NUM_KEYS + j % NUM_KEYS;
                    String expectedValue = x + "|" + x + ".str";
                    Assert.assertEquals(expectedValue, valueMap.get(key));
                }
            }
        }
    }
}
