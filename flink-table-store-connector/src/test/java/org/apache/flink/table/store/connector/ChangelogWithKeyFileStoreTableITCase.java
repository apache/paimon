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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

/** Tests for changelog table with primary keys. */
public class ChangelogWithKeyFileStoreTableITCase extends AbstractTestBase {

    private String path;
    private String failingName;

    @Before
    public void before() throws IOException {
        path = TEMPORARY_FOLDER.newFolder().toPath().toString();
        // for failure tests
        failingName = UUID.randomUUID().toString();
    }

    @Test
    public void testBatchRandom() throws Exception {
        TableEnvironment bEnv =
                TableEnvironmentTestUtils.create(
                        EnvironmentSettings.newInstance().inBatchMode().build());
        testRandom(bEnv, false);
    }

    @Test
    public void testStreamingRandom() throws Exception {
        TableEnvironment sEnv =
                TableEnvironmentTestUtils.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        // set checkpoint interval to a random number to emulate different speed of commit
        sEnv.getConfig()
                .getConfiguration()
                .set(
                        CHECKPOINTING_INTERVAL,
                        Duration.ofMillis(ThreadLocalRandom.current().nextInt(900) + 100));
        testRandom(sEnv, ThreadLocalRandom.current().nextBoolean());
    }

    /**
     * Randomly update {@code numParts} partitions and {@code numKeys} keys for about {@code limit}
     * records. For the final {@code numParts * numKeys} records, keys are updated to some specific
     * values for result checking.
     */
    private void testRandom(TableEnvironment tEnv, boolean enableFailure) throws Exception {
        int numParts = 4;
        int numKeys = 64;
        int numValues = 1024;
        int limit = 10000;

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
                        + "  'bucket' = '4'"
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
                                + (limit + numParts * numKeys - 1)
                                + "',"
                                + "  'number-of-rows' = '"
                                + (limit + numParts * numKeys)
                                + "',"
                                + "  'rows-per-second' = '"
                                + (limit / 5 + ThreadLocalRandom.current().nextInt(limit / 10))
                                + "'"
                                + ")")
                .await();

        // for the last `numParts * numKeys` records, we update every key to a specific value
        String ptSql =
                String.format(
                        "IF(i >= %d, CAST((i - %d) / %d AS STRING), CAST(CAST(FLOOR(RAND() * %d) AS INT) AS STRING)) AS pt",
                        limit, limit, numKeys, numParts);
        String kSql =
                String.format(
                        "IF(i >= %d, MOD(i - %d, %d), CAST(FLOOR(RAND() * %d) AS INT)) AS k",
                        limit, limit, numKeys, numKeys);
        String v1Sql =
                String.format(
                        "IF(i >= %d, i, CAST(FLOOR(RAND() * %d) AS BIGINT)) AS v1",
                        limit, numValues);
        String v2Sql = "CAST(i AS STRING) || '.str' AS v2";
        tEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY VIEW myView AS SELECT %s, %s, %s, %s FROM `default_catalog`.`default_database`.`S`",
                        ptSql, kSql, v1Sql, v2Sql));

        // run test SQL
        if (enableFailure) {
            FailingAtomicRenameFileSystem.reset(failingName, 100, 1000);
        }
        FailingAtomicRenameFileSystem.retryArtificialException(
                () ->
                        tEnv.executeSql(
                                        "INSERT INTO T /*+ OPTIONS('sink.parallelism' = '2') */ SELECT * FROM myView")
                                .await());

        // check for result
        TableEnvironment bEnv =
                TableEnvironmentTestUtils.create(
                        EnvironmentSettings.newInstance().inBatchMode().build());
        bEnv.executeSql(
                String.format(
                        "CREATE CATALOG testCatalog WITH ('type'='table-store', 'warehouse'='%s')",
                        path));
        bEnv.executeSql("USE CATALOG testCatalog");
        Map<String, String> actual = new HashMap<>();
        try (CloseableIterator<Row> it = bEnv.executeSql("SELECT * FROM T").collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                String key = row.getField(0) + "|" + row.getField(1);
                String value = row.getField(2) + "|" + row.getField(3);
                actual.put(key, value);
            }
        }

        Assert.assertEquals(numParts * numKeys, actual.size());
        for (int i = 0; i < numParts; i++) {
            for (int j = 0; j < numKeys; j++) {
                String key = i + "|" + j;
                int x = limit + i * numKeys + j;
                String expectedValue = x + "|" + x + ".str";
                Assert.assertEquals(expectedValue, actual.get(key));
            }
        }
    }
}
