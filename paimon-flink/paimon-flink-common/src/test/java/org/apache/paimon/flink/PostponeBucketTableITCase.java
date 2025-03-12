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

import org.apache.paimon.flink.util.AbstractTestBase;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for postpone bucket tables. */
public class PostponeBucketTableITCase extends AbstractTestBase {

    private static final int TIMEOUT = 120;

    @Test
    public void testWriteThenCompact() throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment tEnv =
                tableEnvironmentBuilder()
                        .batchMode()
                        .setConf(TableConfigOptions.TABLE_DML_SYNC, true)
                        .build();

        tEnv.executeSql(
                "CREATE CATALOG mycat WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql(
                "CREATE TABLE T (\n"
                        + "  pt INT,\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (pt, k) NOT ENFORCED\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'bucket' = '-2'\n"
                        + ")");

        int numPartitions = 3;
        int numKeys = 100;
        List<String> values = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numKeys; j++) {
                values.add(String.format("(%d, %d, %d)", i, j, i * numKeys + j));
            }
        }
        tEnv.executeSql("INSERT INTO T VALUES " + String.join(", ", values)).await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T"))).isEmpty();

        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            expected.add(
                    String.format(
                            "+I[%d, %d]",
                            i, (i * numKeys + i * numKeys + numKeys - 1) * numKeys / 2));
        }
        String query = "SELECT pt, SUM(v) FROM T GROUP BY pt";
        assertThat(collect(tEnv.executeSql(query))).hasSameElementsAs(expected);

        values.clear();
        int changedPartition = 1;
        for (int j = 0; j < numKeys; j++) {
            values.add(
                    String.format(
                            "(%d, %d, %d)",
                            changedPartition, j, -(changedPartition * numKeys + j)));
        }
        tEnv.executeSql("INSERT INTO T VALUES " + String.join(", ", values)).await();
        assertThat(collect(tEnv.executeSql(query))).hasSameElementsAs(expected);

        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();

        expected.clear();
        for (int i = 0; i < numPartitions; i++) {
            int val = (i * numKeys + i * numKeys + numKeys - 1) * numKeys / 2;
            if (i == changedPartition) {
                val *= -1;
            }
            expected.add(String.format("+I[%d, %d]", i, val));
        }
        assertThat(collect(tEnv.executeSql(query))).hasSameElementsAs(expected);
    }

    @Test
    public void testOverwrite() throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment tEnv =
                tableEnvironmentBuilder()
                        .batchMode()
                        .setConf(TableConfigOptions.TABLE_DML_SYNC, true)
                        .build();

        tEnv.executeSql(
                "CREATE CATALOG mycat WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql(
                "CREATE TABLE T (\n"
                        + "  pt INT,\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (pt, k) NOT ENFORCED\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'bucket' = '-2'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO T VALUES (1, 10, 110), (1, 20, 120), (2, 10, 210), (2, 20, 220)")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T"))).isEmpty();
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        assertThat(collect(tEnv.executeSql("SELECT k, v, pt FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[10, 110, 1]", "+I[20, 120, 1]", "+I[10, 210, 2]", "+I[20, 220, 2]");

        // no compact, so the result is the same
        tEnv.executeSql("INSERT INTO T VALUES (2, 40, 240)").await();
        assertThat(collect(tEnv.executeSql("SELECT k, v, pt FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[10, 110, 1]", "+I[20, 120, 1]", "+I[10, 210, 2]", "+I[20, 220, 2]");

        tEnv.executeSql("INSERT OVERWRITE T VALUES (2, 20, 221), (2, 30, 230)").await();
        assertThat(collect(tEnv.executeSql("SELECT k, v, pt FROM T")))
                .containsExactlyInAnyOrder("+I[10, 110, 1]", "+I[20, 120, 1]");
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        // overwrite should also clean up files in bucket = -2 directory,
        // which the record with key = 40
        assertThat(collect(tEnv.executeSql("SELECT k, v, pt FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[10, 110, 1]", "+I[20, 120, 1]", "+I[20, 221, 2]", "+I[30, 230, 2]");
    }

    @Timeout(TIMEOUT)
    @Test
    public void testLookupChangelogProducer() throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment bEnv =
                tableEnvironmentBuilder()
                        .batchMode()
                        .setConf(TableConfigOptions.TABLE_DML_SYNC, true)
                        .build();

        String createCatalogSql =
                "CREATE CATALOG mycat WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")";
        bEnv.executeSql(createCatalogSql);
        bEnv.executeSql("USE CATALOG mycat");
        bEnv.executeSql(
                "CREATE TABLE T (\n"
                        + "  pt INT,\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (pt, k) NOT ENFORCED\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'bucket' = '-2',\n"
                        + "  'changelog-producer' = 'lookup'\n"
                        + ")");

        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(1)
                        .checkpointIntervalMs(1000)
                        .build();
        sEnv.executeSql(createCatalogSql);
        sEnv.executeSql("USE CATALOG mycat");
        TableResult streamingSelect = sEnv.executeSql("SELECT k, v, pt FROM T");
        JobClient client = streamingSelect.getJobClient().get();
        CloseableIterator<Row> it = streamingSelect.collect();

        bEnv.executeSql(
                        "INSERT INTO T VALUES (1, 10, 110), (1, 20, 120), (2, 10, 210), (2, 20, 220)")
                .await();
        assertThat(collect(bEnv.executeSql("SELECT * FROM T"))).isEmpty();
        bEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        assertThat(collect(bEnv.executeSql("SELECT k, v, pt FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[10, 110, 1]", "+I[20, 120, 1]", "+I[10, 210, 2]", "+I[20, 220, 2]");
        assertThat(collect(client, it, 4))
                .containsExactlyInAnyOrder(
                        "+I[10, 110, 1]", "+I[20, 120, 1]", "+I[10, 210, 2]", "+I[20, 220, 2]");

        bEnv.executeSql("INSERT INTO T VALUES (1, 20, 121), (2, 30, 230)").await();
        bEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        assertThat(collect(bEnv.executeSql("SELECT k, v, pt FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[10, 110, 1]",
                        "+I[20, 121, 1]",
                        "+I[10, 210, 2]",
                        "+I[20, 220, 2]",
                        "+I[30, 230, 2]");
        assertThat(collect(client, it, 3))
                .containsExactlyInAnyOrder("-U[20, 120, 1]", "+U[20, 121, 1]", "+I[30, 230, 2]");

        it.close();
    }

    @Test
    public void testRescaleBucket() throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment tEnv =
                tableEnvironmentBuilder()
                        .batchMode()
                        .setConf(TableConfigOptions.TABLE_DML_SYNC, true)
                        .build();

        tEnv.executeSql(
                "CREATE CATALOG mycat WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql(
                "CREATE TABLE T (\n"
                        + "  pt INT,\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (pt, k) NOT ENFORCED\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'bucket' = '-2',\n"
                        + "  'postpone.default-bucket-num' = '2'\n"
                        + ")");

        int numPartitions = 3;
        int numKeys = 100;
        List<String> values = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numKeys; j++) {
                values.add(String.format("(%d, %d, %d)", i, j, i * numKeys + j));
            }
        }
        tEnv.executeSql("INSERT INTO T VALUES " + String.join(", ", values)).await();
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();

        List<String> expectedBuckets = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            expectedBuckets.add(String.format("+I[{%d}, 2]", i));
        }
        String bucketSql =
                "SELECT `partition`, COUNT(DISTINCT bucket) FROM `T$files` GROUP BY `partition`";
        assertThat(collect(tEnv.executeSql(bucketSql))).hasSameElementsAs(expectedBuckets);

        List<String> expectedData = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            expectedData.add(
                    String.format(
                            "+I[%d, %d]",
                            i, (i * numKeys + i * numKeys + numKeys - 1) * numKeys / 2));
        }
        String query = "SELECT pt, SUM(v) FROM T GROUP BY pt";
        assertThat(collect(tEnv.executeSql(query))).hasSameElementsAs(expectedData);

        // before rescaling, write some files in bucket = -2 directory,
        // these files should not be touched by rescaling
        values.clear();
        int changedPartition = 1;
        for (int j = 0; j < numKeys; j++) {
            values.add(
                    String.format(
                            "(%d, %d, %d)",
                            changedPartition, j, -(changedPartition * numKeys + j)));
        }
        tEnv.executeSql("INSERT INTO T VALUES " + String.join(", ", values)).await();

        tEnv.executeSql(
                "CALL sys.rescale(`table` => 'default.T', `bucket_num` => 4, `partition` => 'pt="
                        + changedPartition
                        + "')");
        expectedBuckets.clear();
        for (int i = 0; i < numPartitions; i++) {
            expectedBuckets.add(String.format("+I[{%d}, %d]", i, i == changedPartition ? 4 : 2));
        }
        assertThat(collect(tEnv.executeSql(bucketSql))).hasSameElementsAs(expectedBuckets);
        assertThat(collect(tEnv.executeSql(query))).hasSameElementsAs(expectedData);

        // rescaling bucket should not touch the files in bucket = -2 directory
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        assertThat(collect(tEnv.executeSql(bucketSql))).hasSameElementsAs(expectedBuckets);

        expectedData.clear();
        for (int i = 0; i < numPartitions; i++) {
            int val = (i * numKeys + i * numKeys + numKeys - 1) * numKeys / 2;
            if (i == changedPartition) {
                val *= -1;
            }
            expectedData.add(String.format("+I[%d, %d]", i, val));
        }
        assertThat(collect(tEnv.executeSql(query))).hasSameElementsAs(expectedData);
    }

    @Timeout(TIMEOUT)
    @Test
    public void testInputChangelogProducer() throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(1)
                        .checkpointIntervalMs(500)
                        .build();
        String createCatalog =
                "CREATE CATALOG mycat WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")";
        sEnv.executeSql(createCatalog);
        sEnv.executeSql("USE CATALOG mycat");
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE S (\n"
                        + "  i INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'fields.i.kind' = 'sequence',\n"
                        + "  'fields.i.start' = '0',\n"
                        + "  'fields.i.end' = '199',\n"
                        + "  'number-of-rows' = '200',\n"
                        + "  'rows-per-second' = '50'\n"
                        + ")");
        sEnv.executeSql(
                "CREATE TABLE T (\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'bucket' = '-2',\n"
                        + "  'changelog-producer' = 'input',\n"
                        + "  'continuous.discovery-interval' = '1ms'\n"
                        + ")");
        sEnv.executeSql(
                "CREATE TEMPORARY VIEW V AS SELECT MOD(i, 2) AS x, IF(MOD(i, 2) = 0, 1, 1000) AS y FROM S");
        sEnv.executeSql("INSERT INTO T SELECT SUM(y), x FROM V GROUP BY x").await();

        TableEnvironment bEnv =
                tableEnvironmentBuilder()
                        .batchMode()
                        .parallelism(2)
                        .setConf(TableConfigOptions.TABLE_DML_SYNC, true)
                        .build();
        bEnv.executeSql(createCatalog);
        bEnv.executeSql("USE CATALOG mycat");
        bEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();

        // if the read order when compacting is wrong, this check will fail
        assertThat(collect(bEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder("+U[100, 0]", "+U[100000, 1]");
        TableResult streamingSelect =
                sEnv.executeSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id' = '1') */");
        JobClient client = streamingSelect.getJobClient().get();
        CloseableIterator<Row> it = streamingSelect.collect();
        // if the number of changelog is not sufficient, this call will fail
        collect(client, it, 400 - 2);
    }

    private List<String> collect(TableResult result) throws Exception {
        List<String> ret = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                ret.add(it.next().toString());
            }
        }
        return ret;
    }

    private List<String> collect(JobClient client, CloseableIterator<Row> it, int limit)
            throws Exception {
        AtomicBoolean shouldStop = new AtomicBoolean(false);
        Thread timerThread =
                new Thread(
                        () -> {
                            try {
                                for (int i = 0; i < TIMEOUT; i++) {
                                    Thread.sleep(1000);
                                    if (shouldStop.get()) {
                                        return;
                                    }
                                }
                                client.cancel().get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        timerThread.start();

        List<String> ret = new ArrayList<>();
        for (int i = 0; i < limit && it.hasNext(); i++) {
            ret.add(it.next().toString());
        }

        shouldStop.set(true);
        timerThread.join();
        return ret;
    }
}
