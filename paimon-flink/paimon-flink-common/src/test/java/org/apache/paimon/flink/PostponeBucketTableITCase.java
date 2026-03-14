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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for postpone bucket tables. */
public class PostponeBucketTableITCase extends AbstractTestBase {

    private static final int TIMEOUT = 120;

    @Test
    public void testRetractOnPartialUpdate() {
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
                        + "  k INT,\n"
                        + "  v1 INT,\n"
                        + "  v2 INT,\n"
                        + "  row_kind_col STRING,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'bucket' = '-2',\n"
                        + "  'merge-engine' = 'partial-update',\n"
                        + "  'rowkind.field' = 'row_kind_col'\n"
                        + ")");

        boolean writeFixedBucket = ThreadLocalRandom.current().nextBoolean();
        tEnv.executeSql(
                String.format(
                        "ALTER TABLE T SET ('postpone.batch-write-fixed-bucket' = '%s')",
                        writeFixedBucket));
        if (writeFixedBucket) {
            assertThatCode(() -> tEnv.executeSql("INSERT INTO T VALUES (1, 1, 1, '-U')").await())
                    .doesNotThrowAnyException();
        } else {
            assertThatThrownBy(
                            () -> tEnv.executeSql("INSERT INTO T VALUES (1, 1, 1, '-D')").await())
                    .rootCause()
                    .hasMessageContaining(
                            "By default, Partial update can not accept delete records");
        }
    }

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
                        + "  'bucket' = '-2',\n"
                        + "  'postpone.batch-write-fixed-bucket' = 'false'\n"
                        + ")");

        int numPartitions = 3;
        int numKeys = 100;
        List<String> values = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numKeys; j++) {
                values.add(String.format("(%d, %d, %d)", i, j, i * numKeys + j));
            }
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        if (random.nextBoolean()) {
            tEnv.executeSql("INSERT INTO T VALUES " + String.join(", ", values)).await();
        } else {
            tEnv.executeSql(
                            "INSERT INTO T /*+ OPTIONS('partition.sink-strategy'='hash') */ VALUES "
                                    + String.join(", ", values))
                    .await();
        }
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
    public void testOverwriteWithoutBatchWriteFixedBucket() throws Exception {
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
                        + "  'postpone.batch-write-fixed-bucket' = 'false'\n"
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

    @Test
    public void testOverwriteWithBatchWriteFixedBucket() throws Exception {
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

        // write postpone bucket with partition 1 and 2
        tEnv.executeSql(
                        "INSERT INTO T /*+ OPTIONS ('postpone.batch-write-fixed-bucket' = 'false') */ "
                                + "VALUES (1, 10, 110), (1, 20, 120), (2, 10, 210), (2, 20, 220)")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT k, v, pt FROM T"))).isEmpty();

        // batch overite partition 2 and the new data can be read
        tEnv.executeSql("INSERT OVERWRITE T VALUES (2, 20, 221), (2, 30, 230)").await();
        assertThat(collect(tEnv.executeSql("SELECT k, v, pt FROM T")))
                .containsExactlyInAnyOrder("+I[20, 221, 2]", "+I[30, 230, 2]");

        // compact then partition 1 can be read
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
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
                        + "  'changelog-producer' = 'lookup',\n"
                        + "  'postpone.batch-write-fixed-bucket' = 'false'\n"
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
                        + "  'postpone.default-bucket-num' = '2',\n"
                        + "  'postpone.batch-write-fixed-bucket' = 'false'\n"
                        + ")");

        int numKeys = 100;
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < numKeys; j++) {
                values.add(String.format("(%d, %d, %d)", i, j, i * numKeys + j));
            }
        }
        tEnv.executeSql("INSERT INTO T VALUES " + String.join(", ", values)).await();
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();

        List<String> expectedBuckets = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            expectedBuckets.add(String.format("+I[{%d}, 2]", i));
        }
        String bucketSql =
                "SELECT `partition`, COUNT(DISTINCT bucket) FROM `T$files` GROUP BY `partition`";
        assertThat(collect(tEnv.executeSql(bucketSql))).hasSameElementsAs(expectedBuckets);

        List<String> expectedData = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
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
        for (int j = 0; j < numKeys; j++) {
            values.add(String.format("(1, %d, 0)", j));
            values.add(String.format("(2, %d, 1)", j));
        }
        tEnv.executeSql("INSERT INTO T VALUES " + String.join(", ", values)).await();

        tEnv.executeSql(
                "CALL sys.rescale(`table` => 'default.T', `bucket_num` => 4, `partition` => 'pt=1')");
        tEnv.executeSql(
                "CALL sys.rescale(`table` => 'default.T', `bucket_num` => 8, `partition` => 'pt=2')");
        expectedBuckets.clear();
        expectedBuckets.add("+I[{0}, 2]");
        expectedBuckets.add("+I[{1}, 4]");
        expectedBuckets.add("+I[{2}, 8]");
        assertThat(collect(tEnv.executeSql(bucketSql))).hasSameElementsAs(expectedBuckets);
        assertThat(collect(tEnv.executeSql(query))).hasSameElementsAs(expectedData);

        // rescaling bucket should not touch the files in bucket = -2 directory
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        assertThat(collect(tEnv.executeSql(bucketSql))).hasSameElementsAs(expectedBuckets);

        expectedData.clear();
        expectedData.add(String.format("+I[0, %d]", (numKeys - 1) * numKeys / 2));
        expectedData.add("+I[1, 0]");
        expectedData.add(String.format("+I[2, %d]", numKeys));
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

    @Test
    public void testPostponeWriteNotExpireSnapshots() throws Exception {
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
                        + "  'snapshot.num-retained.min' = '3',\n"
                        + "  'snapshot.num-retained.max' = '3'\n"
                        + ")");

        tEnv.executeSql(
                String.format(
                        "ALTER TABLE T SET ('postpone.batch-write-fixed-bucket' = '%s')",
                        ThreadLocalRandom.current().nextBoolean()));

        for (int i = 0; i < 5; i++) {
            tEnv.executeSql(String.format("INSERT INTO T VALUES (%d, 0, 0)", i)).await();
        }

        assertThat(collect(tEnv.executeSql("SELECT COUNT(*) FROM `T$snapshots`")))
                .containsExactlyInAnyOrder("+I[5]");
    }

    @Timeout(TIMEOUT)
    @Test
    public void testLookupPostponeBucketTable() throws Exception {
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
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'bucket' = '-2',\n"
                        + "  'postpone.batch-write-fixed-bucket' = 'false'\n"
                        + ")");
        bEnv.executeSql("CREATE TABLE SRC (i INT, `proctime` AS PROCTIME())");

        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(1)
                        .checkpointIntervalMs(200)
                        .build();
        sEnv.executeSql(createCatalogSql);
        sEnv.executeSql("USE CATALOG mycat");
        TableResult streamingSelect =
                sEnv.executeSql(
                        "SELECT i, v FROM SRC LEFT JOIN T "
                                + "FOR SYSTEM_TIME AS OF SRC.proctime AS D ON SRC.i = D.k");

        JobClient client = streamingSelect.getJobClient().get();
        CloseableIterator<Row> it = streamingSelect.collect();

        bEnv.executeSql("INSERT INTO T VALUES (1, 10), (2, 20), (3, 30)").await();
        bEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();

        // lookup join
        bEnv.executeSql("INSERT INTO SRC VALUES (1), (2), (3)").await();
        assertThat(collect(client, it, 3))
                .containsExactlyInAnyOrder("+I[1, 10]", "+I[2, 20]", "+I[3, 30]");

        // rescale and re-join
        bEnv.executeSql("CALL sys.rescale(`table` => 'default.T', `bucket_num` => 5)").await();
        bEnv.executeSql("INSERT INTO SRC VALUES (1), (2), (3)").await();
        assertThat(collect(client, it, 3))
                .containsExactlyInAnyOrder("+I[1, 10]", "+I[2, 20]", "+I[3, 30]");

        it.close();
    }

    @Timeout(TIMEOUT)
    @Test
    public void testLookupPostponeBucketPartitionedTable() throws Exception {
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
                        + "  k INT,\n"
                        + "  pt INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k, pt) NOT ENFORCED\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'bucket' = '-2',\n"
                        + "  'postpone.batch-write-fixed-bucket' = 'false'\n"
                        + ")");
        bEnv.executeSql("CREATE TABLE SRC (i INT, pt INT, `proctime` AS PROCTIME())");

        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(1)
                        .checkpointIntervalMs(200)
                        .build();
        sEnv.executeSql(createCatalogSql);
        sEnv.executeSql("USE CATALOG mycat");
        TableResult streamingSelect =
                sEnv.executeSql(
                        "SELECT i, D.pt, v FROM SRC LEFT JOIN T "
                                + "FOR SYSTEM_TIME AS OF SRC.proctime AS D ON SRC.i = D.k AND SRC.pt = D.pt");

        JobClient client = streamingSelect.getJobClient().get();
        CloseableIterator<Row> it = streamingSelect.collect();

        bEnv.executeSql("INSERT INTO T VALUES (1, 1, 10), (2, 2, 20), (3, 2, 30)").await();
        bEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();

        // rescale for partitions to different num buckets and lookup join
        bEnv.executeSql(
                        "CALL sys.rescale(`table` => 'default.T', `bucket_num` => 5, `partition` => 'pt=1')")
                .await();
        bEnv.executeSql(
                        "CALL sys.rescale(`table` => 'default.T', `bucket_num` => 8, `partition` => 'pt=2')")
                .await();
        bEnv.executeSql("INSERT INTO SRC VALUES (1, 1), (2, 2), (3, 2)").await();
        assertThat(collect(client, it, 3))
                .containsExactlyInAnyOrder("+I[1, 1, 10]", "+I[2, 2, 20]", "+I[3, 2, 30]");

        it.close();
    }

    @Test
    public void testDeletionVector() throws Exception {
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
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'bucket' = '-2',\n"
                        + "  'postpone.batch-write-fixed-bucket' = 'false',\n"
                        + "  'deletion-vectors.enabled' = 'true'\n"
                        + ")");

        tEnv.executeSql("INSERT INTO T VALUES (1, 10), (2, 20), (3, 30), (4, 40)").await();
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder("+I[1, 10]", "+I[2, 20]", "+I[3, 30]", "+I[4, 40]");

        tEnv.executeSql("INSERT INTO T VALUES (1, 11), (5, 51)").await();
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[1, 11]", "+I[2, 20]", "+I[3, 30]", "+I[4, 40]", "+I[5, 51]");

        tEnv.executeSql("INSERT INTO T VALUES (2, 52), (3, 32)").await();
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[1, 11]", "+I[2, 52]", "+I[3, 32]", "+I[4, 40]", "+I[5, 51]");
    }

    @Test
    public void testSameKeyPreserveOrder() throws Exception {
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
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'bucket' = '-2',\n"
                        + "  'postpone.batch-write-fixed-bucket' = 'false'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO T /*+ OPTIONS('sink.parallelism' = '2') */ VALUES (1, 10), (1, 20), (1, 30), (1, 40)")
                .await();
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder("+I[1, 40]");
    }

    @Test
    public void testAvroUnsupportedTypes() throws Exception {
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
                        + "  k INT,\n"
                        + "  v TIMESTAMP(9),\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'bucket' = '-2',\n"
                        + "  'postpone.batch-write-fixed-bucket' = 'false'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO T VALUES (1, TIMESTAMP '2025-06-11 16:35:45.123456789'), (2, CAST(NULL AS TIMESTAMP(9)))")
                .await();
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder("+I[1, 2025-06-11T16:35:45.123456789]", "+I[2, null]");
    }

    @Timeout(TIMEOUT)
    @Test
    public void testNoneChangelogProducer() throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment sEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(2)
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
                "CREATE TABLE T (\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'bucket' = '-2',\n"
                        + "  'postpone.batch-write-fixed-bucket' = 'false',\n"
                        + "  'changelog-producer' = 'none',\n"
                        + "  'scan.remove-normalize' = 'true',\n"
                        + "  'continuous.discovery-interval' = '1ms'\n"
                        + ")");

        TableEnvironment bEnv =
                tableEnvironmentBuilder()
                        .batchMode()
                        .parallelism(1)
                        .setConf(TableConfigOptions.TABLE_DML_SYNC, true)
                        .build();
        bEnv.executeSql(createCatalog);
        bEnv.executeSql("USE CATALOG mycat");
        bEnv.executeSql("INSERT INTO T VALUES (1, 10), (2, 20), (1, 100)").await();
        bEnv.executeSql("INSERT INTO T VALUES (1, 101), (3, 31)").await();
        bEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();

        TableResult streamingSelect = sEnv.executeSql("SELECT * FROM T");
        Thread.sleep(1000);

        bEnv.executeSql("INSERT INTO T VALUES (1, 102), (4, 42)").await();
        bEnv.executeSql("INSERT INTO T VALUES (1, 103), (5, 53)").await();
        bEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();

        assertThat(collect(bEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[1, 103]", "+I[2, 20]", "+I[3, 31]", "+I[4, 42]", "+I[5, 53]");
        JobClient client = streamingSelect.getJobClient().get();
        CloseableIterator<Row> it = streamingSelect.collect();
        assertThat(collect(client, it, 7))
                .containsExactlyInAnyOrder(
                        "+I[1, 101]",
                        "+I[2, 20]",
                        "+I[3, 31]",
                        "+I[1, 102]",
                        "+I[4, 42]",
                        "+I[1, 103]",
                        "+I[5, 53]");
    }

    @Test
    public void testWriteFixedBucketWithDifferentBucketNumber() throws Exception {
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
                        + "  k INT,\n"
                        + "  v STRING,\n"
                        + "  pt STRING,"
                        + "  PRIMARY KEY (k, pt) NOT ENFORCED\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'bucket' = '-2',\n"
                        // upgrade will affect rescale job and the validation
                        + "  'overwrite-upgrade' = 'false'\n"
                        + ")");

        // use sink.parallelism
        List<String> values1 = new ArrayList<>();
        List<String> expected1 = new ArrayList<>();
        for (int i = 1; i <= 8; i++) {
            values1.add(String.format("(%d, '%c', 'pt1')", i, (i - 1 + 'a')));
            values1.add(String.format("(%d, '%c', 'pt2')", i, (i - 1 + 'a')));
            expected1.add(String.format("+I[%d, %c, pt1]", i, (i - 1 + 'a')));
            expected1.add(String.format("+I[%d, %c, pt2]", i, (i - 1 + 'a')));
        }
        tEnv.executeSql(
                        "INSERT INTO T /*+ OPTIONS('sink.parallelism' = '4') */ VALUES "
                                + String.join(",", values1))
                .await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrderElementsOf(expected1);
        assertThat(
                        collectRow(tEnv.executeSql("SELECT * FROM `T$buckets`")).stream()
                                .map(row -> (Integer) row.getField(1))
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(0, 1, 2, 3);
        assertThat(collect(tEnv.executeSql("SELECT * FROM `T$files` WHERE level > 0"))).isEmpty();

        // test bucket number strategy: pt1 = 3, pt2 = 4, pt3 = 5 (runtime)
        tEnv.executeSql(
                "CALL sys.rescale(`table` => 'default.T', `bucket_num` => 3, `partition` => 'pt=pt1')");

        List<String> values2 = new ArrayList<>();
        List<String> expected2 = new ArrayList<>();
        for (int i = 1; i <= 8; i++) {
            values2.add(String.format("(%d, '%c', 'pt1')", i, (i - 1 + 'A')));
            values2.add(String.format("(%d, '%c', 'pt2')", i, (i - 1 + 'A')));
            values2.add(String.format("(%d, '%c', 'pt3')", i, (i - 1 + 'A')));
            expected2.add(String.format("+I[%d, %c, pt1]", i, (i - 1 + 'A')));
            expected2.add(String.format("+I[%d, %c, pt2]", i, (i - 1 + 'A')));
            expected2.add(String.format("+I[%d, %c, pt3]", i, (i - 1 + 'A')));
        }
        for (int i = 9; i <= 16; i++) {
            values2.add(String.format("(%d, '%d', 'pt1')", i, i));
            values2.add(String.format("(%d, '%d', 'pt2')", i, i));
            values2.add(String.format("(%d, '%d', 'pt3')", i, i));
            expected2.add(String.format("+I[%d, %d, pt1]", i, i));
            expected2.add(String.format("+I[%d, %d, pt2]", i, i));
            expected2.add(String.format("+I[%d, %d, pt3]", i, i));
        }

        tEnv.executeSql(
                        "INSERT INTO T /*+ OPTIONS('sink.parallelism' = '5') */ VALUES "
                                + String.join(",", values2))
                .await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrderElementsOf(expected2);
        Map<String, Set<Integer>> partitionBuckets = new HashMap<>();
        for (Row row : collectRow(tEnv.executeSql("SELECT * FROM `T$buckets`"))) {
            partitionBuckets
                    .computeIfAbsent((String) row.getField(0), p -> new HashSet<>())
                    .add((Integer) row.getField(1));
        }
        assertThat(partitionBuckets).hasSize(3);
        assertThat(partitionBuckets.get("{pt1}")).containsExactly(0, 1, 2);
        assertThat(partitionBuckets.get("{pt2}")).containsExactly(0, 1, 2, 3);
        assertThat(partitionBuckets.get("{pt3}")).containsExactly(0, 1, 2, 3, 4);
        assertThat(collect(tEnv.executeSql("SELECT * FROM `T$files` WHERE level > 0"))).isEmpty();
    }

    @Test
    public void testWriteFixedBucketThenWritePostponeBucket() throws Exception {
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
                        + "  k INT,\n"
                        + "  v STRING,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'bucket' = '-2'\n"
                        + ")");

        // use sink.parallelism
        tEnv.executeSql(
                        "INSERT INTO T /*+ OPTIONS('sink.parallelism' = '4') */ "
                                + "VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h');")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[1, a]",
                        "+I[2, b]",
                        "+I[3, c]",
                        "+I[4, d]",
                        "+I[5, e]",
                        "+I[6, f]",
                        "+I[7, g]",
                        "+I[8, h]");
        assertThat(
                        collectRow(tEnv.executeSql("SELECT * FROM `T$buckets`")).stream()
                                .map(row -> (Integer) row.getField(1))
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(0, 1, 2, 3);
        assertThat(collect(tEnv.executeSql("SELECT * FROM `T$files` WHERE level > 0"))).isEmpty();

        // write to postpone bucket, new record cannot be read before compact
        tEnv.executeSql(
                        "INSERT INTO T /*+ OPTIONS('postpone.batch-write-fixed-bucket' = 'false') */ "
                                + "VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E'), (6, 'F'), (7, 'G'), (8, 'H'), "
                                + "(9, '9'), (10, '10'), (11, '11'), (12, '12'), (13, '13'), (14, '14'), (15, '15'), (16, '16');")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[1, a]",
                        "+I[2, b]",
                        "+I[3, c]",
                        "+I[4, d]",
                        "+I[5, e]",
                        "+I[6, f]",
                        "+I[7, g]",
                        "+I[8, h]");
        assertThat(
                        collectRow(tEnv.executeSql("SELECT * FROM `T$buckets`")).stream()
                                .map(row -> (Integer) row.getField(1))
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(-2, 0, 1, 2, 3);
        assertThat(collect(tEnv.executeSql("SELECT * FROM `T$files` WHERE level > 0"))).isEmpty();

        // compact and check result again
        boolean forceUpLevel0 = ThreadLocalRandom.current().nextBoolean();
        if (forceUpLevel0) {
            tEnv.executeSql("ALTER TABLE T set ('compaction.force-up-level-0' = 'true')").await();
        }
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();

        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[1, A]",
                        "+I[2, B]",
                        "+I[3, C]",
                        "+I[4, D]",
                        "+I[5, E]",
                        "+I[6, F]",
                        "+I[7, G]",
                        "+I[8, H]",
                        "+I[9, 9]",
                        "+I[10, 10]",
                        "+I[11, 11]",
                        "+I[12, 12]",
                        "+I[13, 13]",
                        "+I[14, 14]",
                        "+I[15, 15]",
                        "+I[16, 16]");
        assertThat(
                        collectRow(tEnv.executeSql("SELECT * FROM `T$buckets`")).stream()
                                .map(row -> (Integer) row.getField(1))
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(0, 1, 2, 3);
        if (forceUpLevel0) {
            assertThat(collect(tEnv.executeSql("SELECT * FROM `T$files` WHERE level = 0")))
                    .isEmpty();
        }
    }

    @Test
    public void testWritePostponeBucketThenWriteFixedBucket() throws Exception {
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
                        + "  k INT,\n"
                        + "  v STRING,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'bucket' = '-2'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO T /*+ OPTIONS('postpone.batch-write-fixed-bucket' = 'false') */ "
                                + "VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h');")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T"))).isEmpty();

        // use sink.parallelism
        tEnv.executeSql(
                        "INSERT INTO T /*+ OPTIONS('sink.parallelism' = '4') */ "
                                + "VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E'), (6, 'F'), (7, 'G'), (8, 'H'), "
                                + "(9, '9'), (10, '10'), (11, '11'), (12, '12'), (13, '13'), (14, '14'), (15, '15'), (16, '16');")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[1, A]",
                        "+I[2, B]",
                        "+I[3, C]",
                        "+I[4, D]",
                        "+I[5, E]",
                        "+I[6, F]",
                        "+I[7, G]",
                        "+I[8, H]",
                        "+I[9, 9]",
                        "+I[10, 10]",
                        "+I[11, 11]",
                        "+I[12, 12]",
                        "+I[13, 13]",
                        "+I[14, 14]",
                        "+I[15, 15]",
                        "+I[16, 16]");
        assertThat(
                        collectRow(tEnv.executeSql("SELECT * FROM `T$buckets`")).stream()
                                .map(row -> (Integer) row.getField(1))
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(-2, 0, 1, 2, 3);
        assertThat(collect(tEnv.executeSql("SELECT * FROM `T$files` WHERE level > 0"))).isEmpty();

        // compact and check result again
        boolean forceUpLevel0 = ThreadLocalRandom.current().nextBoolean();
        if (forceUpLevel0) {
            tEnv.executeSql("ALTER TABLE T set ('compaction.force-up-level-0' = 'true')").await();
        }
        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();

        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder(
                        "+I[1, a]",
                        "+I[2, b]",
                        "+I[3, c]",
                        "+I[4, d]",
                        "+I[5, e]",
                        "+I[6, f]",
                        "+I[7, g]",
                        "+I[8, h]",
                        "+I[9, 9]",
                        "+I[10, 10]",
                        "+I[11, 11]",
                        "+I[12, 12]",
                        "+I[13, 13]",
                        "+I[14, 14]",
                        "+I[15, 15]",
                        "+I[16, 16]");
        assertThat(
                        collectRow(tEnv.executeSql("SELECT * FROM `T$buckets`")).stream()
                                .map(row -> (Integer) row.getField(1))
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(0, 1, 2, 3);
        if (forceUpLevel0) {
            assertThat(collect(tEnv.executeSql("SELECT * FROM `T$files` WHERE level = 0")))
                    .isEmpty();
        }
    }

    @Test
    public void testCompactPostponeThenWriteFixedBucket() throws Exception {
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
                        + "  k INT,\n"
                        + "  v STRING,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'bucket' = '-2'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO T /*+ OPTIONS('postpone.batch-write-fixed-bucket' = 'false') */ VALUES (1, 'a')")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T"))).isEmpty();

        tEnv.executeSql("CALL sys.compact(`table` => 'default.T')").await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder("+I[1, a]");
        tEnv.executeSql("INSERT INTO T VALUES (1, 'A')").await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T")))
                .containsExactlyInAnyOrder("+I[1, A]");
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

    private List<Row> collectRow(TableResult result) {
        try (CloseableIterator<Row> iter = result.collect()) {
            return ImmutableList.copyOf(iter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
