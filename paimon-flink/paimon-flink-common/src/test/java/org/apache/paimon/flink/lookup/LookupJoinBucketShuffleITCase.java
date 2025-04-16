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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.flink.CatalogITCaseBase;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for custom lookup shuffle. */
public class LookupJoinBucketShuffleITCase extends CatalogITCaseBase {

    private static final int BUCKET_NUMBER = 5;

    private static final int ROW_NUMBER = 100;

    /**
     * Test the non-pk full cached table where join key count is 1 and join key equals the bucket
     * key.
     */
    @Test
    public void testBucketShuffleForNonPrimaryTableInFullCacheModeCase1() throws Exception {
        String nonPrimaryKeyDimTable = createNonPrimaryKeyDimTable("col1");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1")
                        .replace("DIM", nonPrimaryKeyDimTable);
        testBucketNumberCases(query);
    }

    /**
     * Test the non-pk full cached table where join key count is 2, join keys equal the bucket keys.
     */
    @Test
    public void testBucketShuffleForNonPrimaryTableInFullCacheModeCase2() throws Exception {
        String nonPrimaryKeyDimTable = createNonPrimaryKeyDimTable("col1,col2");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1 AND T.col2 = D.col2")
                        .replace("DIM", nonPrimaryKeyDimTable);
        testBucketNumberCases(query);
    }

    /**
     * Test the non-pk full cached table where join key count is 2, bucket key is one of join key.
     */
    @Test
    public void testBucketShuffleForNonPrimaryTableInFullCacheModeCase3() throws Exception {
        String nonPrimaryKeyDimTable = createNonPrimaryKeyDimTable("col2");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1 AND T.col2 = D.col2")
                        .replace("DIM", nonPrimaryKeyDimTable);
        testBucketNumberCases(query);
    }

    /**
     * Test the non-pk full cached table where join keys contain constant keys, bucket key is one of
     * join key.
     */
    @Test
    public void testBucketShuffleForNonPrimaryTableInFullCacheModeCase4() throws Exception {
        String nonPrimaryKeyDimTable = createNonPrimaryKeyDimTable("col2");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1 AND T.col2 = D.col2 AND "
                                + "D.col4 = CAST('2024-06-09' AS DATE) AND D.col5 = 123.45 ")
                        .replace("DIM", nonPrimaryKeyDimTable);
        testBucketNumberCases(query);
    }

    /** Test the pk full cached table where join key count is 1, join key equals the bucket key. */
    @Test
    public void testBucketShuffleForPrimaryTableInFullCacheModeCase1() throws Exception {
        String primaryKeyDimTable = createPrimaryKeyDimTable(1, true, "col1");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1")
                        .replace("DIM", primaryKeyDimTable);
        testBucketNumberCases(query);
    }

    /** Test the pk full cached table where join key count is 2, join keys equal the bucket keys. */
    @Test
    public void testBucketShuffleForPrimaryTableInFullCacheModeCase2() throws Exception {
        String primaryKeyDimTable = createPrimaryKeyDimTable(2, true, "col1,col2");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1 AND T.col2 = D.col2")
                        .replace("DIM", primaryKeyDimTable);
        testBucketNumberCases(query);
    }

    /** Test the pk full cached table where join key count is 2, bucket key is one of join key. */
    @Test
    public void testBucketShuffleForPrimaryTableInFullCacheModeCase3() throws Exception {
        String primaryKeyDimTable = createPrimaryKeyDimTable(2, true, "col2");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1 AND T.col2 = D.col2")
                        .replace("DIM", primaryKeyDimTable);
        testBucketNumberCases(query);
    }

    /**
     * Test the pk full cached table where join keys contain constant keys, bucket key is one of
     * join key.
     */
    @Test
    public void testBucketShuffleForPrimaryTableInFullCacheModeCase4() throws Exception {
        String primaryKeyDimTable = createPrimaryKeyDimTable(2, true, "col2");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1 AND T.col2 = D.col2 AND "
                                + "D.col4 = CAST('2024-06-09' AS DATE) AND D.col5 = 123.45 ")
                        .replace("DIM", primaryKeyDimTable);
        testBucketNumberCases(query);
    }

    /** Test the pk auto cached table where join key count is 1, join key equals the bucket key. */
    @Test
    public void testBucketShuffleForPrimaryTableInAutoCacheModeCase1() throws Exception {
        String primaryKeyDimTable = createPrimaryKeyDimTable(1, false, "col1");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1")
                        .replace("DIM", primaryKeyDimTable);
        testBucketNumberCases(query);
    }

    /** Test the pk auto cached table where join key count is 2, join keys equal the bucket keys. */
    @Test
    public void testBucketShuffleForPrimaryTableInAutoCacheModeCase2() throws Exception {
        String primaryKeyDimTable = createPrimaryKeyDimTable(2, false, "col1,col2");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1 "
                                + "AND T.col2 = D.col2")
                        .replace("DIM", primaryKeyDimTable);
        testBucketNumberCases(query);
    }

    /** Test the pk auto cached table where join key count is 2, bucket key is one of join key. */
    @Test
    public void testBucketShuffleForPrimaryTableInAutoCacheModeCase3() throws Exception {
        String primaryKeyDimTable = createPrimaryKeyDimTable(2, false, "col2");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1 "
                                + "AND T.col2 = D.col2")
                        .replace("DIM", primaryKeyDimTable);
        testBucketNumberCases(query);
    }

    /**
     * Test the pk auto cached table where join keys contain constant keys, bucket key is one of
     * join key.
     */
    @Test
    public void testBucketShuffleForPrimaryTableInAutoCacheModeCase4() throws Exception {
        String primaryKeyDimTable = createPrimaryKeyDimTable(2, false, "col2");
        String query =
                ("SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.col1, D.col2 FROM T JOIN DIM "
                                + "for system_time as of T.proc_time AS D ON T.col1 = D.col1 AND T.col2 = D.col2 AND "
                                + "D.col4 = CAST('2024-06-09' AS DATE) AND D.col5 = 123.45")
                        .replace("DIM", primaryKeyDimTable);
        testBucketNumberCases(query);
    }

    private void testBucketNumberCases(String query) throws Exception {
        List<Row> groundTruthRows = getGroundTruthRows();
        // Test the case that bucket number = lookup parallelism.
        sEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, BUCKET_NUMBER);
        List<Row> result1 = streamSqlBlockIter(query).collect(ROW_NUMBER);
        assertThat(result1).containsExactlyInAnyOrderElementsOf(groundTruthRows);
        // Test the case that bucket number > lookup parallelism.
        sEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 3);
        List<Row> result2 = streamSqlBlockIter(query).collect(ROW_NUMBER);
        assertThat(result2).containsExactlyInAnyOrderElementsOf(groundTruthRows);
        // Test the case that bucket number < lookup parallelism.
        sEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 8);
        List<Row> result3 = streamSqlBlockIter(query).collect(ROW_NUMBER);
        assertThat(result3).containsExactlyInAnyOrderElementsOf(groundTruthRows);
    }

    private String createPrimaryKeyDimTable(
            int primaryKeyNumber, boolean fullCache, String bucketKey) {
        createSourceTable();
        String tableName = "DIM_PRIMARY";
        if (fullCache) {
            tableName += "_full_cache";
        } else {
            tableName += "_auto";
        }
        if (bucketKey != null) {
            tableName += "_" + bucketKey.split(",").length;
        }
        String ddl;
        if (primaryKeyNumber == 1) {
            tableName += "_1";
            ddl =
                    String.format(
                            "CREATE TABLE %s (col1 INT, col2 STRING, col3 INT, col4 DATE, col5 DECIMAL(10, 2), "
                                    + "PRIMARY KEY (col1) NOT ENFORCED) WITH"
                                    + " ('continuous.discovery-interval'='1 ms', 'bucket'='%s'",
                            tableName, BUCKET_NUMBER);
        } else {
            tableName += "_2";
            ddl =
                    String.format(
                            "CREATE TABLE %s (col1 INT, col2 STRING, col3 INT, col4 DATE, col5 DECIMAL(10, 2), "
                                    + "PRIMARY KEY (col1, col2) NOT ENFORCED) WITH"
                                    + " ('continuous.discovery-interval'='1 ms', 'bucket'='%s'",
                            tableName, BUCKET_NUMBER);
        }
        if (bucketKey != null) {
            ddl += ", 'bucket-key' = '" + bucketKey + "'";
        }
        if (fullCache) {
            ddl += " ,'lookup.cache' = 'full')";
        } else {
            ddl += " )";
        }
        batchSql(ddl);
        StringBuilder dml = new StringBuilder(String.format("INSERT INTO %s VALUES ", tableName));
        for (int index = 1; index < ROW_NUMBER; ++index) {
            dml.append(
                    String.format(
                            "(%s, '%s', %s, CAST('2024-06-09' AS DATE), 123.45), ",
                            index, index * 10, index * 10));
        }
        dml.append(
                String.format(
                        "(%s, '%s', %s, CAST('2024-06-09' AS DATE), 123.45)",
                        ROW_NUMBER, ROW_NUMBER * 10, ROW_NUMBER * 10));
        batchSql(dml.toString());
        return tableName;
    }

    private String createNonPrimaryKeyDimTable(String bucketKey) {
        createSourceTable();
        String tableName = "DIM";
        if (bucketKey != null) {
            tableName += "_" + bucketKey.split(",").length;
        }
        String ddl =
                String.format(
                        "CREATE TABLE %s (col1 INT, col2 STRING, col3 INT, col4 DATE,"
                                + " col5 DECIMAL(10, 2)) WITH"
                                + " ('continuous.discovery-interval'='1 ms', 'bucket'='%s'",
                        tableName, BUCKET_NUMBER);
        if (bucketKey != null) {
            ddl += ", 'bucket-key' = '" + bucketKey + "')";
        }
        batchSql(ddl);
        StringBuilder dml = new StringBuilder(String.format("INSERT INTO %s VALUES ", tableName));
        for (int index = 1; index < ROW_NUMBER; ++index) {
            dml.append(
                    String.format(
                            "(%s, '%s', %s, CAST('2024-06-09' AS DATE), 123.45), ",
                            index, index * 10, index * 10));
        }
        dml.append(
                String.format(
                        "(%s, '%s', %s, CAST('2024-06-09' AS DATE), 123.45)",
                        ROW_NUMBER, ROW_NUMBER * 10, ROW_NUMBER * 10));
        batchSql(dml.toString());
        return tableName;
    }

    private void createSourceTable() {
        String ddl = "CREATE TABLE T (col1 INT, col2 STRING, col3 INT, `proc_time` AS PROCTIME())";
        batchSql(ddl);
        StringBuilder dml = new StringBuilder("INSERT INTO T VALUES ");
        for (int index = 1; index < ROW_NUMBER; ++index) {
            dml.append(String.format("(%s, '%s', %s), ", index, index * 10, index * 10));
        }
        dml.append(String.format("(%s, '%s', %s)", ROW_NUMBER, ROW_NUMBER * 10, ROW_NUMBER * 10));
        batchSql(dml.toString());
    }

    private List<Row> getGroundTruthRows() {
        List<Row> results = new ArrayList<>();
        for (int index = 1; index <= ROW_NUMBER; ++index) {
            results.add(Row.of(index, String.valueOf(index * 10)));
        }
        return results;
    }
}
