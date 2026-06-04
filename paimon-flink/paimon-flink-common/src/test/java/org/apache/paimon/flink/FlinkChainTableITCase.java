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

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for chain table using Flink SQL. */
public class FlinkChainTableITCase extends CatalogITCaseBase {

    private List<String> collectResult(String query) throws Exception {
        List<String> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(query).collect()) {
            while (it.hasNext()) {
                result.add(it.next().toString());
            }
        }
        return result;
    }

    private void createChainTable(String tableName) {
        sql(
                "CREATE TABLE %s ("
                        + "  t1 BIGINT,"
                        + "  t2 BIGINT,"
                        + "  t3 STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,t1',"
                        + "  'bucket-key' = 't1',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 't2',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")",
                tableName);
    }

    private void setupChainTableBranches(String tableName) {
        sql("CALL sys.create_branch('%s.%s', 'snapshot')", tEnv.getCurrentDatabase(), tableName);
        sql("CALL sys.create_branch('%s.%s', 'delta')", tEnv.getCurrentDatabase(), tableName);

        sql(
                "ALTER TABLE %s SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")",
                tableName);
        sql(
                "ALTER TABLE `%s$branch_snapshot` SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")",
                tableName);
        sql(
                "ALTER TABLE `%s$branch_delta` SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")",
                tableName);
    }

    @Test
    public void testChainTable() throws Exception {
        createChainTable("chain_test");
        setupChainTableBranches("chain_test");

        // Write main branch
        sql(
                "INSERT OVERWRITE chain_test PARTITION (dt = '20250810')"
                        + " VALUES (1, 1, '1'), (2, 1, '1')");

        // Write delta branch
        sql(
                "INSERT OVERWRITE `chain_test$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (1, 1, '1'), (2, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test$branch_delta` PARTITION (dt = '20250810')"
                        + " VALUES (1, 2, '1-1'), (3, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test$branch_delta` PARTITION (dt = '20250811')"
                        + " VALUES (2, 2, '1-1'), (4, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test$branch_delta` PARTITION (dt = '20250812')"
                        + " VALUES (3, 2, '1-1'), (4, 2, '1-1'), (7, 1, 'd7')");
        sql(
                "INSERT OVERWRITE `chain_test$branch_delta` PARTITION (dt = '20250813')"
                        + " VALUES (5, 1, '1'), (6, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test$branch_delta` PARTITION (dt = '20250814')"
                        + " VALUES (5, 2, '1-1'), (6, 2, '1-1')");

        // Write snapshot branch
        sql(
                "INSERT OVERWRITE `chain_test$branch_snapshot` PARTITION (dt = '20250810')"
                        + " VALUES (1, 2, '1-1'), (2, 1, '1'), (3, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test$branch_snapshot` PARTITION (dt = '20250812')"
                        + " VALUES (1, 2, '1-1'), (2, 2, '1-1'), (3, 2, '1-1'), (4, 2, '1-1')");
        sql(
                "INSERT OVERWRITE `chain_test$branch_snapshot` PARTITION (dt = '20250814')"
                        + " VALUES (1, 2, '1-1'), (2, 2, '1-1'), (3, 2, '1-1'), (4, 2, '1-1'), (5, 1, '1'), (6, 1, '1')");

        // Main read - partition exists in main branch, read directly
        assertThat(collectResult("SELECT * FROM chain_test WHERE dt = '20250810'"))
                .containsExactlyInAnyOrder("+I[1, 1, 1, 20250810]", "+I[2, 1, 1, 20250810]");

        // Snapshot read - partition exists in snapshot branch, read directly
        assertThat(collectResult("SELECT * FROM chain_test WHERE dt = '20250814'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, 20250814]",
                        "+I[2, 2, 1-1, 20250814]",
                        "+I[3, 2, 1-1, 20250814]",
                        "+I[4, 2, 1-1, 20250814]",
                        "+I[5, 1, 1, 20250814]",
                        "+I[6, 1, 1, 20250814]");

        // Chain read - no prior snapshot, delta only
        assertThat(collectResult("SELECT * FROM chain_test WHERE dt = '20250809'"))
                .containsExactlyInAnyOrder("+I[1, 1, 1, 20250809]", "+I[2, 1, 1, 20250809]");

        // Chain read - has prior snapshot (anchor=20250810), merge snapshot + delta
        assertThat(collectResult("SELECT * FROM chain_test WHERE dt = '20250811'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, 20250811]",
                        "+I[2, 2, 1-1, 20250811]",
                        "+I[3, 1, 1, 20250811]",
                        "+I[4, 1, 1, 20250811]");

        // Chain read with non-partition filter
        assertThat(collectResult("SELECT * FROM chain_test WHERE dt = '20250811' AND t1 = 1"))
                .containsExactlyInAnyOrder("+I[1, 2, 1-1, 20250811]");
        assertThat(collectResult("SELECT * FROM chain_test WHERE dt = '20250811' AND t1 = 4"))
                .containsExactlyInAnyOrder("+I[4, 1, 1, 20250811]");
        assertThat(collectResult("SELECT * FROM chain_test WHERE dt = '20250811' AND t1 = 7"))
                .isEmpty();

        // Cross-partition filter
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test WHERE dt IN ('20250811', '20250812') AND t1 = 1"))
                .containsExactlyInAnyOrder("+I[1, 2, 1-1, 20250811]", "+I[1, 2, 1-1, 20250812]");

        // Snapshot read with filter
        assertThat(collectResult("SELECT * FROM chain_test WHERE dt = '20250812' AND t1 = 7"))
                .isEmpty();

        // Multi partition read
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test WHERE dt IN ('20250810', '20250811', '20250812')"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, 20250810]",
                        "+I[2, 1, 1, 20250810]",
                        "+I[1, 2, 1-1, 20250811]",
                        "+I[2, 2, 1-1, 20250811]",
                        "+I[3, 1, 1, 20250811]",
                        "+I[4, 1, 1, 20250811]",
                        "+I[1, 2, 1-1, 20250812]",
                        "+I[2, 2, 1-1, 20250812]",
                        "+I[3, 2, 1-1, 20250812]",
                        "+I[4, 2, 1-1, 20250812]");

        // Incremental read - read delta branch only
        assertThat(collectResult("SELECT * FROM `chain_test$branch_delta` WHERE dt = '20250811'"))
                .containsExactlyInAnyOrder("+I[2, 2, 1-1, 20250811]", "+I[4, 1, 1, 20250811]");

        // Multi partition incremental read
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_test$branch_delta` WHERE dt IN ('20250810', '20250811', '20250812')"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, 20250810]",
                        "+I[3, 1, 1, 20250810]",
                        "+I[2, 2, 1-1, 20250811]",
                        "+I[4, 1, 1, 20250811]",
                        "+I[3, 2, 1-1, 20250812]",
                        "+I[4, 2, 1-1, 20250812]",
                        "+I[7, 1, d7, 20250812]");

        // Hybrid read - full query UNION ALL incremental query
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test WHERE dt = '20250811'"
                                        + " UNION ALL"
                                        + " SELECT * FROM `chain_test$branch_delta` WHERE dt = '20250811'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, 20250811]",
                        "+I[2, 2, 1-1, 20250811]",
                        "+I[3, 1, 1, 20250811]",
                        "+I[4, 1, 1, 20250811]",
                        "+I[2, 2, 1-1, 20250811]",
                        "+I[4, 1, 1, 20250811]");
    }

    @Test
    public void testHourlyChainTable() throws Exception {
        sql(
                "CREATE TABLE chain_test_hourly ("
                        + "  t1 BIGINT,"
                        + "  t2 BIGINT,"
                        + "  t3 STRING,"
                        + "  dt STRING,"
                        + "  `hour` STRING"
                        + ") PARTITIONED BY (dt, `hour`) WITH ("
                        + "  'primary-key' = 'dt,hour,t1',"
                        + "  'bucket-key' = 't1',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 't2',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt $hour:00:00',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd HH:mm:ss'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_test_hourly', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_test_hourly', 'delta')", db);
        sql(
                "ALTER TABLE chain_test_hourly SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta')");
        sql(
                "ALTER TABLE `chain_test_hourly$branch_snapshot` SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta')");
        sql(
                "ALTER TABLE `chain_test_hourly$branch_delta` SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta')");

        // Write main branch
        sql(
                "INSERT OVERWRITE chain_test_hourly PARTITION (dt = '20250810', `hour` = '22')"
                        + " VALUES (1, 1, '1'), (2, 1, '1')");

        // Write delta branch
        sql(
                "INSERT OVERWRITE `chain_test_hourly$branch_delta` PARTITION (dt = '20250810', `hour` = '21')"
                        + " VALUES (1, 1, '1'), (2, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_hourly$branch_delta` PARTITION (dt = '20250810', `hour` = '22')"
                        + " VALUES (1, 2, '1-1'), (3, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_hourly$branch_delta` PARTITION (dt = '20250810', `hour` = '23')"
                        + " VALUES (2, 2, '1-1'), (4, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_hourly$branch_delta` PARTITION (dt = '20250811', `hour` = '00')"
                        + " VALUES (3, 2, '1-1'), (4, 2, '1-1')");
        sql(
                "INSERT OVERWRITE `chain_test_hourly$branch_delta` PARTITION (dt = '20250811', `hour` = '01')"
                        + " VALUES (5, 1, '1'), (6, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_hourly$branch_delta` PARTITION (dt = '20250811', `hour` = '02')"
                        + " VALUES (5, 2, '1-1'), (6, 2, '1-1')");

        // Write snapshot branch
        sql(
                "INSERT OVERWRITE `chain_test_hourly$branch_snapshot` PARTITION (dt = '20250810', `hour` = '22')"
                        + " VALUES (1, 2, '1-1'), (2, 1, '1'), (3, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_hourly$branch_snapshot` PARTITION (dt = '20250811', `hour` = '00')"
                        + " VALUES (1, 2, '1-1'), (2, 2, '1-1'), (3, 2, '1-1'), (4, 2, '1-1')");
        sql(
                "INSERT OVERWRITE `chain_test_hourly$branch_snapshot` PARTITION (dt = '20250811', `hour` = '02')"
                        + " VALUES (1, 2, '1-1'), (2, 2, '1-1'), (3, 2, '1-1'), (4, 2, '1-1'), (5, 1, '1'), (6, 1, '1')");

        // Main read
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_hourly WHERE dt = '20250810' AND `hour` = '22'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, 20250810, 22]", "+I[2, 1, 1, 20250810, 22]");

        // Snapshot read
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_hourly WHERE dt = '20250811' AND `hour` = '02'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, 20250811, 02]",
                        "+I[2, 2, 1-1, 20250811, 02]",
                        "+I[3, 2, 1-1, 20250811, 02]",
                        "+I[4, 2, 1-1, 20250811, 02]",
                        "+I[5, 1, 1, 20250811, 02]",
                        "+I[6, 1, 1, 20250811, 02]");

        // Chain read - no prior snapshot
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_hourly WHERE dt = '20250810' AND `hour` = '21'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, 20250810, 21]", "+I[2, 1, 1, 20250810, 21]");

        // Chain read - has prior snapshot
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_hourly WHERE dt = '20250810' AND `hour` = '23'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, 20250810, 23]",
                        "+I[2, 2, 1-1, 20250810, 23]",
                        "+I[3, 1, 1, 20250810, 23]",
                        "+I[4, 1, 1, 20250810, 23]");

        // Chain read with non-partition filter
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_hourly WHERE dt = '20250810' AND `hour` = '23' AND t1 = 1"))
                .containsExactlyInAnyOrder("+I[1, 2, 1-1, 20250810, 23]");

        // Multi partition read
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_hourly WHERE dt = '20250810' AND `hour` IN ('22', '23')"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, 20250810, 22]",
                        "+I[2, 1, 1, 20250810, 22]",
                        "+I[1, 2, 1-1, 20250810, 23]",
                        "+I[2, 2, 1-1, 20250810, 23]",
                        "+I[3, 1, 1, 20250810, 23]",
                        "+I[4, 1, 1, 20250810, 23]");

        // Incremental read
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_test_hourly$branch_delta` WHERE dt = '20250810' AND `hour` = '23'"))
                .containsExactlyInAnyOrder(
                        "+I[2, 2, 1-1, 20250810, 23]", "+I[4, 1, 1, 20250810, 23]");

        // Hybrid read
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_hourly WHERE dt = '20250810' AND `hour` = '23'"
                                        + " UNION ALL"
                                        + " SELECT * FROM `chain_test_hourly$branch_delta` WHERE dt = '20250810' AND `hour` = '23'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, 20250810, 23]",
                        "+I[2, 2, 1-1, 20250810, 23]",
                        "+I[3, 1, 1, 20250810, 23]",
                        "+I[4, 1, 1, 20250810, 23]",
                        "+I[2, 2, 1-1, 20250810, 23]",
                        "+I[4, 1, 1, 20250810, 23]");
    }

    @Test
    public void testChainTableWithPartialUpdate() throws Exception {
        sql(
                "CREATE TABLE chain_test_partial ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v1 STRING,"
                        + "  v2 STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'partial-update',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_test_partial', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_test_partial', 'delta')", db);
        sql(
                "ALTER TABLE chain_test_partial SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta')");
        sql(
                "ALTER TABLE `chain_test_partial$branch_snapshot` SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta')");
        sql(
                "ALTER TABLE `chain_test_partial$branch_delta` SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta')");

        // Write main branch
        sql(
                "INSERT OVERWRITE chain_test_partial PARTITION (dt = '20250810')"
                        + " VALUES (1, 1, 'a', 'A'), (2, 1, 'b', 'B')");

        // Write delta branch
        sql(
                "INSERT OVERWRITE `chain_test_partial$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (3, 1, 'c', 'C'), (4, 1, 'd', 'D')");
        sql(
                "INSERT OVERWRITE `chain_test_partial$branch_delta` PARTITION (dt = '20250810')"
                        + " VALUES (1, 2, CAST(NULL AS STRING), 'A1'),"
                        + " (2, 2, 'b1', CAST(NULL AS STRING)),"
                        + " (3, 1, 'c', 'C')");
        sql(
                "INSERT OVERWRITE `chain_test_partial$branch_delta` PARTITION (dt = '20250811')"
                        + " VALUES (1, 3, 'a1', CAST(NULL AS STRING)),"
                        + " (2, 3, CAST(NULL AS STRING), 'B1'),"
                        + " (5, 1, 'e', 'E')");
        sql(
                "INSERT OVERWRITE `chain_test_partial$branch_delta` PARTITION (dt = '20250812')"
                        + " VALUES (1, 4, CAST(NULL AS STRING), 'A2'),"
                        + " (5, 2, 'e1', CAST(NULL AS STRING))");

        // Write snapshot branch
        sql(
                "INSERT OVERWRITE `chain_test_partial$branch_snapshot` PARTITION (dt = '20250810')"
                        + " VALUES (1, 2, 'a', 'A1'), (2, 2, 'b1', 'B'), (3, 1, 'c', 'C')");
        sql(
                "INSERT OVERWRITE `chain_test_partial$branch_snapshot` PARTITION (dt = '20250812')"
                        + " VALUES (1, 4, 'a1', 'A2'), (2, 3, 'b1', 'B1'), (3, 1, 'c', 'C'), (5, 2, 'e1', 'E')");

        // Main read
        assertThat(collectResult("SELECT * FROM chain_test_partial WHERE dt = '20250810'"))
                .containsExactlyInAnyOrder("+I[1, 1, a, A, 20250810]", "+I[2, 1, b, B, 20250810]");

        // Snapshot read
        assertThat(collectResult("SELECT * FROM chain_test_partial WHERE dt = '20250812'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 4, a1, A2, 20250812]",
                        "+I[2, 3, b1, B1, 20250812]",
                        "+I[3, 1, c, C, 20250812]",
                        "+I[5, 2, e1, E, 20250812]");

        // Chain read - no prior snapshot
        assertThat(collectResult("SELECT * FROM chain_test_partial WHERE dt = '20250809'"))
                .containsExactlyInAnyOrder("+I[3, 1, c, C, 20250809]", "+I[4, 1, d, D, 20250809]");

        // Chain read - has prior snapshot, partial-update merge
        assertThat(collectResult("SELECT * FROM chain_test_partial WHERE dt = '20250811'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 3, a1, A1, 20250811]",
                        "+I[2, 3, b1, B1, 20250811]",
                        "+I[3, 1, c, C, 20250811]",
                        "+I[5, 1, e, E, 20250811]");

        // Multi partition read
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_partial WHERE dt IN ('20250810', '20250811', '20250812')"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, a, A, 20250810]",
                        "+I[2, 1, b, B, 20250810]",
                        "+I[1, 3, a1, A1, 20250811]",
                        "+I[2, 3, b1, B1, 20250811]",
                        "+I[3, 1, c, C, 20250811]",
                        "+I[5, 1, e, E, 20250811]",
                        "+I[1, 4, a1, A2, 20250812]",
                        "+I[2, 3, b1, B1, 20250812]",
                        "+I[3, 1, c, C, 20250812]",
                        "+I[5, 2, e1, E, 20250812]");

        // Incremental read - delta branch with partial-update raw data
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_test_partial$branch_delta` WHERE dt = '20250811'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 3, a1, null, 20250811]",
                        "+I[2, 3, null, B1, 20250811]",
                        "+I[5, 1, e, E, 20250811]");
    }

    @Test
    public void testChainTableWithGroupPartition() throws Exception {
        sql(
                "CREATE TABLE chain_test_group ("
                        + "  t1 BIGINT,"
                        + "  t2 BIGINT,"
                        + "  t3 STRING,"
                        + "  region STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (region, dt) WITH ("
                        + "  'primary-key' = 'region,dt,t1',"
                        + "  'bucket-key' = 't1',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 't2',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'chain-table.chain-partition-keys' = 'dt'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_test_group', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_test_group', 'delta')", db);
        sql(
                "ALTER TABLE chain_test_group SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta')");
        sql(
                "ALTER TABLE `chain_test_group$branch_snapshot` SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta')");
        sql(
                "ALTER TABLE `chain_test_group$branch_delta` SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta')");

        // Write main branch
        sql(
                "INSERT OVERWRITE chain_test_group PARTITION (region = 'CN', dt = '20250810')"
                        + " VALUES (1, 1, '1'), (2, 1, '1')");
        sql(
                "INSERT OVERWRITE chain_test_group PARTITION (region = 'US', dt = '20250810')"
                        + " VALUES (11, 1, '1'), (12, 1, '1')");

        // Write delta branch - CN
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_delta` PARTITION (region = 'CN', dt = '20250809')"
                        + " VALUES (1, 1, '1'), (2, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_delta` PARTITION (region = 'CN', dt = '20250810')"
                        + " VALUES (1, 2, '1-1'), (3, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_delta` PARTITION (region = 'CN', dt = '20250811')"
                        + " VALUES (2, 2, '1-1'), (4, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_delta` PARTITION (region = 'CN', dt = '20250812')"
                        + " VALUES (3, 2, '1-1'), (4, 2, '1-1')");

        // Write delta branch - US
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_delta` PARTITION (region = 'US', dt = '20250809')"
                        + " VALUES (11, 1, '1'), (12, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_delta` PARTITION (region = 'US', dt = '20250810')"
                        + " VALUES (11, 2, '1-1'), (13, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_delta` PARTITION (region = 'US', dt = '20250811')"
                        + " VALUES (12, 2, '1-1'), (14, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_delta` PARTITION (region = 'US', dt = '20250812')"
                        + " VALUES (13, 2, '1-1'), (14, 2, '1-1')");

        // Write snapshot branch - CN
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_snapshot` PARTITION (region = 'CN', dt = '20250810')"
                        + " VALUES (1, 2, '1-1'), (2, 1, '1'), (3, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_snapshot` PARTITION (region = 'CN', dt = '20250812')"
                        + " VALUES (1, 2, '1-1'), (2, 2, '1-1'), (3, 2, '1-1'), (4, 2, '1-1')");

        // Write snapshot branch - US
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_snapshot` PARTITION (region = 'US', dt = '20250810')"
                        + " VALUES (11, 2, '1-1'), (12, 1, '1'), (13, 1, '1')");
        sql(
                "INSERT OVERWRITE `chain_test_group$branch_snapshot` PARTITION (region = 'US', dt = '20250812')"
                        + " VALUES (11, 2, '1-1'), (12, 2, '1-1'), (13, 2, '1-1'), (14, 2, '1-1')");

        // Main read - both regions
        assertThat(collectResult("SELECT * FROM chain_test_group WHERE dt = '20250810'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, CN, 20250810]",
                        "+I[2, 1, 1, CN, 20250810]",
                        "+I[11, 1, 1, US, 20250810]",
                        "+I[12, 1, 1, US, 20250810]");

        // Snapshot read
        assertThat(collectResult("SELECT * FROM chain_test_group WHERE dt = '20250812'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, CN, 20250812]",
                        "+I[2, 2, 1-1, CN, 20250812]",
                        "+I[3, 2, 1-1, CN, 20250812]",
                        "+I[4, 2, 1-1, CN, 20250812]",
                        "+I[11, 2, 1-1, US, 20250812]",
                        "+I[12, 2, 1-1, US, 20250812]",
                        "+I[13, 2, 1-1, US, 20250812]",
                        "+I[14, 2, 1-1, US, 20250812]");

        // Snapshot read with region filter
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_group WHERE region = 'CN' AND dt = '20250812'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, CN, 20250812]",
                        "+I[2, 2, 1-1, CN, 20250812]",
                        "+I[3, 2, 1-1, CN, 20250812]",
                        "+I[4, 2, 1-1, CN, 20250812]");

        // Chain read - no prior snapshot
        assertThat(collectResult("SELECT * FROM chain_test_group WHERE dt = '20250809'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, CN, 20250809]",
                        "+I[2, 1, 1, CN, 20250809]",
                        "+I[11, 1, 1, US, 20250809]",
                        "+I[12, 1, 1, US, 20250809]");

        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_group WHERE region = 'CN' AND dt = '20250809'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, CN, 20250809]", "+I[2, 1, 1, CN, 20250809]");

        // Chain read - has prior snapshot, each group independent
        assertThat(collectResult("SELECT * FROM chain_test_group WHERE dt = '20250811'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, CN, 20250811]",
                        "+I[2, 2, 1-1, CN, 20250811]",
                        "+I[3, 1, 1, CN, 20250811]",
                        "+I[4, 1, 1, CN, 20250811]",
                        "+I[11, 2, 1-1, US, 20250811]",
                        "+I[12, 2, 1-1, US, 20250811]",
                        "+I[13, 1, 1, US, 20250811]",
                        "+I[14, 1, 1, US, 20250811]");

        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_group WHERE region = 'CN' AND dt = '20250811'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, CN, 20250811]",
                        "+I[2, 2, 1-1, CN, 20250811]",
                        "+I[3, 1, 1, CN, 20250811]",
                        "+I[4, 1, 1, CN, 20250811]");

        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_group WHERE region = 'US' AND dt = '20250811'"))
                .containsExactlyInAnyOrder(
                        "+I[11, 2, 1-1, US, 20250811]",
                        "+I[12, 2, 1-1, US, 20250811]",
                        "+I[13, 1, 1, US, 20250811]",
                        "+I[14, 1, 1, US, 20250811]");

        // Multi partition read
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_test_group WHERE dt IN ('20250810', '20250811', '20250812')"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, CN, 20250810]",
                        "+I[2, 1, 1, CN, 20250810]",
                        "+I[1, 2, 1-1, CN, 20250811]",
                        "+I[2, 2, 1-1, CN, 20250811]",
                        "+I[3, 1, 1, CN, 20250811]",
                        "+I[4, 1, 1, CN, 20250811]",
                        "+I[1, 2, 1-1, CN, 20250812]",
                        "+I[2, 2, 1-1, CN, 20250812]",
                        "+I[3, 2, 1-1, CN, 20250812]",
                        "+I[4, 2, 1-1, CN, 20250812]",
                        "+I[11, 1, 1, US, 20250810]",
                        "+I[12, 1, 1, US, 20250810]",
                        "+I[11, 2, 1-1, US, 20250811]",
                        "+I[12, 2, 1-1, US, 20250811]",
                        "+I[13, 1, 1, US, 20250811]",
                        "+I[14, 1, 1, US, 20250811]",
                        "+I[11, 2, 1-1, US, 20250812]",
                        "+I[12, 2, 1-1, US, 20250812]",
                        "+I[13, 2, 1-1, US, 20250812]",
                        "+I[14, 2, 1-1, US, 20250812]");

        // Incremental read
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_test_group$branch_delta` WHERE dt = '20250811'"))
                .containsExactlyInAnyOrder(
                        "+I[2, 2, 1-1, CN, 20250811]",
                        "+I[4, 1, 1, CN, 20250811]",
                        "+I[12, 2, 1-1, US, 20250811]",
                        "+I[14, 1, 1, US, 20250811]");

        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_test_group$branch_delta` WHERE region = 'CN' AND dt = '20250811'"))
                .containsExactlyInAnyOrder(
                        "+I[2, 2, 1-1, CN, 20250811]", "+I[4, 1, 1, CN, 20250811]");
    }
}
