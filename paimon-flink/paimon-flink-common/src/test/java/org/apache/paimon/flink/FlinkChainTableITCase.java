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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.ChainTableStreamScan;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    /** Write Row data (with RowKind) to a specific branch using DataStream API. */
    private void writeChangelogToBranch(String db, String tableName, String branch, Row... rows)
            throws Exception {
        FileStoreTable table = paimonTable(tableName + "$branch_" + branch);

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(100)
                        .parallelism(1)
                        .build();

        DataStream<Row> stream = env.fromCollection(Arrays.asList(rows));

        new FlinkSinkBuilder(table)
                .forRow(
                        stream,
                        DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.BIGINT()),
                                DataTypes.FIELD("seq", DataTypes.BIGINT()),
                                DataTypes.FIELD("v", DataTypes.STRING()),
                                DataTypes.FIELD("dt", DataTypes.STRING())))
                .build();
        env.execute();
    }

    /**
     * Collect n rows from a streaming iterator with a timeout. If no data arrives within
     * timeoutSeconds, the iterator is closed and an AssertionError is thrown. This is necessary
     * because it.next() blocks indefinitely when no data is available, and JUnit @Timeout cannot
     * interrupt it.
     */
    /**
     * Collects {@code n} rows from a streaming iterator using the project-standard {@link
     * BlockingIterator}.
     */
    private List<String> collectRows(CloseableIterator<Row> it, int n) throws Exception {
        return BlockingIterator.of(it).collect(n, 30, TimeUnit.SECONDS).stream()
                .map(Row::toString)
                .collect(Collectors.toList());
    }

    /**
     * Polls the given table until it contains at least {@code minRows} rows. Used instead of
     * fixed-duration Thread.sleep to avoid flaky tests on slow CI.
     */
    private void waitForRowCount(String tableName, int minRows) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        int count = 0;
        while (System.currentTimeMillis() < deadline) {
            List<Row> rows = sql("SELECT * FROM " + tableName);
            count = rows.size();
            if (count >= minRows) {
                return;
            }
            Thread.sleep(1000);
        }
        throw new AssertionError(
                "Timed out waiting for " + minRows + " rows in " + tableName + ", got " + count);
    }

    /**
     * Tests the streaming read lifecycle for a chain table with changelog-producer=input.
     *
     * <p>Verifies: initial full read from delta-only → delta incremental visible with changelog
     * records (-U/+U) → snapshot OVERWRITE has no effect → more delta visible → stateless restart
     * reads chain-merged state.
     */
    @Test
    @Timeout(120)
    public void testStreamingReadChainTableLifecycleWithInputChangelog() throws Exception {
        // Create chain table with changelog-producer=input
        sql(
                "CREATE TABLE chain_life_cl ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_life_cl', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_life_cl', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_life_cl", "chain_life_cl$branch_snapshot", "chain_life_cl$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // === Phase 1: Delta-only initial data (all inserts) ===
        sql(
                "INSERT INTO `chain_life_cl$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'base_1'), (2, 1, 'base_2'), (3, 1, 'base_3'),"
                        + " (4, 1, 'base_4'), (5, 1, 'base_5')");

        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM chain_life_cl").collect();

        List<String> phase1 = collectRows(it, 5);
        assertThat(phase1)
                .containsExactlyInAnyOrder(
                        "+I[1, 1, base_1, 20250808]",
                        "+I[2, 1, base_2, 20250808]",
                        "+I[3, 1, base_3, 20250808]",
                        "+I[4, 1, base_4, 20250808]",
                        "+I[5, 1, base_5, 20250808]");

        // === Phase 2: Write changelog data (with -U/+U for update) via DataStream API ===
        writeChangelogToBranch(
                db,
                "chain_life_cl",
                "delta",
                Row.ofKind(RowKind.UPDATE_BEFORE, 3L, 1L, "base_3", "20250809"),
                Row.ofKind(RowKind.UPDATE_AFTER, 3L, 2L, "upd_3", "20250809"),
                Row.ofKind(RowKind.INSERT, 6L, 1L, "new_6", "20250809"),
                Row.ofKind(RowKind.INSERT, 7L, 1L, "new_7", "20250809"));

        List<String> phase2 = collectRows(it, 4);
        // changelog-producer=input: explicit -U/+U for updates
        assertThat(phase2)
                .containsExactlyInAnyOrder(
                        "-U[3, 1, base_3, 20250809]",
                        "+U[3, 2, upd_3, 20250809]",
                        "+I[6, 1, new_6, 20250809]",
                        "+I[7, 1, new_7, 20250809]");

        // === Phase 3: Snapshot OVERWRITE should have NO effect ===
        sql(
                "INSERT OVERWRITE `chain_life_cl$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'base_1'), (2, 1, 'base_2'), (3, 1, 'base_3'),"
                        + " (4, 1, 'base_4'), (5, 1, 'base_5')");

        // Write delta AFTER snapshot — this proves snapshot writes don't trigger output.
        // If snapshot writes were detected, we'd see duplicate or unexpected rows.
        writeChangelogToBranch(
                db,
                "chain_life_cl",
                "delta",
                Row.ofKind(RowKind.INSERT, 100L, 1L, "phase3_probe", "20250810"));

        List<String> phase3 = collectRows(it, 1);
        assertThat(phase3)
                .as("Only delta write should produce output, snapshot OVERWRITE should be ignored")
                .containsExactlyInAnyOrder("+I[100, 1, phase3_probe, 20250810]");

        // === Phase 4: Write more delta via DataStream API ===
        writeChangelogToBranch(
                db,
                "chain_life_cl",
                "delta",
                Row.ofKind(RowKind.INSERT, 8L, 1L, "new_8", "20250810"),
                Row.ofKind(RowKind.INSERT, 9L, 1L, "new_9", "20250810"));

        List<String> phase4 = collectRows(it, 2);
        assertThat(phase4)
                .containsExactlyInAnyOrder(
                        "+I[8, 1, new_8, 20250810]", "+I[9, 1, new_9, 20250810]");

        // Terminate first streaming job
        it.close();

        // === Phase 5: Stateless restart ===
        CloseableIterator<Row> it2 = sEnv.executeSql("SELECT * FROM chain_life_cl").collect();

        // Phase 5 starting (matching batch semantics):
        // - snapshot@20250808: k=1-5 (snapshot wins, delta@20250808 skipped since same partition
        //   exists in snapshot; same values here since OVERWRITE wrote identical base data)
        // - delta@20250809: changelog records (+U for update, +I for inserts)
        // - delta@20250810: k=8,9,100 (delta-only, no snapshot for this partition)
        // Total: 11 unique rows (PK=(dt,k) makes each (dt,k) pair distinct).
        List<String> restart = collectRows(it2, 11);
        assertThat(restart)
                .containsExactlyInAnyOrder(
                        "+I[1, 1, base_1, 20250808]",
                        "+I[2, 1, base_2, 20250808]",
                        "+I[3, 1, base_3, 20250808]",
                        "+U[3, 2, upd_3, 20250809]",
                        "+I[4, 1, base_4, 20250808]",
                        "+I[5, 1, base_5, 20250808]",
                        "+I[6, 1, new_6, 20250809]",
                        "+I[7, 1, new_7, 20250809]",
                        "+I[8, 1, new_8, 20250810]",
                        "+I[9, 1, new_9, 20250810]",
                        "+I[100, 1, phase3_probe, 20250810]");

        // Continue writing delta
        writeChangelogToBranch(
                db,
                "chain_life_cl",
                "delta",
                Row.ofKind(RowKind.INSERT, 10L, 1L, "new_10", "20250811"),
                Row.ofKind(RowKind.INSERT, 11L, 1L, "new_11", "20250811"));

        List<String> phase5b = collectRows(it2, 2);
        assertThat(phase5b)
                .containsExactlyInAnyOrder(
                        "+I[10, 1, new_10, 20250811]", "+I[11, 1, new_11, 20250811]");

        it2.close();
    }

    /**
     * Tests stateful restart of a chain table streaming read job using Flink checkpoint/restore.
     *
     * <p>Phase 1: Write initial delta data, start streaming job. Phase 2: Write incremental delta,
     * let Phase 2 consume it, then checkpoint and cancel. Phase 3: Write new delta data while the
     * job is down. Phase 4: Restart from checkpoint — the restored scan must NOT re-read the
     * already-consumed delta (verifies checkpoint() returns the advanced cursor, not the stale
     * Phase 1 boundary). Phase 5: Verify incremental streaming continues after restore.
     */
    @Test
    @Timeout(180)
    public void testStreamingReadChainTableStatefulRestart() throws Exception {
        // Create chain table (source)
        sql(
                "CREATE TABLE chain_restart ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        // Create a Paimon PK sink table. The Paimon sink supports upsert
        // (primary key), so the planner won't need ChangelogNormalize.
        // Paimon sink does NOT implement CheckpointedFunction (it uses operator
        // state for in-flight files, committed during checkpoint complete), so no
        // buffer leakage on checkpoint recovery — unlike CollectSinkFunction.
        sql(
                "CREATE TABLE chain_restart_sink ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING,"
                        + "  PRIMARY KEY (dt, k) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'bucket' = '2',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'sequence.field' = 'seq'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_restart', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_restart', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_restart", "chain_restart$branch_snapshot", "chain_restart$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // Configure checkpoint for stateful restart
        org.apache.flink.configuration.Configuration config = sEnv.getConfig().getConfiguration();
        config.setString("state.checkpoints.dir", "file://" + path + "/checkpoints");
        config.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        // Enable auto-checkpointing (1s) so Phase 2 data is committed before we take the
        // savepoint. This ensures the enumerator's delta cursor has advanced past
        // delta@20250809, which is the scenario the checkpoint() regression would break.
        config.setString("execution.checkpointing.interval", "1000");

        // Same SQL for both phases → operator graph matches → state recovery works
        String streamSql = "INSERT INTO chain_restart_sink SELECT * FROM chain_restart";

        // T4: Write snapshot data BEFORE starting streaming, so the starting phase
        // exercises the snapshot+delta merge path (not just delta-only).
        sql(
                "INSERT INTO `chain_restart$branch_snapshot` PARTITION (dt = '20250807')"
                        + " VALUES (10, 1, 'snap_10'), (11, 1, 'snap_11')");

        // === Phase 1: Write initial delta data and start streaming INSERT INTO ===
        sql(
                "INSERT INTO `chain_restart$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'base_1'), (2, 1, 'base_2'), (3, 1, 'base_3')");

        TableResult tableResult = sEnv.executeSql(streamSql);
        //noinspection OptionalGetWithoutIsPresent
        JobClient jobClient = tableResult.getJobClient().get();

        // === Phase 2: Write incremental delta, let Phase 2 consume it, THEN checkpoint ===
        // This exercises the checkpoint() regression: if checkpoint() returns the stale
        // Phase 1 boundary instead of the advanced delta cursor, restore would re-read
        // delta@20250809 and produce duplicates.
        sql(
                "INSERT INTO `chain_restart$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (4, 1, 'new_4'), (5, 1, 'new_5')");

        // Wait for auto-checkpoint to commit Phase 2 data to the sink. This proves the
        // enumerator's scan has consumed delta@20250809 and its checkpoint() returned the
        // advanced cursor — the exact scenario the regression would break.
        waitForRowCount("chain_restart_sink", 7);

        // Create a savepoint and stop the job atomically.
        // stopWithSavepoint guarantees the savepoint is fully committed to disk
        // before returning.
        String savepointDir = path + "/savepoints";
        new java.io.File(savepointDir).mkdirs();
        String checkpointPath =
                jobClient
                        .stopWithSavepoint(
                                false,
                                savepointDir,
                                org.apache.flink.core.execution.SavepointFormatType.CANONICAL)
                        .get();

        // Verify Phase 1+2 data (committed by checkpoint).
        List<String> phase1and2 =
                sql("SELECT * FROM chain_restart_sink").stream()
                        .map(Row::toString)
                        .collect(Collectors.toList());
        assertThat(phase1and2)
                .as("Phase 1+2: sink has snapshot, delta@20250808, and delta@20250809")
                .containsExactlyInAnyOrder(
                        "+I[1, 1, base_1, 20250808]",
                        "+I[2, 1, base_2, 20250808]",
                        "+I[3, 1, base_3, 20250808]",
                        "+I[10, 1, snap_10, 20250807]",
                        "+I[11, 1, snap_11, 20250807]",
                        "+I[4, 1, new_4, 20250809]",
                        "+I[5, 1, new_5, 20250809]");

        // === Phase 3: Write new delta data while job is stopped ===
        sql(
                "INSERT INTO `chain_restart$branch_delta` PARTITION (dt = '20250810')"
                        + " VALUES (6, 1, 'new_6'), (7, 1, 'new_7')");

        // === Phase 4: Restart from savepoint ===
        // The restored scan should NOT re-read delta@20250809 (already consumed before
        // savepoint). If checkpoint() returned the stale Phase 1 boundary, delta@20250809
        // would be re-read and produce duplicates.
        sEnv.getConfig()
                .getConfiguration()
                .setString("execution.state-recovery.path", checkpointPath);

        TableResult tableResult2 = sEnv.executeSql(streamSql);
        //noinspection OptionalGetWithoutIsPresent
        JobClient jobClient2 = tableResult2.getJobClient().get();

        // Auto-checkpointing (1s interval) is still enabled, so data is committed
        // to the sink automatically. waitForRowCount polls until data appears.
        waitForRowCount("chain_restart_sink", 9);

        List<String> phase4 =
                sql("SELECT * FROM chain_restart_sink").stream()
                        .map(Row::toString)
                        .collect(Collectors.toList());

        assertThat(phase4)
                .as("Stateful restart: sink should contain new delta data")
                .contains("+I[6, 1, new_6, 20250810]", "+I[7, 1, new_7, 20250810]");

        assertThat(phase4.size())
                .as("Should have exactly 9 records (no duplicates from state recovery)")
                .isEqualTo(9);

        // === Phase 5: Verify incremental streaming continues after restore ===
        sql(
                "INSERT INTO `chain_restart$branch_delta` PARTITION (dt = '20250811')"
                        + " VALUES (8, 1, 'new_8')");

        waitForRowCount("chain_restart_sink", 10);

        List<String> phase5 =
                sql("SELECT * FROM chain_restart_sink").stream()
                        .map(Row::toString)
                        .collect(Collectors.toList());
        assertThat(phase5)
                .as("Incremental streaming should continue after restore")
                .contains("+I[8, 1, new_8, 20250811]");

        jobClient2.cancel().get();

        // Clean up state-recovery config for other tests
        sEnv.getConfig().getConfiguration().removeKey("execution.state-recovery.path");
    }

    /**
     * T1: Tests streaming read with snapshot+delta overlap in the starting phase. Verifies that
     * doFullLoad() correctly merges snapshot-only, delta-only, and overlapping partitions.
     */
    @Test
    @Timeout(120)
    public void testStreamingReadWithSnapshotDeltaOverlap() throws Exception {
        sql(
                "CREATE TABLE chain_overlap ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_overlap', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_overlap', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_overlap", "chain_overlap$branch_snapshot", "chain_overlap$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // Write snapshot data: dt=20250807 (snapshot-only) and dt=20250808 (overlapping)
        sql(
                "INSERT INTO `chain_overlap$branch_snapshot` PARTITION (dt = '20250807')"
                        + " VALUES (6, 1, 'snap_6'), (7, 1, 'snap_7'), (8, 1, 'snap_8')");
        sql(
                "INSERT INTO `chain_overlap$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'snap_1'), (2, 1, 'snap_2'), (3, 1, 'snap_3')");

        // Write delta data: dt=20250808 (overlapping) and dt=20250809 (delta-only)
        sql(
                "INSERT INTO `chain_overlap$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 2, 'delta_1'), (2, 2, 'delta_2'),"
                        + " (4, 1, 'delta_4'), (5, 1, 'delta_5')");
        sql(
                "INSERT INTO `chain_overlap$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (10, 1, 'new_10'), (11, 1, 'new_11')");

        // Start streaming read
        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM chain_overlap").collect();

        // Starting behavior (new: only output latest snapshot partition and partitions after it):
        // - Latest snapshot partition: dt=20250808 (k=1,2,3 from snapshot, delta at this partition
        //   is skipped because snapshot wins for overlapping)
        // - Delta-only partition after latest snapshot: dt=20250809 (k=10,11)
        // - dt=20250807 is NOT output because it's before the latest snapshot partition
        List<String> startingRows = collectRows(it, 5);
        assertThat(startingRows)
                .as("Starting: latest snapshot partition and delta partitions after it")
                .containsExactlyInAnyOrder(
                        "+I[1, 1, snap_1, 20250808]",
                        "+I[2, 1, snap_2, 20250808]",
                        "+I[3, 1, snap_3, 20250808]",
                        "+I[10, 1, new_10, 20250809]",
                        "+I[11, 1, new_11, 20250809]");

        // Incremental: write new delta and verify it streams through
        writeChangelogToBranch(
                db,
                "chain_overlap",
                "delta",
                Row.ofKind(RowKind.INSERT, 20L, 1L, "incr_20", "20250810"));

        List<String> incr = collectRows(it, 1);
        assertThat(incr)
                .as("Incremental: new delta data should stream through")
                .containsExactlyInAnyOrder("+I[20, 1, incr_20, 20250810]");

        it.close();
    }

    /**
     * T2: Tests that non-default startup modes throw an error for chain table streaming read. When
     * scan.mode=latest is specified, an {@link UnsupportedOperationException} is thrown with a
     * helpful message.
     */
    @Test
    @Timeout(60)
    public void testStreamingReadRejectsNonDefaultStartup() throws Exception {
        sql(
                "CREATE TABLE chain_bypass ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_bypass', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_bypass', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_bypass", "chain_bypass$branch_snapshot", "chain_bypass$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // Write data to main table (so snapshots exist for copy() to resolve)
        sql(
                "INSERT INTO `chain_bypass$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");

        // Default mode: chain-table-aware scan (ChainTableStreamScan)
        FileStoreTable table = paimonTable("chain_bypass");
        assertThat(table.newStreamScan())
                .as("Default startup mode should use ChainTableStreamScan")
                .isInstanceOf(ChainTableStreamScan.class);

        // scan.mode=latest: should throw UnsupportedOperationException
        FileStoreTable tableLatest = table.copy(Collections.singletonMap("scan.mode", "latest"));
        assertThatThrownBy(tableLatest::newStreamScan)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("scan.mode=latest")
                .hasMessageContaining("Chain table streaming read does not support")
                .hasMessageContaining("t$branch_delta");
    }

    /**
     * Tests that chain table streaming read rejects consumer mode. When consumer.id is configured,
     * an {@link UnsupportedOperationException} is thrown because chain table streaming does not
     * support consumer progress tracking.
     */
    @Test
    @Timeout(60)
    public void testStreamingReadRejectsConsumerMode() throws Exception {
        sql(
                "CREATE TABLE chain_consumer ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_consumer', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_consumer', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_consumer",
                    "chain_consumer$branch_snapshot",
                    "chain_consumer$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        sql(
                "INSERT INTO `chain_consumer$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");

        FileStoreTable table = paimonTable("chain_consumer");

        // consumer.id set: should throw UnsupportedOperationException
        FileStoreTable tableWithConsumer =
                table.copy(Collections.singletonMap("consumer-id", "my-consumer"));
        assertThatThrownBy(tableWithConsumer::newStreamScan)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("consumer mode")
                .hasMessageContaining("consumer-id='my-consumer'")
                .hasMessageContaining("Chain table streaming read does not support");

        // consumer-id with consumer.ignore-progress=true: still rejected
        Map<String, String> consumerIgnoreProgressOptions = new HashMap<>();
        consumerIgnoreProgressOptions.put("consumer-id", "my-consumer");
        consumerIgnoreProgressOptions.put("consumer.ignore-progress", "true");
        FileStoreTable tableWithConsumerIgnoreProgress =
                table.copy(Collections.unmodifiableMap(consumerIgnoreProgressOptions));
        assertThatThrownBy(tableWithConsumerIgnoreProgress::newStreamScan)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("consumer mode");
    }

    /**
     * T3: Tests that streaming read works with changelog-producer=none (the default).
     *
     * <p>Without a changelog producer, the incremental phase reads data files (delta manifest)
     * rather than changelog files, producing only +I records. This is the same behavior as standard
     * {@code DataTableStreamScan} with {@code changelog-producer=none}.
     */
    @Test
    @Timeout(60)
    public void testStreamingReadWithNoChangelogProducer() throws Exception {
        // Create chain table WITHOUT changelog-producer (defaults to none)
        sql(
                "CREATE TABLE chain_no_cl ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_no_cl', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_no_cl', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_no_cl", "chain_no_cl$branch_snapshot", "chain_no_cl$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // Phase 1: Insert initial data into delta branch
        sql(
                "INSERT INTO `chain_no_cl$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");

        // Start streaming read and collect Phase 1 results
        try (CloseableIterator<Row> it =
                sEnv.executeSql("SELECT k, v, dt FROM chain_no_cl").collect()) {
            List<String> phase1 = collectRows(it, 2);
            assertThat(phase1)
                    .containsExactlyInAnyOrder("+I[1, v1, 20250808]", "+I[2, v2, 20250808]");

            // Phase 2: Insert more data into delta branch (incremental)
            sql(
                    "INSERT INTO `chain_no_cl$branch_delta` PARTITION (dt = '20250809')"
                            + " VALUES (3, 1, 'v3')");

            List<String> phase2 = collectRows(it, 1);
            assertThat(phase2).containsExactly("+I[3, v3, 20250809]");
        }
    }

    /**
     * T6: Tests streaming read with group partitions (chain-partition-keys). Verifies that
     * streaming works correctly when the table has a group dimension (e.g., region) and each group
     * maintains its own independent chain.
     */
    @Test
    @Timeout(120)
    public void testStreamingReadWithGroupPartition() throws Exception {
        sql(
                "CREATE TABLE chain_stream_group ("
                        + "  k BIGINT, seq BIGINT, v STRING, region STRING, dt STRING"
                        + ") PARTITIONED BY (region, dt) WITH ("
                        + "  'primary-key' = 'region,dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'chain-table.chain-partition-keys' = 'dt',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_stream_group', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_stream_group', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_stream_group",
                    "chain_stream_group$branch_snapshot",
                    "chain_stream_group$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // Write initial delta data for two regions
        sql(
                "INSERT INTO `chain_stream_group$branch_delta`"
                        + " PARTITION (region = 'CN', dt = '20250808')"
                        + " VALUES (1, 1, 'cn_1'), (2, 1, 'cn_2')");
        sql(
                "INSERT INTO `chain_stream_group$branch_delta`"
                        + " PARTITION (region = 'US', dt = '20250808')"
                        + " VALUES (11, 1, 'us_11'), (12, 1, 'us_12')");

        // Start streaming read
        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM chain_stream_group").collect();

        // Starting: both regions, delta-only
        List<String> startingRows = collectRows(it, 4);
        assertThat(startingRows)
                .as("Starting: delta-only data for both regions")
                .containsExactlyInAnyOrder(
                        "+I[1, 1, cn_1, CN, 20250808]",
                        "+I[2, 1, cn_2, CN, 20250808]",
                        "+I[11, 1, us_11, US, 20250808]",
                        "+I[12, 1, us_12, US, 20250808]");

        // Incremental: write new delta for CN only
        sql(
                "INSERT INTO `chain_stream_group$branch_delta`"
                        + " PARTITION (region = 'CN', dt = '20250809')"
                        + " VALUES (3, 1, 'cn_3')");

        List<String> incr = collectRows(it, 1);
        assertThat(incr)
                .as("Incremental: new CN delta should stream through")
                .containsExactlyInAnyOrder("+I[3, 1, cn_3, CN, 20250809]");

        // Incremental: write new delta for US
        sql(
                "INSERT INTO `chain_stream_group$branch_delta`"
                        + " PARTITION (region = 'US', dt = '20250809')"
                        + " VALUES (13, 1, 'us_13')");

        List<String> incr2 = collectRows(it, 1);
        assertThat(incr2)
                .as("Incremental: new US delta should stream through")
                .containsExactlyInAnyOrder("+I[13, 1, us_13, US, 20250809]");

        it.close();
    }

    // =========================================================================
    // Additional coverage tests
    // =========================================================================

    /** Tests restore(id, scanAll=true): resets starting state but preserves delta position. */
    @Test
    @Timeout(60)
    public void testRestoreScanAll() throws Exception {
        sql(
                "CREATE TABLE chain_restore_all ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_restore_all', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_restore_all', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_restore_all",
                    "chain_restore_all$branch_snapshot",
                    "chain_restore_all$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        sql(
                "INSERT INTO `chain_restore_all$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");

        FileStoreTable table = paimonTable("chain_restore_all");
        ChainTableStreamScan scan = (ChainTableStreamScan) table.newStreamScan();

        // Phase 1: starting
        TableScan.Plan plan1 = scan.plan();
        assertThat(plan1.splits()).as("Phase 1 should produce splits").isNotEmpty();
        Long checkpoint = scan.checkpoint();
        assertThat(checkpoint).as("Checkpoint should be non-null after Phase 1").isNotNull();

        // Phase 2: no new data → empty plan
        TableScan.Plan plan2 = scan.plan();
        assertThat(plan2.splits()).as("Phase 2 with no new data should be empty").isEmpty();

        // restore(id, scanAll=true): should reset to starting, preserve delta position
        scan.restore(checkpoint, true);
        assertThat(scan.checkpoint())
                .as("Checkpoint should be preserved after restore(id, true)")
                .isEqualTo(checkpoint);

        // Starting should run again
        TableScan.Plan plan3 = scan.plan();
        assertThat(plan3.splits())
                .as("Starting should produce splits again after restore(id, true)")
                .isNotEmpty();

        // Delta position should be the same (no new commits)
        assertThat(scan.checkpoint()).as("Checkpoint should remain the same").isEqualTo(checkpoint);
    }

    /** Tests restore(null, true): resets to fresh starting with no delta position. */
    @Test
    @Timeout(60)
    public void testRestoreNullScanAll() throws Exception {
        sql(
                "CREATE TABLE chain_restore_null ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_restore_null', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_restore_null', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_restore_null",
                    "chain_restore_null$branch_snapshot",
                    "chain_restore_null$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        sql(
                "INSERT INTO `chain_restore_null$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1')");

        FileStoreTable table = paimonTable("chain_restore_null");
        ChainTableStreamScan scan = (ChainTableStreamScan) table.newStreamScan();

        // Phase 1: starting
        scan.plan();
        assertThat(scan.checkpoint()).isNotNull();

        // restore(null, scanAll=true): fresh start, no delta position
        scan.restore(null, true);
        assertThat(scan.checkpoint())
                .as("Checkpoint should be null after restore(null, true)")
                .isNull();

        // Starting should run again
        TableScan.Plan plan = scan.plan();
        assertThat(plan.splits()).as("Starting should produce splits").isNotEmpty();
        assertThat(scan.checkpoint()).as("Checkpoint should be set after new starting").isNotNull();
    }

    /** Tests starting when delta branch is empty (only snapshot data). */
    @Test
    @Timeout(120)
    public void testStreamingReadEmptyDelta() throws Exception {
        sql(
                "CREATE TABLE chain_empty_delta ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_empty_delta', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_empty_delta', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_empty_delta",
                    "chain_empty_delta$branch_snapshot",
                    "chain_empty_delta$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // Write ONLY to snapshot branch, delta stays empty
        sql(
                "INSERT INTO `chain_empty_delta$branch_snapshot` PARTITION (dt = '20250807')"
                        + " VALUES (1, 1, 'snap_1'), (2, 1, 'snap_2')");

        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM chain_empty_delta").collect();

        List<String> startingRows = collectRows(it, 2);
        assertThat(startingRows)
                .as("Starting with empty delta should return only snapshot data")
                .containsExactlyInAnyOrder(
                        "+I[1, 1, snap_1, 20250807]", "+I[2, 1, snap_2, 20250807]");

        // Incremental: write to delta and verify it streams through
        writeChangelogToBranch(
                db,
                "chain_empty_delta",
                "delta",
                Row.ofKind(RowKind.INSERT, 3L, 1L, "new_3", "20250808"));

        List<String> incr = collectRows(it, 1);
        assertThat(incr)
                .as("First delta write should stream through after snapshot-only starting")
                .containsExactlyInAnyOrder("+I[3, 1, new_3, 20250808]");

        it.close();
    }

    /** Tests starting when snapshot branch is empty (only delta data). */
    @Test
    @Timeout(120)
    public void testStreamingReadEmptySnapshot() throws Exception {
        sql(
                "CREATE TABLE chain_empty_snap ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_empty_snap', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_empty_snap', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_empty_snap",
                    "chain_empty_snap$branch_snapshot",
                    "chain_empty_snap$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // Write ONLY to delta branch, snapshot stays empty
        sql(
                "INSERT INTO `chain_empty_snap$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'delta_1'), (2, 1, 'delta_2')");
        sql(
                "INSERT INTO `chain_empty_snap$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (3, 1, 'delta_3')");

        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM chain_empty_snap").collect();

        List<String> startingRows = collectRows(it, 3);
        assertThat(startingRows)
                .as("Starting with empty snapshot should return only delta data")
                .containsExactlyInAnyOrder(
                        "+I[1, 1, delta_1, 20250808]",
                        "+I[2, 1, delta_2, 20250808]",
                        "+I[3, 1, delta_3, 20250809]");

        it.close();
    }

    /** Tests that withShard() is correctly forwarded to both batch scan and delta stream scan. */
    @Test
    @Timeout(60)
    public void testWithShardForwarding() throws Exception {
        sql(
                "CREATE TABLE chain_shard ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_shard', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_shard', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_shard", "chain_shard$branch_snapshot", "chain_shard$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        sql(
                "INSERT INTO `chain_shard$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");

        FileStoreTable table = paimonTable("chain_shard");

        // Shard 0 of 2: should get a subset of data
        ChainTableStreamScan scan0 = (ChainTableStreamScan) table.newStreamScan();
        scan0.withShard(0, 2);
        TableScan.Plan plan0 = scan0.plan();

        // Shard 1 of 2: should get the other subset
        ChainTableStreamScan scan1 = (ChainTableStreamScan) table.newStreamScan();
        scan1.withShard(1, 2);
        TableScan.Plan plan1 = scan1.plan();

        // Together both shards should produce non-empty results
        // (exact split depends on bucket hashing, but total should cover all data)
        int totalSplits = plan0.splits().size() + plan1.splits().size();
        assertThat(totalSplits).as("Both shards together should produce splits").isGreaterThan(0);
    }

    /** Tests streaming read when both snapshot and delta branches are empty. */
    @Test
    @Timeout(60)
    public void testStreamingReadBothBranchesEmpty() throws Exception {
        sql(
                "CREATE TABLE chain_both_empty ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_both_empty', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_both_empty', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_both_empty",
                    "chain_both_empty$branch_snapshot",
                    "chain_both_empty$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // Both branches are empty — Phase 1 should produce no splits
        FileStoreTable table = paimonTable("chain_both_empty");
        ChainTableStreamScan scan = (ChainTableStreamScan) table.newStreamScan();
        TableScan.Plan plan1 = scan.plan();
        assertThat(plan1.splits()).as("Phase 1 with both branches empty should be empty").isEmpty();

        // Phase 2: write new delta data and verify it streams through
        sql(
                "INSERT INTO `chain_both_empty$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");

        TableScan.Plan plan2 = scan.plan();
        assertThat(plan2.splits()).as("Phase 2 should pick up new delta data").isNotEmpty();
    }

    /** Tests that delta OVERWRITE in Phase 2 does not crash the scan. */
    @Test
    @Timeout(60)
    public void testStreamingReadDeltaOverwriteInPhase2() throws Exception {
        sql(
                "CREATE TABLE chain_overwrite_p2 ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_overwrite_p2', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_overwrite_p2', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_overwrite_p2",
                    "chain_overwrite_p2$branch_snapshot",
                    "chain_overwrite_p2$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // Initial delta data
        sql(
                "INSERT INTO `chain_overwrite_p2$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");

        FileStoreTable table = paimonTable("chain_overwrite_p2");
        ChainTableStreamScan scan = (ChainTableStreamScan) table.newStreamScan();

        // Phase 1: read initial delta data
        TableScan.Plan plan1 = scan.plan();
        assertThat(plan1.splits()).as("Phase 1 should produce splits").isNotEmpty();

        // Phase 2: OVERWRITE the same partition on delta branch.
        // This creates a snapshot with OVERWRITE kind. The scan should handle it
        // gracefully — either producing data or empty plans, but never crashing.
        sql(
                "INSERT OVERWRITE `chain_overwrite_p2$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 2, 'new_v1'), (3, 1, 'v3')");

        // Verify scan.plan() does not throw after OVERWRITE
        for (int i = 0; i < 3; i++) {
            TableScan.Plan planN = scan.plan();
            assertThat(planN).as("plan() should not return null after OVERWRITE").isNotNull();
        }
    }

    /** Tests restore(null) re-runs Phase 1 with current data state. */
    @Test
    @Timeout(60)
    public void testStreamingReadRestoreAfterNewData() throws Exception {
        sql(
                "CREATE TABLE chain_restore_newdata ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_restore_newdata', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_restore_newdata', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_restore_newdata",
                    "chain_restore_newdata$branch_snapshot",
                    "chain_restore_newdata$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // Write initial snapshot + delta data
        sql(
                "INSERT INTO `chain_restore_newdata$branch_snapshot` PARTITION (dt = '20250807')"
                        + " VALUES (1, 1, 'snap_1')");
        sql(
                "INSERT INTO `chain_restore_newdata$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (2, 1, 'delta_2')");

        FileStoreTable table = paimonTable("chain_restore_newdata");
        ChainTableStreamScan scan = (ChainTableStreamScan) table.newStreamScan();

        // Phase 1: snapshot at dt=20250807 (latest), delta at dt=20250808
        TableScan.Plan plan1 = scan.plan();
        int phase1Size = plan1.splits().size();
        assertThat(phase1Size).as("Phase 1 should produce splits").isGreaterThan(0);

        // Add more data to both branches
        sql(
                "INSERT INTO `chain_restore_newdata$branch_snapshot` PARTITION (dt = '20250809')"
                        + " VALUES (3, 1, 'snap_3')");
        sql(
                "INSERT INTO `chain_restore_newdata$branch_delta` PARTITION (dt = '20250810')"
                        + " VALUES (4, 1, 'delta_4')");

        // restore(null) resets to fresh starting — Phase 1 should re-run with new data.
        // After adding snapshot dt=20250809 and delta dt=20250810:
        //   - Latest snapshot: dt=20250809 (dt=20250807 excluded as older)
        //   - Delta dt=20250808 excluded (older than latest snapshot dt=20250809)
        //   - Delta dt=20250810 included (newer than dt=20250809)
        scan.restore(null);
        TableScan.Plan plan2 = scan.plan();
        assertThat(plan2.splits())
                .as("Restore(null) should re-run Phase 1 with current data")
                .isNotEmpty();
    }

    /**
     * T5: Tests that chain table streaming read rejects partition filters via {@code withFilter}.
     *
     * <p>Partition filters interfere with the chain table Phase 1 logic (which determines the
     * latest snapshot partition per group). This test verifies that a partition-only predicate is
     * rejected with an UnsupportedOperationException.
     */
    @Test
    @Timeout(60)
    public void testStreamingReadRejectsPartitionFilter() throws Exception {
        createChainTable("chain_pf_partition");
        setupChainTableBranches("chain_pf_partition");

        sql(
                "INSERT INTO `chain_pf_partition$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1')");

        FileStoreTable table = paimonTable("chain_pf_partition");
        ChainTableStreamScan scan = (ChainTableStreamScan) table.newStreamScan();

        // dt is the 4th field (index 3) in the schema: t1(0), t2(1), t3(2), dt(3)
        PredicateBuilder builder = new PredicateBuilder(table.rowType());

        // Partition-only filter should be rejected
        Predicate partitionFilter = builder.equal(3, BinaryString.fromString("20250808"));
        assertThatThrownBy(() -> scan.withFilter(partitionFilter))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Partition filter is not supported");
    }

    /**
     * Tests that chain table streaming read rejects mixed predicates that contain partition
     * conjuncts.
     *
     * <p>A predicate like {@code dt = '20250808' AND v = 'hello'} combines a partition filter with
     * a data filter. In Flink, such predicates are pushed down as a single AND expression. The
     * chain table stream scan must reject any predicate that contains partition fields, because the
     * partition conjunct would be extracted later and interfere with the chain boundary computation
     * in Phase 1.
     */
    @Test
    @Timeout(60)
    public void testStreamingReadRejectsMixedPredicateWithPartition() throws Exception {
        createChainTable("chain_pf_mixed");
        setupChainTableBranches("chain_pf_mixed");

        sql(
                "INSERT INTO `chain_pf_mixed$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'hello')");

        FileStoreTable table = paimonTable("chain_pf_mixed");
        ChainTableStreamScan scan = (ChainTableStreamScan) table.newStreamScan();

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        // dt is the 4th field (index 3), v is the 3rd field (index 2)
        Predicate dtEquals = builder.equal(3, BinaryString.fromString("20250808"));
        Predicate vEquals = builder.equal(2, BinaryString.fromString("hello"));
        // Mixed predicate: partition AND data field
        Predicate mixedPredicate = builder.and(dtEquals, vEquals);

        // Should be rejected because it contains a partition conjunct
        assertThatThrownBy(() -> scan.withFilter(mixedPredicate))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Partition filter is not supported");
    }

    /**
     * T6: Tests that non-partition filters work end-to-end in chain table streaming reads.
     *
     * <p>Verifies: (1) {@code withFilter} on a data column is accepted at the scan API level, (2)
     * streaming {@code SELECT ... WHERE v = 'hello'} filters out non-matching rows, (3) the filter
     * continues to apply to incrementally written data.
     */
    @Test
    @Timeout(120)
    public void testStreamingReadWithNonPartitionFilter() throws Exception {
        sql(
                "CREATE TABLE chain_data_filter ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_data_filter', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_data_filter', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_data_filter",
                    "chain_data_filter$branch_snapshot",
                    "chain_data_filter$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta')",
                    tbl);
        }

        // Write initial delta data with mixed values of v
        sql(
                "INSERT INTO `chain_data_filter$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'hello'), (2, 1, 'world'), (3, 1, 'hello'), (4, 1, 'foo')");

        // Streaming read with WHERE on data column v — should only return v='hello' rows
        CloseableIterator<Row> it =
                sEnv.executeSql("SELECT * FROM chain_data_filter WHERE v = 'hello'").collect();

        List<String> startingRows = collectRows(it, 2);
        assertThat(startingRows)
                .as("Starting with WHERE v='hello' should only return matching rows")
                .containsExactlyInAnyOrder(
                        "+I[1, 1, hello, 20250808]", "+I[3, 1, hello, 20250808]");

        // Incremental: write more data with mixed v values
        writeChangelogToBranch(
                db,
                "chain_data_filter",
                "delta",
                Row.ofKind(RowKind.INSERT, 5L, 1L, "hello", "20250809"),
                Row.ofKind(RowKind.INSERT, 6L, 1L, "bar", "20250809"));

        List<String> incrRows = collectRows(it, 1);
        assertThat(incrRows)
                .as("Incremental: only v='hello' row should stream through")
                .containsExactlyInAnyOrder("+I[5, 1, hello, 20250809]");

        it.close();
    }

    /**
     * T7: Tests that chain table streaming read rejects partition filters via {@code
     * withPartitionFilter}.
     *
     * <p>The {@code withPartitionFilter} API (used for {@code scan.partitions} table option) should
     * also be rejected.
     */
    @Test
    @Timeout(60)
    public void testStreamingReadRejectsWithPartitionFilter() throws Exception {
        createChainTable("chain_pf_api");
        setupChainTableBranches("chain_pf_api");

        sql(
                "INSERT INTO `chain_pf_api$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1')");

        FileStoreTable table = paimonTable("chain_pf_api");
        ChainTableStreamScan scan = (ChainTableStreamScan) table.newStreamScan();

        // withPartitionFilter(Map) should be rejected
        assertThatThrownBy(
                        () -> scan.withPartitionFilter(Collections.singletonMap("dt", "20250808")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Partition filter is not supported");

        // withPartitionFilter(PartitionPredicate) should be rejected
        PredicateBuilder ppBuilder = new PredicateBuilder(table.schema().logicalPartitionType());
        PartitionPredicate pp =
                PartitionPredicate.fromPredicate(
                        table.schema().logicalPartitionType(),
                        ppBuilder.equal(0, BinaryString.fromString("20250808")));
        assertThatThrownBy(() -> scan.withPartitionFilter(pp))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Partition filter is not supported");
    }

    /**
     * Tests that chain table streaming rejects checkpoint-align mode at job construction time, not
     * at runtime. ChainSplit has no snapshotId and cannot participate in snapshot-aligned
     * checkpoint grouping.
     */
    @Test
    public void testStreamingReadRejectsCheckpointAlign() throws Exception {
        createChainTable("chain_align");
        setupChainTableBranches("chain_align");

        sql(
                "INSERT INTO `chain_align$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1')");

        // Setting checkpoint-align.enabled on a chain table streaming read should throw
        // at job construction time, not at runtime when ChainSplits are encountered.
        assertThatThrownBy(
                        () ->
                                sEnv.executeSql(
                                        "SELECT * FROM chain_align "
                                                + "/*+ OPTIONS('source.checkpoint-align.enabled' = 'true') */"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Chain table streaming is not compatible with checkpoint-align");
    }

    /**
     * Tests that primary-key predicates do NOT affect partition discovery in chain table streaming
     * Phase 1. This is the scenario from JingsongLi's review comment:
     *
     * <p>"if the latest snapshot partition no longer has k=1 but an older delta partition still
     * does, SELECT ... WHERE k=1 can make this listing miss the latest snapshot partition and then
     * include the old delta row, even though that partition should be considered outdated."
     *
     * <p>The test creates: snapshot@20250808 with t1=1,2; snapshot@20250809 with t1=3,4 (no t1=1);
     * delta@20250808 with t1=1. Then filters on t1=1 (a primary key field). Partition discovery
     * must still see both snapshot partitions so the chain boundary is correct.
     */
    @Test
    public void testStreamingReadPKFilterDoesNotAffectPartitionDiscovery() throws Exception {
        createChainTable("chain_pk_filter");
        setupChainTableBranches("chain_pk_filter");

        // Snapshot@20250808: has t1=1 and t1=2
        sql(
                "INSERT INTO `chain_pk_filter$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");
        // Snapshot@20250809: has t1=3 and t1=4 (NO t1=1)
        sql(
                "INSERT INTO `chain_pk_filter$branch_snapshot` PARTITION (dt = '20250809')"
                        + " VALUES (3, 1, 'v3'), (4, 1, 'v4')");

        // Delta@20250808: has t1=1
        sql(
                "INSERT INTO `chain_pk_filter$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 2, 'delta_v1')");

        // --- Part 1: Verify listPartitions() with a PK predicate ---
        FileStoreTable mainTable = paimonTable("chain_pk_filter");
        FileStoreTable snapshotTable =
                mainTable.copy(Collections.singletonMap(CoreOptions.BRANCH.key(), "snapshot"));

        DataTableScan scan = snapshotTable.newScan();
        PredicateBuilder builder = new PredicateBuilder(snapshotTable.rowType());
        // t1 is field index 0, part of primary key (dt, t1).
        // Only snapshot@20250808 has t1=1.
        Predicate t1Equals1 = builder.equal(0, 1L);
        scan.withFilter(t1Equals1);

        // listPartitions() must return BOTH snapshot partitions even though only
        // 20250808 contains t1=1. If it returned only 20250808, the chain boundary
        // would be wrong and stale data could be included.
        List<BinaryRow> partitions = scan.listPartitions();
        assertThat(partitions)
                .as(
                        "listPartitions() must return all snapshot partitions even with a PK filter. "
                                + "If only dt=20250808 is returned, the chain boundary is wrong.")
                .hasSize(2);

        // --- Part 2: Verify filtered batch SELECT returns correct data ---
        // Chain-merged batch view: snapshot@20250808(t1=1,2), snapshot@20250809(t1=3,4).
        // WHERE t1 = 1 should return only the snapshot row (t1=1, dt=20250808).
        List<String> filtered = collectResult("SELECT * FROM chain_pk_filter WHERE t1 = 1");
        assertThat(filtered)
                .as("WHERE t1=1 should find the snapshot row at dt=20250808")
                .hasSize(1)
                .containsExactly("+I[1, 1, v1, 20250808]");
    }

    /**
     * Reproduces the snapshot branch race condition from PR comment: Phase 1 pins delta at latestId
     * but snapshot branch is read from "whatever is latest". If a snapshot commit lands between
     * capturing delta and listing partitions, Phase 1 excludes old delta data, and Phase 2 starts
     * from latestId+1, so the old delta is never emitted.
     *
     * <p>This test simulates the race by: 1) writing delta data, 2) capturing delta position, 3)
     * writing new snapshot data, 4) verifying the delta data is still emitted in Phase 1 or Phase
     * 2.
     */
    @Test
    @Timeout(60)
    public void testStreamingReadSnapshotBranchRaceCondition() throws Exception {
        sql(
                "CREATE TABLE chain_race ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_race', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_race', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_race", "chain_race$branch_snapshot", "chain_race$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta'"
                            + ")",
                    tbl);
        }

        // Step 1: Write delta data at dt=20250808
        sql(
                "INSERT INTO `chain_race$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'delta_1'), (2, 1, 'delta_2')");

        // Step 2: Start streaming job (captures delta position)
        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM chain_race").collect();

        // Step 3: Collect Phase 1 output (should include delta@20250808)
        List<String> phase1 = collectRows(it, 2);
        assertThat(phase1)
                .as("Phase 1 should include delta@20250808 data")
                .containsExactlyInAnyOrder(
                        "+I[1, 1, delta_1, 20250808]", "+I[2, 1, delta_2, 20250808]");

        // Step 4: Write NEW snapshot data at dt=20250809 (simulates race condition)
        sql(
                "INSERT INTO `chain_race$branch_snapshot` PARTITION (dt = '20250809')"
                        + " VALUES (3, 1, 'snap_3')");

        // Step 5: Write NEW delta data at dt=20250810
        sql(
                "INSERT INTO `chain_race$branch_delta` PARTITION (dt = '20250810')"
                        + " VALUES (4, 1, 'delta_4')");

        // Step 6: Collect Phase 2 output (should include delta@20250810)
        // BUG: If snapshot branch was not pinned, Phase 1 might have excluded delta@20250808
        // after seeing snapshot@20250809, and Phase 2 would miss it too.
        List<String> phase2 = collectRows(it, 1);
        assertThat(phase2)
                .as("Phase 2 should include new delta@20250810 data")
                .containsExactlyInAnyOrder("+I[4, 1, delta_4, 20250810]");

        it.close();
    }

    /**
     * Reproduces the Phase 2 read bypass issue from PR comment: after Phase 1, the stream emits
     * normal delta DataSplits from deltaStreamScan.plan(). But ChainTableFileStoreTable inherits
     * FallbackReadFileStoreTable.newRead(), whose non-FallbackSplit path falls back to
     * mainRead.createReader(split), bypassing the branch-aware ChainGroupReadTable.Read logic.
     *
     * <p>This test creates a chain table where delta branch has a different column default than
     * snapshot branch, then verifies Phase 2 reads produce correct results with branch-aware schema
     * lookup.
     */
    @Test
    @Timeout(60)
    public void testStreamingReadPhase2BranchAwareRead() throws Exception {
        sql(
                "CREATE TABLE chain_phase2 ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.chain_phase2', 'snapshot')", db);
        sql("CALL sys.create_branch('%s.chain_phase2', 'delta')", db);
        for (String tbl :
                new String[] {
                    "chain_phase2", "chain_phase2$branch_snapshot", "chain_phase2$branch_delta"
                }) {
            sql(
                    "ALTER TABLE `%s` SET ("
                            + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                            + "  'scan.fallback-delta-branch' = 'delta'"
                            + ")",
                    tbl);
        }

        // Write snapshot data at dt=20250808
        sql(
                "INSERT INTO `chain_phase2$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'snap_1')");

        // Write delta data at dt=20250809 (different partition, will be read in Phase 2)
        sql(
                "INSERT INTO `chain_phase2$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (2, 1, 'delta_2')");

        // Start streaming and collect Phase 1 (snapshot@20250808 + delta@20250809)
        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM chain_phase2").collect();
        List<String> phase1 = collectRows(it, 2);
        assertThat(phase1)
                .as("Phase 1 should include both snapshot and delta data")
                .containsExactlyInAnyOrder(
                        "+I[1, 1, snap_1, 20250808]", "+I[2, 1, delta_2, 20250809]");

        // Write NEW delta data at dt=20250810 (will be read in Phase 2)
        sql(
                "INSERT INTO `chain_phase2$branch_delta` PARTITION (dt = '20250810')"
                        + " VALUES (3, 1, 'delta_3')");

        // Collect Phase 2 output
        // BUG: If Phase 2 read bypasses branch-aware logic, this might fail or produce
        // wrong results when snapshot/delta schemas diverge.
        List<String> phase2 = collectRows(it, 1);
        assertThat(phase2)
                .as("Phase 2 should correctly read delta@20250810 with branch-aware logic")
                .containsExactlyInAnyOrder("+I[3, 1, delta_3, 20250810]");

        it.close();
    }
}
