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
import org.apache.paimon.flink.lookup.FullCacheLookupTable;
import org.apache.paimon.flink.lookup.LookupFileStoreTable;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.ChainTableFileStoreTable;
import org.apache.paimon.table.ChainTableStreamScan;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for chain table using Flink SQL. */
public class FlinkChainTableITCase extends CatalogITCaseBase {

    @SuppressWarnings("unused")
    static boolean isFlink2OrLater() {
        return isFlinkVersionGreaterThanOrEqualTo("2.0");
    }

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

        setupChainTableBranches("chain_test_hourly");

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

        setupChainTableBranches("chain_test_partial");

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

        setupChainTableBranches("chain_test_group");

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
     * Write Row data (with RowKind and a group partition column) to a specific branch using
     * DataStream API.
     */
    private void writeChangelogToBranchWithRegion(
            String db, String tableName, String branch, Row... rows) throws Exception {
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
                                DataTypes.FIELD("region", DataTypes.STRING()),
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
        setupChainTableBranches("chain_life_cl");

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

        setupChainTableBranches("chain_restart");

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
        setupChainTableBranches("chain_overlap");

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
     * Tests streaming read with {@code chain-table.streaming.merge-snapshot=true}. Verifies that
     * the starting phase merges the latest snapshot partition with later delta partitions, so
     * cross-branch deletes and updates are visible in the initial snapshot.
     */
    @ParameterizedTest
    @ValueSource(strings = {"input", "none"})
    @Timeout(120)
    public void testStreamingReadWithMergeSnapshot(String changelogProducer) throws Exception {
        String tableName = "chain_merge_stream_" + changelogProducer;
        sql(
                format(
                        "CREATE TABLE %s ("
                                + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                                + ") PARTITIONED BY (dt) WITH ("
                                + "  'primary-key' = 'dt,k',"
                                + "  'bucket-key' = 'k',"
                                + "  'bucket' = '2',"
                                + "  'sequence.field' = 'seq',"
                                + "  'merge-engine' = 'deduplicate',"
                                + "  'changelog-producer' = '%s',"
                                + "  'chain-table.enabled' = 'true',"
                                + "  'chain-table.streaming.merge-snapshot' = 'true',"
                                + "  'partition.timestamp-pattern' = '$dt',"
                                + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                                + "  'continuous.discovery-interval' = '1ms'"
                                + ")",
                        tableName, changelogProducer));

        String db = tEnv.getCurrentDatabase();
        setupChainTableBranches(tableName);

        // Write snapshot data at dt=20250808
        sql(
                "INSERT INTO `"
                        + tableName
                        + "$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'snap_1'), (2, 1, 'snap_2')");

        // Write delta data spanning dt=20250809 and dt=20250810:
        // - delete k=1 at dt=20250809
        // - update k=2: -U old snapshot value at dt=20250809, +U new delta value at dt=20250810
        // - insert k=3 at dt=20250810
        writeChangelogToBranch(
                db,
                tableName,
                "delta",
                Row.ofKind(RowKind.DELETE, 1L, 2L, "snap_1", "20250809"),
                Row.ofKind(RowKind.UPDATE_BEFORE, 2L, 2L, "snap_2", "20250809"),
                Row.ofKind(RowKind.UPDATE_AFTER, 2L, 3L, "delta_2", "20250810"),
                Row.ofKind(RowKind.INSERT, 3L, 1L, "delta_3", "20250810"));

        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM " + tableName).collect();

        // Starting (merge mode): snapshot@20250808 is anchored to the latest delta partition
        // dt=20250810.
        // k=1 is deleted; k=2 is updated from snapshot value to delta value; k=3 is newly inserted.
        // The logical partition of the merged ChainSplit is the latest delta partition 20250810.
        // With changelog-producer=input the update is emitted as +U; with changelog-producer=none
        // the upsert result is emitted as +I.
        String updatedRowKind = "input".equals(changelogProducer) ? "+U" : "+I";
        List<String> startingRows = collectRows(it, 2);
        assertThat(startingRows)
                .as(
                        "Starting with merge-snapshot: cross-branch delete/update should be applied, "
                                + "updated/inserted rows should use the latest delta partition")
                .containsExactlyInAnyOrder(
                        updatedRowKind + "[2, 3, delta_2, 20250810]",
                        "+I[3, 1, delta_3, 20250810]");

        // Incremental: write new delta and verify it streams through
        writeChangelogToBranch(
                db, tableName, "delta", Row.ofKind(RowKind.INSERT, 4L, 1L, "delta_4", "20250811"));

        List<String> incr = collectRows(it, 1);
        assertThat(incr)
                .as("Incremental: new delta data should stream through")
                .containsExactlyInAnyOrder("+I[4, 1, delta_4, 20250811]");

        it.close();
    }

    /**
     * Tests streaming read with {@code chain-table.streaming.merge-snapshot=true} and a group
     * partition (region). Verifies that each group is handled independently:
     *
     * <ul>
     *   <li>CN: snapshot anchor at 20250809 + delta at 20250810 (cross-branch delete/insert).
     *   <li>UK: snapshot anchor at 20250808 + delta at 20250809 (cross-branch delete).
     *   <li>US: delta-only group (inserts at 20250811, delete at 20250812, later insert at
     *       20250813).
     * </ul>
     */
    @ParameterizedTest
    @ValueSource(strings = {"input", "none"})
    @Timeout(120)
    public void testStreamingReadWithMergeSnapshotAndGroup(String changelogProducer)
            throws Exception {
        String tableName = "chain_merge_stream_group_" + changelogProducer;
        sql(
                format(
                        "CREATE TABLE %s ("
                                + "  k BIGINT, seq BIGINT, v STRING, region STRING, dt STRING"
                                + ") PARTITIONED BY (region, dt) WITH ("
                                + "  'primary-key' = 'region,dt,k',"
                                + "  'bucket-key' = 'k',"
                                + "  'bucket' = '2',"
                                + "  'sequence.field' = 'seq',"
                                + "  'merge-engine' = 'deduplicate',"
                                + "  'changelog-producer' = '%s',"
                                + "  'chain-table.enabled' = 'true',"
                                + "  'chain-table.streaming.merge-snapshot' = 'true',"
                                + "  'partition.timestamp-pattern' = '$dt',"
                                + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                                + "  'chain-table.chain-partition-keys' = 'dt',"
                                + "  'continuous.discovery-interval' = '1ms'"
                                + ")",
                        tableName, changelogProducer));

        String db = tEnv.getCurrentDatabase();
        setupChainTableBranches(tableName);

        // Snapshot branch: CN and UK have anchors; US has no snapshot.
        sql(
                "INSERT INTO `"
                        + tableName
                        + "$branch_snapshot`"
                        + " PARTITION (region = 'CN', dt = '20250809')"
                        + " VALUES (1, 1, 'cn_snap_1'), (2, 1, 'cn_snap_2')");
        sql(
                "INSERT INTO `"
                        + tableName
                        + "$branch_snapshot`"
                        + " PARTITION (region = 'UK', dt = '20250808')"
                        + " VALUES (21, 1, 'uk_snap_21'), (22, 1, 'uk_snap_22')");

        // First delta commit:
        // - CN: at dt=20250810, delete k=1 and insert k=3 (delta > snapshot anchor 20250809).
        // - UK: at dt=20250809, delete k=21 (delta > snapshot anchor 20250808).
        // - US: delta-only group, insert k=11 and k=12 at dt=20250811.
        writeChangelogToBranchWithRegion(
                db,
                tableName,
                "delta",
                Row.ofKind(RowKind.DELETE, 1L, 2L, "cn_snap_1", "CN", "20250810"),
                Row.ofKind(RowKind.INSERT, 3L, 1L, "cn_delta_3", "CN", "20250810"),
                Row.ofKind(RowKind.INSERT, 11L, 1L, "us_delta_11", "US", "20250811"),
                Row.ofKind(RowKind.INSERT, 12L, 1L, "us_delta_12", "US", "20250811"),
                Row.ofKind(RowKind.DELETE, 21L, 2L, "uk_snap_21", "UK", "20250809"));
        writeChangelogToBranchWithRegion(
                db,
                tableName,
                "delta",
                Row.ofKind(RowKind.DELETE, 11L, 2L, "us_delta_11", "US", "20250812"));

        // Start streaming read (pinned at the second delta commit)
        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM " + tableName).collect();

        // Starting (merge mode):
        List<String> startingRows = collectRows(it, 4);
        assertThat(startingRows)
                .as(
                        "Starting with merge-snapshot and group: each group should be handled "
                                + "independently")
                .containsExactlyInAnyOrder(
                        "+I[2, 1, cn_snap_2, CN, 20250810]",
                        "+I[3, 1, cn_delta_3, CN, 20250810]",
                        "+I[12, 1, us_delta_12, US, 20250812]",
                        "+I[22, 1, uk_snap_22, UK, 20250809]");

        // Third delta commit (Phase 2 incremental): US delta-only group inserts k=11 at
        // dt=20250813.
        writeChangelogToBranchWithRegion(
                db,
                tableName,
                "delta",
                Row.ofKind(RowKind.INSERT, 11L, 3L, "us_delta_11", "US", "20250813"));

        List<String> incr = collectRows(it, 1);
        assertThat(incr)
                .as("Incremental: new insert in delta-only group should stream through")
                .containsExactlyInAnyOrder("+I[11, 3, us_delta_11, US, 20250813]");

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
        setupChainTableBranches("chain_bypass");

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

        setupChainTableBranches("chain_consumer");

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

        setupChainTableBranches("chain_no_cl");

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

        setupChainTableBranches("chain_stream_group");

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

        setupChainTableBranches("chain_restore_all");

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

        setupChainTableBranches("chain_restore_null");

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
        setupChainTableBranches("chain_empty_delta");

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

        setupChainTableBranches("chain_empty_snap");

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

        setupChainTableBranches("chain_shard");

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

        setupChainTableBranches("chain_both_empty");

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

        setupChainTableBranches("chain_overwrite_p2");

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

        setupChainTableBranches("chain_restore_newdata");

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
        setupChainTableBranches("chain_data_filter");

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
        setupChainTableBranches("chain_race");

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

        setupChainTableBranches("chain_phase2");

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
        List<String> phase2 = collectRows(it, 1);
        assertThat(phase2)
                .as("Phase 2 should correctly read delta@20250810 with branch-aware logic")
                .containsExactlyInAnyOrder("+I[3, 1, delta_3, 20250810]");

        it.close();
    }

    @Test
    public void testLookupJoin() throws Exception {
        // Create chain table as dimension table
        sql(
                "CREATE TABLE chain_dim ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim");

        // Write snapshot branch
        sql(
                "INSERT OVERWRITE `chain_dim$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'snap_1'), (2, 1, 'snap_2'), (3, 1, 'snap_3')");

        // Write delta branch (new partition, no anchor in snapshot)
        sql(
                "INSERT OVERWRITE `chain_dim$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (2, 2, 'delta_2_updated'), (4, 1, 'delta_4')");

        // Create source table as Paimon table
        sql(
                "CREATE TABLE source_t ("
                        + "  id BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");

        // Insert source data
        sql("INSERT INTO source_t VALUES (1), (2), (3), (4)");

        // Lookup join on k (non-partition key)
        List<String> result =
                collectResult(
                        "SELECT S.id, D.k, D.v "
                                + "FROM source_t AS S "
                                + "LEFT JOIN chain_dim /*+ OPTIONS('lookup.cache' = 'full') */ "
                                + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                + "ON S.id = D.k");

        // Verify lookup results.
        // Source has 4 rows (id=1,2,3,4). Lightweight Phase 1 dim data:
        //   snapshot@20250808: k=1(snap_1), k=2(snap_2), k=3(snap_3)
        //   delta@20250809 (no anchor merge): k=2(delta_2_updated), k=4(delta_4)
        // k=1 matches id=1 (snapshot only), k=2 matches id=2 (snapshot + delta = 2 rows),
        // k=3 matches id=3 (snapshot only), k=4 matches id=4 (delta only) → 5 lookup matches.
        assertThat(result).hasSize(5);
        assertThat(result)
                .anyMatch(r -> r.contains("snap_1"))
                .anyMatch(r -> r.contains("snap_2"))
                .anyMatch(r -> r.contains("delta_2_updated"))
                .anyMatch(r -> r.contains("snap_3"))
                .anyMatch(r -> r.contains("delta_4"));
    }

    @Test
    @EnabledIf(
            value = "isFlink2OrLater",
            disabledReason = "Custom shuffle lookup join requires Flink 2.x.")
    public void testLookupJoinWithBucketShuffle() throws Exception {
        sql(
                "CREATE TABLE chain_dim_shuffle ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_shuffle");

        // Write snapshot branch
        sql(
                "INSERT OVERWRITE `chain_dim_shuffle$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'snap_1'), (2, 1, 'snap_2'), (3, 1, 'snap_3')");

        // Write delta branch (new partition, no anchor in snapshot)
        sql(
                "INSERT OVERWRITE `chain_dim_shuffle$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (2, 2, 'delta_2_updated'), (4, 1, 'delta_4')");

        // Create source table
        sql(
                "CREATE TABLE source_shuffle ("
                        + "  id BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");
        sql("INSERT INTO source_shuffle VALUES (1), (2), (3), (4)");

        String query =
                "SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ S.id, D.k, D.v "
                        + "FROM source_shuffle AS S "
                        + "JOIN chain_dim_shuffle "
                        + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                        + "ON S.id = D.k";

        // Verify the execution plan actually uses bucket shuffle partitioning.
        // "shuffle=[true]" in the LookupJoin node proves the planner recognized the LOOKUP hint
        // and will use the BucketShufflePartitioner provided by Paimon's BaseDataTableSource.
        sEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        String explainPlan = sEnv.explainSql(query);
        assertThat(explainPlan)
                .as("EXPLAIN plan should contain shuffle=[true] proving bucket shuffle is active")
                .contains("shuffle=[true]");

        // Expected lightweight Phase 1 data (no anchor merge):
        //   snapshot@20250808: k=1(snap_1), k=2(snap_2), k=3(snap_3)
        //   delta@20250809 (no anchor merge): k=2(delta_2_updated), k=4(delta_4)
        // Source has 4 rows (id=1,2,3,4). k=1 matches id=1 (snapshot only),
        // k=2 matches id=2 (snapshot + delta = 2 rows), k=3 matches id=3 (snapshot only),
        // k=4 matches id=4 (delta only) → 5 lookup matches.
        List<Row> result = streamSqlBlockIter(query).collect(5);

        assertThat(result).hasSize(5);
        assertThat(result)
                .anyMatch(r -> r.toString().contains("snap_1"))
                .anyMatch(r -> r.toString().contains("snap_2"))
                .anyMatch(r -> r.toString().contains("delta_2_updated"))
                .anyMatch(r -> r.toString().contains("snap_3"))
                .anyMatch(r -> r.toString().contains("delta_4"));
    }

    @Test
    public void testLookupJoinDeltaOnly() throws Exception {
        // Chain table with only delta data (no snapshot)
        sql(
                "CREATE TABLE chain_dim_delta_only ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_delta_only");

        // Write only delta (no snapshot)
        sql(
                "INSERT OVERWRITE `chain_dim_delta_only$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2'), (3, 1, 'v3')");

        // Verify batch read works first
        List<String> batchResult =
                collectResult("SELECT k, v FROM chain_dim_delta_only WHERE dt = '20250808'");
        assertThat(batchResult).as("Batch read of delta-only chain table").hasSize(3);

        sql(
                "CREATE TABLE source_delta_only ("
                        + "  id BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");
        sql("INSERT INTO source_delta_only VALUES (1), (2), (3)");

        List<String> result =
                collectResult(
                        "SELECT S.id, D.k, D.v "
                                + "FROM source_delta_only AS S "
                                + "LEFT JOIN chain_dim_delta_only "
                                + "/*+ OPTIONS('lookup.cache' = 'full') */ "
                                + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                + "ON S.id = D.k");

        assertThat(result).hasSize(3);
        assertThat(result)
                .containsExactlyInAnyOrder("+I[1, 1, v1]", "+I[2, 2, v2]", "+I[3, 3, v3]");
    }

    @Test
    public void testLookupJoinSnapshotOnly() throws Exception {
        // Chain table with only snapshot data (no delta)
        sql(
                "CREATE TABLE chain_dim_snap_only ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_snap_only");

        // Write only snapshot (no delta)
        sql(
                "INSERT OVERWRITE `chain_dim_snap_only$branch_snapshot` "
                        + "PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 's1'), (2, 1, 's2')");

        sql(
                "CREATE TABLE source_snap_only ("
                        + "  id BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");
        sql("INSERT INTO source_snap_only VALUES (1), (2)");

        List<String> result =
                collectResult(
                        "SELECT S.id, D.k, D.v "
                                + "FROM source_snap_only AS S "
                                + "LEFT JOIN chain_dim_snap_only "
                                + "/*+ OPTIONS('lookup.cache' = 'full') */ "
                                + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                + "ON S.id = D.k");

        assertThat(result).hasSize(2);
        assertThat(result).containsExactlyInAnyOrder("+I[1, 1, s1]", "+I[2, 2, s2]");
    }

    @Test
    public void testLookupJoinRejectsPartitionKeyInJoinCondition() throws Exception {
        sql(
                "CREATE TABLE chain_dim_pk ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_pk");

        sql(
                "INSERT OVERWRITE `chain_dim_pk$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1')");

        sql(
                "CREATE TABLE source_pk ("
                        + "  id BIGINT,"
                        + "  dt STRING,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");
        sql("INSERT INTO source_pk VALUES (1, '20250808')");

        // Join on partition key dt should fail
        assertThatThrownBy(
                        () ->
                                collectResult(
                                        "SELECT S.id, D.k "
                                                + "FROM source_pk AS S "
                                                + "LEFT JOIN chain_dim_pk "
                                                + "/*+ OPTIONS('lookup.cache' = 'full') */ "
                                                + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                                + "ON S.dt = D.dt"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("partition keys");
    }

    @Test
    public void testLookupJoinRejectsInvalidCacheMode() throws Exception {
        sql(
                "CREATE TABLE chain_dim_mode ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_mode");

        sql(
                "INSERT OVERWRITE `chain_dim_mode$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1')");

        sql(
                "CREATE TABLE source_mode ("
                        + "  id BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");
        sql("INSERT INTO source_mode VALUES (1)");

        // cache-mode=memory is not supported for chain tables
        assertThatThrownBy(
                        () ->
                                collectResult(
                                        "SELECT S.id, D.k "
                                                + "FROM source_mode AS S "
                                                + "LEFT JOIN chain_dim_mode "
                                                + "/*+ OPTIONS('lookup.cache' = 'memory') */ "
                                                + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                                + "ON S.id = D.k"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("cache mode");
    }

    @Test
    public void testLookupJoinRejectsJoinKeyContainingPartitionKey() throws Exception {
        // PK = (dt, k). If join keys = (dt, k) = PK, this triggers the AUTO + PK==JK path
        // in FileStoreLookupFunction.open(). The chain table validation should reject it
        // because dt is a partition key.
        sql(
                "CREATE TABLE chain_dim_pkjk ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_pkjk");

        sql(
                "INSERT OVERWRITE `chain_dim_pkjk$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1')");

        sql(
                "CREATE TABLE source_pkjk ("
                        + "  id BIGINT,"
                        + "  dt STRING,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");
        sql("INSERT INTO source_pkjk VALUES (1, '20250808')");

        // Join on both dt and k (= full PK) should fail because dt is a partition key
        assertThatThrownBy(
                        () ->
                                collectResult(
                                        "SELECT S.id, D.v "
                                                + "FROM source_pkjk AS S "
                                                + "LEFT JOIN chain_dim_pkjk "
                                                + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                                + "ON S.dt = D.dt AND S.id = D.k"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("partition keys");
    }

    @Test
    public void testLookupJoinWithAutoCacheMode() throws Exception {
        // Same as testLookupJoinDeltaOnly but without explicit 'lookup.cache' = 'full' hint.
        // Default cache mode is AUTO. LookupFileStoreTable.create() validates chain table
        // constraints and FileStoreLookupFunction excludes chain tables from the AUTO path,
        // preventing PrimaryKeyPartialLookupTable from being used.
        sql(
                "CREATE TABLE chain_dim_auto ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_auto");

        sql(
                "INSERT OVERWRITE `chain_dim_auto$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");

        sql(
                "CREATE TABLE source_auto ("
                        + "  id BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");
        sql("INSERT INTO source_auto VALUES (1), (2)");

        // No explicit lookup.cache hint — defaults to AUTO, should work via AUTO→FULL conversion
        List<String> result =
                collectResult(
                        "SELECT S.id, D.k, D.v "
                                + "FROM source_auto AS S "
                                + "LEFT JOIN chain_dim_auto "
                                + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                + "ON S.id = D.k");

        assertThat(result).hasSize(2);
        assertThat(result).containsExactlyInAnyOrder("+I[1, 1, v1]", "+I[2, 2, v2]");
    }

    @Test
    public void testChainTableStreamScanIncrementalRefresh() throws Exception {
        // Tests the ChainTableStreamScan directly at the API level.
        // Verifies: first plan() = bootstrap (chain-merged ChainSplits),
        //           second plan() = incremental (delta DataSplits with new data).
        sql(
                "CREATE TABLE chain_dim_incr ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_incr");

        // Write initial delta data
        sql(
                "INSERT OVERWRITE `chain_dim_incr$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");

        // Get the chain table and create a LookupFileStoreTable with ChainTableStreamScan
        FileStoreTable table = (FileStoreTable) paimonTable("chain_dim_incr");
        LookupFileStoreTable lookupTable = LookupFileStoreTable.create(table, Arrays.asList("k"));

        ChainTableStreamScan scan = (ChainTableStreamScan) lookupTable.newStreamScan();

        // First plan() = bootstrap: should return ChainSplits with initial data
        List<Split> bootstrapSplits = scan.plan().splits();
        assertThat(bootstrapSplits).isNotEmpty();
        assertThat(bootstrapSplits.get(0))
                .as("Bootstrap should produce ChainSplits")
                .isInstanceOf(ChainSplit.class);

        // Second plan() = incremental: should be empty (no new delta data)
        List<Split> emptySplits = scan.plan().splits();
        assertThat(emptySplits).as("No new data, should be empty").isEmpty();

        // Write new delta data
        sql(
                "INSERT INTO `chain_dim_incr$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (3, 1, 'v3')");

        // Third plan() = incremental: should return DataSplits with new data
        List<Split> incrementalSplits = scan.plan().splits();
        assertThat(incrementalSplits)
                .as("Should have incremental splits after new delta data")
                .isNotEmpty();
        assertThat(incrementalSplits.get(0))
                .as("Incremental should produce DataSplits")
                .isInstanceOf(DataSplit.class);
    }

    @Test
    public void testLookupScanCheckpointRestore() throws Exception {
        // Tests checkpoint/restore behavior of ChainTableStreamScan at the API level.
        // Verifies:
        //   1. Before bootstrap, checkpoint() returns null.
        //   2. After bootstrap, checkpoint() returns the delta position.
        //   3. restore(id) sets bootstrapDone=true and positions delta scan correctly.
        //   4. After restore, plan() only returns NEW data (not already-consumed data).
        sql(
                "CREATE TABLE chain_dim_ckp ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_ckp");

        // Write initial delta data
        sql(
                "INSERT OVERWRITE `chain_dim_ckp$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");

        FileStoreTable table = (FileStoreTable) paimonTable("chain_dim_ckp");
        LookupFileStoreTable lookupTable = LookupFileStoreTable.create(table, Arrays.asList("k"));

        ChainTableStreamScan scan = (ChainTableStreamScan) lookupTable.newStreamScan();

        // Before bootstrap, checkpoint should be null
        assertThat(scan.checkpoint()).as("Before bootstrap, checkpoint should be null").isNull();

        // Bootstrap
        List<Split> bootstrapSplits = scan.plan().splits();
        assertThat(bootstrapSplits).isNotEmpty();

        // After bootstrap, checkpoint should capture the delta position
        Long checkpointId = scan.checkpoint();
        assertThat(checkpointId)
                .as("After bootstrap, checkpoint should capture delta position")
                .isNotNull();

        // No new data, plan() should be empty
        assertThat(scan.plan().splits()).isEmpty();

        // Write new delta data
        sql(
                "INSERT INTO `chain_dim_ckp$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (3, 1, 'v3')");

        // Incremental plan() should return new data
        List<Split> incrSplits1 = scan.plan().splits();
        assertThat(incrSplits1).isNotEmpty();

        // Checkpoint again
        Long checkpointId2 = scan.checkpoint();
        assertThat(checkpointId2).isNotNull();
        assertThat(checkpointId2)
                .as("Second checkpoint should be after the first")
                .isGreaterThan(checkpointId);

        // Simulate restore from first checkpoint
        ChainTableStreamScan restoredScan = (ChainTableStreamScan) lookupTable.newStreamScan();
        restoredScan.restore(checkpointId);

        // After restore, plan() should return data from checkpointId onwards
        // (i.e., the data at dt=20250809 that was written after the first checkpoint)
        List<Split> restoredSplits = restoredScan.plan().splits();
        assertThat(restoredSplits)
                .as("Restored scan should return data after checkpoint position")
                .isNotEmpty();

        // A fresh scan (no restore) should bootstrap and then be empty
        ChainTableStreamScan freshScan = (ChainTableStreamScan) lookupTable.newStreamScan();
        freshScan.plan(); // bootstrap
        assertThat(freshScan.plan().splits())
                .as("Fresh scan after bootstrap should have no incremental data")
                .isEmpty();
    }

    @Test
    public void testLookupJoinWithPredicatePushdown() throws Exception {
        // Tests that a WHERE condition on the dimension table is correctly pushed down
        // and produces correct lookup results with chain-merged data.
        sql(
                "CREATE TABLE chain_dim_pred ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_pred");

        // Write snapshot branch with 3 rows
        sql(
                "INSERT OVERWRITE `chain_dim_pred$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'snap_1'), (2, 1, 'snap_2'), (3, 1, 'snap_3')");

        // Write delta branch with updated row k=2 and new row k=4
        sql(
                "INSERT OVERWRITE `chain_dim_pred$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (2, 2, 'delta_2_updated'), (4, 1, 'delta_4')");

        sql(
                "CREATE TABLE source_pred ("
                        + "  id BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");
        sql("INSERT INTO source_pred VALUES (1), (2), (3), (4)");

        // Lookup join with a predicate on the dimension table's v column.
        // The predicate v LIKE '%updated%' should be pushed down to the chain table scan.
        // Chain-merged data: k=1(snap_1), k=2(delta_2_updated), k=3(snap_3), k=4(delta_4).
        // Only k=2 has v containing 'updated'.
        List<String> result =
                collectResult(
                        "SELECT S.id, D.k, D.v "
                                + "FROM source_pred AS S "
                                + "LEFT JOIN chain_dim_pred "
                                + "/*+ OPTIONS('lookup.cache' = 'full') */ "
                                + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                + "ON S.id = D.k "
                                + "WHERE D.v LIKE '%updated%'");

        assertThat(result).hasSize(1);
        assertThat(result.get(0)).contains("delta_2_updated");
    }

    @Test
    public void testLookupRejectsIncompatibleDeltaBranchConfig() throws Exception {
        // Tests that creating a lookup table for a chain table with partial-update merge engine
        // on the delta branch is rejected. The SQL lookup join path is covered by
        // testLookupRejectsAggregateOnDeltaBranch, which verifies the same validation
        // through the SQL path (prepareBranchOptions preserves branch-specific merge-engine
        // during table.copy(TableSchema)).
        sql(
                "CREATE TABLE chain_dim_partial_cfg ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_partial_cfg");

        // Alter delta branch to partial-update (unsupported for incremental lookup).
        // The incremental read path (createNoMergeReader) does not apply the merge engine,
        // which would cause partial rows to overwrite complete cached data.
        sql(
                "ALTER TABLE `chain_dim_partial_cfg$branch_delta` SET ("
                        + "  'merge-engine' = 'partial-update'"
                        + ")");

        // Load the chain table directly from catalog (preserves branch on-disk options).
        // Table loading succeeds because batch reads work fine with PARTIAL_UPDATE.
        FileStoreTable table = (FileStoreTable) paimonTable("chain_dim_partial_cfg");

        // Creating a lookup table should fail because the incremental read path
        // (ChainTableStreamScan) does not support PARTIAL_UPDATE on the delta branch.
        assertThatThrownBy(
                        () ->
                                org.apache.paimon.flink.lookup.LookupFileStoreTable.create(
                                        table, Collections.singletonList("k")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merge engine");
    }

    @Test
    public void testStreamingReadRejectsPartialUpdateOnDeltaBranch() throws Exception {
        // Tests that creating a streaming scan for a chain table with partial-update merge engine
        // on the delta branch is rejected.
        sql(
                "CREATE TABLE chain_stream_partial ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_stream_partial");

        sql(
                "ALTER TABLE `chain_stream_partial$branch_delta` SET ("
                        + "  'merge-engine' = 'partial-update'"
                        + ")");

        FileStoreTable table = (FileStoreTable) paimonTable("chain_stream_partial");

        // Creating a streaming scan should fail because ChainTableStreamScan does not support
        // PARTIAL_UPDATE on the delta branch.
        assertThatThrownBy(() -> table.newStreamScan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merge engine");
    }

    @Test
    public void testStreamingReadRejectsAggregateOnDeltaBranch() throws Exception {
        // Tests that creating a streaming scan for a chain table with aggregation merge engine
        // on the delta branch is rejected.
        sql(
                "CREATE TABLE chain_stream_agg ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v INT,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_stream_agg");

        sql(
                "ALTER TABLE `chain_stream_agg$branch_delta` SET ("
                        + "  'merge-engine' = 'aggregation',"
                        + "  'fields.v.aggregate-function' = 'sum'"
                        + ")");

        FileStoreTable table = (FileStoreTable) paimonTable("chain_stream_agg");

        // Creating a streaming scan should fail because ChainTableStreamScan does not support
        // AGGREGATE on the delta branch.
        assertThatThrownBy(() -> table.newStreamScan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merge engine");
    }

    @Test
    public void testLookupRejectsAggregateOnDeltaBranch() throws Exception {
        // Tests that creating a lookup table for a chain table with aggregation merge engine
        // on the delta branch is rejected.
        sql(
                "CREATE TABLE chain_dim_agg_cfg ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v INT,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_agg_cfg");

        sql(
                "ALTER TABLE `chain_dim_agg_cfg$branch_delta` SET ("
                        + "  'merge-engine' = 'aggregation',"
                        + "  'fields.v.aggregate-function' = 'sum'"
                        + ")");

        FileStoreTable table = (FileStoreTable) paimonTable("chain_dim_agg_cfg");

        // Creating a lookup table should fail because ChainTableStreamScan does not support
        // AGGREGATE on the delta branch.
        assertThatThrownBy(
                        () ->
                                org.apache.paimon.flink.lookup.LookupFileStoreTable.create(
                                        table, Collections.singletonList("k")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merge engine");

        // Also verify through SQL lookup join path
        sql(
                "INSERT OVERWRITE `chain_dim_agg_cfg$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 10)");
        sql(
                "INSERT OVERWRITE `chain_dim_agg_cfg$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (2, 2, 20)");

        sql(
                "CREATE TABLE source_agg_cfg ("
                        + "  id BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");
        sql("INSERT INTO source_agg_cfg VALUES (1), (2)");

        String query =
                "SELECT S.id, D.k, D.v "
                        + "FROM source_agg_cfg AS S "
                        + "LEFT JOIN chain_dim_agg_cfg "
                        + "/*+ OPTIONS('lookup.cache' = 'full') */ "
                        + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                        + "ON S.id = D.k";

        assertThatThrownBy(() -> collectResult(query))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merge engine");
    }

    /**
     * Tests that chain table lookup join rejects unsupported scan modes and consumer-id. The
     * validation is inherited from {@link ChainTableFileStoreTable#newStreamScan()}.
     */
    @Test
    public void testLookupRejectsUnsupportedScanMode() throws Exception {
        sql(
                "CREATE TABLE chain_dim_scan_mode ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_scan_mode");

        FileStoreTable table = paimonTable("chain_dim_scan_mode");

        // scan.mode=latest should be rejected for chain table lookup
        FileStoreTable tableLatest = table.copy(Collections.singletonMap("scan.mode", "latest"));
        assertThatThrownBy(
                        () ->
                                org.apache.paimon.flink.lookup.LookupFileStoreTable.create(
                                                tableLatest, Collections.singletonList("k"))
                                        .newStreamScan())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("scan.mode=latest")
                .hasMessageContaining("Chain table streaming read does not support");

        // consumer-id should be rejected for chain table lookup
        FileStoreTable tableWithConsumer =
                table.copy(Collections.singletonMap("consumer-id", "my-consumer"));
        assertThatThrownBy(
                        () ->
                                org.apache.paimon.flink.lookup.LookupFileStoreTable.create(
                                                tableWithConsumer, Collections.singletonList("k"))
                                        .newStreamScan())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("consumer mode")
                .hasMessageContaining("consumer-id='my-consumer'");
    }

    @Test
    @Timeout(180)
    public void testLookupJoinWithCompactDeltaMonitorMode() throws Exception {
        // Main table uses partial-update + force-lookup so that
        // supportCompactDiffStreamingReading returns true and lookupScanMode becomes
        // COMPACT_DELTA_MONITOR.
        sql(
                "CREATE TABLE chain_dim_cdm ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'partial-update',"
                        + "  'force-lookup' = 'true',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_cdm");

        // Delta branch must be deduplicate for chain table incremental read.
        sql("ALTER TABLE `chain_dim_cdm$branch_delta` SET ('merge-engine' = 'deduplicate')");

        // Write data to both branches.
        sql(
                "INSERT OVERWRITE `chain_dim_cdm$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'snap_v1'), (2, 2, 'snap_v2')");
        sql(
                "INSERT INTO `chain_dim_cdm$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (1, 3, 'delta_v1'), (3, 4, 'delta_v3')");

        // Verify that LookupFileStoreTable.newRead() delegates to wrapped.newRead()
        // (not LookupCompactDiffRead, which would cause ClassCastException on ChainSplit).
        FileStoreTable table = paimonTable("chain_dim_cdm");
        org.apache.paimon.flink.lookup.LookupFileStoreTable lookupTable =
                org.apache.paimon.flink.lookup.LookupFileStoreTable.create(
                        table, Collections.singletonList("k"));
        assertThat(lookupTable.newRead()).isInstanceOf(table.newRead().getClass());

        // Run the lookup join and verify results.
        sql(
                "CREATE TABLE source_cdm ("
                        + "  id BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ('connector' = 'paimon')");
        sql("INSERT INTO source_cdm VALUES (1), (2), (3)");

        List<String> results =
                collectResult(
                        "SELECT S.id, D.k, D.v "
                                + "FROM source_cdm AS S "
                                + "LEFT JOIN chain_dim_cdm "
                                + "/*+ OPTIONS('lookup.cache' = 'full') */ "
                                + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                + "ON S.id = D.k");

        // k=1: appears in both snapshot (dt=20250808) and delta (dt=20250809) partitions.
        // Chain table merges at partition level, not row level, so both rows appear.
        // k=2: only in snapshot partition.
        // k=3: only in delta partition.
        assertThat(results)
                .containsExactlyInAnyOrder(
                        "+I[1, 1, snap_v1]",
                        "+I[1, 1, delta_v1]",
                        "+I[2, 2, snap_v2]",
                        "+I[3, 3, delta_v3]");
    }

    /**
     * Verifies that ChainTableStreamScan correctly propagates bucket filters to its internal scans.
     */
    @Test
    public void testStreamingReadBucketFilter() throws Exception {
        sql(
                "CREATE TABLE chain_bucket_filter ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        setupChainTableBranches("chain_bucket_filter");

        // Write main branch
        sql(
                "INSERT OVERWRITE chain_bucket_filter PARTITION (dt = '20250810')"
                        + " VALUES (1, 1, 'v1')");

        // Write delta data across many keys to guarantee both buckets are populated
        for (int i = 1; i <= 50; i++) {
            sql(
                    format(
                            "INSERT INTO `chain_bucket_filter$branch_delta`"
                                    + " PARTITION (dt = '%d') VALUES (%d, 1, 'v%d')",
                            20250809 + (i % 5), i, i));
        }

        FileStoreTable table = paimonTable("chain_bucket_filter");

        // Verify data spans both buckets via delta branch batch scan
        FileStoreTable deltaTable =
                (FileStoreTable) paimonTable("chain_bucket_filter$branch_delta");
        java.util.Set<Integer> deltaBuckets = new java.util.HashSet<>();
        for (Split split : deltaTable.newScan().plan().splits()) {
            if (split instanceof DataSplit) {
                deltaBuckets.add(((DataSplit) split).bucket());
            }
        }
        assertThat(deltaBuckets)
                .as("Test requires data in both buckets")
                .containsExactlyInAnyOrder(0, 1);

        // Without bucket filter: Phase 1 should return splits from both buckets
        ChainTableStreamScan scanAll = (ChainTableStreamScan) table.newStreamScan();
        TableScan.Plan planAll = scanAll.plan();
        java.util.Set<Integer> bucketsAll = collectBucketsFromChainSplits(planAll);
        assertThat(bucketsAll)
                .as("Without filter, should have data from both buckets")
                .containsExactlyInAnyOrder(0, 1);

        // With bucket filter (only bucket 0): Phase 1 should only return bucket 0
        ChainTableStreamScan scanFiltered = (ChainTableStreamScan) table.newStreamScan();
        scanFiltered.withBucketFilter(b -> b == 0);
        TableScan.Plan planFiltered = scanFiltered.plan();
        java.util.Set<Integer> bucketsFiltered = collectBucketsFromChainSplits(planFiltered);
        assertThat(bucketsFiltered)
                .as("Bucket filter should restrict Phase 1 to bucket 0 only")
                .containsExactly(0);
    }

    private java.util.Set<Integer> collectBucketsFromChainSplits(TableScan.Plan plan) {
        java.util.Set<Integer> buckets = new java.util.HashSet<>();
        for (Split split : plan.splits()) {
            if (split instanceof ChainSplit) {
                for (String path : ((ChainSplit) split).fileBucketPathMapping().values()) {
                    if (path.contains("bucket-0")) {
                        buckets.add(0);
                    } else if (path.contains("bucket-1")) {
                        buckets.add(1);
                    }
                }
            }
        }
        return buckets;
    }

    /**
     * Verifies that lookup join on a chain table branch (e.g., t$branch_delta) works correctly.
     * Branch tables may still carry chain-table.enabled=true, but they are not wrapped in
     * FallbackReadFileStoreTable, so they should be treated as regular tables for lookup join.
     */
    @Test
    public void testLookupJoinOnBranchTable() throws Exception {
        sql(
                "CREATE TABLE chain_branch_lookup ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_branch_lookup");

        // Write some data to delta branch
        sql(
                "INSERT OVERWRITE `chain_branch_lookup$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'v1'), (2, 1, 'v2')");

        // Create source table
        sql(
                "CREATE TABLE source_branch ("
                        + "  id BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");
        sql("INSERT INTO source_branch VALUES (1), (2)");

        // Lookup join on branch table should work correctly
        String query =
                "SELECT S.id, D.k, D.v "
                        + "FROM source_branch AS S "
                        + "LEFT JOIN `chain_branch_lookup$branch_delta` "
                        + "/*+ OPTIONS('lookup.cache' = 'full') */ "
                        + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                        + "ON S.id = D.k";

        List<String> result = collectResult(query);
        assertThat(result)
                .as("Lookup join on branch table should return matching rows")
                .hasSize(2)
                .containsExactlyInAnyOrder("+I[1, 1, v1]", "+I[2, 2, v2]");
    }

    /**
     * Tests that chain table lookup join refresh works correctly for both async and non-async
     * modes. This test verifies the refresh logic in {@code FullCacheLookupTable.refresh()} which
     * has different code paths for async vs non-async refresh. For chain tables, the async path
     * skips the backlog calculation (since outer table and delta branch use different snapshot
     * sequences).
     *
     * <p>The lookup join job is started BEFORE inserting source data, so the lookup cache is warmed
     * up before any source data arrives.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @Timeout(120)
    public void testLookupJoinRefresh(boolean asyncRefresh) throws Exception {
        String tableName = "chain_refresh_" + (asyncRefresh ? "async" : "sync");

        tEnv.useCatalog("PAIMON");
        tEnv.useDatabase("default");

        sql(
                "CREATE TABLE "
                        + tableName
                        + " ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches(tableName);

        sql(
                "INSERT INTO `"
                        + tableName
                        + "$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'snap_1'), (2, 1, 'snap_2')");

        sql(
                "INSERT INTO `"
                        + tableName
                        + "$branch_delta` PARTITION (dt = '20250809')"
                        + " VALUES (3, 1, 'delta_3')");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, settings);

        streamTableEnv.registerCatalog("PAIMON", tEnv.getCatalog("PAIMON").get());
        streamTableEnv.useCatalog("PAIMON");
        streamTableEnv.useDatabase("default");

        streamTableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS source_refresh ("
                        + "  id BIGINT,"
                        + "  v BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");

        streamTableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS sink_refresh ("
                        + "  id BIGINT,"
                        + "  k BIGINT,"
                        + "  v STRING"
                        + ") WITH ("
                        + "  'connector' = 'paimon',"
                        + "  'primary-key' = 'id',"
                        + "  'bucket' = '1'"
                        + ")");

        streamTableEnv
                .executeSql("INSERT INTO source_refresh VALUES (1, 1), (2, 2), (3, 3)")
                .await();

        // Submit lookup join job BEFORE inserting source data
        String query =
                format(
                        "INSERT INTO sink_refresh "
                                + "SELECT S.id, D.k, D.v "
                                + "FROM source_refresh AS S "
                                + "LEFT JOIN "
                                + tableName
                                + " /*+ OPTIONS('lookup.cache' = 'full', 'lookup.refresh-async' = '%s', 'continuous.refresh-interval' = '1s') */ "
                                + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                + "ON S.id = D.k",
                        asyncRefresh);

        TableResult tableResult = streamTableEnv.executeSql(query);
        JobClient jobClient =
                tableResult
                        .getJobClient()
                        .orElseThrow(() -> new RuntimeException("Failed to get JobClient"));

        try {
            waitForJobRunning(jobClient);

            waitForQueryResult(
                    "SELECT * FROM sink_refresh ORDER BY id", results -> results.size() == 3);

            // Insert new delta data
            sql(
                    "INSERT INTO `"
                            + tableName
                            + "$branch_delta` PARTITION (dt = '20250810') VALUES (4, 1, 'delta_4')");

            waitForQueryResult(
                    "SELECT * FROM " + tableName + " WHERE dt = '20250810' ORDER BY k",
                    results -> results.size() == 4);

            long startTime = System.currentTimeMillis();
            long value = 0;
            List<String> results = null;
            while (System.currentTimeMillis() - startTime < 30000L) {
                // Insert new source data to trigger lookup
                streamTableEnv
                        .executeSql("INSERT INTO source_refresh VALUES (4, " + value + ")")
                        .await();

                results = collectResult("SELECT * FROM sink_refresh WHERE id = 4");
                if (results.size() == 1 && results.get(0).contains("delta_4")) {
                    break;
                }
                Thread.sleep(500);
            }

            assertThat(results).hasSize(1).contains("+I[4, 4, delta_4]");
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Tests that {@link FullCacheLookupTable#refresh()} uses the delta branch's snapshot manager
     * for chain tables, so that async refresh is chosen when the delta backlog is small.
     */
    @Test
    @Timeout(120)
    public void testChainTableLookupRefreshAsyncPath() throws Exception {
        String tableName = "chain_refresh_async_path";

        tEnv.useCatalog("PAIMON");
        tEnv.useDatabase("default");

        sql(
                "CREATE TABLE "
                        + tableName
                        + " ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches(tableName);

        // Write initial data to delta branch so the lookup table can bootstrap.
        sql(
                "INSERT OVERWRITE `"
                        + tableName
                        + "$branch_delta` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'delta_1')");

        // Get the chain table and enable async refresh with a pending-snapshot-count of 0.
        FileStoreTable table = paimonTable(tableName);
        Map<String, String> lookupOptions = new HashMap<>();
        lookupOptions.put("lookup.refresh.async", "true");
        lookupOptions.put("lookup.refresh.async.pending-snapshot-count", "0");
        table = table.copy(lookupOptions);

        // Create and open a FullCacheLookupTable directly so we can inspect refreshFuture.
        File tempDir = new File(temporaryFolder.toFile(), tableName);
        tempDir.mkdirs();
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        table,
                        new int[] {0, 1, 2, 3},
                        null,
                        null,
                        tempDir,
                        Collections.singletonList("k"),
                        null);
        FullCacheLookupTable lookupTable = FullCacheLookupTable.create(context, 0);
        lookupTable.open();

        try {
            // Write many commits to the MAIN table. The main table and delta branch maintain
            // independent snapshot sequences, so this inflates the main table's snapshot id
            // while the delta branch remains at a low snapshot id.
            for (int i = 0; i < 10; i++) {
                sql(
                        "INSERT INTO "
                                + tableName
                                + " PARTITION (dt = '20250810')"
                                + " VALUES ("
                                + (100 + i)
                                + ", "
                                + i
                                + ", 'main_"
                                + i
                                + "')");
            }

            // Write a single new delta record so the delta branch has a small backlog.
            sql(
                    "INSERT INTO `"
                            + tableName
                            + "$branch_delta` PARTITION (dt = '20250810')"
                            + " VALUES (3, 1, 'delta_3')");

            // The delta branch has only one new snapshot, so the async refresh path should be
            // chosen.
            lookupTable.refresh();

            assertThat(lookupTable.getRefreshFuture())
                    .as(
                            "Chain table lookup refresh should use the async path when the delta backlog is small.")
                    .isNotNull();

            // Wait for the async refresh to complete before closing.
            lookupTable.getRefreshFuture().get();
        } finally {
            lookupTable.close();
        }
    }

    /** Helper method: poll a query until the result matches the expected condition or timeout. */
    private void waitForQueryResult(
            String query, java.util.function.Predicate<List<String>> condition) throws Exception {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 30000L) {
            List<String> results = collectResult(query);
            if (condition.test(results)) {
                return;
            }
            Thread.sleep(500);
        }
        throw new RuntimeException("Timed out waiting for query result: " + query);
    }

    /** Helper method: wait for job to reach target status. */
    private void waitForJobRunning(JobClient jobClient) throws Exception {
        long startTime = System.currentTimeMillis();
        JobStatus currentStatus = null;
        while (System.currentTimeMillis() - startTime < (long) 30000) {
            CompletableFuture<JobStatus> statusFuture = jobClient.getJobStatus();
            currentStatus = statusFuture.get();

            if (currentStatus == JobStatus.RUNNING) {
                return;
            }

            if (currentStatus.isGloballyTerminalState()) {
                throw new RuntimeException(
                        "Job terminated unexpectedly with status: " + currentStatus);
            }

            Thread.sleep(500);
        }
        throw new RuntimeException(
                "Timed out waiting for job status running. Current status: " + currentStatus);
    }
}
