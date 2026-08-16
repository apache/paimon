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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.CatalogITCaseBase;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CompactChainTableProcedure}. */
public class CompactChainTableProcedureITCase extends CatalogITCaseBase {

    private List<String> collectResult(String query) throws Exception {
        List<String> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(query).collect()) {
            while (it.hasNext()) {
                result.add(it.next().toString());
            }
        }
        return result;
    }

    private void createChainTableWithDate(String tableName) {
        sql(
                "CREATE TABLE %s ("
                        + "  t1 BIGINT,"
                        + "  t2 BIGINT,"
                        + "  t3 STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,t1',"
                        + "  'bucket-key' = 't1',"
                        + "  'bucket' = '1',"
                        + "  'sequence.field' = 't2',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")",
                tableName);
    }

    private void setupChainTableBranches(String tableName) {
        String db = tEnv.getCurrentDatabase();
        sql("CALL sys.create_branch('%s.%s', 'snapshot')", db, tableName);
        sql("CALL sys.create_branch('%s.%s', 'delta')", db, tableName);

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

    @Nullable
    @Override
    protected Boolean sqlSyncMode() {
        return true;
    }

    @Test
    public void testCompactChainTableBasic() throws Exception {
        // Create chain table
        createChainTableWithDate("chain_compact_t1");
        setupChainTableBranches("chain_compact_t1");

        // Compact empty partition
        sql("CALL sys.compact_chain_table('default.chain_compact_t1', 'dt=20260224')");

        // Insert snapshot data
        sql(
                "INSERT INTO `chain_compact_t1$branch_snapshot` PARTITION (dt = '20260222') VALUES (0, 1, '0')");
        sql(
                "INSERT INTO `chain_compact_t1$branch_snapshot` PARTITION (dt = '20260223') VALUES (1, 1, '1')");

        // Insert delta data
        sql(
                "INSERT INTO `chain_compact_t1$branch_delta` PARTITION (dt = '20260224') VALUES (2, 2, '2')");

        // Before compaction: verify chain read shows snapshot + delta
        assertThat(collectResult("SELECT * FROM chain_compact_t1 WHERE dt = '20260224'"))
                .containsExactlyInAnyOrder("+I[1, 1, 1, 20260224]", "+I[2, 2, 2, 20260224]");

        sql("CALL sys.compact_chain_table('default.chain_compact_t1', 'dt=20260224')");

        // After compaction: verify snapshot branch has merged data
        assertThat(collectResult("SELECT * FROM chain_compact_t1 WHERE dt = '20260224'"))
                .containsExactlyInAnyOrder("+I[1, 1, 1, 20260224]", "+I[2, 2, 2, 20260224]");
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_compact_t1$branch_snapshot WHERE dt = '20260223'"))
                .containsExactlyInAnyOrder("+I[1, 1, 1, 20260223]");
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_compact_t1$branch_snapshot` WHERE dt = '20260224'"))
                .containsExactlyInAnyOrder("+I[1, 1, 1, 20260224]", "+I[2, 2, 2, 20260224]");
    }

    @Test
    public void testCompactChainTableOverwrite() throws Exception {
        // Create chain table
        createChainTableWithDate("chain_compact_t2");
        setupChainTableBranches("chain_compact_t2");

        sql("INSERT INTO `chain_compact_t2` PARTITION (dt = '20260222') VALUES (1, 1, '1')");

        // Insert snapshot data
        sql(
                "INSERT INTO `chain_compact_t2$branch_snapshot` PARTITION (dt = '20260223') VALUES (2, 1, '2')");
        sql(
                "INSERT INTO `chain_compact_t2$branch_snapshot` PARTITION (dt = '20260224') VALUES (3, 1, '3')");
        sql(
                "INSERT INTO `chain_compact_t2$branch_snapshot` PARTITION (dt = '20260225') VALUES (3, 2, '3-1')");

        // Insert delta data
        sql(
                "INSERT INTO `chain_compact_t2$branch_delta` PARTITION (dt = '20260225') VALUES (4, 2, '4')");

        // First call should skip because partition exists
        sql("CALL sys.compact_chain_table('default.chain_compact_t2', 'dt=20260225')");

        assertThat(collectResult("SELECT * FROM chain_compact_t2 WHERE dt = '20260225'"))
                .containsExactlyInAnyOrder("+I[3, 2, 3-1, 20260225]");

        sql("CALL sys.compact_chain_table('default.chain_compact_t2', 'dt=20260225', true)");

        // Check snapshots commit_kind
        assertThat(
                        collectResult(
                                "SELECT snapshot_id, commit_kind FROM `chain_compact_t2$branch_snapshot$snapshots`"))
                .containsExactlyInAnyOrder(
                        "+I[1, APPEND]", "+I[2, APPEND]", "+I[3, APPEND]", "+I[4, OVERWRITE]");

        // Verify snapshot branch now has the data
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_compact_t2$branch_snapshot` WHERE dt = '20260225'"))
                .containsExactlyInAnyOrder("+I[3, 1, 3, 20260225]", "+I[4, 2, 4, 20260225]");
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_compact_t2$branch_snapshot` WHERE dt <> '20260225'"))
                .containsExactlyInAnyOrder("+I[2, 1, 2, 20260223]", "+I[3, 1, 3, 20260224]");
    }

    @Test
    public void testCompactChainTableMultiplePartitions() throws Exception {
        sql(
                "CREATE TABLE chain_compact_t3 ("
                        + "  t1 BIGINT,"
                        + "  t2 BIGINT,"
                        + "  t3 STRING,"
                        + "  dt STRING,"
                        + "  hr STRING"
                        + ") PARTITIONED BY (dt, hr) WITH ("
                        + "  'primary-key' = 'dt,hr,t1',"
                        + "  'bucket-key' = 't1',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 't2',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt $hr:00:00',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd HH:mm:ss'"
                        + ")");
        setupChainTableBranches("chain_compact_t3");

        // Write snapshot branch data
        sql(
                "INSERT INTO `chain_compact_t3$branch_snapshot` PARTITION (dt = '20250810', hr = '20') VALUES (0, 1, '0')");
        sql(
                "INSERT INTO `chain_compact_t3$branch_snapshot` PARTITION (dt = '20250810', hr = '22') VALUES (1, 1, '1'),(2, 1, '1')");

        // Write delta branch data
        sql(
                "INSERT INTO `chain_compact_t3$branch_delta` PARTITION (dt = '20250810', hr = '21') VALUES (1, 1, '1'),(2, 1, '1')");
        sql(
                "INSERT INTO `chain_compact_t3$branch_delta` PARTITION (dt = '20250810', hr = '22') VALUES (1, 2, '1-1'),(3, 1, '1')");
        sql(
                "INSERT INTO `chain_compact_t3$branch_delta` PARTITION (dt = '20250810', hr = '23') VALUES (2, 2, '1-1'),(4, 1, '1')");

        // Compact partition 20250810/hr=23
        sql("CALL sys.compact_chain_table('default.chain_compact_t3', 'dt = 20250810, hr = 23')");

        // Verify chain read still works correctly
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_compact_t3 WHERE dt = '20250810' AND hr = '23'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, 20250810, 23]",
                        "+I[2, 2, 1-1, 20250810, 23]",
                        "+I[4, 1, 1, 20250810, 23]");

        // Write more snapshot and delta data
        sql(
                "INSERT INTO `chain_compact_t3$branch_snapshot` PARTITION (dt = '20250811', hr = '00') VALUES (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'),(4, 2, '1-1')");
        sql(
                "INSERT INTO `chain_compact_t3$branch_snapshot` PARTITION (dt = '20250811', hr = '02') VALUES (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'),(4, 2, '1-1'),(5, 1, '1'),(6, 1, '1')");
        sql(
                "INSERT INTO `chain_compact_t3$branch_delta` PARTITION (dt = '20250811', hr = '00') VALUES (3, 2, '1-1'),(4, 2, '1-1')");
        sql(
                "INSERT INTO `chain_compact_t3$branch_delta` PARTITION (dt = '20250811', hr = '01') VALUES (5, 1, '1'),(6, 1, '1')");
        sql(
                "INSERT INTO `chain_compact_t3$branch_delta` PARTITION (dt = '20250811', hr = '02') VALUES (5, 2, '1-1'),(6, 2, '1-1')");

        // Compact partition 20250811/hr=02 without overwrite
        sql("CALL sys.compact_chain_table('default.chain_compact_t3', 'dt=20250811,hr=02')");

        // Verify data is unchanged after skip
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_compact_t3 WHERE dt = '20250811' AND hr = '02'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, 20250811, 02]",
                        "+I[2, 2, 1-1, 20250811, 02]",
                        "+I[3, 2, 1-1, 20250811, 02]",
                        "+I[4, 2, 1-1, 20250811, 02]",
                        "+I[5, 1, 1, 20250811, 02]",
                        "+I[6, 1, 1, 20250811, 02]");

        // Compact with overwrite to test overwrite path
        sql("CALL sys.compact_chain_table('default.chain_compact_t3', 'dt=20250811,hr=02', true)");

        // Check snapshots commit_kind
        assertThat(
                        collectResult(
                                "SELECT snapshot_id, commit_kind FROM `chain_compact_t3$branch_snapshot$snapshots`"))
                .containsExactlyInAnyOrder(
                        "+I[1, APPEND]",
                        "+I[2, APPEND]",
                        "+I[3, APPEND]",
                        "+I[4, APPEND]",
                        "+I[5, APPEND]",
                        "+I[6, OVERWRITE]");

        // Check all snapshot partition data
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_compact_t3 WHERE dt = '20250811' AND hr = '02'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, 20250811, 02]",
                        "+I[2, 2, 1-1, 20250811, 02]",
                        "+I[3, 2, 1-1, 20250811, 02]",
                        "+I[4, 2, 1-1, 20250811, 02]",
                        "+I[5, 2, 1-1, 20250811, 02]",
                        "+I[6, 2, 1-1, 20250811, 02]");
        assertThat(
                        collectResult(
                                "SELECT * FROM chain_compact_t3 WHERE dt = '20250811' AND hr = '00'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 1-1, 20250811, 00]",
                        "+I[2, 2, 1-1, 20250811, 00]",
                        "+I[3, 2, 1-1, 20250811, 00]",
                        "+I[4, 2, 1-1, 20250811, 00]");
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_compact_t3$branch_snapshot` WHERE dt = '20250810' AND hr = '23'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, 20250810, 23]",
                        "+I[2, 2, 1-1, 20250810, 23]",
                        "+I[4, 1, 1, 20250810, 23]");
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_compact_t3$branch_snapshot` WHERE dt = '20250810' AND hr = '22'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, 20250810, 22]", "+I[2, 1, 1, 20250810, 22]");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCompactChainTableWithGroupPartition(boolean overwrite) throws Exception {
        sql(
                String.format(
                        "CREATE TABLE chain_compact_t4 ("
                                + "  t1 BIGINT,"
                                + "  t2 BIGINT,"
                                + "  t3 STRING,"
                                + "  region STRING,"
                                + "  dt STRING,"
                                + "  hr STRING"
                                + ") PARTITIONED BY (region, dt, hr) "
                                + " WITH ("
                                + "  'dynamic-partition-overwrite' = '%s',"
                                + "  'primary-key' = 'region,dt,hr,t1',"
                                + "  'bucket-key' = 't1',"
                                + "  'bucket' = '1',"
                                + "  'sequence.field' = 't2',"
                                + "  'merge-engine' = 'deduplicate',"
                                + "  'chain-table.enabled' = 'true',"
                                + "  'partition.timestamp-pattern' = '$dt $hr:00:00',"
                                + "  'partition.timestamp-formatter' = 'yyyyMMdd HH:mm:ss',"
                                + "  'chain-table.chain-partition-keys' = 'dt,hr'"
                                + ")",
                        overwrite));
        setupChainTableBranches("chain_compact_t4");

        // Write snapshot branch data
        sql(
                "INSERT INTO `chain_compact_t4$branch_snapshot` PARTITION (region='CN', dt = '20250810', hr = '20') VALUES (1, 1, '1')");
        sql(
                "INSERT INTO `chain_compact_t4$branch_snapshot` PARTITION (region='CN', dt = '20250810', hr = '21') VALUES (2, 1, '1')");
        sql(
                "INSERT INTO `chain_compact_t4$branch_snapshot` PARTITION (region='CN', dt = '20250810', hr = '22') VALUES (3, 1, '1')");
        sql(
                "INSERT INTO `chain_compact_t4$branch_snapshot` PARTITION (region='UK', dt = '20250810', hr = '21') VALUES (21, 1, '1')");
        sql(
                "INSERT INTO `chain_compact_t4$branch_snapshot` PARTITION (region='FR', dt = '20250810', hr = '22') VALUES (31, 1, '1')");
        sql(
                "INSERT INTO `chain_compact_t4$branch_snapshot` PARTITION (region='CA', dt = '20250810', hr = '21') VALUES (41, 1, '1')");

        // Write delta branch data
        sql(
                "INSERT INTO `chain_compact_t4$branch_delta` PARTITION (region='CN', dt = '20250810', hr = '22') VALUES (4, 1, '1')");
        sql(
                "INSERT INTO `chain_compact_t4$branch_delta` PARTITION (region='US', dt = '20250810', hr = '22') VALUES (11, 1, '1')");
        sql(
                "INSERT INTO `chain_compact_t4$branch_delta` PARTITION (region='UK', dt = '20250810', hr = '22') VALUES (22, 1, '1')");

        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_compact_t4$branch_snapshot` WHERE dt = '20250810' AND hr = '22'"))
                .containsExactlyInAnyOrder(
                        "+I[3, 1, 1, CN, 20250810, 22]", "+I[31, 1, 1, FR, 20250810, 22]");

        sql("CALL sys.compact_chain_table('default.chain_compact_t4', 'dt=20250810,hr=22', true)");

        assertThat(
                        collectResult(
                                "select snapshot_id,commit_kind from `chain_compact_t4$branch_snapshot$snapshots`"))
                .containsExactlyInAnyOrder(
                        "+I[1, APPEND]",
                        "+I[2, APPEND]",
                        "+I[3, APPEND]",
                        "+I[4, APPEND]",
                        "+I[5, APPEND]",
                        "+I[6, APPEND]",
                        "+I[7, OVERWRITE]");

        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_compact_t4$branch_snapshot` WHERE dt = '20250810' AND hr = '21'"))
                .containsExactlyInAnyOrder(
                        "+I[2, 1, 1, CN, 20250810, 21]",
                        "+I[21, 1, 1, UK, 20250810, 21]",
                        "+I[41, 1, 1, CA, 20250810, 21]");

        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_compact_t4$branch_snapshot` WHERE dt = '20250810' AND hr = '22'"))
                .containsExactlyInAnyOrder(
                        "+I[2, 1, 1, CN, 20250810, 22]",
                        "+I[4, 1, 1, CN, 20250810, 22]",
                        "+I[11, 1, 1, US, 20250810, 22]",
                        "+I[21, 1, 1, UK, 20250810, 22]",
                        "+I[22, 1, 1, UK, 20250810, 22]",
                        "+I[31, 1, 1, FR, 20250810, 22]");
    }
}
