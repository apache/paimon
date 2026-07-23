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

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for chain table deletion-vector support using Flink SQL. */
public class FlinkChainTableDeletionVectorITCase extends FlinkChainTableITCaseBase {

    @Test
    public void testChainTableWithDeletionVectors() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CREATE TABLE chain_dv_t1 ("
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
                        + "  'deletion-vectors.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'compaction.min.file-num' = '100',"
                        + "  'num-sorted-run.compaction-trigger' = '20'"
                        + ")");

        setupChainTableBranches("chain_dv_t1");

        sql(
                "INSERT INTO `chain_dv_t1$branch_snapshot` PARTITION (dt = '20260222')"
                        + " VALUES (1, 1, '1'), (6, 1, '1')");
        sql(
                "INSERT INTO `chain_dv_t1$branch_snapshot` PARTITION (dt = '20260223')"
                        + " VALUES (1, 2, '2'), (2, 2, '2'), (3, 1, '1')");

        sql(
                "INSERT INTO `chain_dv_t1$branch_delta` PARTITION (dt = '20260224')"
                        + " VALUES (4, 1, '1'), (5, 1, '1')");

        // Delete rows from both branches to produce deletion vectors
        sql("DELETE FROM `chain_dv_t1$branch_snapshot` WHERE dt = '20260223' AND t1 = 3");
        sql("DELETE FROM `chain_dv_t1$branch_delta` WHERE dt = '20260224' AND t1 = 4");
        sql("UPDATE `chain_dv_t1$branch_delta` SET t2=5, t3='5' WHERE dt = '20260224' AND t1 = 5");
        assertThat(collectResult("SELECT * FROM chain_dv_t1 WHERE dt = '20260224'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 2, 20260224]", "+I[2, 2, 2, 20260224]", "+I[5, 5, 5, 20260224]");
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_dv_t1$branch_delta$table_indexes` WHERE `partition`='{20260224}'"))
                .isNotEmpty();
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_dv_t1$branch_snapshot$table_indexes` WHERE `partition`='{20260223}'"))
                .isNotEmpty();

        // Compact both snapshot partitions that have deletion vectors, then verify no pending DVs
        // remain in the snapshot branch via the $table_indexes system table.
        sql("CALL sys.compact_chain_table('default.chain_dv_t1', 'dt=20260224')");

        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_dv_t1$branch_snapshot` WHERE dt = '20260223'"))
                .containsExactlyInAnyOrder("+I[1, 2, 2, 20260223]", "+I[2, 2, 2, 20260223]");

        assertThat(collectResult("SELECT * FROM `chain_dv_t1$branch_delta` WHERE dt = '20260224'"))
                .containsExactlyInAnyOrder("+I[5, 5, 5, 20260224]");

        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_dv_t1$branch_snapshot` WHERE dt = '20260224'"))
                .containsExactlyInAnyOrder(
                        "+I[1, 2, 2, 20260224]", "+I[2, 2, 2, 20260224]", "+I[5, 5, 5, 20260224]");
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_dv_t1$branch_snapshot$table_indexes` WHERE `partition`='{20260224}'"))
                .as("Compacted snapshot files should not have deletion vectors")
                .isEmpty();
        assertThat(
                        collectResult(
                                "SELECT * FROM `chain_dv_t1$branch_snapshot$files` WHERE `partition`='{20260224}'"))
                .as("Compacted snapshot files should not have deletion vectors")
                .isNotEmpty();
    }

    @Test
    @Timeout(120)
    public void testStreamingReadWithDeletionVectors() throws Exception {
        String tableName = "chain_dv_stream";
        sql(
                "CREATE TABLE "
                        + tableName
                        + " ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  region STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (region, dt) WITH ("
                        + "  'primary-key' = 'region,dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '1',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'deletion-vectors.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'chain-table.chain-partition-keys' = 'dt',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        setupChainTableBranches(tableName);

        // Write snapshot data and delete one row to produce a deletion vector in snapshot branch.
        sql(
                "INSERT INTO `"
                        + tableName
                        + "$branch_snapshot` PARTITION (region = 'CN', dt = '20260223')"
                        + " VALUES (1, 1, '1'), (2, 1, '2'), (3, 1, '3')");
        sql("DELETE FROM `" + tableName + "$branch_snapshot` WHERE region = 'CN' AND k = 3");

        CloseableIterator<Row> it = sEnv.executeSql("SELECT * FROM " + tableName).collect();

        List<String> startingRows = collectRows(it, 2);
        assertThat(startingRows)
                .as("Starting phase should apply the snapshot deletion vector")
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, CN, 20260223]", "+I[2, 1, 2, CN, 20260223]");

        // Write delta with cross-partition delete, update and insert.
        writeChangelogToBranchWithRegion(
                db,
                tableName,
                "delta",
                Row.ofKind(RowKind.DELETE, 1L, 2L, "1", "CN", "20260224"),
                Row.ofKind(RowKind.UPDATE_BEFORE, 2L, 2L, "2", "CN", "20260224"),
                Row.ofKind(RowKind.UPDATE_AFTER, 2L, 3L, "2-1", "CN", "20260224"),
                Row.ofKind(RowKind.INSERT, 4L, 1L, "4", "CN", "20260224"));

        // Incremental phase: explicit changelog should stream through.
        List<String> deltaRows = collectRows(it, 4);
        assertThat(deltaRows)
                .as("Incremental delta changelog should stream through with deletion vectors")
                .containsExactlyInAnyOrder(
                        "-D[1, 2, 1, CN, 20260224]",
                        "-U[2, 2, 2, CN, 20260224]",
                        "+U[2, 3, 2-1, CN, 20260224]",
                        "+I[4, 1, 4, CN, 20260224]");

        it.close();
    }

    /**
     * Tests streaming read with {@code chain-table.streaming.merge-snapshot=true} and deletion
     * vectors enabled. Verifies that the full-load phase correctly applies cross-branch deletes
     * that are stored as deletion-vector bitmaps in the delta branch.
     */
    @Test
    @Timeout(120)
    public void testStreamingReadWithMergeSnapshotAndDeletionVectors() throws Exception {
        String tableName = "chain_merge_dv_stream";
        sql(
                "CREATE TABLE "
                        + tableName
                        + " ("
                        + "  k BIGINT, seq BIGINT, v STRING, dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '1',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'changelog-producer' = 'input',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'deletion-vectors.enabled' = 'true',"
                        + "  'chain-table.streaming.merge-snapshot' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'continuous.discovery-interval' = '1ms'"
                        + ")");

        String db = tEnv.getCurrentDatabase();
        setupChainTableBranches(tableName);

        // Write snapshot data at dt=20250808
        sql(
                "INSERT INTO `"
                        + tableName
                        + "$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'snap_1'), (2, 1, 'snap_2')");

        // Write delta data spanning dt=20250809 and dt=20250810:
        // - delete k=1 at dt=20250809 (stored as DV bitmap in delta)
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
        // dt=20250810. k=1 is deleted by the delta DV; k=2 is updated; k=3 is newly inserted.
        List<String> startingRows = collectRows(it, 2);
        assertThat(startingRows)
                .as(
                        "Starting with merge-snapshot and DV: cross-branch delete/update should be "
                                + "applied")
                .containsExactlyInAnyOrder(
                        "+U[2, 3, delta_2, 20250810]", "+I[3, 1, delta_3, 20250810]");

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
     * Tests lookup join on a chain table with deletion vectors enabled. Writes changelog records
     * (including a cross-partition delete) to the delta branch and verifies that lookup join can
     * read matching rows from both snapshot and delta branches.
     */
    @Test
    public void testLookupJoinWithDeletionVectors() throws Exception {
        sql(
                "CREATE TABLE chain_dim_dv ("
                        + "  k BIGINT,"
                        + "  seq BIGINT,"
                        + "  v STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,k',"
                        + "  'bucket-key' = 'k',"
                        + "  'bucket' = '1',"
                        + "  'sequence.field' = 'seq',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'chain-table.streaming.merge-snapshot' = 'true',"
                        + "  'deletion-vectors.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")");
        setupChainTableBranches("chain_dim_dv");

        String db = tEnv.getCurrentDatabase();

        // Snapshot branch has k=1,2,3 in partition 20250808
        sql(
                "INSERT OVERWRITE `chain_dim_dv$branch_snapshot` PARTITION (dt = '20250808')"
                        + " VALUES (1, 1, 'snap_1'), (2, 1, 'snap_2'), (3, 1, 'snap_3')");

        // Delta branch updates k=2 and inserts k=4 in partition 20250809,
        // then deletes k=1 via a -D changelog record in a different partition (20250809).
        writeChangelogToBranch(
                db,
                "chain_dim_dv",
                "delta",
                Row.ofKind(RowKind.DELETE, 1L, 2L, "snap_1", "20250809"),
                Row.ofKind(RowKind.UPDATE_BEFORE, 2L, 2L, "snap_2", "20250809"),
                Row.ofKind(RowKind.UPDATE_AFTER, 2L, 3L, "delta_2_updated", "20250809"),
                Row.ofKind(RowKind.INSERT, 4L, 1L, "delta_4", "20250809"));

        // Create source table
        sql(
                "CREATE TABLE source_dv_delete ("
                        + "  id BIGINT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + "  'connector' = 'paimon'"
                        + ")");
        sql("INSERT INTO source_dv_delete VALUES (1), (2), (3), (4)");

        // Lookup join on k
        List<String> result =
                collectResult(
                        "SELECT S.id, D.k, D.v "
                                + "FROM source_dv_delete AS S "
                                + "LEFT JOIN chain_dim_dv /*+ OPTIONS('lookup.cache' = 'full') */ "
                                + "FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                + "ON S.id = D.k");

        // Verify lookup results:
        // Merge Snapshot in Phase 1 dim data:
        //   Snapshot@20250808 (anchor): k=1(snap_1), k=2(snap_2), k=3(snap_3)
        //   Delta@20250809 (with anchor merge) changelog: delete k=1, update k=2 to
        // delta_2_updated, insert k=4
        // Expected lookup matches: k=1(snap_1), k=2(delta_2_updated), k=3(snap_3), k=4(delta_4)
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[1, 1, snap_1]",
                        "+I[2, 2, delta_2_updated]",
                        "+I[3, 3, snap_3]",
                        "+I[4, 4, delta_4]");
    }
}
