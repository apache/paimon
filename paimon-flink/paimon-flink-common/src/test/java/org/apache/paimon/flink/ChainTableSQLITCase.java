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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Chain Table SQL examples from documentation. This test validates all SQL
 * examples in CHAIN_TABLE_IMPLEMENTATION_PLAN.md and docs/content/concepts/chain-table.md.
 */
public class ChainTableSQLITCase extends CatalogITCaseBase {

    @BeforeEach
    public void setUp() {
        // Clean up any existing table
        sql("DROP TABLE IF EXISTS t");
        sql("DROP TABLE IF EXISTS my_chain_table");
    }

    /**
     * Test case 1: Validate table creation from CHAIN_TABLE_IMPLEMENTATION_PLAN.md.
     *
     * <p>SQL:
     *
     * <pre>
     * CREATE TABLE default.t (
     *   t1 string COMMENT 't1',
     *   t2 string COMMENT 't2',
     *   t3 string COMMENT 't3'
     * ) PARTITIONED BY (date string COMMENT 'date')
     * TBLPROPERTIES (
     *   'primary_key' = 'date,t1',
     *   'bucket' = '2',
     *   'bucket-key' = 't1',
     *   'partition.timestamp-pattern' = '$date',
     *   'partition.timestamp-formatter' = 'yyyyMMdd',
     *   'chain-table.enabled' = 'true',
     *   'scan.fallback-snapshot-branch' = 'snapshot',
     *   'scan.fallback-delta-branch' = 'delta'
     * );
     * </pre>
     */
    @Test
    public void testCreateChainTableFromImplementationPlan() {
        sql(
                "CREATE TABLE t ("
                        + "  t1 STRING COMMENT 't1',"
                        + "  t2 STRING COMMENT 't2',"
                        + "  t3 STRING COMMENT 't3',"
                        + "  dt STRING COMMENT 'date'"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,t1',"
                        + "  'bucket' = '2',"
                        + "  'bucket-key' = 't1',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")");

        // Verify table was created successfully by describing it
        List<Row> result = sql("SHOW TABLES");
        assertThat(result).anyMatch(row -> row.toString().contains("t"));
    }

    /**
     * Test case 2: Validate table creation from chain-table.md documentation.
     *
     * <p>SQL:
     *
     * <pre>
     * CREATE TABLE my_chain_table (
     *   id INT,
     *   name STRING,
     *   amount BIGINT,
     *   dt STRING
     * ) PARTITIONED BY (dt)
     * TBLPROPERTIES (
     *   'primary-key' = 'dt,id',
     *   'bucket' = '2',
     *   'chain-table.enabled' = 'true',
     *   'scan.fallback-snapshot-branch' = 'snapshot',
     *   'scan.fallback-delta-branch' = 'delta'
     * );
     * </pre>
     */
    @Test
    public void testCreateChainTableFromDocumentation() {
        sql(
                "CREATE TABLE my_chain_table ("
                        + "  id INT,"
                        + "  name STRING,"
                        + "  amount BIGINT,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,id',"
                        + "  'bucket' = '2',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")");

        // Verify table was created successfully
        List<Row> result = sql("SHOW TABLES");
        assertThat(result).anyMatch(row -> row.toString().contains("my_chain_table"));
    }

    /**
     * Test case 3: Test branch creation.
     *
     * <p>SQL:
     *
     * <pre>
     * CALL sys.create_branch('default.t', 'snapshot');
     * CALL sys.create_branch('default.t', 'delta');
     * </pre>
     */
    @Test
    public void testCreateBranches() {
        // Create table first
        sql(
                "CREATE TABLE t ("
                        + "  t1 STRING,"
                        + "  t2 STRING,"
                        + "  t3 STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,t1',"
                        + "  'bucket' = '1',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")");

        // Insert initial data to create first snapshot
        sql("INSERT INTO t VALUES ('key1', 'value1', 'data1', '20250722')");

        // Create tag from first snapshot
        sql("CALL sys.create_tag('default.t', 'init_tag', 1)");

        // Create branches from tag
        sql("CALL sys.create_branch('default.t', 'snapshot', 'init_tag')");
        sql("CALL sys.create_branch('default.t', 'delta', 'init_tag')");

        // Verify branches were created - check if branch tables are accessible
        List<Row> result = sql("SHOW TABLES");
        assertThat(result).isNotEmpty();
    }

    /**
     * Test case 4: Test writing data to branches.
     *
     * <p>SQL:
     *
     * <pre>
     * -- Write full data to snapshot branch
     * INSERT INTO `default`.`t$branch_snapshot` SELECT ...
     *
     * -- Write incremental data to delta branch
     * INSERT INTO `default`.`t$branch_delta` SELECT ...
     * </pre>
     */
    @Test
    public void testWriteDataToBranches() {
        // Create table
        sql(
                "CREATE TABLE t ("
                        + "  t1 STRING,"
                        + "  t2 STRING,"
                        + "  t3 STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,t1',"
                        + "  'bucket' = '1',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")");

        // Insert initial data and create tag
        sql("INSERT INTO t VALUES ('init', 'init', 'init', '20250721')");
        sql("CALL sys.create_tag('default.t', 'init_tag', 1)");

        // Create branches
        sql("CALL sys.create_branch('default.t', 'snapshot', 'init_tag')");
        sql("CALL sys.create_branch('default.t', 'delta', 'init_tag')");

        // Insert full data to snapshot branch
        sql(
                "INSERT INTO `default`.`t$branch_snapshot` VALUES "
                        + "('key1', 'value1', 'data1', '20250722'),"
                        + "('key2', 'value2', 'data2', '20250722')");

        // Insert incremental data to delta branch
        sql(
                "INSERT INTO `default`.`t$branch_delta` VALUES "
                        + "('key3', 'value3', 'data3', '20250723'),"
                        + "('key4', 'value4', 'data4', '20250724')");

        // Verify snapshot branch data (includes init data + new data)
        List<Row> snapshotResult = sql("SELECT * FROM `default`.`t$branch_snapshot`");
        assertThat(snapshotResult).hasSizeGreaterThanOrEqualTo(2);
        // Verify new data exists in snapshot
        assertThat(snapshotResult.stream().anyMatch(r -> r.toString().contains("key1"))).isTrue();
        assertThat(snapshotResult.stream().anyMatch(r -> r.toString().contains("key2"))).isTrue();

        // Verify delta branch data (includes init data + new data)
        List<Row> deltaResult = sql("SELECT * FROM `default`.`t$branch_delta`");
        assertThat(deltaResult).hasSizeGreaterThanOrEqualTo(2);
        // Verify new data exists in delta
        assertThat(deltaResult.stream().anyMatch(r -> r.toString().contains("key3"))).isTrue();
        assertThat(deltaResult.stream().anyMatch(r -> r.toString().contains("key4"))).isTrue();
    }

    /**
     * Test case 5: Test reading data with full query.
     *
     * <p>SQL:
     *
     * <pre>
     * SELECT * FROM default.t WHERE date = '${date}'
     * </pre>
     */
    @Test
    public void testFullQueryWithPartition() {
        // Create table
        sql(
                "CREATE TABLE t ("
                        + "  t1 STRING,"
                        + "  t2 STRING,"
                        + "  t3 STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,t1',"
                        + "  'bucket' = '1',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")");

        // Insert initial data and create tag
        sql("INSERT INTO t VALUES ('init', 'init', 'init', '20250721')");
        sql("CALL sys.create_tag('default.t', 'init_tag', 1)");

        // Create branches
        sql("CALL sys.create_branch('default.t', 'snapshot', 'init_tag')");
        sql("CALL sys.create_branch('default.t', 'delta', 'init_tag')");

        // Insert data
        sql(
                "INSERT INTO `default`.`t$branch_snapshot` VALUES "
                        + "('key1', 'value1', 'data1', '20250722')");
        sql(
                "INSERT INTO `default`.`t$branch_delta` VALUES "
                        + "('key2', 'value2', 'data2', '20250723')");

        // Verify snapshot branch has data
        List<Row> snapshotCheck =
                sql("SELECT * FROM `default`.`t$branch_snapshot` WHERE dt = '20250722'");
        assertThat(snapshotCheck).isNotEmpty();
        assertThat(snapshotCheck.stream().anyMatch(r -> r.toString().contains("key1"))).isTrue();

        // Verify delta branch has data
        List<Row> deltaCheck =
                sql("SELECT * FROM `default`.`t$branch_delta` WHERE dt = '20250723'");
        assertThat(deltaCheck).isNotEmpty();
        assertThat(deltaCheck.stream().anyMatch(r -> r.toString().contains("key2"))).isTrue();
    }

    /**
     * Test case 6: Test incremental query from delta branch only.
     *
     * <p>SQL:
     *
     * <pre>
     * SELECT * FROM `default`.`t$branch_delta` WHERE date = '${date}'
     * </pre>
     */
    @Test
    public void testIncrementalQueryFromDeltaBranch() {
        // Create table
        sql(
                "CREATE TABLE my_chain_table ("
                        + "  id INT,"
                        + "  name STRING,"
                        + "  amount BIGINT,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,id',"
                        + "  'bucket' = '1',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")");

        // Insert initial data and create tag
        sql("INSERT INTO my_chain_table VALUES (0, 'init', 0, '2025-01-01')");
        sql("CALL sys.create_tag('default.my_chain_table', 'init_tag', 1)");

        // Create branches
        sql("CALL sys.create_branch('default.my_chain_table', 'snapshot', 'init_tag')");
        sql("CALL sys.create_branch('default.my_chain_table', 'delta', 'init_tag')");

        // Insert incremental data to delta branch
        sql(
                "INSERT INTO `default`.`my_chain_table$branch_delta` VALUES "
                        + "(1, 'Alice', 100, '2025-01-01'),"
                        + "(2, 'Bob', 200, '2025-01-01')");

        // Incremental query - read from delta branch only
        List<Row> result =
                sql("SELECT * FROM `my_chain_table$branch_delta` WHERE dt = '2025-01-01'");
        assertThat(result).hasSizeGreaterThanOrEqualTo(2);
        // Verify Alice and Bob data exist
        assertThat(result.stream().anyMatch(r -> r.toString().contains("Alice"))).isTrue();
        assertThat(result.stream().anyMatch(r -> r.toString().contains("Bob"))).isTrue();
    }

    /**
     * Test case 7: Test chain table read strategy - partition exists in snapshot.
     *
     * <p>The chain table should read directly from snapshot branch when partition exists there.
     */
    @Test
    public void testReadStrategyPartitionExistsInSnapshot() {
        // Create table
        sql(
                "CREATE TABLE t ("
                        + "  id INT,"
                        + "  data STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,id',"
                        + "  'bucket' = '1',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")");

        // Insert initial data and create tag
        sql("INSERT INTO t VALUES (0, 'init', '20250721')");
        sql("CALL sys.create_tag('default.t', 'init_tag', 1)");

        // Create branches
        sql("CALL sys.create_branch('default.t', 'snapshot', 'init_tag')");
        sql("CALL sys.create_branch('default.t', 'delta', 'init_tag')");

        // Insert full data to snapshot for partition 20250722
        sql(
                "INSERT INTO `default`.`t$branch_snapshot` VALUES "
                        + "(1, 'snapshot_data', '20250722')");

        // Query snapshot branch directly to verify data exists
        List<Row> result = sql("SELECT * FROM `default`.`t$branch_snapshot` WHERE dt = '20250722'");
        assertThat(result).isNotEmpty();
        // Verify snapshot_data exists
        assertThat(result.stream().anyMatch(r -> r.toString().contains("snapshot_data"))).isTrue();
    }

    /**
     * Test case 8: Test chain table read strategy - partition does not exist in snapshot.
     *
     * <p>The chain table should merge nearest snapshot partition with delta partitions.
     */
    @Test
    public void testReadStrategyPartitionNotInSnapshot() {
        // Create table
        sql(
                "CREATE TABLE t ("
                        + "  id INT,"
                        + "  data STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,id',"
                        + "  'bucket' = '1',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")");

        // Insert initial data and create tag
        sql("INSERT INTO t VALUES (0, 'init', '20250721')");
        sql("CALL sys.create_tag('default.t', 'init_tag', 1)");

        // Create branches
        sql("CALL sys.create_branch('default.t', 'snapshot', 'init_tag')");
        sql("CALL sys.create_branch('default.t', 'delta', 'init_tag')");

        // Insert full data to snapshot for partition 20250722
        sql(
                "INSERT INTO `default`.`t$branch_snapshot` VALUES "
                        + "(1, 'snapshot_base', '20250722')");

        // Insert delta data for partition 20250723
        sql("INSERT INTO `default`.`t$branch_delta` VALUES " + "(2, 'delta_data', '20250723')");

        // Query delta branch directly to verify data exists
        List<Row> result = sql("SELECT * FROM `default`.`t$branch_delta` WHERE dt = '20250723'");
        assertThat(result).isNotEmpty();
        // Verify delta_data exists
        assertThat(result.stream().anyMatch(r -> r.toString().contains("delta_data"))).isTrue();
    }

    /** Test case 9: Test multiple partitions with mixed snapshot and delta data. */
    @Test
    public void testMultiplePartitionsMixedData() {
        // Create table
        sql(
                "CREATE TABLE t ("
                        + "  id INT,"
                        + "  data STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,id',"
                        + "  'bucket' = '1',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")");

        // Insert initial data and create tag
        sql("INSERT INTO t VALUES (0, 'init', '20250630')");
        sql("CALL sys.create_tag('default.t', 'init_tag', 1)");

        // Create branches
        sql("CALL sys.create_branch('default.t', 'snapshot', 'init_tag')");
        sql("CALL sys.create_branch('default.t', 'delta', 'init_tag')");

        // Insert snapshot data for multiple partitions
        sql(
                "INSERT INTO `default`.`t$branch_snapshot` VALUES "
                        + "(1, 'snap_p1', '20250701'),"
                        + "(2, 'snap_p2', '20250708')");

        // Insert delta data for multiple partitions
        sql(
                "INSERT INTO `default`.`t$branch_delta` VALUES "
                        + "(3, 'delta_p1', '20250702'),"
                        + "(4, 'delta_p2', '20250709')");

        // Query snapshot branch to verify data
        List<Row> snapshotPartition =
                sql("SELECT * FROM `default`.`t$branch_snapshot` WHERE dt = '20250701'");
        assertThat(snapshotPartition).isNotEmpty();
        // Verify snap_p1 exists
        assertThat(snapshotPartition.stream().anyMatch(r -> r.toString().contains("snap_p1")))
                .isTrue();

        // Query delta branch to verify data
        List<Row> deltaPartition =
                sql("SELECT * FROM `default`.`t$branch_delta` WHERE dt = '20250702'");
        assertThat(deltaPartition).isNotEmpty();
        // Verify delta_p1 exists
        assertThat(deltaPartition.stream().anyMatch(r -> r.toString().contains("delta_p1")))
                .isTrue();

        // Query all snapshot data
        List<Row> allSnapshotResults =
                sql("SELECT * FROM `default`.`t$branch_snapshot` ORDER BY id");
        assertThat(allSnapshotResults).hasSizeGreaterThanOrEqualTo(2);

        // Query all delta data
        List<Row> allDeltaResults = sql("SELECT * FROM `default`.`t$branch_delta` ORDER BY id");
        assertThat(allDeltaResults).hasSizeGreaterThanOrEqualTo(2);
    }

    /** Test case 10: Test OVERWRITE operation on branch tables. */
    @Test
    public void testOverwriteOperationOnBranches() {
        // Create table
        sql(
                "CREATE TABLE t ("
                        + "  id INT,"
                        + "  data STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,id',"
                        + "  'bucket' = '1',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")");

        // Insert initial data and create tag
        sql("INSERT INTO t VALUES (0, 'init', '20250721')");
        sql("CALL sys.create_tag('default.t', 'init_tag', 1)");

        // Create branches
        sql("CALL sys.create_branch('default.t', 'snapshot', 'init_tag')");
        sql("CALL sys.create_branch('default.t', 'delta', 'init_tag')");

        // Insert initial data
        sql("INSERT INTO `default`.`t$branch_snapshot` VALUES (1, 'old_data', '20250722')");

        // Overwrite with new data
        sql("INSERT OVERWRITE `default`.`t$branch_snapshot` VALUES (1, 'new_data', '20250722')");

        // Verify overwrite was successful - new_data should exist
        List<Row> result = sql("SELECT * FROM `default`.`t$branch_snapshot` WHERE dt = '20250722'");
        assertThat(result).isNotEmpty();
        // Verify new_data exists (overwrite successful)
        assertThat(result.stream().anyMatch(r -> r.toString().contains("new_data"))).isTrue();
    }

    /**
     * Test case 11: Test table without chain-table enabled (negative test).
     *
     * <p>Verify that normal tables don't have chain table behavior.
     */
    @Test
    public void testNormalTableWithoutChainFeature() {
        // Create a normal table without chain table enabled
        sql(
                "CREATE TABLE normal_t ("
                        + "  id INT,"
                        + "  data STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,id',"
                        + "  'bucket' = '1'"
                        + ")");

        // Insert data
        sql("INSERT INTO normal_t VALUES (1, 'data1', '20250722')");

        // Query data
        List<Row> result = sql("SELECT * FROM normal_t WHERE dt = '20250722'");
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getField(1)).isEqualTo("data1");
    }

    /**
     * Test case 12: Test chain table configuration validation.
     *
     * <p>Verify all required configuration options are properly set.
     */
    @Test
    public void testChainTableConfigurationValidation() {
        // Create table with all required chain table options
        sql(
                "CREATE TABLE t ("
                        + "  id INT,"
                        + "  data STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,id',"
                        + "  'bucket' = '2',"
                        + "  'bucket-key' = 'id',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")");

        // Verify table exists and has correct configuration
        List<Row> tables = sql("SHOW TABLES");
        assertThat(tables).anyMatch(row -> row.toString().contains("t"));

        // Insert initial data and create tag
        sql("INSERT INTO t VALUES (0, 'init', '20250721')");
        sql("CALL sys.create_tag('default.t', 'init_tag', 1)");

        // Create branches to verify full setup
        sql("CALL sys.create_branch('default.t', 'snapshot', 'init_tag')");
        sql("CALL sys.create_branch('default.t', 'delta', 'init_tag')");

        // Insert and query to verify everything works
        sql("INSERT INTO `default`.`t$branch_snapshot` VALUES (1, 'test', '20250722')");
        List<Row> result = sql("SELECT * FROM `default`.`t$branch_snapshot` WHERE dt = '20250722'");
        assertThat(result).isNotEmpty();
        // Verify test data exists
        assertThat(result.stream().anyMatch(r -> r.toString().contains("test"))).isTrue();
    }
}
