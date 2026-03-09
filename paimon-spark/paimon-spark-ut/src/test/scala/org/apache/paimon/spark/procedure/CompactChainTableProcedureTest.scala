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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamTest

/** Test compact_chain_table procedure. See [[CompactChainTableProcedure]]. */
class CompactChainTableProcedureTest extends PaimonSparkTestBase with StreamTest {

  test("Paimon Procedure: compact_chain_table - basic test") {
    // Create chain table
    spark.sql("""
                |CREATE TABLE IF NOT EXISTS chain_compact_t1 (
                |    `t1` BIGINT COMMENT 't1',
                |    `t2` BIGINT COMMENT 't2',
                |    `t3` STRING COMMENT 't3'
                |  ) PARTITIONED BY (`date` STRING COMMENT 'date')
                |TBLPROPERTIES (
                |     'chain-table.enabled' = 'true',
                |     'primary-key' = 'date,t1',
                |     'sequence.field' = 't2',
                |     'bucket-key' = 't1',
                |     'bucket' = '1',
                |     'partition.timestamp-pattern' = '$date',
                |     'partition.timestamp-formatter' = 'yyyyMMdd'
                |)
                |""".stripMargin)

    // Create branches
    setupChainTableBranches("chain_compact_t1")

    // Insert snapshot data
    spark.sql(
      "insert into `chain_compact_t1$branch_snapshot` partition (date = '20260223') values (1, 1, '1')")

    // Insert delta data
    spark.sql(
      "insert into `chain_compact_t1$branch_delta` partition (date = '20260224') values (2, 2, '2')")

    // Before compaction: verify chain read shows snapshot + delta
    checkAnswer(
      sql("SELECT * FROM `chain_compact_t1` where date = '20260224'"),
      Seq(Row(1, 1, "1", "20260224"), Row(2, 2, "2", "20260224"))
    )

    checkAnswer(
      spark.sql(
        "CALL sys.compact_chain_table(table => '`chain_compact_t1`', partition => 'date=\"20260224\"')"),
      Row(true) :: Nil)

    // After compaction: verify snapshot branch has merged data
    checkAnswer(
      sql("SELECT * FROM `chain_compact_t1` where date = '20260224'"),
      Seq(Row(1, 1, "1", "20260224"), Row(2, 2, "2", "20260224"))
    )
    checkAnswer(
      sql("SELECT * FROM `chain_compact_t1$branch_snapshot` where date = '20260223'"),
      Seq(Row(1, 1, "1", "20260223"))
    )
    checkAnswer(
      sql("SELECT * FROM `chain_compact_t1$branch_snapshot` where date = '20260224'"),
      Seq(Row(1, 1, "1", "20260224"), Row(2, 2, "2", "20260224"))
    )

  }

  test("Paimon Procedure: compact_chain_table - overwrite test") {
    // Create chain table
    spark.sql("""
                |CREATE TABLE IF NOT EXISTS chain_compact_t2 (
                |    `t1` BIGINT COMMENT 't1',
                |    `t2` BIGINT COMMENT 't2',
                |    `t3` STRING COMMENT 't3'
                |  ) PARTITIONED BY (`date` STRING COMMENT 'date')
                |TBLPROPERTIES (
                |     'chain-table.enabled' = 'true',
                |     'primary-key' = 'date,t1',
                |     'sequence.field' = 't2',
                |     'bucket-key' = 't1',
                |     'bucket' = '1',
                |     'partition.timestamp-pattern' = '$date',
                |     'partition.timestamp-formatter' = 'yyyyMMdd'
                |  )
                |""".stripMargin)

    // Create branches
    setupChainTableBranches("chain_compact_t2")

    // Insert snapshot data
    spark.sql(
      "insert into `chain_compact_t2$branch_snapshot` partition (date = '20260224') values (1, 1, '1')")
    spark.sql(
      "insert into `chain_compact_t2$branch_snapshot` partition (date = '20260225') values (1, 2, '1-1')")

    // Insert delta data
    spark.sql(
      "insert into `chain_compact_t2$branch_delta` partition (date = '20260225') values (2, 2, '2')")

    // First call should fail because partition exists
    checkAnswer(
      spark.sql(
        "CALL sys.compact_chain_table(table => '`chain_compact_t2`', partition => 'date=\"20260225\"')"),
      Row(false) :: Nil)

    checkAnswer(
      sql("SELECT * FROM `chain_compact_t2` where date = '20260225'"),
      Seq(Row(1, 2, "1-1", "20260225"))
    )

    checkAnswer(
      spark.sql(
        "CALL sys.compact_chain_table(table => '`chain_compact_t2`', partition => 'date=\"20260225\"', overwrite => true)"),
      Row(true) :: Nil)

    // Verify snapshot branch now has the data
    checkAnswer(
      sql("SELECT * FROM `chain_compact_t2` where date = '20260225'"),
      Seq(Row(1, 1, "1", "20260225"), Row(2, 2, "2", "20260225"))
    )
  }

  test("Paimon Procedure: compact_chain_table - test multiple partitions") {
    spark.sql("""
                |CREATE TABLE IF NOT EXISTS chain_compact_multi (
                |    `t1` BIGINT COMMENT 't1',
                |    `t2` BIGINT COMMENT 't2',
                |    `t3` STRING COMMENT 't3'
                |  ) PARTITIONED BY (`dt` STRING COMMENT 'dt', `hour` STRING COMMENT 'hour')
                |TBLPROPERTIES (
                |     'chain-table.enabled' = 'true',
                |     'primary-key' = 'dt,hour,t1',
                |     'sequence.field' = 't2',
                |     'bucket-key' = 't1',
                |     'bucket' = '2',
                |     'partition.timestamp-pattern' = '$dt $hour:00:00',
                |     'partition.timestamp-formatter' = 'yyyyMMdd HH:mm:ss'
                |  )
                |""".stripMargin)

    // Create branches
    setupChainTableBranches("chain_compact_multi")

    // Write snapshot branch data
    spark.sql(
      "insert into `chain_compact_multi$branch_snapshot` partition (dt = '20250810', hour = '22') values (1, 1, '1'),(2, 1, '1')")

    // Write delta branch data
    spark.sql(
      "insert into `chain_compact_multi$branch_delta` partition (dt = '20250810', hour = '21') values (1, 1, '1'),(2, 1, '1')")
    spark.sql(
      "insert into `chain_compact_multi$branch_delta` partition (dt = '20250810', hour = '22') values (1, 2, '1-1' ),(3, 1, '1' )")
    spark.sql(
      "insert into `chain_compact_multi$branch_delta` partition (dt = '20250810', hour = '23') values (2, 2, '1-1' ),(4, 1, '1' )")

    // Compact partition 20250810/hour=23
    checkAnswer(
      spark.sql(
        "CALL sys.compact_chain_table(table => '`chain_compact_multi`', partition => 'dt=\"20250810\",hour=\"23\"')"),
      Row(true) :: Nil
    )

    // Verify chain read still works correctly
    checkAnswer(
      sql("SELECT * FROM `chain_compact_multi` where dt = '20250810' and hour = '23'"),
      Seq(
        Row(1, 1, "1", "20250810", "23"),
        Row(2, 2, "1-1", "20250810", "23"),
        Row(4, 1, "1", "20250810", "23")
      )
    )

    spark.sql(
      "insert into `chain_compact_multi$branch_snapshot` partition (dt = '20250811', hour = '00') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1')")
    spark.sql(
      "insert into `chain_compact_multi$branch_snapshot` partition (dt = '20250811', hour = '02') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1'), (5, 1, '1' ), (6, 1, '1')")
    spark.sql(
      "insert into `chain_compact_multi$branch_delta` partition (dt = '20250811', hour = '00') values (3, 2, '1-1' ),(4, 2, '1-1')")
    spark.sql(
      "insert into `chain_compact_multi$branch_delta` partition (dt = '20250811', hour = '01') values (5, 1, '1' ),(6, 1, '1' )")
    spark.sql(
      "insert into `chain_compact_multi$branch_delta` partition (dt = '20250811', hour = '02') values (5, 2, '1-1' ),(6, 2, '1-1' )")

    // Compact partition 20250811/hour=02 without overwrite
    checkAnswer(
      spark.sql(
        "CALL sys.compact_chain_table(table => '`chain_compact_multi`', partition => 'dt=\"20250811\",hour=\"02\"')"),
      Row(false) :: Nil
    )

    // Verify data is unchanged after skip
    checkAnswer(
      sql("SELECT * FROM `chain_compact_multi` where dt = '20250811' and hour = '02'"),
      Seq(
        Row(1, 2, "1-1", "20250811", "02"),
        Row(2, 2, "1-1", "20250811", "02"),
        Row(3, 2, "1-1", "20250811", "02"),
        Row(4, 2, "1-1", "20250811", "02"),
        Row(5, 1, "1", "20250811", "02"),
        Row(6, 1, "1", "20250811", "02")
      )
    )

    // Compact with overwrite to test overwrite path
    checkAnswer(
      spark.sql(
        "CALL sys.compact_chain_table(table => '`chain_compact_multi`', partition => 'dt=\"20250811\",hour=\"02\"', overwrite => true)"),
      Row(true) :: Nil
    )
    checkAnswer(
      sql("select snapshot_id,commit_kind from `chain_compact_multi$branch_snapshot$snapshots`"),
      Seq(
        Row(1, "APPEND"),
        Row(2, "APPEND"),
        Row(3, "APPEND"),
        Row(4, "APPEND"),
        Row(5, "OVERWRITE")
      )
    )

    // Check all snapshot partition data
    checkAnswer(
      sql("SELECT * FROM `chain_compact_multi` where dt = '20250811' and hour = '02'"),
      Seq(
        Row(1, 2, "1-1", "20250811", "02"),
        Row(2, 2, "1-1", "20250811", "02"),
        Row(3, 2, "1-1", "20250811", "02"),
        Row(4, 2, "1-1", "20250811", "02"),
        Row(5, 2, "1-1", "20250811", "02"),
        Row(6, 2, "1-1", "20250811", "02")
      )
    )
    checkAnswer(
      sql("SELECT * FROM `chain_compact_multi` where dt = '20250811' and hour = '00'"),
      Seq(
        Row(1, 2, "1-1", "20250811", "00"),
        Row(2, 2, "1-1", "20250811", "00"),
        Row(3, 2, "1-1", "20250811", "00"),
        Row(4, 2, "1-1", "20250811", "00")
      )
    )
    checkAnswer(
      sql(
        "SELECT * FROM `chain_compact_multi$branch_snapshot` where dt = '20250810' and hour = '23'"),
      Seq(
        Row(1, 1, "1", "20250810", "23"),
        Row(2, 2, "1-1", "20250810", "23"),
        Row(4, 1, "1", "20250810", "23")
      )
    )
    checkAnswer(
      sql(
        "SELECT * FROM `chain_compact_multi$branch_snapshot` where dt = '20250810' and hour = '22'"),
      Seq(Row(1, 1, "1", "20250810", "22"), Row(2, 1, "1", "20250810", "22"))
    )
  }

  def setupChainTableBranches(tableName: String): Unit = {
    spark.sql(s"CALL sys.create_branch('$tableName', 'snapshot');")
    spark.sql(s"CALL sys.create_branch('$tableName', 'delta');")

    // Set branch properties
    spark.sql(
      s"ALTER TABLE $tableName SET tblproperties (" +
        "'scan.fallback-snapshot-branch' = 'snapshot', " +
        "'scan.fallback-delta-branch' = 'delta')")
    spark.sql(
      s"ALTER TABLE `$tableName$$branch_snapshot` SET tblproperties (" +
        "'scan.fallback-snapshot-branch' = 'snapshot'," +
        "'scan.fallback-delta-branch' = 'delta')")
    spark.sql(
      s"ALTER TABLE `$tableName$$branch_delta` SET tblproperties (" +
        "'scan.fallback-snapshot-branch' = 'snapshot'," +
        "'scan.fallback-delta-branch' = 'delta')")
  }
}
