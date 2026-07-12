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
class CompactChainTableProcedureTest extends PaimonSparkTestBase {

  test("Paimon Procedure: compact_chain_table - basic test") {
    withTable("chain_compact_t1") {
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

      // Compact empty partition
      checkAnswer(
        spark.sql(
          "CALL sys.compact_chain_table(table => '`chain_compact_t1`', partition => 'date=\"20260224\"')"),
        Row(false) :: Nil)

      // Insert snapshot data
      spark.sql(
        "insert into `chain_compact_t1$branch_snapshot` partition (date = '20260222') values (0, 1, '0')")
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
  }

  test("Paimon Procedure: compact_chain_table - overwrite test") {
    withTable("chain_compact_t2") {
      // Create chain table
      spark.sql("""
                  |CREATE TABLE IF NOT EXISTS chain_compact_t2 (
                  |    `t1` BIGINT COMMENT 't1',
                  |    `t2` BIGINT COMMENT 't2',
                  |    `t3` STRING COMMENT 't3'
                  |  ) PARTITIONED BY (`date` STRING COMMENT 'date')
                  |TBLPROPERTIES (
                  |     'dynamic-partition-overwrite' = 'false',
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

      spark.sql("insert into `chain_compact_t2` partition (date = '20260222') values (1, 1, '1')")

      // Insert snapshot data
      spark.sql(
        "insert into `chain_compact_t2$branch_snapshot` partition (date = '20260223') values (2, 1, '2')")
      spark.sql(
        "insert into `chain_compact_t2$branch_snapshot` partition (date = '20260224') values (3, 1, '3')")
      spark.sql(
        "insert into `chain_compact_t2$branch_snapshot` partition (date = '20260225') values (3, 2, '3-1')")

      // Insert delta data
      spark.sql(
        "insert into `chain_compact_t2$branch_delta` partition (date = '20260225') values (4, 2, '4')")

      // First call should fail because partition exists
      checkAnswer(
        spark.sql(
          "CALL sys.compact_chain_table(table => '`chain_compact_t2`', partition => 'date=\"20260225\"')"),
        Row(false) :: Nil)

      checkAnswer(
        sql("SELECT * FROM `chain_compact_t2` where date = '20260225'"),
        Seq(Row(3, 2, "3-1", "20260225"))
      )

      checkAnswer(
        spark.sql(
          "CALL sys.compact_chain_table(table => '`chain_compact_t2`', partition => 'date=\"20260225\"', overwrite => true)"),
        Row(true) :: Nil)

      // Verify snapshot branch now has the data
      checkAnswer(
        sql("SELECT * FROM `chain_compact_t2$branch_snapshot` where date = '20260225'"),
        Seq(Row(3, 1, "3", "20260225"), Row(4, 2, "4", "20260225"))
      )
      checkAnswer(
        sql("SELECT * FROM `chain_compact_t2$branch_snapshot` where date != '20260225'"),
        Seq(Row(2, 1, "2", "20260223"), Row(3, 1, "3", "20260224"))
      )

      // Check snapshots commit_kind
      checkAnswer(
        sql("select snapshot_id, commit_kind from `chain_compact_t2$branch_snapshot$snapshots`"),
        Seq(Row(1, "APPEND"), Row(2, "APPEND"), Row(3, "APPEND"), Row(4, "OVERWRITE"))
      )
    }
  }

  test("Paimon Procedure: compact_chain_table - test multiple partitions") {
    withTable("chain_compact_t3") {
      spark.sql("""
                  |CREATE TABLE IF NOT EXISTS chain_compact_t3 (
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
      setupChainTableBranches("chain_compact_t3")

      // Write snapshot branch data
      spark.sql(
        "insert into `chain_compact_t3$branch_snapshot` partition (dt = '20250810', hour = '20') values (0, 1, '0')")
      spark.sql(
        "insert into `chain_compact_t3$branch_snapshot` partition (dt = '20250810', hour = '22') values (1, 1, '1'),(2, 1, '1')")

      // Write delta branch data
      spark.sql(
        "insert into `chain_compact_t3$branch_delta` partition (dt = '20250810', hour = '21') values (1, 1, '1'),(2, 1, '1')")
      spark.sql(
        "insert into `chain_compact_t3$branch_delta` partition (dt = '20250810', hour = '22') values (1, 2, '1-1' ),(3, 1, '1' )")
      spark.sql(
        "insert into `chain_compact_t3$branch_delta` partition (dt = '20250810', hour = '23') values (2, 2, '1-1' ),(4, 1, '1' )")

      // Compact partition 20250810/hour=23
      checkAnswer(
        spark.sql(
          "CALL sys.compact_chain_table(table => '`chain_compact_t3`', partition => 'dt=\"20250810\",hour=\"23\"')"),
        Row(true) :: Nil
      )

      // Verify chain read still works correctly
      checkAnswer(
        sql("SELECT * FROM `chain_compact_t3` where dt = '20250810' and hour = '23'"),
        Seq(
          Row(1, 1, "1", "20250810", "23"),
          Row(2, 2, "1-1", "20250810", "23"),
          Row(4, 1, "1", "20250810", "23")
        )
      )

      spark.sql(
        "insert into `chain_compact_t3$branch_snapshot` partition (dt = '20250811', hour = '00') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1')")
      spark.sql(
        "insert into `chain_compact_t3$branch_snapshot` partition (dt = '20250811', hour = '02') values (1, 2, '1-1'),(2, 2, '1-1'),(3, 2, '1-1'), (4, 2, '1-1'), (5, 1, '1' ), (6, 1, '1')")
      spark.sql(
        "insert into `chain_compact_t3$branch_delta` partition (dt = '20250811', hour = '00') values (3, 2, '1-1' ),(4, 2, '1-1')")
      spark.sql(
        "insert into `chain_compact_t3$branch_delta` partition (dt = '20250811', hour = '01') values (5, 1, '1' ),(6, 1, '1' )")
      spark.sql(
        "insert into `chain_compact_t3$branch_delta` partition (dt = '20250811', hour = '02') values (5, 2, '1-1' ),(6, 2, '1-1' )")

      // Compact partition 20250811/hour=02 without overwrite
      checkAnswer(
        spark.sql(
          "CALL sys.compact_chain_table(table => '`chain_compact_t3`', partition => 'dt=\"20250811\",hour=\"02\"')"),
        Row(false) :: Nil
      )

      // Verify data is unchanged after skip
      checkAnswer(
        sql("SELECT * FROM `chain_compact_t3` where dt = '20250811' and hour = '02'"),
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
          "CALL sys.compact_chain_table(table => '`chain_compact_t3`', partition => 'dt=\"20250811\",hour=\"02\"', overwrite => true)"),
        Row(true) :: Nil
      )
      checkAnswer(
        sql("select snapshot_id,commit_kind from `chain_compact_t3$branch_snapshot$snapshots`"),
        Seq(
          Row(1, "APPEND"),
          Row(2, "APPEND"),
          Row(3, "APPEND"),
          Row(4, "APPEND"),
          Row(5, "APPEND"),
          Row(6, "OVERWRITE")
        )
      )

      // Check all snapshot partition data
      checkAnswer(
        sql("SELECT * FROM `chain_compact_t3` where dt = '20250811' and hour = '02'"),
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
        sql("SELECT * FROM `chain_compact_t3` where dt = '20250811' and hour = '00'"),
        Seq(
          Row(1, 2, "1-1", "20250811", "00"),
          Row(2, 2, "1-1", "20250811", "00"),
          Row(3, 2, "1-1", "20250811", "00"),
          Row(4, 2, "1-1", "20250811", "00")
        )
      )
      checkAnswer(
        sql(
          "SELECT * FROM `chain_compact_t3$branch_snapshot` where dt = '20250810' and hour = '23'"),
        Seq(
          Row(1, 1, "1", "20250810", "23"),
          Row(2, 2, "1-1", "20250810", "23"),
          Row(4, 1, "1", "20250810", "23")
        )
      )
      checkAnswer(
        sql(
          "SELECT * FROM `chain_compact_t3$branch_snapshot` where dt = '20250810' and hour = '22'"),
        Seq(Row(1, 1, "1", "20250810", "22"), Row(2, 1, "1", "20250810", "22"))
      )
    }
  }

  test("Paimon Procedure: compact_chain_table - test multiple partitions with group") {
    Seq(true, false).foreach(
      overwrite => {
        withTable("chain_compact_t4") {
          spark.sql(
            s"""
               |CREATE TABLE IF NOT EXISTS chain_compact_t4 (
               |    `t1` BIGINT COMMENT 't1',
               |    `t2` BIGINT COMMENT 't2',
               |    `t3` STRING COMMENT 't3'
               |  ) PARTITIONED BY (`region` STRING COMMENT 'region', `dt` STRING COMMENT 'dt', `hour` STRING COMMENT 'hour')
               |TBLPROPERTIES (
               |     'dynamic-partition-overwrite' = '$overwrite',
               |     'chain-table.enabled' = 'true',
               |     'primary-key' = 'region,dt,hour,t1',
               |     'sequence.field' = 't2',
               |     'bucket-key' = 't1',
               |     'bucket' = '1',
               |     'partition.timestamp-pattern' = '$$dt $$hour:00:00',
               |     'partition.timestamp-formatter' = 'yyyyMMdd HH:mm:ss',
               |     'merge-engine' = 'deduplicate',
               |     'chain-table.chain-partition-keys' = 'dt,hour'
               |  )
               |""".stripMargin)

          // Create branches
          setupChainTableBranches("chain_compact_t4")

          // Write snapshot branch data
          spark.sql(
            "insert into `chain_compact_t4$branch_snapshot` partition (region='CN', dt = '20250810', hour = '20') values (1, 1, '1')")
          spark.sql(
            "insert into `chain_compact_t4$branch_snapshot` partition (region='CN', dt = '20250810', hour = '21') values (2, 1, '1')")
          spark.sql(
            "insert into `chain_compact_t4$branch_snapshot` partition (region='CN', dt = '20250810', hour = '22') values (3, 1, '1')")
          spark.sql(
            "insert into `chain_compact_t4$branch_snapshot` partition (region='UK', dt = '20250810', hour = '21') values (21, 1, '1')")
          spark.sql(
            "insert into `chain_compact_t4$branch_snapshot` partition (region='FR', dt = '20250810', hour = '22') values (31, 1, '1')")
          spark.sql(
            "insert into `chain_compact_t4$branch_snapshot` partition (region='CA', dt = '20250810', hour = '21') values (41, 1, '1')")

          // Write delta branch data
          spark.sql(
            "insert into `chain_compact_t4$branch_delta` partition (region='CN', dt = '20250810', hour = '22') values (4, 1, '1')")
          spark.sql(
            "insert into `chain_compact_t4$branch_delta` partition (region='US', dt = '20250810', hour = '22') values (11, 1, '1')")
          spark.sql(
            "insert into `chain_compact_t4$branch_delta` partition (region='UK', dt = '20250810', hour = '22') values (22, 1, '1' )")

          checkAnswer(
            sql(
              "SELECT * FROM `chain_compact_t4$branch_snapshot` where dt = '20250810' and hour = '22'"),
            Seq(
              Row(3, 1, "1", "CN", "20250810", "22"),
              Row(31, 1, "1", "FR", "20250810", "22")
            )
          )
          checkAnswer(
            spark.sql(
              "CALL sys.compact_chain_table(table => '`chain_compact_t4`', partition => 'dt=\"20250810\", hour=\"22\"', overwrite => true)"),
            Row(true) :: Nil
          )
          checkAnswer(
            sql("select snapshot_id,commit_kind from `chain_compact_t4$branch_snapshot$snapshots`"),
            Seq(
              Row(1, "APPEND"),
              Row(2, "APPEND"),
              Row(3, "APPEND"),
              Row(4, "APPEND"),
              Row(5, "APPEND"),
              Row(6, "APPEND"),
              Row(7, "OVERWRITE")
            )
          )

          checkAnswer(
            sql(
              "SELECT * FROM `chain_compact_t4$branch_snapshot` where dt = '20250810' and hour = '21'"),
            Seq(
              Row(2, 1, "1", "CN", "20250810", "21"),
              Row(21, 1, "1", "UK", "20250810", "21"),
              Row(41, 1, "1", "CA", "20250810", "21")
            )
          )

          checkAnswer(
            sql(
              "SELECT * FROM `chain_compact_t4$branch_snapshot` where dt = '20250810' and hour = '22'"),
            Seq(
              Row(2, 1, "1", "CN", "20250810", "22"),
              Row(4, 1, "1", "CN", "20250810", "22"),
              Row(11, 1, "1", "US", "20250810", "22"),
              Row(21, 1, "1", "UK", "20250810", "22"),
              Row(22, 1, "1", "UK", "20250810", "22"),
              Row(31, 1, "1", "FR", "20250810", "22")
            )
          )
        }
      })
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
