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

package org.apache.paimon.spark.commands

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

class DataEvolutionMetadataScanCompatibilityTest extends PaimonSparkTestBase {

  private val modeOption = "data-evolution.metadata-memory-optimization.mode"
  private val options =
    "'bucket'='-1','row-tracking.enabled'='true'," +
      "'data-evolution.enabled'='true','write-only'='true'"

  Seq(2, 3).foreach {
    pages =>
      test(s"streaming metadata mode preserves SELECT with $pages sort-buffer pages") {
        withTable("t") {
          sql(s"""CREATE TABLE t (id INT, value INT) TBLPROPERTIES (
                 |$options,'page-size'='64 kb','sort-spill-buffer-size'='${pages * 64} kb')
                 |""".stripMargin)
          sql("INSERT INTO t VALUES (1,10),(2,20)")
          checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, 10), Row(2, 20)))
          sql(s"ALTER TABLE t SET TBLPROPERTIES ('$modeOption'='streaming')")
          checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, 10), Row(2, 20)))
          checkAnswer(sql("SELECT * FROM t WHERE id=2 LIMIT 1"), Seq(Row(2, 20)))
        }
      }
  }

  test("streaming metadata mode preserves fallback rows and current partition precedence") {
    withTable("t") {
      sql(s"""CREATE TABLE t (id INT, value INT, p INT) PARTITIONED BY (p)
             |TBLPROPERTIES ($options)""".stripMargin)
      sql("INSERT INTO t VALUES (1,10,1)")
      sql("CALL sys.create_branch('test.t','backup')")
      sql("INSERT INTO `t$branch_backup` VALUES (2,20,2)")
      sql("ALTER TABLE t SET TBLPROPERTIES ('scan.fallback-branch'='backup')")
      val original = sql("SELECT *,_ROW_ID FROM t").collect().toSeq
      checkAnswer(sql("SELECT id,value,p FROM t"), Seq(Row(1, 10, 1), Row(2, 20, 2)))
      sql(s"ALTER TABLE t SET TBLPROPERTIES ('$modeOption'='streaming')")
      checkAnswer(sql("SELECT *,_ROW_ID FROM t"), original)
      checkAnswer(sql("SELECT id,value,p FROM t WHERE p=2"), Seq(Row(2, 20, 2)))
      checkAnswer(sql("SELECT *,_ROW_ID FROM t"), original)
    }
  }

  test("streaming metadata mode preserves nonempty audit-log reads") {
    withTable("t") {
      sql(s"CREATE TABLE t (id INT, value INT) TBLPROPERTIES ($options)")
      sql("INSERT INTO t VALUES (1,10)")
      val original = sql("SELECT * FROM `t$audit_log`").collect().toSeq
      assert(original.nonEmpty)
      sql(s"ALTER TABLE t SET TBLPROPERTIES ('$modeOption'='streaming')")
      checkAnswer(sql("SELECT * FROM `t$audit_log`"), original)
    }
  }

  Seq("delta", "auto", "latest-delta").foreach {
    scanMode =>
      test(s"streaming metadata mode preserves nonempty $scanMode reads") {
        withTable("t") {
          sql(s"CREATE TABLE t (id INT, value INT) TBLPROPERTIES ($options)")
          sql("INSERT INTO t VALUES (1,10)")
          sql("INSERT INTO t VALUES (2,20)")
          def read(mode: String) = {
            val reader = spark.read.option(modeOption, mode)
            if (scanMode == "latest-delta") reader.option("scan.mode", "latest-delta")
            else {
              reader.option("incremental-between", "1,2")
              reader.option("incremental-between-scan-mode", scanMode)
            }
            reader.table("t")
          }
          checkAnswer(read("off"), Seq(Row(2, 20)))
          checkAnswer(read("streaming"), Seq(Row(2, 20)))
          checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, 10), Row(2, 20)))
          assert(loadTable("t").snapshotManager().latestSnapshot().id() == 2L)
        }
      }
  }
}
