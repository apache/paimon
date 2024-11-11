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

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest

class AlterBranchProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._
  test("Paimon Procedure: alter schema structure and test $branch syntax.") {
    withTempDir {
      checkpointDir =>
        // define a change-log table and test `forEachBatch` api
        spark.sql(s"""
                     |CREATE TABLE T (a INT, b STRING)
                     |TBLPROPERTIES ('primary-key'='a', 'bucket'='3')
                     |""".stripMargin)
        val location = loadTable("T").location().toString

        val inputData = MemoryStream[(Int, String)]
        val stream = inputData
          .toDS()
          .toDF("a", "b")
          .writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .foreachBatch {
            (batch: Dataset[Row], _: Long) =>
              batch.write.format("paimon").mode("append").save(location)
          }
          .start()

        val query = () => spark.sql("SELECT * FROM T ORDER BY a")
        try {
          // snapshot-1
          inputData.addData((1, "a"))
          stream.processAllAvailable()
          checkAnswer(query(), Row(1, "a") :: Nil)

          // snapshot-2
          inputData.addData((2, "b"))
          stream.processAllAvailable()
          checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

          // snapshot-3
          inputData.addData((2, "b2"))
          stream.processAllAvailable()
          checkAnswer(query(), Row(1, "a") :: Row(2, "b2") :: Nil)

          val table = loadTable("T")
          val branchManager = table.branchManager()

          // create branch with tag
          checkAnswer(
            spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => 's_2', snapshot => 2)"),
            Row(true) :: Nil)
          checkAnswer(
            spark.sql(
              "CALL paimon.sys.create_branch(table => 'test.T', branch => 'snapshot_branch', tag => 's_2')"),
            Row(true) :: Nil)
          assert(branchManager.branchExists("snapshot_branch"))

          spark.sql("INSERT INTO T VALUES (1, 'APPLE'), (2,'DOG'), (2, 'horse')")
          spark.sql("ALTER TABLE `T$branch_snapshot_branch` ADD COLUMNS(c INT)")
          spark.sql(
            "INSERT INTO `T$branch_snapshot_branch` VALUES " + "(1,'cherry', 100), (2,'bird', 200), (3, 'wolf', 400)")

          checkAnswer(
            spark.sql("SELECT * FROM T ORDER BY a, b"),
            Row(1, "APPLE") :: Row(2, "horse") :: Nil)
          checkAnswer(
            spark.sql("SELECT * FROM `T$branch_snapshot_branch` ORDER BY a, b,c"),
            Row(1, "cherry", 100) :: Row(2, "bird", 200) :: Row(3, "wolf", 400) :: Nil)
          assert(branchManager.branchExists("snapshot_branch"))
        }
    }
  }
}
