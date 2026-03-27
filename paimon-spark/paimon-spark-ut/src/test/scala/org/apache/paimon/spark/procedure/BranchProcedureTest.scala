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

class BranchProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._
  test("Paimon Procedure: create, query, write and delete branch") {
    failAfter(streamingTimeout) {
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

            // create tags
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.create_tag(table => 'test.T', tag => 'test_tag', snapshot => 2)"),
              Row(true) :: Nil)
            checkAnswer(
              spark.sql("SELECT tag_name FROM paimon.test.`T$tags`"),
              Row("test_tag") :: Nil)

            // create branch with tag
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.create_branch(table => 'test.T', branch => 'test_branch', tag => 'test_tag')"),
              Row(true) :: Nil)
            val table = loadTable("T")
            val branchManager = table.branchManager()
            assert(branchManager.branchExists("test_branch"))

            // query from branch
            checkAnswer(
              spark.sql("SELECT * FROM `T$branch_test_branch` ORDER BY a"),
              Row(1, "a") :: Row(2, "b") :: Nil
            )
            checkAnswer(
              spark.read.format("paimon").option("branch", "test_branch").table("T").orderBy("a"),
              Row(1, "a") :: Row(2, "b") :: Nil
            )

            // update branch
            spark.sql("INSERT INTO `T$branch_test_branch` VALUES (3, 'c')")
            checkAnswer(
              spark.sql("SELECT * FROM `T$branch_test_branch` ORDER BY a"),
              Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil
            )
            // create tags
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.create_tag(table => 'test.`T$branch_test_branch`', tag => 'test_tag2', snapshot => 3)"),
              Row(true) :: Nil)

            // create branch from another branch.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.create_branch(table => 'test.`T$branch_test_branch`', branch => 'test_branch2', tag => 'test_tag2')"),
              Row(true) :: Nil)
            checkAnswer(
              spark.sql("SELECT * FROM `T$branch_test_branch2` ORDER BY a"),
              Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil
            )

            // create empty branch
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.create_branch(table => 'test.T', branch => 'empty_branch')"),
              Row(true) :: Nil)
            assert(branchManager.branchExists("empty_branch"))
            checkAnswer(
              spark.sql("SELECT * FROM `T$branch_empty_branch` ORDER BY a"),
              Nil
            )

            // delete branch
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.delete_branch(table => 'test.T', branch => 'test_branch')"),
              Row(true) :: Nil)
            assert(!branchManager.branchExists("test_branch"))
            intercept[Exception] {
              spark.sql("SELECT * FROM `T$branch_test_branch` ORDER BY a")
            }

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Branch: read with scan.fallback-branch") {
    withTable("T") {
      sql("""
            |CREATE TABLE T (
            |    dt STRING NOT NULL,
            |    name STRING NOT NULL,
            |    amount BIGINT
            |) PARTITIONED BY (dt)
            |""".stripMargin)

      sql("ALTER TABLE T SET TBLPROPERTIES ('k1' = 'v1')")
      sql("ALTER TABLE T SET TBLPROPERTIES ('k2' = 'v2')")

      sql("CALL sys.create_branch('test.T', 'test')")
      sql("ALTER TABLE T SET TBLPROPERTIES ('scan.fallback-branch' = 'test')")

      sql(
        "INSERT INTO `T$branch_test` VALUES ('20240725', 'apple', 4), ('20240725', 'peach', 10), ('20240726', 'cherry', 3), ('20240726', 'pear', 6)")
      sql("INSERT INTO T VALUES ('20240725', 'apple', 5), ('20240725', 'banana', 7)")

      checkAnswer(
        sql("SELECT * FROM T ORDER BY amount"),
        Seq(
          Row("20240726", "cherry", 3),
          Row("20240725", "apple", 5),
          Row("20240726", "pear", 6),
          Row("20240725", "banana", 7))
      )

      sql("ALTER TABLE T UNSET TBLPROPERTIES ('scan.fallback-branch')")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY amount"),
        Seq(Row("20240725", "apple", 5), Row("20240725", "banana", 7)))
    }
  }
}
