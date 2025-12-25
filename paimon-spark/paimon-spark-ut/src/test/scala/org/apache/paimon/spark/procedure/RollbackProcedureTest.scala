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
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.streaming.StreamTest

class RollbackProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: rollback to snapshot and tag") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // define a change-log table and test `forEachBatch` api
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b STRING)
                       |TBLPROPERTIES ('primary-key'='a', 'bucket'='3')
                       |""".stripMargin)
          val table = loadTable("T")
          val location = table.location().toString

          val inputData = SparkShimLoader.shim.createMemoryStream[(Int, String)]
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

            checkAnswer(
              spark.sql(
                "CALL paimon.sys.create_tag(table => 'test.T', tag => 'test_tag', snapshot => 1)"),
              Row(true) :: Nil)

            // snapshot-2
            inputData.addData((2, "b"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

            // snapshot-3
            inputData.addData((2, "b2"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b2") :: Nil)
            assertThrows[RuntimeException] {
              spark.sql("CALL paimon.sys.rollback(table => 'test.T_exception', version =>  '2')")
            }
            // rollback to snapshot
            checkAnswer(
              spark.sql("CALL paimon.sys.rollback(table => 'test.T', version => '2')"),
              Row(table.latestSnapshot().get().id, 2) :: Nil)
            checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

            // rollback to tag
            val taggedSnapshotId = table.tagManager().getOrThrow("test_tag").trimToSnapshot().id
            checkAnswer(
              spark.sql("CALL paimon.sys.rollback(table => 'test.T', version => 'test_tag')"),
              Row(table.latestSnapshot().get().id, taggedSnapshotId) :: Nil)
            checkAnswer(query(), Row(1, "a") :: Nil)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: rollback to tag check test") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING)
                 |TBLPROPERTIES ('primary-key'='a', 'bucket'='3', 'file.format'='orc')
                 |""".stripMargin)

    val table = loadTable("T")

    val query = () => spark.sql("SELECT * FROM T ORDER BY a")

    // snapshot-1
    spark.sql("insert into T select 1, 'a'")
    checkAnswer(query(), Row(1, "a") :: Nil)

    checkAnswer(
      spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '20250122', snapshot => 1)"),
      Row(true) :: Nil)

    // snapshot-2
    spark.sql("insert into T select 2, 'b'")
    checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

    // snapshot-3
    spark.sql("insert into T select 3, 'c'")
    checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)

    // snapshot-4
    spark.sql("insert into T select 4, 'd'")
    checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil)

    assertThrows[RuntimeException] {
      spark.sql("CALL paimon.sys.rollback(table => 'test.T_exception', version => '4')")
    }
    // rollback to snapshot
    checkAnswer(
      spark.sql("CALL paimon.sys.rollback(table => 'test.T', version => '3')"),
      Row(table.latestSnapshot().get().id, 3) :: Nil)
    checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)

    // version/snapshot/tag can only set one of them
    assertThrows[RuntimeException] {
      spark.sql(
        "CALL paimon.sys.rollback(table => 'test.T', version => '20250122', tag => '20250122')")
    }

    assertThrows[RuntimeException] {
      spark.sql("CALL paimon.sys.rollback(table => 'test.T', version => '20250122', snapshot => 1)")
    }

    assertThrows[RuntimeException] {
      spark.sql("CALL paimon.sys.rollback(table => 'test.T', tag => '20250122', snapshot => 1)")
    }

    // rollback to snapshot
    spark.sql("CALL paimon.sys.rollback(table => 'test.T', snapshot => 2)")
    checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

    // rollback to tag
    spark.sql("CALL paimon.sys.rollback(table => 'test.T', tag => '20250122')")
    checkAnswer(query(), Row(1, "a") :: Nil)
  }

  test("Paimon Procedure: rollback to timestamp") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // define a change-log table and test `forEachBatch` api
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b STRING)
                       |TBLPROPERTIES ('primary-key'='a', 'bucket'='3')
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = SparkShimLoader.shim.createMemoryStream[(Int, String)]
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

          val table = loadTable("T")

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

            val timestamp = System.currentTimeMillis()

            // snapshot-3
            inputData.addData((2, "b2"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b2") :: Nil)

            // rollback to timestamp
            checkAnswer(
              spark.sql(
                s"CALL paimon.sys.rollback_to_timestamp(table => 'test.T', timestamp => $timestamp)"),
              Row(table.latestSnapshot().get().id, 2) :: Nil)
            checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: rollback with cache") {
    sql("CREATE TABLE T (id INT)")
    sql("INSERT INTO T VALUES (1), (2), (3), (4)")
    sql("DELETE FROM T WHERE id = 1")
    sql("CALL sys.rollback(table => 'T', version => '1')")
    sql("DELETE FROM T WHERE id = 1")
    checkAnswer(sql("SELECT * FROM T ORDER BY id"), Seq(Row(2), Row(3), Row(4)))
  }
}
