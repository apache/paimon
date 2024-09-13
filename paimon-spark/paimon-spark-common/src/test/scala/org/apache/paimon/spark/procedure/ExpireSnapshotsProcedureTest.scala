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
import org.apache.paimon.utils.SnapshotManager

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest
import org.assertj.core.api.Assertions.{assertThat, assertThatIllegalArgumentException}

import java.sql.Timestamp

class ExpireSnapshotsProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: expire snapshots") {
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

            // expire
            checkAnswer(
              spark.sql("CALL paimon.sys.expire_snapshots(table => 'test.T', retain_max => 2)"),
              Row(1) :: Nil)

            checkAnswer(
              spark.sql("SELECT snapshot_id FROM paimon.test.`T$snapshots`"),
              Row(2L) :: Row(3L) :: Nil)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: expire snapshots retainMax retainMin value check") {
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

            // expire assert throw exception
            assertThrows[IllegalArgumentException] {
              spark.sql(
                "CALL paimon.sys.expire_snapshots(table => 'test.T', retain_max => 2, retain_min => 3)")
            }
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("test parameter order_than with old timestamp type and new string type") {
    sql(
      "CREATE TABLE T (a INT, b STRING) " +
        "TBLPROPERTIES ( 'num-sorted-run.compaction-trigger' = '999' )")
    val table = loadTable("T")
    val snapshotManager = table.snapshotManager

    // generate 5 snapshot
    for (i <- 1 to 5) {
      sql(s"INSERT INTO T VALUES ($i, '$i')")
    }
    checkSnapshots(snapshotManager, 1, 5)

    // older_than with old timestamp type, like "1724664000000"
    val nanosecond: Long = snapshotManager.latestSnapshot().timeMillis * 1000
    spark.sql(
      s"CALL paimon.sys.expire_snapshots(table => 'test.T', older_than => $nanosecond, max_deletes => 2)")
    checkSnapshots(snapshotManager, 3, 5)

    // older_than with new string type, like "2024-08-26 17:20:00"
    val timestamp = new Timestamp(snapshotManager.latestSnapshot().timeMillis)
    spark.sql(
      s"CALL paimon.sys.expire_snapshots(table => 'test.T', older_than => '${timestamp.toString}', max_deletes => 2)")
    checkSnapshots(snapshotManager, 5, 5)
  }

  def checkSnapshots(sm: SnapshotManager, earliest: Int, latest: Int): Unit = {
    assertThat(sm.snapshotCount).isEqualTo(latest - earliest + 1)
    assertThat(sm.earliestSnapshotId).isEqualTo(earliest)
    assertThat(sm.latestSnapshotId).isEqualTo(latest)
  }
}
