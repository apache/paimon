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
import org.apache.paimon.utils.SnapshotNotExistException

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest

class CreateTagFromTimestampProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: Create tags from snapshots commit-time ") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
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

          try {

            for (i <- 1 to 4) {
              inputData.addData((i, "a"))
              stream.processAllAvailable()
              Thread.sleep(500L)
            }

            val table = loadTable("T")
            val earliestCommitTime = table.snapshotManager.earliestSnapshot.timeMillis
            val commitTime3 = table.snapshotManager.snapshot(3).timeMillis
            val commitTime4 = table.snapshotManager.snapshot(4).timeMillis

            // create tag from timestamp that earlier than the earliest snapshot commit time.
            checkAnswer(
              spark.sql(s"""CALL paimon.sys.create_tag_from_timestamp(
                           |table => 'test.T',
                           | tag => 'test_tag',
                           |  timestamp => ${earliestCommitTime - 1})""".stripMargin),
              Row("test_tag", 1, earliestCommitTime, "null") :: Nil
            )

            // create tag from timestamp that equals to snapshot-3 commit time.
            checkAnswer(
              spark.sql(s"""CALL paimon.sys.create_tag_from_timestamp(
                           |table => 'test.T',
                           | tag => 'test_tag2',
                           |  timestamp => $commitTime3)""".stripMargin),
              Row("test_tag2", 3, commitTime3, "null") :: Nil
            )

            // create tag from timestamp that later than snapshot-3 commit time.
            checkAnswer(
              spark.sql(s"""CALL paimon.sys.create_tag_from_timestamp(
                           |table => 'test.T',
                           |tag => 'test_tag3',
                           |timestamp => ${commitTime3 + 1})""".stripMargin),
              Row("test_tag3", 4, commitTime4, "null") :: Nil
            )

            // create tag from timestamp that later than the latest snapshot commit time and throw SnapshotNotExistException.
            assertThrows[SnapshotNotExistException] {
              spark.sql(s"""CALL paimon.sys.create_tag_from_timestamp(
                           |table => 'test.T',
                           |tag => 'test_tag3',
                           |timestamp => ${Long.MaxValue})""".stripMargin)
            }

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: Create tags from tags commit-time") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
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

          try {
            for (i <- 1 to 2) {
              inputData.addData((i, "a"))
              stream.processAllAvailable()
              Thread.sleep(500L)
            }

            checkAnswer(
              spark.sql(
                "CALL paimon.sys.create_tag(" +
                  "table => 'test.T', tag => 'test_tag', snapshot => 1)"),
              Row(true) :: Nil)

            val table = loadTable("T")
            val latestCommitTime = table.snapshotManager.latestSnapshot().timeMillis
            val tagsCommitTime = table.tagManager().taggedSnapshot("test_tag").timeMillis
            assert(latestCommitTime > tagsCommitTime)

            // make snapshot 1 expire.
            checkAnswer(
              spark.sql("CALL paimon.sys.expire_snapshots(table => 'test.T', retain_max => 1)"),
              Row(1) :: Nil)

            // create tag from timestamp that earlier than the expired snapshot 1.
            checkAnswer(
              spark.sql(s"""CALL paimon.sys.create_tag_from_timestamp(
                           |table => 'test.T',
                           | tag => 'test_tag1',
                           |  timestamp => ${tagsCommitTime - 1})""".stripMargin),
              Row("test_tag1", 1, tagsCommitTime, "null") :: Nil
            )

            // create tag from timestamp that later than the expired snapshot 1.
            checkAnswer(
              spark.sql(s"""CALL paimon.sys.create_tag_from_timestamp(
                           |table => 'test.T',
                           |tag => 'test_tag2',
                           |timestamp => ${tagsCommitTime + 1})""".stripMargin),
              Row("test_tag2", 2, latestCommitTime, "null") :: Nil
            )

          } finally {
            stream.stop()
          }
      }
    }
  }

}
