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

import org.apache.paimon.data.GenericRow
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.{DataSplit, Split}
import org.apache.paimon.utils.SnapshotManager

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.paimon.shims.memstream.MemoryStream
import org.apache.spark.sql.streaming.StreamTest
import org.assertj.core.api.Assertions.{assertThat, assertThatIllegalArgumentException}

import java.sql.Timestamp
import java.util.Collections

import scala.collection.JavaConverters._

class ExpireSnapshotsProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: expire snapshots") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // define a change-log table and test `forEachBatch` api
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b STRING)
                       |TBLPROPERTIES ('primary-key'='a', 'bucket'='3',
                       |'write-only' = 'true', 'snapshot.num-retained.min' = '1')
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

  test("Paimon Procedure: test parameter order_than with string type") {
    sql(
      "CREATE TABLE T (a INT, b STRING) " +
        "TBLPROPERTIES ( 'num-sorted-run.compaction-trigger' = '999'," +
        "'write-only' = 'true', 'snapshot.num-retained.min' = '1')")
    val table = loadTable("T")
    val snapshotManager = table.snapshotManager

    // generate 5 snapshot
    for (i <- 1 to 5) {
      sql(s"INSERT INTO T VALUES ($i, '$i')")
    }
    checkSnapshots(snapshotManager, 1, 5)

    val timestamp = new Timestamp(snapshotManager.latestSnapshot().timeMillis)
    spark.sql(
      s"CALL paimon.sys.expire_snapshots(table => 'test.T', older_than => '${timestamp.toString}', max_deletes => 2)")
    checkSnapshots(snapshotManager, 3, 5)
  }

  test("Paimon Procedure: expire snapshots load table property first") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b STRING)
                       |TBLPROPERTIES ('primary-key'='a', 'bucket'='3',
                       |'snapshot.num-retained.max' = '2',
                       |'snapshot.num-retained.min' = '1',
                       |'write-only' = 'true')
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
              spark.sql("CALL paimon.sys.expire_snapshots(table => 'test.T')"),
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

  test("Paimon Procedure: expire snapshots add options parameter") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b STRING)
                       |TBLPROPERTIES ('primary-key'='a', 'bucket'='3', 'write-only' = 'true')
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

            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_snapshots(table => 'test.T', options => 'snapshot.num-retained.max=2, snapshot.num-retained.min=1')"),
              Row(1L) :: Nil
            )

            checkAnswer(
              spark.sql("SELECT snapshot_id FROM paimon.test.`T$snapshots`"),
              Row(2L) :: Row(3L) :: Nil)
          } finally {
            stream.stop()
          }
      }
    }
  }

  Seq("none", "table", "dynamic", "retain_max").foreach {
    retentionSource =>
      test(s"Paimon Procedure: expire snapshots changelog retention from $retentionSource") {
        val independent = retentionSource != "none"
        val snapshotRetainMax = if (retentionSource == "retain_max") 4 else 1
        val tableRetention = retentionSource match {
          case "table" => ", 'changelog.time-retained' = '7 d'"
          case "retain_max" => ", 'changelog.num-retained.max' = '4'"
          case _ => ""
        }
        sql(
          "CREATE TABLE T (a INT, b INT) TBLPROPERTIES (" +
            "'primary-key' = 'a', 'bucket' = '1', 'write-only' = 'true', " +
            "'changelog-producer' = 'input', 'snapshot.num-retained.min' = '1', " +
            s"'snapshot.num-retained.max' = '$snapshotRetainMax'" + tableRetention + ")")
        val table = loadTable("T")
        val builder = table.newStreamWriteBuilder().withCommitUser("test")
        val writer = builder.newWrite()
        val commit = builder.newCommit()
        try {
          (1 to 4).foreach {
            i =>
              writer.write(GenericRow.of(Int.box(i), Int.box(i * 10)))
              commit.commit(i.toLong, writer.prepareCommit(true, i.toLong))
          }
        } finally {
          writer.close()
          commit.close()
        }
        checkSnapshots(table.snapshotManager(), 1, 4)
        val scan = table.newReadBuilder().newStreamScan()
        scan.restore(1L)
        val firstChangelog = scan.plan().splits()
        assert(readChangelogIds(table, firstChangelog) == List(1))

        val procedureArgs = retentionSource match {
          case "dynamic" => ", options => 'changelog.time-retained=7 d'"
          case "retain_max" => ", retain_max => 1"
          case _ => ""
        }
        checkAnswer(
          spark.sql(s"CALL paimon.sys.expire_snapshots(table => 'test.T'$procedureArgs)"),
          Row(3) :: Nil)
        checkSnapshots(table.snapshotManager(), 4, 4)
        firstChangelog.asScala.foreach {
          split =>
            val dataSplit = split.asInstanceOf[DataSplit]
            dataSplit.dataFiles().asScala.foreach {
              file =>
                assert(
                  table
                    .fileIO()
                    .exists(new Path(dataSplit.bucketPath(), file.fileName())) == independent)
            }
        }
        if (independent) {
          assert(table.changelogManager().earliestLongLivedChangelogId() == 1L)
          assert(table.changelogManager().latestLongLivedChangelogId() == 3L)
          assert(readChangelogIds(table, firstChangelog) == List(1))
          // Operation-local overrides do not change the reader's persisted configuration.
          // Retained history is discovered only when the reader also uses decoupled retention.
          val reloaded = loadTable("T")
          val readTable = retentionSource match {
            case "dynamic" =>
              reloaded.copy(Collections.singletonMap("changelog.time-retained", "7 d"))
            case "retain_max" =>
              assert(reloaded.options().get("snapshot.num-retained.max") == "4")
              assert(reloaded.options().get("changelog.num-retained.max") == "4")
              val coupledScan = reloaded.newReadBuilder().newStreamScan()
              coupledScan.restore(1L)
              // This first plan cannot discover the retained changelog through coupled options.
              assert(coupledScan.plan().splits().isEmpty)
              assert(readChangelogIds(reloaded, firstChangelog) == List(1))
              reloaded.copy(Collections.singletonMap("changelog.num-retained.max", "5"))
            case _ => reloaded
          }
          val restored = readTable.newReadBuilder().newStreamScan()
          restored.restore(1L)
          (1 to 4).foreach(
            i => assert(readChangelogIds(readTable, restored.plan().splits()) == List(i)))
        } else {
          assert(table.changelogManager().safelyGetAllChangelogs().isEmpty)
        }
        if (retentionSource == "dynamic") {
          assert(!loadTable("T").options().containsKey("changelog.time-retained"))
        }
        checkAnswer(sql("SELECT * FROM T"), (1 to 4).map(i => Row(i, i * 10)))
      }
  }

  private def readChangelogIds(table: FileStoreTable, splits: java.util.List[Split]): List[Int] = {
    val iter = table.newReadBuilder().newRead().createReader(splits).toCloseableIterator()
    try {
      iter.asScala.map(_.getInt(0)).toList
    } finally {
      iter.close()
    }
  }

  def checkSnapshots(sm: SnapshotManager, earliest: Int, latest: Int): Unit = {
    assertThat(sm.snapshotCount).isEqualTo(latest - earliest + 1)
    assertThat(sm.earliestSnapshotId).isEqualTo(earliest)
    assertThat(sm.latestSnapshotId).isEqualTo(latest)
  }
}
