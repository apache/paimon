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

package org.apache.paimon.spark

import org.apache.paimon.options.Options
import org.apache.paimon.spark.sources.PaimonSink

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.paimon.shims.memstream.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamTest}

import java.io.File
import java.util.Collections

/**
 * Structured Streaming guarantees exactly-once only if the sink is idempotent for a repeated
 * batchId: when a query fails between the sink returning from `addBatch` and Spark recording the
 * batch as completed, the restarted query replays that micro-batch with its original batchId.
 */
class PaimonSinkIdempotencyTest extends PaimonSparkTestBase with StreamTest {

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.catalog.paimon.cache-enabled", "false")
  }

  import testImplicits._

  private def snapshotCount(tableName: String): Long =
    loadTable(tableName).snapshotManager().snapshotCount()

  private def latestCommitUser(tableName: String): String =
    loadTable(tableName).snapshotManager().latestSnapshot().commitUser()

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  private def runToCompletion(query: StreamingQuery): Unit = {
    try {
      query.processAllAvailable()
    } finally {
      query.stop()
    }
  }

  /**
   * Leave the checkpoint in the state a driver failure leaves behind when it dies after the sink
   * returned from `addBatch` but before Spark recorded the batch: the offset log still has the
   * batch, the commit log does not. The restarted query replays it with the same batchId.
   */
  private def dropCommitLogEntry(checkpointPath: String, batchId: Long): Unit = {
    val commitsDir = new File(checkpointPath, "commits")
    val names = Set(batchId.toString, s".$batchId.crc")
    val entries = commitsDir.listFiles().filter(f => names.contains(f.getName))
    assert(
      entries.exists(_.getName == batchId.toString),
      s"no commit log entry for batch $batchId in $commitsDir")
    entries.foreach(f => assert(f.delete()))
  }

  test("Paimon Sink: replayed micro-batch must not be committed twice") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql("CREATE TABLE T (a INT, b STRING)")
          val location = loadTable("T").location().toString
          val checkpointPath = checkpointDir.getCanonicalPath

          val inputData = MemoryStream[(Int, String)]
          val df = inputData.toDS().toDF("a", "b")
          inputData.addData((1, "a"), (2, "b"), (3, "c"))

          def start(): StreamingQuery =
            df.writeStream
              .option("checkpointLocation", checkpointPath)
              .format("paimon")
              .start(location)

          runToCompletion(start())

          val expected = Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil
          checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), expected)
          assert(snapshotCount("T") == 1)
          assert(
            latestCommitUser("T").startsWith("spark-query-"),
            s"expected a commit user derived from the query id, " +
              s"but got '${latestCommitUser("T")}'"
          )

          dropCommitLogEntry(checkpointPath, 0)
          runToCompletion(start())

          // The replayed batch must be recognised as already committed.
          checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), expected)
          assert(
            snapshotCount("T") == 1,
            s"replaying batch 0 created a second snapshot (${snapshotCount("T")} in total)")
      }
    }
  }

  test("Paimon Sink: replay is recognised when only the query id is available") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointRoot =>
          spark.sql("CREATE TABLE T (a INT, b STRING)")
          val location = loadTable("T").location().toString
          val queryName = "paimon_idempotency"
          // The location never reaches the sink options this way, so the commit user has to come
          // from the query id that Spark persists in the checkpoint metadata.
          val checkpointPath = new File(checkpointRoot, queryName).getCanonicalPath

          withSQLConf("spark.sql.streaming.checkpointLocation" -> checkpointRoot.getCanonicalPath) {
            val inputData = MemoryStream[(Int, String)]
            val df = inputData.toDS().toDF("a", "b")
            inputData.addData((1, "a"), (2, "b"), (3, "c"))

            def start(): StreamingQuery =
              df.writeStream
                .queryName(queryName)
                .format("paimon")
                .start(location)

            runToCompletion(start())

            val expected = Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil
            checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), expected)
            assert(snapshotCount("T") == 1)
            assert(
              latestCommitUser("T").startsWith("spark-query-"),
              s"expected a commit user derived from the query id, " +
                s"but got '${latestCommitUser("T")}'")

            dropCommitLogEntry(checkpointPath, 0)
            runToCompletion(start())

            checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), expected)
            assert(
              snapshotCount("T") == 1,
              s"replaying batch 0 created a second snapshot (${snapshotCount("T")} in total)")
          }
      }
    }
  }

  test("Paimon Sink: replay of a batch that is not the first one is recognised") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql("CREATE TABLE T (a INT, b STRING)")
          val location = loadTable("T").location().toString
          val checkpointPath = checkpointDir.getCanonicalPath

          val inputData = MemoryStream[(Int, String)]
          val df = inputData.toDS().toDF("a", "b")

          def start(): StreamingQuery =
            df.writeStream
              .option("checkpointLocation", checkpointPath)
              .format("paimon")
              .start(location)

          val query = start()
          try {
            inputData.addData((1, "a"))
            query.processAllAvailable()
            inputData.addData((2, "b"))
            query.processAllAvailable()
            inputData.addData((3, "c"))
            query.processAllAvailable()
          } finally {
            query.stop()
          }

          val expected = Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil
          checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), expected)
          assert(snapshotCount("T") == 3)

          dropCommitLogEntry(checkpointPath, 2)
          runToCompletion(start())

          checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), expected)
          assert(
            snapshotCount("T") == 3,
            s"replaying batch 2 created another snapshot (${snapshotCount("T")} in total)")
      }
    }
  }

  test("Paimon Sink: replayed micro-batch of a complete mode query is not committed twice") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql("CREATE TABLE T (city STRING, population LONG)")
          val location = loadTable("T").location().toString
          val checkpointPath = checkpointDir.getCanonicalPath

          val inputData = MemoryStream[(Int, String)]
          val df = inputData
            .toDS()
            .toDF("uid", "city")
            .groupBy("city")
            .count()
            .toDF("city", "population")
          inputData.addData((1, "HZ"), (2, "BJ"), (3, "BJ"))

          def start(): StreamingQuery =
            df.writeStream
              .outputMode("complete")
              .option("checkpointLocation", checkpointPath)
              .format("paimon")
              .start(location)

          runToCompletion(start())

          val expected = Row("BJ", 2L) :: Row("HZ", 1L) :: Nil
          checkAnswer(spark.sql("SELECT * FROM T ORDER BY city"), expected)
          val snapshotsAfterFirstBatch = snapshotCount("T")

          dropCommitLogEntry(checkpointPath, 0)
          runToCompletion(start())

          checkAnswer(spark.sql("SELECT * FROM T ORDER BY city"), expected)
          assert(
            snapshotCount("T") == snapshotsAfterFirstBatch,
            s"replaying batch 0 created another snapshot (${snapshotCount("T")} in total, " +
              s"$snapshotsAfterFirstBatch before the replay)"
          )
      }
    }
  }

  test("Paimon Sink: write.stream.commit-user overrides the derived commit user") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql("CREATE TABLE T (a INT, b STRING)")
          val location = loadTable("T").location().toString
          val checkpointPath = checkpointDir.getCanonicalPath

          val inputData = MemoryStream[(Int, String)]
          val df = inputData.toDS().toDF("a", "b")
          inputData.addData((1, "a"), (2, "b"), (3, "c"))

          def start(): StreamingQuery =
            df.writeStream
              .option("checkpointLocation", checkpointPath)
              .option("write.stream.commit-user", "my-streaming-job")
              .format("paimon")
              .start(location)

          runToCompletion(start())

          val expected = Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil
          checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), expected)
          assert(latestCommitUser("T") == "my-streaming-job")

          dropCommitLogEntry(checkpointPath, 0)
          runToCompletion(start())

          checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), expected)
          assert(
            snapshotCount("T") == 1,
            s"replaying batch 0 created a second snapshot (${snapshotCount("T")} in total)")
      }
    }
  }

  test("Paimon Sink: write.stream.commit-user can come from a session conf") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql("CREATE TABLE T (a INT, b STRING)")
          val location = loadTable("T").location().toString

          withSQLConf("spark.paimon.write.stream.commit-user" -> "job-from-conf") {
            val inputData = MemoryStream[(Int, String)]
            val df = inputData.toDS().toDF("a", "b")
            inputData.addData((1, "a"))

            runToCompletion(
              df.writeStream
                .option("checkpointLocation", checkpointDir.getCanonicalPath)
                .format("paimon")
                .start(location))
          }

          checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "a") :: Nil)
          assert(latestCommitUser("T") == "job-from-conf")
      }
    }
  }

  test("Paimon Sink: addBatch with a repeated batchId must be a no-op") {
    withTempDir {
      checkpointDir =>
        withTable("T2") {
          spark.sql("CREATE TABLE T2 (a INT, b STRING)")
          // Called outside a stream execution there is no query id, so this also covers the
          // checkpoint location fallback.
          val sink = new PaimonSink(
            spark.sqlContext,
            loadTable("T2"),
            Nil,
            OutputMode.Append(),
            Options.fromMap(
              Collections.singletonMap("checkpointLocation", checkpointDir.getCanonicalPath))
          )

          val batch: DataFrame = Seq((1, "a"), (2, "b")).toDF("a", "b")
          sink.addBatch(0L, batch)
          sink.addBatch(0L, batch)

          checkAnswer(spark.sql("SELECT * FROM T2 ORDER BY a"), Row(1, "a") :: Row(2, "b") :: Nil)
          assert(snapshotCount("T2") == 1)
          assert(
            latestCommitUser("T2").startsWith("spark-checkpoint-"),
            s"expected a commit user derived from the checkpoint location, " +
              s"but got '${latestCommitUser("T2")}'"
          )
        }
    }
  }

  test("Paimon Sink: a new query reusing a checkpoint location must not skip its batches") {
    failAfter(streamingTimeout) {
      withTempDir {
        dir =>
          spark.sql("CREATE TABLE T (a INT, b STRING)")
          val location = loadTable("T").location().toString
          val checkpointDir = new File(dir, "cp")

          def runOneBatch(row: (Int, String)): Unit = {
            val inputData = MemoryStream[(Int, String)]
            val df = inputData.toDS().toDF("a", "b")
            inputData.addData(row)
            runToCompletion(
              df.writeStream
                .option("checkpointLocation", checkpointDir.getCanonicalPath)
                .format("paimon")
                .start(location))
          }

          runOneBatch((1, "old"))
          val firstCommitUser = latestCommitUser("T")

          // The checkpoint is dropped and an unrelated query starts at the same location. Its
          // batch ids start at 0 again, so reusing the identity of the previous query would
          // make Paimon skip its data as an already committed replay.
          deleteRecursively(checkpointDir)
          runOneBatch((2, "new"))

          checkAnswer(
            spark.sql("SELECT * FROM T ORDER BY a"),
            Row(1, "old") :: Row(2, "new") :: Nil)
          assert(
            latestCommitUser("T") != firstCommitUser,
            "a query that does not continue the previous checkpoint must not reuse its " +
              "commit user")
      }
    }
  }

  test("Paimon Sink: an equivalent spelling of the checkpoint location keeps the identity") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql("CREATE TABLE T (a INT, b STRING)")
          val location = loadTable("T").location().toString
          val checkpointPath = checkpointDir.getCanonicalPath

          val inputData = MemoryStream[(Int, String)]
          val df = inputData.toDS().toDF("a", "b")
          inputData.addData((1, "a"))

          runToCompletion(
            df.writeStream
              .option("checkpointLocation", checkpointPath)
              .format("paimon")
              .start(location))
          val firstCommitUser = latestCommitUser("T")

          dropCommitLogEntry(checkpointPath, 0)
          // The same checkpoint, written with a trailing separator.
          runToCompletion(
            df.writeStream
              .option("checkpointLocation", checkpointPath + "/")
              .format("paimon")
              .start(location))

          checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "a") :: Nil)
          assert(
            latestCommitUser("T") == firstCommitUser,
            "the same query resuming the same checkpoint must keep its commit user")
      }
    }
  }

  test("Paimon Sink: expiration of a micro-batch completes before the committer closes") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // Async expiration plus a committer that is closed after every micro-batch:
          // maintenance has to run before that close, or expiration never happens.
          spark.sql(
            "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES (" +
              "'snapshot.expire.execution-mode' = 'async', " +
              "'snapshot.num-retained.min' = '1', " +
              "'snapshot.num-retained.max' = '1')")
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, String)]
          val df = inputData.toDS().toDF("a", "b")
          val query = df.writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("paimon")
            .start(location)
          try {
            for (i <- 1 to 4) {
              inputData.addData((i, s"v$i"))
              query.processAllAvailable()
            }
          } finally {
            query.stop()
          }

          assert(
            snapshotCount("T") == 1,
            s"expiration should retain a single snapshot, found ${snapshotCount("T")}")
      }
    }
  }
}
