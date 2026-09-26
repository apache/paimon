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

import org.apache.paimon.catalog.{Catalog, CatalogLoader, DelegateCatalog, Identifier}
import org.apache.paimon.data.{BinaryString, GenericRow}
import org.apache.paimon.manifest.ManifestCommittable
import org.apache.paimon.options.Options
import org.apache.paimon.spark.sources.PaimonSink
import org.apache.paimon.spark.write.{CommitMarker, StreamingWrite, StreamingWriteContext}
import org.apache.paimon.table.{CatalogEnvironment, FileStoreTableFactory}
import org.apache.paimon.table.sink.{CommitCallback, InnerTableCommit}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.paimon.shims.memstream.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamTest}

import java.io.{File, IOException, OutputStream}
import java.util.{Collections, List => JList, Map => JMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

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

  private def causes(e: Throwable): Seq[Throwable] =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(20).toSeq

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

  test("Paimon Sink: complete mode replay on a postpone bucket table is not committed twice") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // A streaming write to a postpone bucket table writes bucket -2, the way a Flink
          // streaming write does: the fixed bucket paths are for a batch job that ends with its
          // commit, and 'postpone.batch-write-fixed-bucket' is not for a stream.
          spark.sql(
            "CREATE TABLE T (city STRING, population LONG) TBLPROPERTIES (" +
              "'primary-key' = 'city', 'bucket' = '-2', 'postpone.default-bucket-num' = '1')")
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

          // The written rows wait in bucket -2 for a compaction, so the table still reads empty.
          checkAnswer(spark.sql("SELECT * FROM T"), Nil)
          checkAnswer(spark.sql("SELECT count(*) FROM `T$buckets` WHERE bucket = -2"), Row(1L))
          val snapshotsAfterFirstBatch = snapshotCount("T")
          assert(
            latestCommitUser("T").startsWith("spark-query-"),
            s"expected the streaming committer to use the commit user derived from the query id, " +
              s"but got '${latestCommitUser("T")}'"
          )

          dropCommitLogEntry(checkpointPath, 0)
          runToCompletion(start())

          assert(
            snapshotCount("T") == snapshotsAfterFirstBatch,
            s"replaying batch 0 created another snapshot (${snapshotCount("T")} in total, " +
              s"$snapshotsAfterFirstBatch before the replay)"
          )

          spark.sql("CALL sys.compact(table => 'T')")
          checkAnswer(
            spark.sql("SELECT * FROM T ORDER BY city"),
            Row("BJ", 2L) :: Row("HZ", 1L) :: Nil)

          // A later batch is not a replay and has to be committed: the replay lookup must not
          // mistake a higher batch id for one that was already committed. Complete mode overwrites
          // what the compaction has just materialized.
          val snapshotsBeforeSecondBatch = snapshotCount("T")
          inputData.addData((4, "SH"))
          runToCompletion(start())

          assert(
            snapshotCount("T") == snapshotsBeforeSecondBatch + 1,
            s"the batch after the replay was not committed (${snapshotCount("T")} snapshots)")
          spark.sql("CALL sys.compact(table => 'T')")
          checkAnswer(
            spark.sql("SELECT * FROM T ORDER BY city"),
            Row("BJ", 2L) :: Row("HZ", 1L) :: Row("SH", 1L) :: Nil)
      }
    }
  }

  test("Paimon Sink: a replay retries the partition registration of the batch it skips") {
    withTempDir {
      checkpointDir =>
        // A commit publishes its snapshot and only then registers the partition in the metastore.
        // If that registration fails, the replay of the batch has to retry it: the snapshot
        // already exists, so the replay must not commit again, but it must not report success
        // before the partition is registered either.
        spark.sql(
          "CREATE TABLE T (city STRING, population LONG, dt STRING) PARTITIONED BY (dt) " +
            "TBLPROPERTIES ('primary-key' = 'city,dt', 'bucket' = '-2', " +
            "'postpone.default-bucket-num' = '1', 'metastore.partitioned-table' = 'true')")
        val base = loadTable("T")
        FailOnceRegistration.reset(paimonCatalog)
        val environment = new CatalogEnvironment(
          base.catalogEnvironment().identifier(),
          base.catalogEnvironment().uuid(),
          FailOnceRegistration.loader,
          null,
          null,
          null,
          false,
          true)
        val table = FileStoreTableFactory.create(
          base.fileIO(),
          base.location(),
          base.schema(),
          new Options(base.options()),
          environment)

        def newSink(): PaimonSink = new PaimonSink(
          spark.sqlContext,
          table,
          Nil,
          OutputMode.Complete(),
          Options.fromMap(
            Collections.singletonMap("checkpointLocation", checkpointDir.getCanonicalPath)))

        val batch: DataFrame = Seq(("HZ", 1L, "2026-09-12")).toDF("city", "population", "dt")

        // The first attempt fails after the snapshot is published.
        val failure = intercept[Exception](newSink().addBatch(0L, batch))
        assert(
          failure.getMessage.contains("metastore unavailable") ||
            Option(failure.getCause).exists(_.getMessage.contains("metastore unavailable")))
        assert(snapshotCount("T") == 1)
        assert(FailOnceRegistration.attempts == 1)
        assert(FailOnceRegistration.registered.isEmpty)

        // The restarted query replays the batch.
        newSink().addBatch(0L, batch)

        assert(snapshotCount("T") == 1, "the replay must not commit a second snapshot")
        assert(
          FailOnceRegistration.attempts == 2,
          "the replay must retry the partition registration that failed after the snapshot")
        assert(
          FailOnceRegistration.registered.contains("2026-09-12"),
          "the partition committed by the replayed batch must be registered")
    }
  }

  test("Paimon Sink: commit.user-prefix names the derived commit user") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // 'commit.user-prefix' names the writers of a table, as it does for a batch write. It
          // only prefixes the derived user, so the identity of the query is unchanged and a
          // replayed micro-batch is still recognised.
          spark.sql(
            "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES ('commit.user-prefix' = 'my-job')")
          val location = loadTable("T").location().toString
          val checkpointPath = checkpointDir.getCanonicalPath

          val inputData = MemoryStream[(Int, String)]
          val df = inputData.toDS().toDF("a", "b")
          inputData.addData((1, "a"))

          def start(): StreamingQuery =
            df.writeStream
              .option("checkpointLocation", checkpointPath)
              .format("paimon")
              .start(location)

          runToCompletion(start())

          assert(
            latestCommitUser("T").startsWith("my-job_spark-query-"),
            s"expected the prefix in front of the derived user, but the commit user is " +
              s"'${latestCommitUser("T")}'"
          )

          dropCommitLogEntry(checkpointPath, 0)
          runToCompletion(start())

          checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "a") :: Nil)
          assert(
            snapshotCount("T") == 1,
            s"replaying batch 0 created a second snapshot (${snapshotCount("T")} in total)")
      }
    }
  }

  test("Paimon Sink: commit.user-prefix must not make two queries share a commit user") {
    failAfter(streamingTimeout) {
      withTempDir {
        dir =>
          // The prefix belongs to every writer of the table, so what follows it still has to be
          // unique to the query: two queries sharing the prefix must not drop each other's
          // batches as replays.
          spark.sql(
            "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES ('commit.user-prefix' = 'my-job')")
          val location = loadTable("T").location().toString

          def runOneBatch(checkpoint: String, row: (Int, String)): Unit = {
            val input = MemoryStream[(Int, String)]
            input.addData(row)
            runToCompletion(
              input
                .toDS()
                .toDF("a", "b")
                .writeStream
                .option("checkpointLocation", new File(dir, checkpoint).getCanonicalPath)
                .format("paimon")
                .start(location))
          }

          runOneBatch("a", (1, "a"))
          val userA = latestCommitUser("T")
          runOneBatch("b", (2, "b"))
          val userB = latestCommitUser("T")

          assert(userA != userB, s"both queries committed as '$userA'")
          checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), Row(1, "a") :: Row(2, "b") :: Nil)
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

  test("Paimon Sink: a new checkpoint must not skip its batches under a named commit user") {
    failAfter(streamingTimeout) {
      withTempDir {
        dir =>
          // A new checkpoint numbers its batches from 0 again, so its identifiers collide with
          // those the previous query published. Only the query id, which is new with the
          // checkpoint, keeps the two apart: a name configured for the writers of the table,
          // which both queries share, must not.
          spark.sql("CREATE TABLE T (a INT, b STRING) TBLPROPERTIES ('commit.user-prefix' = 'job')")
          val location = loadTable("T").location().toString
          val checkpointDir = new File(dir, "cp")

          def runBatches(rows: (Int, String)*): Unit = {
            val inputData = MemoryStream[(Int, String)]
            val query = inputData
              .toDS()
              .toDF("a", "b")
              .writeStream
              .option("checkpointLocation", checkpointDir.getCanonicalPath)
              // No longer an option; a leftover setting must not pin the identity either.
              .option("write.stream.commit-user", "my-job")
              .format("paimon")
              .start(location)
            try {
              rows.foreach {
                row =>
                  inputData.addData(row)
                  query.processAllAvailable()
              }
            } finally {
              query.stop()
            }
          }

          runBatches((1, "old-1"), (2, "old-2"))
          val firstCommitUser = latestCommitUser("T")
          deleteRecursively(checkpointDir)
          runBatches((3, "new-1"), (4, "new-2"), (5, "new-3"))

          checkAnswer(
            spark.sql("SELECT * FROM T ORDER BY a"),
            Row(1, "old-1") :: Row(2, "old-2") :: Row(3, "new-1") :: Row(4, "new-2") ::
              Row(5, "new-3") :: Nil)
          assert(latestCommitUser("T") != firstCommitUser)
          assert(latestCommitUser("T").startsWith("job_spark-query-"))
      }
    }
  }

  test("Paimon Sink: a replay whose snapshot may have expired is refused, not committed twice") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // A replay is recognised by the snapshots of its commit user. Once expiration has
          // removed them, the sink cannot tell whether the batch was committed, and must not
          // commit it again on the chance that it was not.
          spark.sql(
            "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES (" +
              "'snapshot.num-retained.min' = '1', 'snapshot.num-retained.max' = '1')")
          val location = loadTable("T").location().toString
          val checkpointPath = checkpointDir.getCanonicalPath

          val inputData = MemoryStream[(Int, String)]
          val df = inputData.toDS().toDF("a", "b")
          inputData.addData((1, "stream"))

          def start(): StreamingQuery =
            df.writeStream
              .option("checkpointLocation", checkpointPath)
              .format("paimon")
              .start(location)

          runToCompletion(start())
          val streamSnapshot = loadTable("T").snapshotManager().latestSnapshotId()

          // Another writer commits, and its expiration removes the snapshot of the batch.
          spark.sql("INSERT INTO T VALUES (2, 'batch')")
          assert(
            !loadTable("T").snapshotManager().snapshotExists(streamSnapshot),
            "the snapshot of the streaming batch should have expired")

          // The driver failed before recording batch 0, which is replayed on restart.
          dropCommitLogEntry(checkpointPath, 0)
          val failure = intercept[Exception](runToCompletion(start()))
          assert(
            causes(failure).exists(
              e =>
                e.getMessage != null &&
                  e.getMessage.contains("Cannot tell whether micro-batch 1 of this query")),
            s"expected the replay to be refused, but got $failure"
          )

          checkAnswer(
            spark.sql("SELECT * FROM T ORDER BY a"),
            Row(1, "stream") :: Row(2, "batch") :: Nil)
      }
    }
  }

  test("Paimon Sink: a replay is refused after expiration with a session-configured checkpoint") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointRoot =>
          // Reviewer P1 on #10105: a checkpoint configured through the session and a query name
          // never reaches the sink options, so the marker has to be kept in the checkpoint the
          // query actually uses.
          spark.sql(
            "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES (" +
              "'snapshot.num-retained.min' = '1', 'snapshot.num-retained.max' = '1')")
          val location = loadTable("T").location().toString
          val queryName = "paimon_session_checkpoint"
          val checkpointPath = new File(checkpointRoot, queryName).getCanonicalPath

          withSQLConf("spark.sql.streaming.checkpointLocation" -> checkpointRoot.getCanonicalPath) {
            val inputData = MemoryStream[(Int, String)]
            val df = inputData.toDS().toDF("a", "b")
            inputData.addData((1, "stream"))

            def start(): StreamingQuery =
              df.writeStream
                .queryName(queryName)
                .format("paimon")
                .start(location)

            runToCompletion(start())
            assert(
              new File(checkpointPath, "paimon/commit-marker").exists(),
              "the marker belongs in the checkpoint the query uses")

            spark.sql("INSERT INTO T VALUES (2, 'batch')")
            dropCommitLogEntry(checkpointPath, 0)
            val failure = intercept[Exception](runToCompletion(start()))
            assert(
              causes(failure).exists(
                e =>
                  e.getMessage != null &&
                    e.getMessage.contains("Cannot tell whether micro-batch 1 of this query")),
              s"expected the replay to be refused, but got $failure"
            )

            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a"),
              Row(1, "stream") :: Row(2, "batch") :: Nil)
          }
      }
    }
  }

  test("Paimon Sink: a streaming query whose checkpoint cannot be found fails closed") {
    withTable("T2") {
      // Inside a stream execution, but with no checkpoint in the options and no running query to
      // take it from: writing without a marker could commit a replay twice, so the sink refuses.
      spark.sql("CREATE TABLE T2 (a INT, b STRING)")
      val sink =
        new PaimonSink(spark.sqlContext, loadTable("T2"), Nil, OutputMode.Append(), new Options())
      val batch: DataFrame = Seq((1, "a")).toDF("a", "b")

      spark.sparkContext.setLocalProperty(
        "sql.streaming.queryId",
        java.util.UUID.randomUUID().toString)
      try {
        val failure = intercept[IllegalStateException](sink.addBatch(0L, batch))
        assert(failure.getMessage.contains("Cannot find the checkpoint location"))
      } finally {
        spark.sparkContext.setLocalProperty("sql.streaming.queryId", null)
      }
      assert(snapshotCount("T2") == 0)
    }
  }

  test("Paimon Sink: the replay check refuses only what it cannot tell") {
    withTempDir {
      dir =>
        // The first micro-batch of a run may be a replay. With no snapshot of its commit user
        // left, the marker its previous attempt wrote before committing decides: no marker, or
        // one from another batch, means that attempt never committed; a marker older than every
        // retained snapshot means it may have, and the table cannot say.
        spark.sql(
          "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES (" +
            "'snapshot.num-retained.min' = '1', 'snapshot.num-retained.max' = '1')")
        spark.sql("INSERT INTO T VALUES (0, 'seed')")
        val hadoopConf = spark.sessionState.newHadoopConf()

        def attempt(name: String, prepare: CommitMarker => Unit, tableName: String = "T"): Unit = {
          val table = loadTable(tableName)
          val user = s"user-$name"
          val marker = new CommitMarker(new File(dir, name).getCanonicalPath, hadoopConf)
          prepare(marker)
          val context = new StreamingWriteContext(user, Some(marker))
          val builder = table.newStreamWriteBuilder().withCommitUser(user)
          val write = builder.newWrite()
          try {
            write.write(GenericRow.of(Integer.valueOf(1), BinaryString.fromString(name)))
            val messages = write.prepareCommit(true, 6).asScala.toSeq
            context.openCommitter(builder.newCommit().asInstanceOf[InnerTableCommit])
            context.commit(StreamingWrite(context, 5), messages, table)
          } finally {
            write.close()
            context.close()
          }
        }

        def latestId(): Long = loadTable("T").snapshotManager().latestSnapshotId()

        // No marker: the previous attempt never got to its commit.
        attempt("none", _ => ())
        // A marker of another batch says nothing about this one.
        attempt("other", _.write("user-other", 4, 0))
        // A marker of this batch that no expiration has gone past: a commit would be retained.
        attempt("kept", m => m.write("user-kept", 5, latestId()))
        assert(
          spark.sql("SELECT b FROM T WHERE a = 1").collect().map(_.getString(0)).toSet ==
            Set("none", "other", "kept"),
          "each of these attempts should have been committed"
        )

        // A marker of this batch on a table without any snapshot: nothing can have expired.
        withTable("T3") {
          spark.sql("CREATE TABLE T3 (a INT, b STRING)")
          attempt("empty", m => m.write("user-empty", 5, 0), "T3")
          checkAnswer(spark.sql("SELECT * FROM T3"), Row(1, "empty") :: Nil)
        }

        // A marker of this batch older than every retained snapshot: refused.
        val refused = intercept[IllegalStateException](
          attempt("expired", m => m.write("user-expired", 5, latestId() - 2)))
        assert(refused.getMessage.contains("Cannot tell whether micro-batch 6 of this query"))
        assert(spark.sql("SELECT * FROM T WHERE b = 'expired'").collect().isEmpty)

        // What the error says to do once the table shows the batch was not committed: delete
        // the marker it names, and the next attempt commits the batch.
        val named = "(/\\S+/paimon/commit-marker)".r.findFirstIn(refused.getMessage)
        assert(named.isDefined, s"the error should name the marker: ${refused.getMessage}")
        assert(new File(named.get).delete(), s"cannot delete ${named.get}")
        attempt("expired", _ => ())
        checkAnswer(spark.sql("SELECT a FROM T WHERE b = 'expired'"), Row(1) :: Nil)
    }
  }

  private def commitReplayAttempt(marker: CommitMarker, batchId: Long = 0): Unit = {
    val table = loadTable("T")
    val user = "recovery-user"
    val context = new StreamingWriteContext(user, Some(marker))
    val builder = table.newStreamWriteBuilder().withCommitUser(user)
    val write = builder.newWrite()
    try {
      write.write(GenericRow.of(Integer.valueOf(1), BinaryString.fromString("stream")))
      val messages = write.prepareCommit(true, batchId + 1).asScala.toSeq
      context.openCommitter(builder.newCommit().asInstanceOf[InnerTableCommit])
      context.commit(StreamingWrite(context, batchId), messages, table)
    } finally {
      write.close()
      context.close()
    }
  }

  test("Paimon Sink: repeated recovery preserves the first attempt's marker") {
    withTempDir {
      dir =>
        spark.sql(
          "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES (" +
            "'snapshot.num-retained.min' = '1', 'snapshot.num-retained.max' = '1')")
        val conf = spark.sessionState.newHadoopConf()
        val marker = new CommitMarker(dir.getCanonicalPath, conf)
        commitReplayAttempt(marker)
        val original = loadTable("T").snapshotManager().latestSnapshotId()
        val interrupted = new CommitMarker(dir.getCanonicalPath, conf) {
          override def write(user: String, batch: Long, latest: Long): Unit = {
            super.write(user, batch, latest)
            throw new IllegalStateException("driver failed before filtering")
          }
        }
        (1 to 2).foreach {
          _ =>
            assert(
              intercept[IllegalStateException](commitReplayAttempt(interrupted)).getMessage
                .contains("driver failed"))
        }
        assert(marker.latestSnapshotIdBefore("recovery-user", 0).contains(0L))
        spark.sql("INSERT INTO T VALUES (2, 'other')")
        assert(!loadTable("T").snapshotManager().snapshotExists(original))
        assert(
          intercept[IllegalStateException](commitReplayAttempt(marker)).getMessage
            .contains("Cannot tell whether"))
        checkAnswer(spark.sql("SELECT * FROM T WHERE b = 'stream'"), Row(1, "stream") :: Nil)
    }
  }

  Seq(false, true).foreach {
    staleHint =>
      test(s"Paimon Sink: expiration before replay filtering is refused (stale hint: $staleHint)") {
        withTempDir {
          dir =>
            spark.sql(
              "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES (" +
                "'snapshot.num-retained.min' = '1', 'snapshot.num-retained.max' = '1')")
            val conf = spark.sessionState.newHadoopConf()
            val marker = new CommitMarker(dir.getCanonicalPath, conf)
            commitReplayAttempt(marker)
            val original = loadTable("T").snapshotManager().latestSnapshotId()
            val concurrentExpiration = new CommitMarker(dir.getCanonicalPath, conf) {
              override def write(user: String, batch: Long, latest: Long): Unit = {
                super.write(user, batch, latest)
                // Interleave another writer after the old precheck but before actual filtering.
                spark.sql("INSERT INTO T VALUES (2, 'other')")
                val snapshots = loadTable("T").snapshotManager()
                assert(!snapshots.snapshotExists(original))
                if (staleHint) {
                  // Expiration removes snapshot files before advancing the earliest hint.
                  snapshots.commitEarliestHint(original)
                }
              }
            }
            assert(
              intercept[IllegalStateException](commitReplayAttempt(concurrentExpiration)).getMessage
                .contains("Cannot tell whether"))
            checkAnswer(spark.sql("SELECT * FROM T WHERE b = 'stream'"), Row(1, "stream") :: Nil)
        }
      }
  }

  test("Paimon Sink: a corrupt marker must not be treated as an uncommitted batch") {
    withTempDir {
      dir =>
        spark.sql(
          "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES (" +
            "'snapshot.num-retained.min' = '1', 'snapshot.num-retained.max' = '1')")
        val conf = spark.sessionState.newHadoopConf()
        val marker = new CommitMarker(dir.getCanonicalPath, conf)
        commitReplayAttempt(marker)
        spark.sql("INSERT INTO T VALUES (2, 'other')")
        // An older sink could truncate the committed batch's marker during recovery.
        val out = marker.path.getFileSystem(conf).create(marker.path, true)
        out.write("recovery-user\n0".getBytes("UTF-8"))
        out.close()
        val failure = intercept[IllegalStateException](commitReplayAttempt(marker))
        assert(failure.getMessage.contains(marker.path.toString))
        checkAnswer(spark.sql("SELECT * FROM T WHERE b = 'stream'"), Row(1, "stream") :: Nil)
    }
  }

  test("Paimon Sink: an interrupted marker update keeps the previous batch's marker") {
    withTempDir {
      dir =>
        val conf = new Configuration(spark.sessionState.newHadoopConf())
        conf.setClass(
          "fs.file.impl",
          classOf[FailingMarkerFileSystem],
          classOf[org.apache.hadoop.fs.FileSystem])
        conf.setBoolean("fs.file.impl.disable.cache", true)
        val marker = new CommitMarker(dir.getCanonicalPath, conf)
        marker.write("recovery-user", 0, 0)
        FailingMarkerFileSystem.failWrites = true
        try {
          // A same-batch retry must not write, even when the filesystem rejects new writes.
          marker.write("recovery-user", 0, 1)
          intercept[IOException](marker.write("recovery-user", 1, 2))
        } finally {
          FailingMarkerFileSystem.failWrites = false
        }
        assert(marker.latestSnapshotIdBefore("recovery-user", 0).contains(0L))
        marker.write("recovery-user", 1, 2)
        assert(marker.latestSnapshotIdBefore("recovery-user", 1).contains(2L))
        // Recreating the checkpoint gives the same batch number a different identity.
        marker.write("another-query", 0, 3)
        assert(marker.latestSnapshotIdBefore("another-query", 0).contains(3L))
    }
  }

  // Every kind of table the sink writes to: the write and commit paths differ by bucket mode and
  // by what a commit also produces (compactions, deletion vectors, changelog, row ids).
  Seq(
    ("dynamic bucket", "'primary-key' = 'k,pt', 'bucket' = '-1'", false),
    ("cross partition", "'primary-key' = 'k', 'bucket' = '-1'", false),
    (
      "postpone bucket with real buckets",
      "'primary-key' = 'k,pt', 'bucket' = '-2', 'postpone.default-bucket-num' = '1'",
      true),
    (
      "deletion vector",
      "'primary-key' = 'k,pt', 'bucket' = '1', 'deletion-vectors.enabled' = 'true'",
      false),
    (
      "lookup changelog",
      "'primary-key' = 'k,pt', 'bucket' = '1', 'changelog-producer' = 'lookup'",
      false),
    ("row tracking", "'row-tracking.enabled' = 'true'", false)
  ).foreach {
    case (kind, properties, postpone) =>
      test(s"Paimon Sink: a replay into a $kind table is not committed twice") {
        failAfter(streamingTimeout) {
          withTempDir {
            checkpointDir =>
              spark.sql(
                "CREATE TABLE T (k INT, v STRING, pt STRING) PARTITIONED BY (pt) " +
                  s"TBLPROPERTIES ($properties)")
              val seed = if (postpone) {
                // A batch write puts this into a real bucket, so the streaming batches below
                // are appended to a table that already has buckets.
                spark.sql("INSERT INTO T VALUES (0, 'seed', 'p1')")
                Row(0, "seed", "p1") :: Nil
              } else {
                Nil
              }
              val location = loadTable("T").location().toString
              val checkpointPath = checkpointDir.getCanonicalPath

              val inputData = MemoryStream[(Int, String, String)]
              val df = inputData.toDS().toDF("k", "v", "pt")
              def run(): Unit =
                runToCompletion(
                  df.writeStream
                    .option("checkpointLocation", checkpointPath)
                    .format("paimon")
                    .start(location))

              inputData.addData((1, "a", "p1"), (2, "b", "p2"))
              run()
              inputData.addData((3, "c", "p1"))
              run()
              if (postpone) {
                checkAnswer(
                  spark.sql("SELECT count(*) > 0 FROM `T$buckets` WHERE bucket = -2"),
                  Row(true))
              }

              // A primary key table would hide a duplicate behind its merge, so what shows a
              // replay committed again is a new snapshot.
              val snapshots = snapshotCount("T")
              dropCommitLogEntry(checkpointPath, 1)
              run()
              assert(
                snapshotCount("T") == snapshots,
                s"replaying batch 1 created ${snapshotCount("T") - snapshots} more snapshots")

              if (postpone) {
                spark.sql("CALL sys.compact(table => 'T')")
              }
              checkAnswer(
                spark.sql("SELECT k, v, pt FROM T ORDER BY k"),
                seed ++ (Row(1, "a", "p1") :: Row(2, "b", "p2") :: Row(3, "c", "p1") :: Nil))
          }
        }
      }
  }

  test("Paimon Sink: a batch that failed before its commit is committed once on restart") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // The query fails while writing batch 1, before anything is committed. The run's
          // committer has to be closed although the query did not stop cleanly, and the restart
          // replays batch 1, which, never committed, must be committed once.
          CountingCommitCallback.reset()
          spark.sql(
            "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES (" +
              s"'commit.callbacks' = '${classOf[CountingCommitCallback].getName}')")
          val location = loadTable("T").location().toString
          val checkpointPath = checkpointDir.getCanonicalPath

          val inputData = MemoryStream[(Int, String)]
          val df = inputData
            .toDS()
            .map {
              row =>
                if (row._1 == 2 && FailBeforeCommit.armed) {
                  throw new RuntimeException("injected failure before the commit")
                }
                row
            }
            .toDF("a", "b")
          def start(): StreamingQuery =
            df.writeStream
              .option("checkpointLocation", checkpointPath)
              .format("paimon")
              .start(location)

          FailBeforeCommit.armed = true
          try {
            val query = start()
            try {
              inputData.addData((1, "a"))
              query.processAllAvailable()
              inputData.addData((2, "b"))
              intercept[Exception](query.processAllAvailable())
            } finally {
              query.stop()
            }
          } finally {
            FailBeforeCommit.armed = false
          }
          assert(snapshotCount("T") == 1, "only batch 0 was committed before the failure")
          // The query terminates asynchronously; bounded by the timeout of this test.
          while (CountingCommitCallback.closed < CountingCommitCallback.created) {
            Thread.sleep(50)
          }

          runToCompletion(start())

          checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), Row(1, "a") :: Row(2, "b") :: Nil)
          assert(snapshotCount("T") == 2)
      }
    }
  }

  test("Paimon Sink: a query upgraded from per-batch commits keeps committing") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // Before this sink, every micro-batch was a batch write: a random commit user, the
          // batch commit identifier, and nothing under the checkpoint. A query resuming such a
          // checkpoint has a commit user with no history and no marker, on a table whose older
          // snapshots may have expired; its next batch is new and must be committed.
          spark.sql(
            "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES (" +
              "'snapshot.num-retained.min' = '1', 'snapshot.num-retained.max' = '1')")
          val location = loadTable("T").location().toString
          val checkpointPath = checkpointDir.getCanonicalPath

          val inputData = MemoryStream[(Int, String)]
          val df = inputData.toDS().toDF("a", "b")

          val before = df.writeStream
            .option("checkpointLocation", checkpointPath)
            .foreachBatch {
              (batch: DataFrame, _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()
          try {
            inputData.addData((1, "a"))
            before.processAllAvailable()
            inputData.addData((2, "b"))
            before.processAllAvailable()
          } finally {
            before.stop()
          }
          assert(loadTable("T").snapshotManager().earliestSnapshotId() > 1)

          inputData.addData((3, "c"))
          runToCompletion(
            df.writeStream
              .option("checkpointLocation", checkpointPath)
              .format("paimon")
              .start(location))

          checkAnswer(
            spark.sql("SELECT * FROM T ORDER BY a"),
            Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
          assert(latestCommitUser("T").startsWith("spark-query-"))
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

  test("Paimon Sink: maintenance of a micro-batch runs while the query is alive") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // The committer of a query lives until the query terminates, so the maintenance a
          // commit starts runs on while the next micro-batch is written. A committer closed after
          // every micro-batch would cancel an async expiration before it ever ran.
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
            // Bounded by the timeout of this test.
            while (snapshotCount("T") > 1) {
              Thread.sleep(50)
            }
          } finally {
            query.stop()
          }
      }
    }
  }

  test("Paimon Sink: one committer commits every micro-batch of a query and closes with it") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // The committer of a query is created with its first micro-batch and closed when the
          // query terminates, the way a Flink committer lives across checkpoints, so that the
          // maintenance a commit starts is not cancelled and only the first micro-batch of a run
          // has to look up what a previous run committed.
          CountingCommitCallback.reset()
          spark.sql(
            "CREATE TABLE T (a INT, b STRING) TBLPROPERTIES (" +
              s"'commit.callbacks' = '${classOf[CountingCommitCallback].getName}')")
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, String)]
          val query = inputData
            .toDS()
            .toDF("a", "b")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("paimon")
            .start(location)
          try {
            for (i <- 1 to 3) {
              inputData.addData((i, s"v$i"))
              query.processAllAvailable()
            }
            assert(
              CountingCommitCallback.commits == 3,
              s"expected three committed micro-batches, found ${CountingCommitCallback.commits}")
            assert(
              CountingCommitCallback.created == 1,
              s"expected one committer for the whole query, found " +
                s"${CountingCommitCallback.created}"
            )
            assert(
              CountingCommitCallback.closed == 0,
              "the committer must not be closed while the query is running")
          } finally {
            query.stop()
          }

          // The query terminates asynchronously; bounded by the timeout of this test.
          while (CountingCommitCallback.closed == 0) {
            Thread.sleep(50)
          }
          assert(CountingCommitCallback.created == 1)
      }
    }
  }
}

/** Counts the committers of a table, and what they commit. */
private[spark] class CountingCommitCallback extends CommitCallback {

  CountingCommitCallback.createdCount.incrementAndGet()

  override def call(context: CommitCallback.Context): Unit = {
    CountingCommitCallback.commitCount.incrementAndGet()
  }

  override def retry(committable: ManifestCommittable): Unit = {}

  override def close(): Unit = {
    CountingCommitCallback.closedCount.incrementAndGet()
  }
}

private[spark] object CountingCommitCallback {

  private[spark] val createdCount = new AtomicInteger(0)
  private[spark] val commitCount = new AtomicInteger(0)
  private[spark] val closedCount = new AtomicInteger(0)

  def reset(): Unit = {
    createdCount.set(0)
    commitCount.set(0)
    closedCount.set(0)
  }

  def created: Int = createdCount.get()

  def commits: Int = commitCount.get()

  def closed: Int = closedCount.get()
}

/** Makes a streaming write fail while writing, before anything is committed. */
private[spark] object FailBeforeCommit {
  @volatile var armed: Boolean = false
}

/** A catalog whose first partition registration fails, the way a metastore RPC can. */
private[spark] object FailOnceRegistration {

  @volatile private var wrapped: Catalog = _
  @volatile var attempts: Int = 0
  val registered: java.util.Set[String] =
    Collections.synchronizedSet(new java.util.HashSet[String]())

  def reset(catalog: Catalog): Unit = {
    wrapped = catalog
    attempts = 0
    registered.clear()
  }

  // Refers to this object only, so that the loader stays serializable with the table.
  val loader: CatalogLoader = () => new FailOnceCatalog(wrapped)

  private class FailOnceCatalog(catalog: Catalog) extends DelegateCatalog(catalog) {

    override def catalogLoader(): CatalogLoader = loader

    override def createPartitions(
        identifier: Identifier,
        partitions: JList[JMap[String, String]]): Unit = {
      attempts += 1
      if (attempts == 1) {
        throw new RuntimeException("metastore unavailable")
      }
      partitions.asScala.foreach(p => registered.add(p.get("dt")))
    }

    override def alterPartitions(
        identifier: Identifier,
        partitions: JList[org.apache.paimon.partition.PartitionStatistics]): Unit = {}

    override def dropPartitions(
        identifier: Identifier,
        partitions: JList[JMap[String, String]]): Unit = {}
  }
}

/** Fails after opening and partially writing a marker, as a storage failure can. */
private[spark] class FailingMarkerFileSystem extends RawLocalFileSystem {
  override protected def createOutputStreamWithMode(
      path: Path,
      append: Boolean,
      permission: FsPermission): OutputStream = {
    val out = super.createOutputStreamWithMode(path, append, permission)
    if (FailingMarkerFileSystem.failWrites) {
      out.write("partial".getBytes("UTF-8"))
      out.close()
      throw new IOException("marker write interrupted")
    }
    out
  }
}

private[spark] object FailingMarkerFileSystem {
  @volatile var failWrites: Boolean = false
}
