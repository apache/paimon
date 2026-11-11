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

package org.apache.paimon.spark.sources

import org.apache.paimon.CoreOptions
import org.apache.paimon.consumer.Consumer
import org.apache.paimon.data.GenericRow
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.{DataTable, FileStoreTable}
import org.apache.paimon.table.source.OutOfRangeException
import org.apache.paimon.table.system.AuditLogTable

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.streaming.Trigger

import java.io.File
import java.util.{Collections, HashMap}

class PaimonMicroBatchStreamITCase extends PaimonSparkTestBase {

  private val consumerId = "spark-consumer"

  test("initialize stream when table has no snapshots") {
    val sourceTable = createTableWithoutSnapshot()
    assert(sourceTable.snapshotManager().earliestSnapshotId() == null)

    val initial = createStream(sourceTable).initialOffset().asInstanceOf[PaimonSourceOffset]

    assert(initial.snapshotId == 1L)
    assert(initial.index == PaimonSourceOffset.INIT_OFFSET_INDEX)
    assert(!initial.scanSnapshot)
  }

  test("keep legacy offset JSON when consumer is not configured") {
    val sourceTable = createTableWithOneSnapshot()
    val stream = createStream(sourceTable)
    val initial = stream.initialOffset().asInstanceOf[PaimonSourceOffset]

    val latest = latestOffset(stream, initial, ReadLimit.allAvailable())

    assert(latest.totalSplits.isEmpty)
    assert(!latest.json().contains("totalSplits"))
  }

  test("advance past an empty initial full snapshot without a consumer") {
    val sourceTable = createTableWithOneSnapshot()
    spark.sql("INSERT OVERWRITE T SELECT * FROM T WHERE false")

    val stream = createStream(sourceTable)
    val initial = stream.initialOffset().asInstanceOf[PaimonSourceOffset]
    val emptySnapshotId = sourceTable.snapshotManager().latestSnapshotId()
    assert(initial.snapshotId == emptySnapshotId)
    assert(initial.scanSnapshot)

    stream.lastTriggerMillis = System.currentTimeMillis()
    val emptyEnd = latestOffset(stream, initial, ReadLimit.minRows(100L, 60000L))
    assert(emptyEnd.snapshotId == emptySnapshotId + 1L)
    assert(emptyEnd.index == PaimonSourceOffset.INIT_OFFSET_INDEX)
    assert(!emptyEnd.scanSnapshot)

    spark.sql("INSERT INTO T VALUES (20, 'v_20')")
    val nextEnd = latestOffset(stream, emptyEnd, ReadLimit.allAvailable())

    assert(nextEnd.snapshotId == emptyEnd.snapshotId)
    assert(!nextEnd.scanSnapshot)
    assert(stream.planInputPartitions(emptyEnd, nextEnd).nonEmpty)
  }

  test("advance consumer past an empty initial full snapshot") {
    val sourceTable = createTableWithOneSnapshot()
    spark.sql("INSERT OVERWRITE T SELECT * FROM T WHERE false")
    val sourceTableWithConsumer = withConsumer(sourceTable)

    val stream = createStream(sourceTableWithConsumer)
    val initial = stream.initialOffset().asInstanceOf[PaimonSourceOffset]
    val emptySnapshotId = sourceTable.snapshotManager().latestSnapshotId()
    val emptyEnd = latestOffset(stream, initial, ReadLimit.allAvailable())

    assert(emptyEnd.snapshotId == emptySnapshotId + 1L)
    assert(emptyEnd.index == PaimonSourceOffset.INIT_OFFSET_INDEX)
    assert(!emptyEnd.scanSnapshot)
    assert(emptyEnd.emptySnapshotCompleted)

    stream.commit(emptyEnd)
    assert(consumerNextSnapshot(sourceTableWithConsumer) == emptySnapshotId + 1L)
  }

  test("do not advance an empty full snapshot past a delta deferred by ReadMinRows") {
    val sourceTable = createTableWithOneSnapshot()
    spark.sql("INSERT OVERWRITE T SELECT * FROM T WHERE false")
    val stream = createStream(sourceTable)
    val initial = stream.initialOffset().asInstanceOf[PaimonSourceOffset]
    spark.sql("INSERT INTO T VALUES (20, 'v_20')")
    stream.lastTriggerMillis = System.currentTimeMillis()

    val deferred = stream.latestOffset(initial, ReadLimit.minRows(100L, 60000L))

    assert(deferred == null)
  }

  test("resume an empty full snapshot after ReadMinRows delay") {
    val sourceTable = createTableWithOneSnapshot()
    spark.sql("INSERT OVERWRITE T SELECT * FROM T WHERE false")
    val stream = createStream(sourceTable)
    val initial = stream.initialOffset().asInstanceOf[PaimonSourceOffset]
    spark.sql("INSERT INTO T VALUES (20, 'v_20')")

    stream.lastTriggerMillis = System.currentTimeMillis()
    assert(stream.latestOffset(initial, ReadLimit.minRows(100L, 60000L)) == null)

    stream.lastTriggerMillis = System.currentTimeMillis() - 60001L
    val resumed = latestOffset(stream, initial, ReadLimit.minRows(100L, 60000L))
    assert(resumed.snapshotId == sourceTable.snapshotManager().latestSnapshotId())
    assert(!resumed.scanSnapshot)
    assert(stream.planInputPartitions(initial, resumed).nonEmpty)
  }

  test("create consumer only after the initial full snapshot is completely consumed") {
    val sourceTable = withConsumer(createTableWithOneSnapshot())
    val stream = createStream(sourceTable)
    val initial = stream.initialOffset().asInstanceOf[PaimonSourceOffset]

    assert(initial.scanSnapshot)
    val partial = latestOffset(stream, initial, ReadLimit.maxFiles(1))
    assert(partial.index == 0L)
    assert(partial.totalSplits.contains(2L))

    stream.commit(partial)
    assert(!sourceTable.consumerManager().consumer(consumerId).isPresent)

    val complete = latestOffset(stream, partial, ReadLimit.maxFiles(1))
    assert(complete.index == 1L)
    assert(complete.totalSplits.contains(2L))

    stream.commit(complete)
    assert(consumerNextSnapshot(sourceTable) == complete.snapshotId + 1)
  }

  test("protect partial delta snapshot after completing the initial full snapshot") {
    val sourceTable = withConsumer(createTableWithOneSnapshot())
    val stream = createStream(sourceTable)
    val initial = stream.initialOffset().asInstanceOf[PaimonSourceOffset]
    assert(initial.scanSnapshot)

    // Reuse the initial bucket keys so that both snapshots deterministically contain two splits.
    spark.sql("INSERT INTO T VALUES (10, 'v_10_2'), (11, 'v_11_2'), (12, 'v_12_2')")

    val partialDelta = latestOffset(stream, initial, ReadLimit.maxFiles(3))
    assert(partialDelta.snapshotId == initial.snapshotId + 1)
    assert(partialDelta.index == 0L)
    assert(partialDelta.totalSplits.contains(2L))
    assert(!partialDelta.scanSnapshot)
    assert(stream.planInputPartitions(initial, partialDelta).length == 3)

    stream.commit(partialDelta)

    assert(sourceTable.consumerManager().consumer(consumerId).isPresent)
    assert(consumerNextSnapshot(sourceTable) == partialDelta.snapshotId)

    val restartedInitial =
      createStream(sourceTable).initialOffset().asInstanceOf[PaimonSourceOffset]
    assert(restartedInitial.snapshotId == partialDelta.snapshotId)
    assert(restartedInitial.index == PaimonSourceOffset.INIT_OFFSET_INDEX)
    assert(!restartedInitial.scanSnapshot)
  }

  test("restart without Spark checkpoint from consumer progress") {
    val sourceTable = withConsumer(createTableWithOneSnapshot())
    val firstStream = createStream(sourceTable)
    val firstInitial = firstStream.initialOffset().asInstanceOf[PaimonSourceOffset]
    val firstComplete = latestOffset(firstStream, firstInitial, ReadLimit.allAvailable())
    firstStream.commit(firstComplete)

    spark.sql("INSERT INTO T VALUES (20, 'v_20'), (21, 'v_21'), (22, 'v_22')")

    val restartedStream = createStream(sourceTable)
    val restartedInitial = restartedStream.initialOffset().asInstanceOf[PaimonSourceOffset]
    assert(restartedInitial.snapshotId == firstComplete.snapshotId + 1)
    assert(!restartedInitial.scanSnapshot)

    val next = latestOffset(restartedStream, restartedInitial, ReadLimit.allAvailable())
    assert(next.snapshotId == restartedInitial.snapshotId)
    assert(!next.scanSnapshot)
  }

  test("write consumer progress to the scanned branch") {
    val mainTable = createTableWithOneSnapshot()
    mainTable.createTag("branch-base", mainTable.snapshotManager().latestSnapshotId())
    mainTable.createBranch("dev", "branch-base")
    val branchTable = withConsumer(mainTable.switchToBranch("dev").asInstanceOf[FileStoreTable])
    val stream = createStream(branchTable)
    val initial = stream.initialOffset().asInstanceOf[PaimonSourceOffset]
    val complete = latestOffset(stream, initial, ReadLimit.allAvailable())

    stream.commit(complete)

    assert(consumerNextSnapshot(branchTable) == complete.snapshotId + 1)
    assert(!mainTable.consumerManager().consumer(consumerId).isPresent)
  }

  test("consumer progress protects the next snapshot from expiration") {
    val mainTable = createTableWithOneSnapshot()
    val sourceTable = withConsumer(mainTable)
    val stream = createStream(sourceTable)
    val initial = stream.initialOffset().asInstanceOf[PaimonSourceOffset]
    val complete = latestOffset(stream, initial, ReadLimit.allAvailable())
    stream.commit(complete)
    val protectedSnapshot = complete.snapshotId + 1

    spark.sql("INSERT INTO T VALUES (20, 'v_20')")
    spark.sql("INSERT INTO T VALUES (30, 'v_30')")
    spark.sql("INSERT INTO T VALUES (40, 'v_40')")

    expireSnapshotsWithMinimalRetention(mainTable)

    assert(!mainTable.snapshotManager().snapshotExists(complete.snapshotId))
    assert(mainTable.snapshotManager().snapshotExists(protectedSnapshot))
    assert(mainTable.snapshotManager().earliestSnapshotId() == protectedSnapshot)

    val next = latestOffset(stream, complete, ReadLimit.allAvailable())
    assert(next.snapshotId >= protectedSnapshot)
    assert(!next.scanSnapshot)
    assert(stream.planInputPartitions(complete, next).nonEmpty)
  }

  test("fail rather than mix expired checkpoint with a new full scan") {
    val mainTable = createTableWithOneSnapshot()
    val sourceTable = withConsumer(mainTable)
    val initialStream = createStream(sourceTable)
    val initial = initialStream.initialOffset().asInstanceOf[PaimonSourceOffset]
    val complete = latestOffset(initialStream, initial, ReadLimit.allAvailable())
    assert(complete.snapshotCompleted)

    // Simulate a crash before the completed offset advances consumer progress.
    spark.sql("INSERT INTO T VALUES (20, 'v_20')")
    spark.sql("INSERT INTO T VALUES (30, 'v_30')")
    spark.sql("INSERT INTO T VALUES (40, 'v_40')")
    val loggedDeltaEnd = latestOffset(initialStream, complete, ReadLimit.allAvailable())
    assert(!loggedDeltaEnd.scanSnapshot)
    assert(loggedDeltaEnd.snapshotId == mainTable.snapshotManager().latestSnapshotId())

    expireSnapshotsWithMinimalRetention(mainTable)

    assert(!mainTable.snapshotManager().snapshotExists(complete.snapshotId + 1))
    assert(!mainTable.snapshotManager().snapshotExists(complete.snapshotId + 2))
    assert(mainTable.snapshotManager().earliestSnapshotId() == loggedDeltaEnd.snapshotId)

    val restartedStream = createStream(sourceTable)
    val exception = intercept[OutOfRangeException] {
      restartedStream.planInputPartitions(complete, loggedDeltaEnd)
    }
    assert(exception.getMessage.contains("no longer readable"))

    val currentFullEnd = latestOffset(restartedStream, complete, ReadLimit.allAvailable())
    assert(currentFullEnd.scanSnapshot)
    assert(restartedStream.planInputPartitions(complete, currentFullEnd).nonEmpty)
  }

  test("fail when expired recovery start is newer than logged end") {
    val mainTable = createTableWithOneSnapshot()
    val sourceTable = withConsumer(mainTable)
    val initialStream = createStream(sourceTable)
    val initial = initialStream.initialOffset().asInstanceOf[PaimonSourceOffset]
    val complete = latestOffset(initialStream, initial, ReadLimit.allAvailable())
    assert(complete.snapshotCompleted)

    spark.sql("INSERT INTO T VALUES (20, 'v_20')")
    spark.sql("INSERT INTO T VALUES (30, 'v_30')")
    spark.sql("INSERT INTO T VALUES (40, 'v_40')")
    val loggedEnd = latestOffset(initialStream, complete, ReadLimit.allAvailable())
    assert(loggedEnd.snapshotId == complete.snapshotId + 3)

    spark.sql("INSERT INTO T VALUES (50, 'v_50')")
    val latestSnapshotId = mainTable.snapshotManager().latestSnapshotId()
    assert(latestSnapshotId == loggedEnd.snapshotId + 1)

    expireSnapshotsWithMinimalRetention(mainTable)

    assert(!mainTable.snapshotManager().snapshotExists(loggedEnd.snapshotId))
    assert(mainTable.snapshotManager().earliestSnapshotId() == latestSnapshotId)

    val restartedStream = createStream(sourceTable)
    val exception = intercept[OutOfRangeException] {
      restartedStream.planInputPartitions(complete, loggedEnd)
    }
    assert(exception.getMessage.contains("newer than logged end offset"))
  }

  test("resume consumer from long-lived changelog after snapshot expiration") {
    val mainTable = createTableWithOneSnapshot()
    spark.sql("INSERT INTO T VALUES (20, 'v_20')")
    spark.sql("INSERT INTO T VALUES (30, 'v_30')")

    val lifecycleOptions = new HashMap[String, String]()
    lifecycleOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1")
    lifecycleOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1")
    lifecycleOptions.put(CoreOptions.SNAPSHOT_TIME_RETAINED.key(), "0 ms")
    lifecycleOptions.put(CoreOptions.CHANGELOG_NUM_RETAINED_MIN.key(), "1")
    lifecycleOptions.put(CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key(), "10")
    lifecycleOptions.put(CoreOptions.CHANGELOG_TIME_RETAINED.key(), "1 d")
    lifecycleOptions.put(CoreOptions.CONSUMER_CHANGELOG_ONLY.key(), "true")
    val lifecycleTable = mainTable.copy(lifecycleOptions)
    lifecycleTable.consumerManager().resetConsumer(consumerId, new Consumer(1L))
    lifecycleTable
      .newExpireSnapshots()
      .config(lifecycleTable.coreOptions().expireConfig())
      .expire()

    assert(lifecycleTable.snapshotManager().earliestSnapshotId() > 1L)
    assert(lifecycleTable.changelogManager().earliestLongLivedChangelogId() == 1L)

    val stream = createStream(withConsumer(lifecycleTable))
    val initial = stream.initialOffset().asInstanceOf[PaimonSourceOffset]
    assert(initial.snapshotId == 1L)
    assert(!initial.scanSnapshot)

    val end = latestOffset(stream, initial, ReadLimit.maxFiles(1))
    assert(end.snapshotId == 1L)
    assert(stream.planInputPartitions(initial, end).nonEmpty)
  }

  test("Spark query commits consumer progress through the source callback") {
    withTempDir {
      checkpointDir =>
        val mainTable = createTableWithOneSnapshot()
        val query = spark.readStream
          .format("paimon")
          .option(CoreOptions.CONSUMER_ID.key(), consumerId)
          .option("read.stream.maxFilesPerTrigger", "1")
          .load(mainTable.location().toString)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("spark_consumer_memory")
          .outputMode("append")
          .start()

        try {
          query.processAllAvailable()
          // Spark reports a source commit while constructing a later micro-batch.
          spark.sql("INSERT INTO T VALUES (20, 'v_20'), (21, 'v_21'), (22, 'v_22')")
          query.processAllAvailable()

          val consumer = mainTable.consumerManager().consumer(consumerId)
          assert(consumer.isPresent)
          assert(consumer.get().nextSnapshot() >= 2L)
        } finally {
          query.stop()
        }
    }
  }

  test("Spark query restarts after an empty initial full snapshot with a consumer") {
    withTempDir {
      checkpointDir =>
        val sourceTable = createTableWithOneSnapshot()
        spark.sql("INSERT OVERWRITE T SELECT * FROM T WHERE false")
        val location = sourceTable.location().toString
        val emptySnapshotId = sourceTable.snapshotManager().latestSnapshotId()
        val targetTable = createTableWithoutSnapshot("T2")
        val targetLocation = targetTable.location().toString

        val df = spark.readStream
          .format("paimon")
          .option(CoreOptions.CONSUMER_ID.key(), consumerId)
          .load(location)
          .writeStream
          .format("paimon")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)

        val firstQuery = df.start(targetLocation)
        try {
          firstQuery.processAllAvailable()
          checkAnswer(spark.sql("SELECT * FROM T2"), Seq.empty)
          val endOffset = PaimonSourceOffset(firstQuery.lastProgress.sources(0).endOffset)
          assert(endOffset.snapshotId == emptySnapshotId + 1L)
          assert(endOffset.index == PaimonSourceOffset.INIT_OFFSET_INDEX)
          assert(!endOffset.scanSnapshot)
          assert(endOffset.emptySnapshotCompleted)
        } finally {
          firstQuery.stop()
        }

        spark.sql("INSERT INTO T VALUES (20, 'v_20')")

        val restartedQuery = df.start(targetLocation)

        try {
          restartedQuery.processAllAvailable()
          checkAnswer(spark.sql("SELECT * FROM T2"), Seq(Row(20, "v_20")))
        } finally {
          restartedQuery.stop()
        }
    }
  }

  Seq("table", "audit-path", "audit-catalog").foreach {
    source =>
      test(s"Spark $source query discovers retained changelogs without retention overrides") {
        withTempDir {
          checkpointDir =>
            val sourceTable = createChangelogTable()
            appendChangelogRows(sourceTable, 1, 4)
            spark.sql("CALL paimon.sys.expire_snapshots(table => 'test.T', retain_max => 1)")
            assert(sourceTable.snapshotManager().earliestSnapshotId() == 4L)
            assert(sourceTable.changelogManager().earliestLongLivedChangelogId() == 1L)
            assert(!loadTable("T").coreOptions().changelogLifecycleDecoupled())

            val query = retainedChangelogStream(
              source,
              sourceTable,
              Map(CoreOptions.SCAN_SNAPSHOT_ID.key() -> "1")).writeStream
              .format("memory")
              .option("checkpointLocation", checkpointDir.getCanonicalPath)
              .queryName("retained_changelog_rows")
              .outputMode("append")
              .start()
            try {
              query.processAllAvailable()
              checkAnswer(
                spark.table("retained_changelog_rows"),
                expectedChangelogRows(source, 1, 4))
            } finally {
              query.stop()
              spark.catalog.dropTempView("retained_changelog_rows")
            }
        }
      }

      test(s"Spark $source query restores retained changelogs from its checkpoint") {
        withTempDir {
          directory =>
            val sourceTable = createChangelogTable()
            appendChangelogRows(sourceTable, 1, 1)
            val checkpoint = new File(directory, "checkpoint").getCanonicalPath
            val output = new File(directory, "output").getCanonicalPath
            val writer = retainedChangelogStream(
              source,
              sourceTable,
              Map(CoreOptions.CONSUMER_ID.key() -> consumerId)).writeStream
              .format("parquet")
              .option("checkpointLocation", checkpoint)
              .outputMode("append")
            val first = writer.start(output)
            try {
              first.processAllAvailable()
              checkAnswer(spark.read.parquet(output), expectedChangelogRows(source, 1, 1))
            } finally {
              first.stop()
            }
            appendChangelogRows(sourceTable, 2, 4)
            spark.sql("CALL paimon.sys.expire_snapshots(table => 'test.T', retain_max => 1)")
            assert(sourceTable.snapshotManager().earliestSnapshotId() == 4L)
            assert(sourceTable.changelogManager().longLivedChangelogExists(2L))
            assert(!loadTable("T").coreOptions().changelogLifecycleDecoupled())

            val restarted = writer.start(output)
            try {
              restarted.processAllAvailable()
              checkAnswer(spark.read.parquet(output), expectedChangelogRows(source, 1, 4))
            } finally {
              restarted.stop()
            }
        }
      }

  }

  for {
    timestampOption <- Seq(CoreOptions.SCAN_TIMESTAMP_MILLIS, CoreOptions.SCAN_CREATION_TIME_MILLIS)
    availableNow <- Seq(false, true)
  } {
    test(
      s"restore after timestamp history expires (${timestampOption.key()}, availableNow=$availableNow)") {
      withTempDir {
        directory =>
          val sourceTable = createChangelogTable()
          appendChangelogRows(sourceTable, 1, 4)
          spark.sql("CALL paimon.sys.expire_snapshots(table => 'test.T', retain_max => 1)")
          appendChangelogRows(sourceTable, 5, 6)
          val timestamp = sourceTable.snapshotManager().snapshot(5L).timeMillis()
          val checkpoint = new File(directory, "checkpoint").getCanonicalPath
          val output = new File(directory, "output").getCanonicalPath
          val writer = retainedChangelogStream(
            "table",
            sourceTable,
            Map(timestampOption.key() -> timestamp.toString)).writeStream
            .format("parquet")
            .option("checkpointLocation", checkpoint)
            .outputMode("append")
          if (availableNow) {
            writer.trigger(Trigger.AvailableNow())
          }
          val first = writer.start(output)
          try {
            if (availableNow) {
              assert(first.awaitTermination(60000L))
            } else {
              first.processAllAvailable()
            }
            checkAnswer(spark.read.parquet(output), expectedChangelogRows("table", 5, 6))
          } finally {
            first.stop()
          }

          appendChangelogRows(sourceTable, 7, 8)
          sourceTable.newExpireSnapshots().config(sourceTable.coreOptions().expireConfig()).expire()
          assert(!sourceTable.snapshotManager().snapshotExists(4L))
          assert(!sourceTable.changelogManager().longLivedChangelogExists(4L))
          assert(sourceTable.changelogManager().longLivedChangelogExists(3L))
          // Keep the saved position readable only through changelog metadata as well.
          spark.sql("CALL paimon.sys.expire_snapshots(table => 'test.T', retain_max => 1)")
          assert(!sourceTable.snapshotManager().snapshotExists(7L))
          assert(sourceTable.changelogManager().longLivedChangelogExists(7L))
          assert(!loadTable("T").coreOptions().changelogLifecycleDecoupled())
          val readTable =
            loadTable("T").copy(Collections.singletonMap(timestampOption.key(), timestamp.toString))
          intercept[OutOfRangeException] {
            readTable.newStreamScan().plan()
          }

          val restarted = writer.start(output)
          try {
            if (availableNow) {
              assert(restarted.awaitTermination(60000L))
            } else {
              restarted.processAllAvailable()
            }
            // The same checkpoint must resume at 7 without replaying 5 or 6.
            checkAnswer(spark.read.parquet(output), expectedChangelogRows("table", 5, 8))
          } finally {
            restarted.stop()
          }
      }
    }
  }

  test("AvailableNow keeps its full startup snapshot when a new commit arrives") {
    val sourceTable = createChangelogTable()
    appendChangelogRows(sourceTable, 1, 4)
    val stream = createStream(sourceTable)
    // Spark prepares AvailableNow before asking for the initial offset of a fresh query.
    stream.prepareForTriggerAvailableNow()
    appendChangelogRows(sourceTable, 5, 5)
    val initial = stream.initialOffset().asInstanceOf[PaimonSourceOffset]
    assert(initial.snapshotId == 4L)
    assert(initial.scanSnapshot)
    val end = latestOffset(stream, initial, ReadLimit.allAvailable())
    assert(end.snapshotId == 4L)
    assert(stream.planInputPartitions(initial, end).nonEmpty)
  }

  Seq(false, true).foreach {
    initiallyEmpty =>
      test(s"AvailableNow excludes commits after preparation (initiallyEmpty=$initiallyEmpty)") {
        val sourceTable = createChangelogTable()
        if (!initiallyEmpty) {
          appendChangelogRows(sourceTable, 1, 4)
        }
        val stream = createStream(
          sourceTable.copy(Collections.singletonMap(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), "0")))
        var offset = stream.initialOffset().asInstanceOf[PaimonSourceOffset]
        stream.prepareForTriggerAvailableNow()
        val nextId = if (initiallyEmpty) 1 else 5
        appendChangelogRows(sourceTable, nextId, nextId)
        if (!initiallyEmpty) {
          (1 to 4).foreach {
            id =>
              val next = latestOffset(stream, offset, ReadLimit.maxFiles(1))
              assert(next.snapshotId == id.toLong)
              assert(stream.planInputPartitions(offset, next).length == 1)
              offset = next
          }
        }
        assert(stream.latestOffset(offset, ReadLimit.allAvailable()) == null)
      }
  }

  Seq(false, true).foreach {
    audit =>
      test(
        s"retained changelogs do not replace full or overwrite startup snapshots (audit=$audit)") {
        val table = createChangelogTable()
        appendChangelogRows(table, 1, 4)
        spark.sql("CALL paimon.sys.expire_snapshots(table => 'test.T', retain_max => 1)")
        val readTable = loadTable("T")
        val fullSource: DataTable = if (audit) new AuditLogTable(readTable) else readTable
        val full = createStream(fullSource).initialOffset().asInstanceOf[PaimonSourceOffset]
        assert(full.snapshotId == 4L)
        assert(full.scanSnapshot)
        val options = new HashMap[String, String]()
        options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1")
        options.put(CoreOptions.STREAMING_READ_OVERWRITE.key(), "true")
        val overwriteTable = readTable.copy(options)
        val overwriteSource: DataTable =
          if (audit) new AuditLogTable(overwriteTable) else overwriteTable
        val overwrite =
          createStream(overwriteSource).initialOffset().asInstanceOf[PaimonSourceOffset]
        assert(overwrite.snapshotId == 4L)
        assert(!overwrite.scanSnapshot)
      }

  }

  private def retainedChangelogStream(
      source: String,
      table: FileStoreTable,
      options: Map[String, String]): DataFrame = {
    val reader = spark.readStream.format("paimon").options(options)
    source match {
      case "audit-path" => reader.option("read.changelog", "true").load(table.location().toString)
      case "audit-catalog" => reader.table("`T$audit_log`")
      case _ => reader.load(table.location().toString)
    }
  }

  private def expectedChangelogRows(source: String, from: Int, last: Int): Seq[Row] = {
    (from to last).map(i => if (source == "table") Row(i, i * 10) else Row("+I", i, i * 10))
  }

  private def createChangelogTable(): FileStoreTable = {
    spark.sql("""CREATE TABLE T (a INT, b INT) TBLPROPERTIES (
                |'primary-key' = 'a', 'bucket' = '1', 'write-only' = 'true',
                |'changelog-producer' = 'input', 'snapshot.num-retained.min' = '1',
                |'snapshot.num-retained.max' = '4', 'changelog.num-retained.max' = '4',
                |'consumer.changelog-only' = 'true')""".stripMargin)
    loadTable("T")
  }

  private def appendChangelogRows(table: FileStoreTable, from: Int, last: Int): Unit = {
    val builder = table.newStreamWriteBuilder().withCommitUser("retained-changelog-test")
    val writer = builder.newWrite()
    val commit = builder.newCommit()
    try {
      (from to last).foreach {
        i =>
          Option(table.snapshotManager().latestSnapshotId()).foreach {
            latest =>
              val previousTime = table.snapshotManager().snapshot(latest).timeMillis()
              while (System.currentTimeMillis() <= previousTime) {
                Thread.sleep(1L)
              }
          }
          writer.write(GenericRow.of(Int.box(i), Int.box(i * 10)))
          commit.commit(i.toLong, writer.prepareCommit(true, i.toLong))
      }
    } finally {
      writer.close()
      commit.close()
    }
  }

  private def createTableWithOneSnapshot(): FileStoreTable = {
    createTableWithoutSnapshot()
    spark.sql("INSERT INTO T VALUES (10, 'v_10'), (11, 'v_11'), (12, 'v_12')")
    loadTable("T")
  }

  private def createTableWithoutSnapshot(tableName: String = "T"): FileStoreTable = {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(s"""CREATE TABLE $tableName (a INT, b STRING)
                 |TBLPROPERTIES (
                 |  'bucket' = '2',
                 |  'bucket-key' = 'a',
                 |  'file.format' = 'parquet'
                 |)""".stripMargin)
    loadTable(tableName)
  }

  private def withConsumer(table: FileStoreTable): FileStoreTable = {
    table.copy(Collections.singletonMap(CoreOptions.CONSUMER_ID.key(), consumerId))
  }

  private def createStream(table: DataTable): PaimonMicroBatchStream = {
    new PaimonMicroBatchStream(table, table.newReadBuilder(), "unused")
  }

  private def latestOffset(
      stream: PaimonMicroBatchStream,
      start: PaimonSourceOffset,
      limit: ReadLimit): PaimonSourceOffset = {
    stream.latestOffset(start, limit).asInstanceOf[PaimonSourceOffset]
  }

  private def expireSnapshotsWithMinimalRetention(table: FileStoreTable): Unit = {
    val expireOptions = new HashMap[String, String]()
    expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1")
    expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1")
    expireOptions.put(CoreOptions.SNAPSHOT_TIME_RETAINED.key(), "0 ms")
    val expireTable = table.copy(expireOptions)
    expireTable
      .newExpireSnapshots()
      .config(expireTable.coreOptions().expireConfig())
      .expire()
  }

  private def consumerNextSnapshot(table: FileStoreTable): Long = {
    table.consumerManager().consumer(consumerId).get().nextSnapshot()
  }
}
