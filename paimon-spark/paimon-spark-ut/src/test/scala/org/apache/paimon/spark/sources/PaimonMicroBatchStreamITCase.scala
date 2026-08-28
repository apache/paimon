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
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.OutOfRangeException

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.read.streaming.ReadLimit

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

  private def createStream(table: FileStoreTable): PaimonMicroBatchStream = {
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
