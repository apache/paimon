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

import org.apache.paimon.spark.sources.PaimonSourceOffset

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamTest, Trigger}
import org.junit.jupiter.api.Assertions

import java.util.concurrent.TimeUnit

class PaimonSourceTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Source: default scan mode") {
    withTempDir {
      checkpointDir =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, _) =
          prepareTableAndGetLocation(3, true)

        val query = spark.readStream
          .format("paimon")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          query.processAllAvailable()
          var totalStreamingData = snapshotData
          // in the default mode without any related configs, only data written in the last time will be read.
          checkAnswer(currentResult(), totalStreamingData)

          spark.sql("INSERT INTO T VALUES (40, 'v_40'), (41, 'v_41'), (42, 'v_42')")
          query.processAllAvailable()
          totalStreamingData ++= (Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil)
          checkAnswer(currentResult(), totalStreamingData)

          spark.sql("INSERT INTO T VALUES (50, 'v_50'), (51, 'v_51'), (52, 'v_52')")
          query.processAllAvailable()
          totalStreamingData ++= (Row(50, "v_50") :: Row(51, "v_51") :: Row(52, "v_52") :: Nil)
          checkAnswer(currentResult(), totalStreamingData)
        } finally {
          query.stop()
        }
    }
  }

  test("Paimon Source: default and from-snapshot scan mode with scan.snapshot-id") {
    withTempDirs {
      (checkpointDir1, checkpointDir2) =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, _) =
          prepareTableAndGetLocation(3, true)

        // set scan.snapshot-id = 3, this query can read the latest changes.
        val query1 = spark.readStream
          .format("paimon")
          .option("scan.snapshot-id", 3)
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir1.getCanonicalPath)
          .queryName("mem_table1")
          .outputMode("append")
          .start()

        // set scan.snapshot-id = 4, this query will read data from the next commit.
        val query2 = spark.readStream
          .format("paimon")
          .option("scan.snapshot-id", 4)
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir2.getCanonicalPath)
          .queryName("mem_table2")
          .outputMode("append")
          .start()

        val currentResult1 = () => spark.sql("SELECT * FROM mem_table1")
        val currentResult2 = () => spark.sql("SELECT * FROM mem_table2")
        try {
          query1.processAllAvailable()
          query2.processAllAvailable()
          var totalStreamingData1 = latestChanges
          var totalStreamingData2 = Seq.empty[Row]
          checkAnswer(currentResult1(), totalStreamingData1)
          checkAnswer(currentResult2(), totalStreamingData2)

          spark.sql("INSERT INTO T VALUES (40, 'v_40'), (41, 'v_41'), (42, 'v_42')")
          query1.processAllAvailable()
          query2.processAllAvailable()
          totalStreamingData1 =
            totalStreamingData1 ++ (Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil)
          totalStreamingData2 =
            totalStreamingData2 ++ (Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil)
          checkAnswer(currentResult1(), totalStreamingData1)
          checkAnswer(currentResult2(), totalStreamingData2)
        } finally {
          query1.stop()
          query2.stop()
        }
    }
  }

  test("Paimon Source: default and from-timestamp scan mode with scan.timestamp-millis") {
    withTempDirs {
      (checkpointDir1, checkpointDir2) =>
        // timestamp that is before this table is created and data is written.
        val ts1 = System.currentTimeMillis()
        val TableSnapshotState(_, location, snapshotData, latestChanges, _) =
          prepareTableAndGetLocation(3, true)
        // timestamp that is after this table is created and data is written.
        val ts2 = System.currentTimeMillis()

        val query1 = spark.readStream
          .format("paimon")
          .option("scan.timestamp-millis", ts1)
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir1.getCanonicalPath)
          .queryName("mem_table1")
          .outputMode("append")
          .start()

        val query2 = spark.readStream
          .format("paimon")
          .option("scan.mode", "from-timestamp")
          .option("scan.timestamp-millis", ts2)
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir2.getCanonicalPath)
          .queryName("mem_table2")
          .outputMode("append")
          .start()

        val currentResult1 = () => spark.sql("SELECT * FROM mem_table1")
        val currentResult2 = () => spark.sql("SELECT * FROM mem_table2")
        try {
          query1.processAllAvailable()
          query2.processAllAvailable()
          checkAnswer(currentResult1(), snapshotData)
          checkAnswer(currentResult2(), Seq.empty)

          spark.sql("INSERT INTO T VALUES (40, 'v_40'), (41, 'v_41'), (42, 'v_42')")
          query1.processAllAvailable()
          query2.processAllAvailable()
          val totalStreamingData1 =
            snapshotData ++ (Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil)
          val totalStreamingData2 = Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil
          checkAnswer(currentResult1(), totalStreamingData1)
          checkAnswer(currentResult2(), totalStreamingData2)
        } finally {
          query1.stop()
          query2.stop()
        }
    }
  }

  test("Paimon Source: latest and latest-full scan mode") {
    withTempDirs {
      (checkpointDir1, checkpointDir2) =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, _) =
          prepareTableAndGetLocation(3, true)

        val query1 = spark.readStream
          .format("paimon")
          .option("scan.mode", "latest-full")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir1.getCanonicalPath)
          .queryName("mem_table1")
          .outputMode("append")
          .start()

        val query2 = spark.readStream
          .format("paimon")
          .option("scan.mode", "latest")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir2.getCanonicalPath)
          .queryName("mem_table2")
          .outputMode("append")
          .start()

        val currentResult1 = () => spark.sql("SELECT * FROM mem_table1")
        val currentResult2 = () => spark.sql("SELECT * FROM mem_table2")
        try {
          query1.processAllAvailable()
          query2.processAllAvailable()
          // query1 uses the latest-full mode, which will scan the whole snapshot, not just the changes.
          var totalStreamingData1 = snapshotData
          // query2 uses the latest mode, which will only scan the changes.
          var totalStreamingData2 = latestChanges
          checkAnswer(currentResult1(), totalStreamingData1)
          checkAnswer(currentResult2(), totalStreamingData2)

          spark.sql("INSERT INTO T VALUES (40, 'v_40'), (41, 'v_41'), (42, 'v_42')")
          query1.processAllAvailable()
          query2.processAllAvailable()
          totalStreamingData1 =
            totalStreamingData1 ++ (Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil)
          totalStreamingData2 =
            totalStreamingData2 ++ (Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil)
          checkAnswer(currentResult1(), totalStreamingData1)
          checkAnswer(currentResult2(), totalStreamingData2)
        } finally {
          query1.stop()
          query2.stop()
        }
    }
  }

  test("Paimon Source: from-snapshot and from-snapshot-full scan mode") {
    withTempDirs {
      (checkpointDir1, checkpointDir2) =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, _) =
          prepareTableAndGetLocation(3, true)

        val query1 = spark.readStream
          .format("paimon")
          .option("scan.mode", "from-snapshot-full")
          .option("scan.snapshot-id", 3)
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir1.getCanonicalPath)
          .queryName("mem_table1")
          .outputMode("append")
          .start()

        val query2 = spark.readStream
          .format("paimon")
          .option("scan.mode", "from-snapshot")
          .option("scan.snapshot-id", 3)
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir2.getCanonicalPath)
          .queryName("mem_table2")
          .outputMode("append")
          .start()

        val currentResult1 = () => spark.sql("SELECT * FROM mem_table1")
        val currentResult2 = () => spark.sql("SELECT * FROM mem_table2")
        try {
          query1.processAllAvailable()
          query2.processAllAvailable()
          // query1 uses the from-snapshot-full mode, which will scan the whole snapshot, not just the changes.
          var totalStreamingData1 = snapshotData
          // query2 uses the from-snapshot mode, which will only scan the changes.
          var totalStreamingData2 = latestChanges
          checkAnswer(currentResult1(), totalStreamingData1)
          checkAnswer(currentResult2(), totalStreamingData2)

          spark.sql("INSERT INTO T VALUES (40, 'v_40'), (41, 'v_41'), (42, 'v_42')")
          query1.processAllAvailable()
          query2.processAllAvailable()
          totalStreamingData1 =
            totalStreamingData1 ++ (Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil)
          totalStreamingData2 =
            totalStreamingData2 ++ (Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil)
          checkAnswer(currentResult1(), totalStreamingData1)
          checkAnswer(currentResult2(), totalStreamingData2)
        } finally {
          query1.stop()
          query2.stop()
        }
    }
  }

  test("Paimon Source: Trigger AvailableNow") {
    withTempDir {
      checkpointDir =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, _) =
          prepareTableAndGetLocation(3, true)

        val query = spark.readStream
          .format("paimon")
          .option("scan.mode", "latest")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .trigger(Trigger.AvailableNow())
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          Assertions.assertTrue(query.isActive)
          query.processAllAvailable()
          checkAnswer(currentResult(), latestChanges)

          spark.sql("INSERT INTO T VALUES (40, 'v_40'), (41, 'v_41'), (42, 'v_42')")
          Assertions.assertFalse(query.isActive)
          query.processAllAvailable()
          // The query has been stopped after all available data at the start of the query have been read.
          // So no more data will be read.
          checkAnswer(currentResult(), latestChanges)
        } finally {
          query.stop()
        }
    }
  }

  test("Paimon Source: Trigger AvailableNow + latest scan mode + maxFilesPerTrigger read limit") {
    withTempDir {
      checkpointDir =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, snapshotToDataSplitNum) =
          prepareTableAndGetLocation(3, true)

        val query = spark.readStream
          .format("paimon")
          .option("scan.mode", "latest")
          .option("read.stream.maxFilesPerTrigger", "1")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .trigger(Trigger.AvailableNow())
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          Assertions.assertTrue(query.isActive)
          query.processAllAvailable()
          // 2 is the number of bucket, also is the number of DataSplit which is generated by planning.
          Assertions.assertEquals(2, query.recentProgress.count(_.numInputRows != 0))
          Assertions.assertEquals(
            PaimonSourceOffset(3L, 1L, scanSnapshot = false),
            PaimonSourceOffset(query.lastProgress.sources(0).endOffset))
          checkAnswer(currentResult(), latestChanges)

          spark.sql("INSERT INTO T VALUES (40, 'v_40'), (41, 'v_41'), (42, 'v_42')")
          Assertions.assertFalse(query.isActive)
        } finally {
          query.stop()
        }
    }
  }

  test("Paimon Source: Default Trigger + from-snapshot scan mode + maxBytesPerTrigger read limit") {
    withTempDir {
      checkpointDir =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, snapshotToDataSplitNum) =
          prepareTableAndGetLocation(3, false)
        val totalDataSplitNum = snapshotToDataSplitNum.values.sum

        val query = spark.readStream
          .format("paimon")
          .option("scan.mode", "from-snapshot")
          .option("scan.snapshot-id", 1)
          // Set 1 byte to maxBytesPerTrigger, that will lead to return only one data split.
          .option("read.stream.maxBytesPerTrigger", "1")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          query.processAllAvailable()
          // Each batch can consume one data split.
          // Therefore it takes totalDataSplitNum batches to consume all the data.
          Assertions.assertEquals(
            totalDataSplitNum,
            query.recentProgress.count(_.numInputRows != 0))
          Assertions.assertEquals(
            PaimonSourceOffset(3L, 1L, scanSnapshot = false),
            PaimonSourceOffset(query.lastProgress.sources(0).endOffset))
          var totalStreamingData = snapshotData
          checkAnswer(currentResult(), totalStreamingData)

          spark.sql("INSERT INTO T VALUES (40, 'v_40'), (41, 'v_41'), (42, 'v_42')")
          query.processAllAvailable()
          // Two additional batches are required to consume the new data.
          Assertions.assertEquals(
            totalDataSplitNum + 2,
            query.recentProgress.count(_.numInputRows != 0))
          Assertions.assertEquals(
            PaimonSourceOffset(4L, 1L, scanSnapshot = false),
            PaimonSourceOffset(query.lastProgress.sources(0).endOffset))
          totalStreamingData =
            totalStreamingData ++ (Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil)
          checkAnswer(currentResult(), totalStreamingData)
        } finally {
          query.stop()
        }
    }
  }

  test("Paimon Source: default scan mode + maxRowsPerTrigger read limit") {
    withTempDir {
      checkpointDir =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, _) =
          prepareTableAndGetLocation(3, true)

        val query = spark.readStream
          .format("paimon")
          .option("read.stream.maxRowsPerTrigger", "10")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          query.processAllAvailable()
          // Only 9 rows in this table. Only one batch can consume all the data.
          Assertions.assertEquals(1, query.recentProgress.count(_.numInputRows != 0))
          Assertions.assertEquals(
            PaimonSourceOffset(3L, 1L, scanSnapshot = true),
            PaimonSourceOffset(query.lastProgress.sources(0).endOffset))
          checkAnswer(currentResult(), snapshotData)
        } finally {
          query.stop()
        }
    }
  }

  test("Paimon Source: default scan mode + minRowsPerTrigger and maxTriggerDelayMs read limits") {
    withTempDirs {
      (checkpointDir1, checkpointDir2) =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, _) =
          prepareTableAndGetLocation(3, true)

        assertThrows[StreamingQueryException] {
          spark.readStream
            .format("paimon")
            .option("read.stream.minRowsPerTrigger", "10")
            .load(location)
            .writeStream
            .format("memory")
            .option("checkpointLocation", checkpointDir1.getCanonicalPath)
            .queryName("mem_table")
            .outputMode("append")
            .start()
            .processAllAvailable()
        }

        val query = spark.readStream
          .format("paimon")
          .option("read.stream.minRowsPerTrigger", "5")
          .option("read.stream.maxTriggerDelayMs", "5000")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir2.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          query.processAllAvailable()
          // One batch can consume all the data.
          Assertions.assertEquals(1, query.recentProgress.count(_.numInputRows != 0))
          var totalStreamingData = snapshotData
          checkAnswer(currentResult(), totalStreamingData)

          spark.sql("INSERT INTO T VALUES (40, 'v_40'), (41, 'v_41'), (42, 'v_42')")
          query.processAllAvailable()
          // There are only 3 new rows，at least 5 rows can trigger a batch.
          // So there latest committed offset is not changed.
          Assertions.assertEquals(1, query.recentProgress.count(_.numInputRows != 0))
          Assertions.assertEquals(
            PaimonSourceOffset(3L, 1L, scanSnapshot = true),
            PaimonSourceOffset(query.lastProgress.sources(0).endOffset))
          checkAnswer(currentResult(), totalStreamingData)

          Thread.sleep(5000)
          query.processAllAvailable()
          // Now 5s passed, a batch can be triggered even if there are only 3 rows, but the maximum delay time(5s) is reached.
          Assertions.assertEquals(2, query.recentProgress.count(_.numInputRows != 0))
          Assertions.assertEquals(
            PaimonSourceOffset(4L, 1L, scanSnapshot = false),
            PaimonSourceOffset(query.lastProgress.sources(0).endOffset))
          totalStreamingData =
            totalStreamingData ++ (Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil)
          checkAnswer(currentResult(), totalStreamingData)
        } finally {
          query.stop()
        }
    }
  }

  test(
    "Paimon Source: from-snapshot scan mode + maxRowsPerTrigger, minRowsPerTrigger and maxTriggerDelayMs read limits") {
    withTempDir {
      checkpointDir =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, _) =
          prepareTableAndGetLocation(0, false)

        val query = spark.readStream
          .format("paimon")
          .option("scan.mode", "from-snapshot")
          .option("scan.snapshot-id", 1)
          .option("read.stream.maxRowsPerTrigger", "6")
          .option("read.stream.minRowsPerTrigger", "4")
          .option("read.stream.maxTriggerDelayMs", "5000")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          spark.sql("INSERT INTO T VALUES (10, 'v_10'), (11, 'v_11'), (12, 'v_12')")
          query.processAllAvailable()
          // The first batch will always be triggered when use ReadMinRows read limit.
          Assertions.assertEquals(1, query.recentProgress.count(_.numInputRows != 0))

          spark.sql("INSERT INTO T VALUES (20, 'v_20'), (21, 'v_21'), (22, 'v_22')")
          query.processAllAvailable()
          Assertions.assertEquals(1, query.recentProgress.count(_.numInputRows != 0))
          Assertions.assertEquals(3L, query.lastProgress.numInputRows)

          spark.sql(
            """
              |INSERT INTO T VALUES (30, 'v_30'), (31, 'v_31'), (32, 'v_32'), (33, 'v_33'),
              | (34, 'v_34'), (35, 'v_35'), (36, 'v_36'), (37, 'v_37'), (38, 'v_38'), (39, 'v_39')
              | """.stripMargin)
          query.processAllAvailable()

          // TODO not work for bucket-key table?
          // Since the limits of minRowsPerTrigger and maxRowsPerTrigger, not all data can be consumed at this batch.
          // Assertions.assertEquals(3, query.recentProgress.count(_.numInputRows != 0))
          // Assertions.assertTrue(query.recentProgress.map(_.numInputRows).sum < 16)
          // Thread.sleep(6000)

          // the rest rows can trigger a batch. Then all the data are consumed.
          Assertions.assertEquals(3, query.recentProgress.count(_.numInputRows != 0))
          Assertions.assertEquals(16L, query.recentProgress.map(_.numInputRows).sum)
        } finally {
          query.stop()
        }
    }
  }

  test("Paimon Source: Trigger ProcessingTime 5s") {
    withTempDir {
      checkpointDir =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, _) =
          prepareTableAndGetLocation(3, true)

        val query = spark.readStream
          .format("paimon")
          .option("scan.mode", "latest")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          spark.sql("INSERT INTO T VALUES (40, 'v_40'), (41, 'v_41'), (42, 'v_42')")
          spark.sql("INSERT INTO T VALUES (50, 'v_50'), (51, 'v_51'), (52, 'v_52')")
          var totalStreamingData = latestChanges ++ (Row(40, "v_40") :: Row(41, "v_41") :: Row(
            42,
            "v_42") :: Row(50, "v_50") :: Row(51, "v_51") :: Row(52, "v_52") :: Nil)
          Thread.sleep(6 * 1000)
          checkAnswer(currentResult(), totalStreamingData)
        } finally {
          query.stop()
        }
    }
  }

  test("Paimon Source: with error options") {
    withTempDir {
      checkpointDir =>
        val TableSnapshotState(_, location, snapshotData, latestChanges, _) =
          prepareTableAndGetLocation(3, true)

        assertThrows[IllegalArgumentException] {
          spark.readStream
            .format("paimon")
            .option("scan.snapshot-id", 3)
            .option("scan.timestamp-millis", System.currentTimeMillis())
            .load(location)
            .writeStream
            .format("memory")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .queryName("mem_table")
            .outputMode("append")
            .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
            .start()
        }
    }
  }

  test("Paimon Source: not supports compacted-full scan mode in streaming mode") {
    withTempDir {
      checkpointDir =>
        val TableSnapshotState(_, location, _, _, _) =
          prepareTableAndGetLocation(3, true)

        val query = spark.readStream
          .format("paimon")
          .option("scan.mode", "incremental")
          .option("incremental-between", "3,5")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .start()

        assert(intercept[Exception] {
          query.processAllAvailable()
        }.getMessage.contains("Cannot read incremental in streaming mode"))
    }
  }

  test("Paimon Source and Sink") {
    withTempDir {
      checkpointDir =>
        val TableSnapshotState(_, location, _, latestChanges, _) =
          prepareTableAndGetLocation(3, true, tableName = "T1")

        val TableSnapshotState(_, targetLocation, _, _, _) =
          prepareTableAndGetLocation(0, false, tableName = "T2")

        val df = spark.readStream
          .format("paimon")
          .option("scan.snapshot-id", "3")
          .load(location)
          .writeStream
          .format("paimon")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)

        val currentResult = () => spark.sql("SELECT * FROM T2")
        var totalStreamingData = Seq.empty[Row]

        val query1 = df.start(targetLocation)
        try {
          query1.processAllAvailable()
          totalStreamingData = latestChanges
          checkAnswer(currentResult(), totalStreamingData)

          spark.sql("INSERT INTO T1 VALUES (40, 'v_40'), (41, 'v_41'), (42, 'v_42')")
          query1.processAllAvailable()
          totalStreamingData =
            totalStreamingData ++ (Row(40, "v_40") :: Row(41, "v_41") :: Row(42, "v_42") :: Nil)
          checkAnswer(currentResult(), totalStreamingData)
        } finally {
          query1.stop()
        }

        // scan.snapshot-id should be ignored when restarting from a checkpoint
        val query2 = df.start(targetLocation)
        try {
          query2.processAllAvailable()
          // no new data are queried, the target paimon table is not changed.
          checkAnswer(currentResult(), totalStreamingData)

          spark.sql("INSERT INTO T1 VALUES (50, 'v_50'), (51, 'v_51'), (52, 'v_52')")
          query2.processAllAvailable()
          totalStreamingData =
            totalStreamingData ++ (Row(50, "v_50") :: Row(51, "v_51") :: Row(52, "v_52") :: Nil)
          checkAnswer(currentResult(), totalStreamingData)
        } finally {
          query2.stop()
        }
    }
  }

  case class TableSnapshotState(
      table: String,
      location: String,
      snapshotData: Seq[Row],
      latestChanges: Seq[Row],
      snapshotToDataSplitNum: Map[Long, Int]
  )

  /** Create a paimon table, insert some data, return the location of this table. */
  private def prepareTableAndGetLocation(
      snapshotNum: Int,
      hasPk: Boolean,
      tableName: String = "T"): TableSnapshotState = {

    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    val primaryKeysProp = if (hasPk) {
      "'primary-key'='a',"
    } else {
      "'bucket-key'='a',"
    }
    spark.sql(s"""
                 |CREATE TABLE $tableName (a INT, b STRING)
                 |TBLPROPERTIES ($primaryKeysProp 'bucket'='2', 'file.format'='parquet')
                 |""".stripMargin)
    val table = loadTable(tableName)
    val location = table.location().toString

    val mergedData = scala.collection.mutable.TreeMap.empty[Int, String]
    val unmergedData = scala.collection.mutable.ArrayBuffer.empty[(Int, String)]
    var latestChanges = Array.empty[(Int, String)]

    def updateData(row: (Int, String)): Unit = {
      hasPk match {
        case true =>
          mergedData += (row._1 -> row._2)
        case false =>
          unmergedData += row
        case _ =>
          throw new IllegalArgumentException("Please provide write mode explicitly.")
      }
    }

    def currentTableSnapshotState: (Array[Row], Array[Row]) = {
      def toRow(data: (Int, String)) = Row(data._1, data._2)

      hasPk match {
        case true =>
          (mergedData.toArray[(Int, String)].map(toRow), latestChanges.map(toRow))
        case false =>
          (unmergedData.sorted.toArray.map(toRow), latestChanges.map(toRow))
        case _ =>
          throw new IllegalArgumentException("Please provide write mode explicitly.")
      }
    }

    val snapshotToDataSplitNum = scala.collection.mutable.Map.empty[Long, Int]
    val streamScan = table.newStreamScan()
    (1 to snapshotNum).foreach {
      snapshotId =>
        val startId = 10 * snapshotId
        val data = (startId to startId + 2).map {
          id =>
            val row = (id, s"v_$id")
            updateData(row)
            row
        }
        latestChanges = data.toArray
        data.toDF("a", "b").write.format("paimon").mode("append").save(location)

        streamScan.restore(snapshotId)
        val dataSplitNum = streamScan.plan().splits().size()
        snapshotToDataSplitNum += snapshotId.toLong -> dataSplitNum
    }

    val snapshotState = currentTableSnapshotState
    TableSnapshotState(
      "T",
      location,
      snapshotState._1,
      snapshotState._2,
      snapshotToDataSplitNum.toMap)
  }

}
