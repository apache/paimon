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

import org.apache.paimon.CoreOptions
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.manifest.FileSource
import org.apache.paimon.spark.read.BinPackingSplits
import org.apache.paimon.table.source.{DataSplit, DeletionFile, Split}

import org.junit.jupiter.api.Assertions

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable

class BinPackingSplitsTest extends PaimonSparkTestBase {

  test("Paimon: reshuffle splits") {
    withSparkSQLConf("spark.sql.files.minPartitionNum" -> "20") {
      val splitNum = 5
      val fileNum = 100

      val files = scala.collection.mutable.ListBuffer.empty[DataFileMeta]
      0.until(fileNum).foreach(i => files += newDataFile(s"f$i.parquet", 750000, 30000, 29999))

      val dataSplits = mutable.ArrayBuffer.empty[Split]
      0.until(splitNum).foreach {
        i =>
          dataSplits += newDataSplitFromFiles(
            files.zipWithIndex.filter(_._2 % splitNum == i).map(_._1).toSeq,
            rawConvertible = true)
      }

      val binPacking = BinPackingSplits(CoreOptions.fromMap(new JHashMap()))
      val reshuffled = binPacking.pack(dataSplits.toArray)
      Assertions.assertTrue(reshuffled.length > 5)
    }
  }

  test("Paimon: reshuffle one split") {
    val split = newDataSplitFromFiles(
      Seq(newDataFile("f1.parquet", 750000, 30000, 29999)),
      rawConvertible = true)

    val dataSplits: Array[Split] = Array(split)

    val binPacking = BinPackingSplits(CoreOptions.fromMap(new JHashMap()))
    val reshuffled = binPacking.pack(dataSplits)
    Assertions.assertEquals(1, reshuffled.length)
  }

  test("Paimon: pack data evolution splits by split granularity") {
    withSparkSQLConf("spark.sql.files.minPartitionNum" -> "1") {
      val split1 = newDataSplit("split1", Seq(40L, 40L), deletionFileLength = Some(5L))
      val split2 = newDataSplit("split2", Seq(40L, 40L), deletionFileLength = Some(5L))
      val split3 = newDataSplit("split3", Seq(40L, 40L), deletionFileLength = Some(5L))

      val binPacking = BinPackingSplits(
        CoreOptions.fromMap(
          Map(
            "data-evolution.enabled" -> "true",
            "deletion-vectors.enabled" -> "true",
            "source.split.open-file-cost" -> "5 B",
            "source.split.target-size" -> "150 B").asJava),
        readRowSizeRatio = 0.5
      )
      val reshuffled = binPacking.pack(Array[Split](split1, split2, split3))

      // Each split size is 2 * (40 * 0.5 + 5 open cost + 5 deletion file length) = 60.
      // Therefore two whole splits fit into the 150 B target while three do not.
      Assertions.assertEquals(2, reshuffled.length)
      Assertions.assertEquals(2, reshuffled.head.splits.length)
      Assertions.assertSame(split1, reshuffled.head.splits.head)
      Assertions.assertSame(split2, reshuffled.head.splits(1))
      Assertions.assertEquals(1, reshuffled(1).splits.length)
      Assertions.assertSame(split3, reshuffled(1).splits.head)
      reshuffled.flatMap(_.splits).foreach {
        split => Assertions.assertEquals(2, split.asInstanceOf[DataSplit].dataFiles().size())
      }
    }
  }

  test("Paimon: data evolution split packing keeps oversized split whole") {
    withSparkSQLConf("spark.sql.files.minPartitionNum" -> "1") {
      val split = newDataSplit("oversized", Seq(40L, 40L))

      val binPacking = BinPackingSplits(
        CoreOptions.fromMap(
          Map(
            "data-evolution.enabled" -> "true",
            "source.split.open-file-cost" -> "0 B",
            "source.split.target-size" -> "50 B").asJava))
      val reshuffled = binPacking.pack(Array[Split](split))

      Assertions.assertEquals(1, reshuffled.length)
      Assertions.assertEquals(1, reshuffled.head.splits.length)
      Assertions.assertSame(split, reshuffled.head.splits.head)
      Assertions.assertEquals(
        2,
        reshuffled.head.splits.head.asInstanceOf[DataSplit].dataFiles().size())
    }
  }

  test("Paimon: set open-file-cost to 0") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING)")
      for (i <- 1 to 100) {
        sql(s"INSERT INTO t VALUES ($i, 'a')")
      }

      def paimonScan() = getPaimonScan("SELECT * FROM t")

      // default openCostInBytes is 4m, so we will get 400 / 128 = 4 partitions
      withSparkSQLConf("spark.sql.files.minPartitionNum" -> "1") {
        assert(paimonScan().inputPartitions.length == 4)
      }

      withSparkSQLConf(
        "spark.sql.files.minPartitionNum" -> "1",
        "spark.sql.files.openCostInBytes" -> "0") {
        assert(paimonScan().inputPartitions.length == 1)
      }

      // Paimon's conf takes precedence over Spark's
      withSparkSQLConf(
        "spark.sql.files.minPartitionNum" -> "1",
        "spark.sql.files.openCostInBytes" -> "4194304",
        "spark.paimon.source.split.open-file-cost" -> "0") {
        assert(paimonScan().inputPartitions.length == 1)
      }
    }
  }

  test("Paimon: get read splits with column pruning") {
    withTable("t") {
      sql(
        "CREATE TABLE t (a INT, s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING, s9 STRING)")
      sql(
        "INSERT INTO t SELECT  /*+ REPARTITION(10) */ id, uuid(), uuid(), uuid(), uuid(), uuid(), uuid(), uuid(), uuid(), uuid() FROM range(1000000)")

      withSparkSQLConf(
        "spark.sql.files.minPartitionNum" -> "1",
        "spark.sql.files.openCostInBytes" -> "0",
        "spark.paimon.source.split.target-size" -> "32m") {
        for (splitSizeWithColumnPruning <- Seq("true", "false")) {
          withSparkSQLConf(
            "spark.paimon.source.split.target-size-with-column-pruning" -> splitSizeWithColumnPruning) {
            // Select one col
            var partitionCount = getPaimonScan("SELECT s1 FROM t").inputPartitions.length
            if (splitSizeWithColumnPruning.toBoolean) {
              assert(partitionCount == 1)
            } else {
              assert(partitionCount > 1)
            }

            // Select all cols
            partitionCount = getPaimonScan("SELECT * FROM t").inputPartitions.length
            assert(partitionCount > 1)
          }
        }
      }
    }
  }

  private def newDataSplit(
      prefix: String,
      fileSizes: Seq[Long],
      rawConvertible: Boolean = false,
      deletionFileLength: Option[Long] = None): DataSplit = {
    val files = fileSizes.zipWithIndex.map {
      case (fileSize, index) => newDataFile(s"$prefix-$index.parquet", fileSize)
    }
    newDataSplitFromFiles(files, rawConvertible, deletionFileLength, prefix)
  }

  private def newDataSplitFromFiles(
      files: Seq[DataFileMeta],
      rawConvertible: Boolean,
      deletionFileLength: Option[Long] = None,
      deletionFilePrefix: String = "delete"): DataSplit = {
    val builder = DataSplit
      .builder()
      .withSnapshot(1)
      .withBucket(0)
      .withPartition(BinaryRow.EMPTY_ROW)
      .withDataFiles(files.asJava)
      .rawConvertible(rawConvertible)
      .withBucketPath("no use")
    deletionFileLength.foreach {
      length =>
        builder.withDataDeletionFiles(
          files.indices
            .map(index => new DeletionFile(s"$deletionFilePrefix-$index.dv", 0, length, null))
            .asJava)
    }
    builder.build()
  }

  private def newDataFile(
      fileName: String,
      fileSize: Long,
      rowCount: Long = 1,
      maxSequenceNumber: Long = 0): DataFileMeta = {
    DataFileMeta.forAppend(
      fileName,
      fileSize,
      rowCount,
      null,
      0,
      maxSequenceNumber,
      1,
      new java.util.ArrayList[String](),
      null,
      FileSource.APPEND,
      null,
      null,
      null,
      null)
  }
}
