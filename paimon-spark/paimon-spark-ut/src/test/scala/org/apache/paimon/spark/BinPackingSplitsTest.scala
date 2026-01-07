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
import org.apache.paimon.spark.scan.BinPackingSplits
import org.apache.paimon.table.source.{DataSplit, Split}

import org.junit.jupiter.api.Assertions

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable

class BinPackingSplitsTest extends PaimonSparkTestBase {

  test("Paimon: reshuffle splits") {
    withSparkSQLConf(("spark.sql.leafNodeDefaultParallelism", "20")) {
      val splitNum = 5
      val fileNum = 100

      val files = scala.collection.mutable.ListBuffer.empty[DataFileMeta]
      0.until(fileNum).foreach {
        i =>
          val path = s"f$i.parquet"
          files += DataFileMeta.forAppend(
            path,
            750000,
            30000,
            null,
            0,
            29999,
            1,
            new java.util.ArrayList[String](),
            null,
            FileSource.APPEND,
            null,
            null,
            null,
            null)
      }

      val dataSplits = mutable.ArrayBuffer.empty[Split]
      0.until(splitNum).foreach {
        i =>
          dataSplits += DataSplit
            .builder()
            .withSnapshot(1)
            .withBucket(0)
            .withPartition(BinaryRow.EMPTY_ROW)
            .withDataFiles(files.zipWithIndex.filter(_._2 % splitNum == i).map(_._1).toList.asJava)
            .rawConvertible(true)
            .withBucketPath("no use")
            .build()
      }

      val binPacking = BinPackingSplits(CoreOptions.fromMap(new JHashMap()))
      val reshuffled = binPacking.pack(dataSplits.toArray)
      Assertions.assertTrue(reshuffled.length > 5)
    }
  }

  test("Paimon: reshuffle one split") {
    val files = List(
      DataFileMeta.forAppend(
        "f1.parquet",
        750000,
        30000,
        null,
        0,
        29999,
        1,
        new java.util.ArrayList[String](),
        null,
        FileSource.APPEND,
        null,
        null,
        null,
        null)
    ).asJava

    val dataSplits: Array[Split] = Array(
      DataSplit
        .builder()
        .withSnapshot(1)
        .withBucket(0)
        .withPartition(BinaryRow.EMPTY_ROW)
        .withDataFiles(files)
        .rawConvertible(true)
        .withBucketPath("no use")
        .build()
    )

    val binPacking = BinPackingSplits(CoreOptions.fromMap(new JHashMap()))
    val reshuffled = binPacking.pack(dataSplits)
    Assertions.assertEquals(1, reshuffled.length)
  }

  test("Paimon: set open-file-cost to 0") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING)")
      for (i <- 1 to 100) {
        sql(s"INSERT INTO t VALUES ($i, 'a')")
      }

      def paimonScan() = getPaimonScan("SELECT * FROM t")

      // default openCostInBytes is 4m, so we will get 400 / 128 = 4 partitions
      withSparkSQLConf("spark.sql.leafNodeDefaultParallelism" -> "1") {
        assert(paimonScan().inputPartitions.length == 4)
      }

      withSparkSQLConf(
        "spark.sql.files.openCostInBytes" -> "0",
        "spark.sql.leafNodeDefaultParallelism" -> "1") {
        assert(paimonScan().inputPartitions.length == 1)
      }

      // Paimon's conf takes precedence over Spark's
      withSparkSQLConf(
        "spark.sql.files.openCostInBytes" -> "4194304",
        "spark.paimon.source.split.open-file-cost" -> "0",
        "spark.sql.leafNodeDefaultParallelism" -> "1") {
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
}
