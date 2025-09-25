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

import org.apache.paimon.Snapshot.CommitKind
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest
import org.assertj.core.api.Assertions
import org.scalatest.time.{Minutes, Span}

import java.util

import scala.util.Random

/** doc. */
class LiquidClusterProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: cluster for unpartitioned table") {
    failAfter(Span(10, Minutes)) {
      withTempDir {
        checkpointDir =>
          spark.sql(
            s"""
               |CREATE TABLE T (a INT, b INT, c STRING)
               |TBLPROPERTIES ('bucket'='-1','num-levels'='6', 'num-sorted-run.compaction-trigger'='2', 'clustering.columns'='a,b', 'clustering.strategy'='zorder', 'clustering.incremental' = 'true')
               |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, Int, String)]
          val stream = inputData
            .toDS()
            .toDF("a", "b", "c")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            val random = new Random()
            val randomStr = random.nextString(40)
            // first write
            inputData.addData((0, 0, randomStr))
            inputData.addData((0, 1, randomStr))
            inputData.addData((0, 2, randomStr))
            inputData.addData((1, 0, randomStr))
            inputData.addData((1, 1, randomStr))
            inputData.addData((1, 2, randomStr))
            inputData.addData((2, 0, randomStr))
            inputData.addData((2, 1, randomStr))
            inputData.addData((2, 2, randomStr))
            // 由于目前复用了 clustering.columns，所以 SparkWriter 需要关闭写入时的 Cluster，避免inputData写入的文件不可控
            stream.processAllAvailable()

            val result = new util.ArrayList[Row]()
            for (a <- 0 until 3) {
              for (b <- 0 until 3) {
                result.add(Row(a, b, randomStr))
              }
            }
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)

            // first cluster, the outputLevel should be 5
            checkAnswer(spark.sql("CALL paimon.sys.cluster(table => 'T')"), Row(true) :: Nil)

            // first cluster result
            val result2 = new util.ArrayList[Row]()
            result2.add(0, Row(0, 0, randomStr))
            result2.add(1, Row(0, 1, randomStr))
            result2.add(2, Row(1, 0, randomStr))
            result2.add(3, Row(1, 1, randomStr))
            result2.add(4, Row(0, 2, randomStr))
            result2.add(5, Row(1, 2, randomStr))
            result2.add(6, Row(2, 0, randomStr))
            result2.add(7, Row(2, 1, randomStr))
            result2.add(8, Row(2, 2, randomStr))

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result2)

            var clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            var dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().get(0).level()).isEqualTo(5)

            // second write
            inputData.addData((0, 3, null), (1, 3, null), (2, 3, null))
            inputData.addData((3, 0, null), (3, 1, null), (3, 2, null), (3, 3, null))
            stream.processAllAvailable()

            val result3 = new util.ArrayList[Row]()
            result3.addAll(result2)
            for (a <- 0 until 3) {
              result3.add(Row(a, 3, null))
            }
            for (b <- 0 until 4) {
              result3.add(Row(3, b, null))
            }

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result3)

            // second cluster, the outputLevel should be 4
            checkAnswer(spark.sql("CALL paimon.sys.cluster(table => 'T')"), Row(true) :: Nil)
            // second cluster result, level-5 and level-4 are individually ordered
            val result4 = new util.ArrayList[Row]()
            result4.addAll(result2)
            result4.add(Row(0, 3, null))
            result4.add(Row(1, 3, null))
            result4.add(Row(3, 0, null))
            result4.add(Row(3, 1, null))
            result4.add(Row(2, 3, null))
            result4.add(Row(3, 2, null))
            result4.add(Row(3, 3, null))
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result4)

            clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().size()).isEqualTo(2)
            Assertions.assertThat(dataSplits.get(0).dataFiles().get(0).level()).isEqualTo(5)
            Assertions.assertThat(dataSplits.get(0).dataFiles().get(1).level()).isEqualTo(4)

            // full cluster
            checkAnswer(
              spark.sql("CALL paimon.sys.cluster(table => 'T', isFull => true)"),
              Row(true) :: Nil)
            val result5 = new util.ArrayList[Row]()
            result5.add(Row(0, 0, randomStr))
            result5.add(Row(0, 1, randomStr))
            result5.add(Row(1, 0, randomStr))
            result5.add(Row(1, 1, randomStr))
            result5.add(Row(0, 2, randomStr))
            result5.add(Row(0, 3, null))
            result5.add(Row(1, 2, randomStr))
            result5.add(Row(1, 3, null))
            result5.add(Row(2, 0, randomStr))
            result5.add(Row(2, 1, randomStr))
            result5.add(Row(3, 0, null))
            result5.add(Row(3, 1, null))
            result5.add(Row(2, 2, randomStr))
            result5.add(Row(2, 3, null))
            result5.add(Row(3, 2, null))
            result5.add(Row(3, 3, null))
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result5)

            clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().get(0).level()).isEqualTo(5)

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: cluster for partitioned table") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(
            s"""
               |CREATE TABLE T (a INT, b INT, c STRING, pt INT)
               |PARTITIONED BY (pt)
               |TBLPROPERTIES ('bucket'='-1', 'num-levels'='6', 'num-sorted-run.compaction-trigger'='2', 'clustering.columns'='a,b', 'clustering.strategy'='zorder', 'clustering.incremental' = 'true')
               |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, Int, String, Int)]
          val stream = inputData
            .toDS()
            .toDF("a", "b", "c", "pt")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T ORDER BY pt")

          try {
            val random = new Random()
            val randomStr = random.nextString(50)
            // first write
            for (pt <- 0 until 2) {
              val c = if (pt == 0) randomStr else null
              inputData.addData((0, 0, c, pt))
              inputData.addData((0, 1, c, pt))
              inputData.addData((0, 2, c, pt))
              inputData.addData((1, 0, c, pt))
              inputData.addData((1, 1, c, pt))
              inputData.addData((1, 2, c, pt))
              inputData.addData((2, 0, c, pt))
              inputData.addData((2, 1, c, pt))
              inputData.addData((2, 2, c, pt))
            }
            stream.processAllAvailable()

            val result = new util.ArrayList[Row]()
            for (pt <- 0 until 2) {
              for (a <- 0 until 3) {
                for (b <- 0 until 3) {
                  val c = if (pt == 0) randomStr else null
                  result.add(Row(a, b, c, pt))
                }
              }
            }
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)

            // first cluster, the outputLevel should be 5
            checkAnswer(spark.sql("CALL paimon.sys.cluster(table => 'T')"), Row(true) :: Nil)

            // first cluster result
            val result2 = new util.ArrayList[Row]()
            for (pt <- 0 until 2) {
              val c = if (pt == 0) randomStr else null
              result2.add(Row(0, 0, c, pt))
              result2.add(Row(0, 1, c, pt))
              result2.add(Row(1, 0, c, pt))
              result2.add(Row(1, 1, c, pt))
              result2.add(Row(0, 2, c, pt))
              result2.add(Row(1, 2, c, pt))
              result2.add(Row(2, 0, c, pt))
              result2.add(Row(2, 1, c, pt))
              result2.add(Row(2, 2, c, pt))
            }

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result2)

            var clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            var dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(2)
            dataSplits.forEach(
              dataSplit => {
                Assertions.assertThat(dataSplit.dataFiles().size()).isEqualTo(1)
                Assertions.assertThat(dataSplit.dataFiles().get(0).level()).isEqualTo(5)
              })

            // second write
            for (pt <- 0 until 2) {
              inputData.addData((0, 3, null, pt), (1, 3, null, pt), (2, 3, null, pt))
              inputData.addData(
                (3, 0, null, pt),
                (3, 1, null, pt),
                (3, 2, null, pt),
                (3, 3, null, pt))
            }
            stream.processAllAvailable()

            val result3 = new util.ArrayList[Row]()
            for (pt <- 0 until 2) {
              val c = if (pt == 0) randomStr else null
              result3.add(Row(0, 0, c, pt))
              result3.add(Row(0, 1, c, pt))
              result3.add(Row(1, 0, c, pt))
              result3.add(Row(1, 1, c, pt))
              result3.add(Row(0, 2, c, pt))
              result3.add(Row(1, 2, c, pt))
              result3.add(Row(2, 0, c, pt))
              result3.add(Row(2, 1, c, pt))
              result3.add(Row(2, 2, c, pt))
              for (a <- 0 until 3) {
                result3.add(Row(a, 3, null, pt))
              }
              for (b <- 0 until 4) {
                result3.add(Row(3, b, null, pt))
              }
            }
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result3)

            // second cluster
            checkAnswer(spark.sql("CALL paimon.sys.cluster(table => 'T')"), Row(true) :: Nil)
            val result4 = new util.ArrayList[Row]()
            // for partition-0: only file in level-0 will be picked for clustering, outputLevel is 4
            result4.add(Row(0, 0, randomStr, 0))
            result4.add(Row(0, 1, randomStr, 0))
            result4.add(Row(1, 0, randomStr, 0))
            result4.add(Row(1, 1, randomStr, 0))
            result4.add(Row(0, 2, randomStr, 0))
            result4.add(Row(1, 2, randomStr, 0))
            result4.add(Row(2, 0, randomStr, 0))
            result4.add(Row(2, 1, randomStr, 0))
            result4.add(Row(2, 2, randomStr, 0))
            result4.add(Row(0, 3, null, 0))
            result4.add(Row(1, 3, null, 0))
            result4.add(Row(3, 0, null, 0))
            result4.add(Row(3, 1, null, 0))
            result4.add(Row(2, 3, null, 0))
            result4.add(Row(3, 2, null, 0))
            result4.add(Row(3, 3, null, 0))
            // for partition-1:all files will be picked for clustering, outputLevel is 5
            result4.add(Row(0, 0, null, 1))
            result4.add(Row(0, 1, null, 1))
            result4.add(Row(1, 0, null, 1))
            result4.add(Row(1, 1, null, 1))
            result4.add(Row(0, 2, null, 1))
            result4.add(Row(0, 3, null, 1))
            result4.add(Row(1, 2, null, 1))
            result4.add(Row(1, 3, null, 1))
            result4.add(Row(2, 0, null, 1))
            result4.add(Row(2, 1, null, 1))
            result4.add(Row(3, 0, null, 1))
            result4.add(Row(3, 1, null, 1))
            result4.add(Row(2, 2, null, 1))
            result4.add(Row(2, 3, null, 1))
            result4.add(Row(3, 2, null, 1))
            result4.add(Row(3, 3, null, 1))

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result4)

            clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(2)
            dataSplits.forEach(
              dataSplit => {
                if (dataSplit.partition().getInt(0) == 1) {
                  // partition-1
                  Assertions.assertThat(dataSplit.dataFiles().size()).isEqualTo(1)
                  Assertions.assertThat(dataSplit.dataFiles().get(0).level()).isEqualTo(5)
                } else {
                  // partition-0
                  Assertions.assertThat(dataSplit.dataFiles().size()).isEqualTo(2)
                  Assertions.assertThat(dataSplit.dataFiles().get(0).level()).isEqualTo(5)
                  Assertions.assertThat(dataSplit.dataFiles().get(1).level()).isEqualTo(4)
                }
              })
          } finally {
            stream.stop()
          }
      }
    }
  }

  def checkSnapshot(table: FileStoreTable): Unit = {
    Assertions
      .assertThat(table.latestSnapshot().get().commitKind().toString)
      .isEqualTo(CommitKind.COMPACT.toString)
  }

}
