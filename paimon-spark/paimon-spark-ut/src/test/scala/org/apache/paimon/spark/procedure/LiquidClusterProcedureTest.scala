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

import java.util

import scala.util.Random

/** doc. */
class LiquidClusterProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: cluster for unpartitioned table") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(
            s"""
               |CREATE TABLE T (a INT, b INT, c STRING)
               |TBLPROPERTIES ('bucket'='-1', 'num-levels'='6', 'num-sorted-run.compaction-trigger'='2', 'liquid-clustering.columns'='a,b', 'clustering.strategy'='zorder')
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
            var dataSplits = clusteredTable.newSnapshotReader().read().dataSplits();
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

  def checkSnapshot(table: FileStoreTable): Unit = {
    Assertions
      .assertThat(table.latestSnapshot().get().commitKind().toString)
      .isEqualTo(CommitKind.COMPACT.toString)
  }

}
