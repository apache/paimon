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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest
import org.assertj.core.api.Assertions

import java.util

/** doc. */
class LiquidClusterProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: sort compact") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b INT)
                       |TBLPROPERTIES ('bucket'='-1')
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, Int)]
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

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // test zorder sort
            inputData.addData((0, 0))
            inputData.addData((0, 1))
            inputData.addData((0, 2))
            inputData.addData((1, 0))
            inputData.addData((1, 1))
            inputData.addData((1, 2))
            inputData.addData((2, 0))
            inputData.addData((2, 1))
            inputData.addData((2, 2))
            stream.processAllAvailable()

            val result = new util.ArrayList[Row]()
            for (a <- 0 until 3) {
              for (b <- 0 until 3) {
                result.add(Row(a, b))
              }
            }
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)

            checkAnswer(
              spark.sql(
                "CALL paimon.sys.compact(table => 'T', order_strategy => 'zorder', order_by => 'a,b')"),
              Row(true) :: Nil)

            val result2 = new util.ArrayList[Row]()
            result2.add(0, Row(0, 0))
            result2.add(1, Row(0, 1))
            result2.add(2, Row(1, 0))
            result2.add(3, Row(1, 1))
            result2.add(4, Row(0, 2))
            result2.add(5, Row(1, 2))
            result2.add(6, Row(2, 0))
            result2.add(7, Row(2, 1))
            result2.add(8, Row(2, 2))

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result2)

            // test hilbert sort
            val result3 = new util.ArrayList[Row]()
            result3.add(0, Row(0, 0))
            result3.add(1, Row(0, 1))
            result3.add(2, Row(1, 1))
            result3.add(3, Row(1, 0))
            result3.add(4, Row(2, 0))
            result3.add(5, Row(2, 1))
            result3.add(6, Row(2, 2))
            result3.add(7, Row(1, 2))
            result3.add(8, Row(0, 2))

            checkAnswer(
              spark.sql(
                "CALL paimon.sys.compact(table => 'T', order_strategy => 'hilbert', order_by => 'a,b')"),
              Row(true) :: Nil)

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result3)

            // test order sort
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.compact(table => 'T', order_strategy => 'order', order_by => 'a,b')"),
              Row(true) :: Nil)
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: cluster") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(
            s"""
               |CREATE TABLE T (a INT, b INT)
               |TBLPROPERTIES ('bucket'='-1', 'liquid-clustering.columns'='a,b', 'clustering.strategy'='zorder')
               |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, Int)]
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

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // test zorder sort
            inputData.addData((0, 0))
            inputData.addData((0, 1))
            inputData.addData((0, 2))
            inputData.addData((1, 0))
            inputData.addData((1, 1))
            inputData.addData((1, 2))
            inputData.addData((2, 0))
            inputData.addData((2, 1))
            inputData.addData((2, 2))
            stream.processAllAvailable()

            val result = new util.ArrayList[Row]()
            for (a <- 0 until 3) {
              for (b <- 0 until 3) {
                result.add(Row(a, b))
              }
            }
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)

            checkAnswer(spark.sql("CALL paimon.sys.cluster(table => 'T')"), Row(true) :: Nil)

            val result2 = new util.ArrayList[Row]()
            result2.add(0, Row(0, 0))
            result2.add(1, Row(0, 1))
            result2.add(2, Row(1, 0))
            result2.add(3, Row(1, 1))
            result2.add(4, Row(0, 2))
            result2.add(5, Row(1, 2))
            result2.add(6, Row(2, 0))
            result2.add(7, Row(2, 1))
            result2.add(8, Row(2, 2))

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result2)

//            // test hilbert sort
//            val result3 = new util.ArrayList[Row]()
//            result3.add(0, Row(0, 0))
//            result3.add(1, Row(0, 1))
//            result3.add(2, Row(1, 1))
//            result3.add(3, Row(1, 0))
//            result3.add(4, Row(2, 0))
//            result3.add(5, Row(2, 1))
//            result3.add(6, Row(2, 2))
//            result3.add(7, Row(1, 2))
//            result3.add(8, Row(0, 2))
//
//            checkAnswer(
//              spark.sql(
//                "CALL paimon.sys.compact(table => 'T', order_strategy => 'hilbert', order_by => 'a,b')"),
//              Row(true) :: Nil)
//
//            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result3)
//
//            // test order sort
//            checkAnswer(
//              spark.sql(
//                "CALL paimon.sys.compact(table => 'T', order_strategy => 'order', order_by => 'a,b')"),
//              Row(true) :: Nil)
//            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)
          } finally {
            stream.stop()
          }
      }
    }
  }

}
