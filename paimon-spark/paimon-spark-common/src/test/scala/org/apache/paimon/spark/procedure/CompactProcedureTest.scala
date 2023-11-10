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
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest
import org.assertj.core.api.Assertions

import java.util

/** Test sort compact procedure. */
class CompactProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: sort compact") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b INT)
                       |TBLPROPERTIES ('bucket'='-1')
                       |""".stripMargin)
          val location = loadTable("T").location().getPath

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
          spark.sql("SET ")

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
              spark.sql("CALL paimon.sys.compact('T', 'zorder', 'a,b')"),
              Row(true) :: Nil)

            // test order sort
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

            checkAnswer(spark.sql("CALL paimon.sys.compact('T', 'order', 'a,b')"), Row(true) :: Nil)
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("test to where") {
    val conditions = "f0=0,f1=0,f2=0;f0=1,f1=1,f2=1;f0=1,f1=2,f2=2;f3=3"

    val where = CompactProcedure.toWhere(conditions)
    val whereExpected =
      "(f0=0 AND f1=0 AND f2=0) OR (f0=1 AND f1=1 AND f2=1) OR (f0=1 AND f1=2 AND f2=2) OR (f3=3)"

    Assertions.assertThat(where).isEqualTo(whereExpected)
  }
}
