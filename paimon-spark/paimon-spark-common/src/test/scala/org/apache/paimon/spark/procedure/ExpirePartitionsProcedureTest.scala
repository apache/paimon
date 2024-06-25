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

/** IT Case for expire partitions procedure. */
class ExpirePartitionsProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: expire partitions") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (k STRING, pt STRING)
                       |TBLPROPERTIES ('primary-key'='k,pt', 'bucket'='1')
                       | PARTITIONED BY (pt)
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(String, String)]
          val stream = inputData
            .toDS()
            .toDF("k", "pt")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // snapshot-1
            inputData.addData(("a", "2024-06-01"))
            stream.processAllAvailable()

            // snapshot-2
            inputData.addData(("b", "9024-06-01"))
            stream.processAllAvailable()

            checkAnswer(query(), Row("a", "2024-06-01") :: Row("b", "9024-06-01") :: Nil)
            // expire
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T', expiration_time => '1 d'" +
                  ", timestamp_formatter => 'yyyy-MM-dd')"),
              Row(true) :: Nil)

            checkAnswer(query(), Row("b", "9024-06-01") :: Nil)

          } finally {
            stream.stop()
          }
      }
    }
  }
}
