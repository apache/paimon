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

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest

class CreateAndDeleteTagProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: create and delete tag") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // define a pk table and test `forEachBatch` api
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b STRING)
                       |TBLPROPERTIES ('primary-key'='a', 'bucket'='3')
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, String)]
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

          val query = () => spark.sql("SELECT * FROM T ORDER BY a")

          try {
            // snapshot-1
            inputData.addData((1, "a"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Nil)

            // snapshot-2
            inputData.addData((2, "b"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

            // snapshot-3
            inputData.addData((2, "b2"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b2") :: Nil)
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.create_tag(table => 'test.T', tag => 'test_tag', snapshot => 2)"),
              Row(true) :: Nil)
            checkAnswer(
              spark.sql("SELECT tag_name FROM paimon.test.`T$tags`"),
              Row("test_tag") :: Nil)
            checkAnswer(
              spark.sql("CALL paimon.sys.delete_tag(table => 'test.T', tag => 'test_tag')"),
              Row(true) :: Nil)
            checkAnswer(spark.sql("SELECT tag_name FROM paimon.test.`T$tags`"), Nil)
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.create_tag(table => 'test.T', tag => 'test_latestSnapshot_tag')"),
              Row(true) :: Nil)
            checkAnswer(
              spark.sql("SELECT tag_name FROM paimon.test.`T$tags`"),
              Row("test_latestSnapshot_tag") :: Nil)
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.delete_tag(table => 'test.T', tag => 'test_latestSnapshot_tag')"),
              Row(true) :: Nil)
            checkAnswer(spark.sql("SELECT tag_name FROM paimon.test.`T$tags`"), Nil)

            // snapshot-4
            inputData.addData((2, "c1"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a"), Row(2, "c1") :: Nil)

            checkAnswer(
              spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => 's4')"),
              Row(true) :: Nil)

            // snapshot-5
            inputData.addData((3, "c2"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "c1") :: Row(3, "c2") :: Nil)

            checkAnswer(
              spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => 's5')"),
              Row(true) :: Nil)

            checkAnswer(
              spark.sql("SELECT tag_name FROM paimon.test.`T$tags`"),
              Row("s4") :: Row("s5") :: Nil)

            checkAnswer(
              spark.sql("CALL paimon.sys.delete_tag(table => 'test.T', tag => 's4,s5')"),
              Row(true) :: Nil)

            checkAnswer(spark.sql("SELECT tag_name FROM paimon.test.`T$tags`"), Nil)
          } finally {
            stream.stop()
          }
      }
    }
  }
}
