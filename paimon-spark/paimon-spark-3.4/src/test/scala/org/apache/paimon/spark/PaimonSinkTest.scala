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

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{col, mean, window}
import org.apache.spark.sql.streaming.StreamTest

import java.sql.Date

class PaimonSinkTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Sink: forEachBatch") {
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
              (batch: Dataset[Row], id: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T ORDER BY a")

          try {
            inputData.addData((1, "a"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Nil)

            inputData.addData((2, "b"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

            inputData.addData((2, "b2"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b2") :: Nil)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Sink: append mode") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // define a pk table and sink into it in append mode
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
            .format("paimon")
            .start(location)

          val query = () => spark.sql("SELECT * FROM T ORDER BY a")

          try {
            inputData.addData((1, "a"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Nil)

            inputData.addData((2, "b"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

            inputData.addData((2, "b2"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b2") :: Nil)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Sink: complete mode") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // define an append-only table and sink into it in complete mode
          spark.sql(s"""
                       |CREATE TABLE T (city String, population Long)
                       |TBLPROPERTIES ('bucket'='3')
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, String)]
          val stream = inputData.toDS
            .toDF("uid", "city")
            .groupBy("city")
            .count()
            .toDF("city", "population")
            .writeStream
            .outputMode("complete")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("paimon")
            .start(location)

          val query = () => spark.sql("SELECT * FROM T ORDER BY city")

          try {
            inputData.addData((1, "HZ"), (2, "BJ"), (3, "BJ"))
            stream.processAllAvailable()
            checkAnswer(query(), Row("BJ", 2L) :: Row("HZ", 1L) :: Nil)

            inputData.addData((4, "SH"), (5, "BJ"), (6, "HZ"))
            stream.processAllAvailable()
            checkAnswer(query(), Row("BJ", 3L) :: Row("HZ", 2L) :: Row("SH", 1L) :: Nil)

            inputData.addData((7, "HZ"), (8, "SH"))
            stream.processAllAvailable()
            checkAnswer(query(), Row("BJ", 3L) :: Row("HZ", 3L) :: Row("SH", 2L) :: Nil)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Sink: update mode") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // define a pk table and sink into it in update mode
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b STRING)
                       |TBLPROPERTIES ('primary-key'='a', 'bucket'='3')
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, String)]
          intercept[RuntimeException] {
            inputData
              .toDF()
              .writeStream
              .option("checkpointLocation", checkpointDir.getCanonicalPath)
              .outputMode("update")
              .format("paimon")
              .start(location)
          }
      }
    }
  }

  test("Paimon Sink: aggregation and watermark") {
    withTempDir {
      checkpointDir =>
        // define an append-only table and sink into it with aggregation and watermark in append mode
        spark.sql(s"""
                     |CREATE TABLE T (start Timestamp, stockId INT, avg_price DOUBLE)
                     |TBLPROPERTIES ('bucket'='3')
                     |""".stripMargin)
        val location = loadTable("T").location().toString

        val inputData = MemoryStream[(Long, Int, Double)]
        val data = inputData.toDS
          .toDF("time", "stockId", "price")
          .selectExpr("CAST(time AS timestamp) AS timestamp", "stockId", "price")
          .withWatermark("timestamp", "10 seconds")
          .groupBy(window($"timestamp", "5 seconds"), col("stockId"))
          .agg(mean("price").as("avg_price"))
          .select("window.start", "stockId", "avg_price")

        val stream =
          data.writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("paimon")
            .start(location)

        val query = () =>
          spark.sql(
            "SELECT CAST(start as BIGINT) AS start, stockId, avg_price FROM T ORDER BY start, stockId")

        try {
          inputData.addData((101L, 1, 1.0d), (102, 1, 2.0d), (104, 2, 20.0d))
          stream.processAllAvailable()
          inputData.addData((105L, 2, 40.0d), (107, 2, 60.0d), (115, 3, 300.0d))
          stream.processAllAvailable()
          inputData.addData((200L, 99, 99.9d))
          stream.processAllAvailable()
          checkAnswer(
            query(),
            Row(100L, 1, 1.5d) :: Row(100L, 2, 20.0d) :: Row(105L, 2, 50.0d) :: Row(
              115L,
              3,
              300.0d) :: Nil)
        } finally {
          if (stream != null) {
            stream.stop()
          }
        }
    }
  }

  test("Paimon Sink: enable schema evolution") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // define a pk table and sink into it with schema evolution in append mode
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b STRING)
                       |TBLPROPERTIES ('primary-key'='a', 'bucket'='3')
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val date = Date.valueOf("2023-08-10")
          spark.sql("INSERT INTO T VALUES (1, '2023-08-09'), (2, '2023-08-09')")
          checkAnswer(
            spark.sql("SELECT * FROM T ORDER BY a, b"),
            Row(1, "2023-08-09") :: Row(2, "2023-08-09") :: Nil)

          val inputData = MemoryStream[(Long, Date, Int)]
          val stream = inputData
            .toDS()
            .toDF("a", "b", "c")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .option("write.merge-schema", "true")
            .option("write.merge-schema.explicit-cast", "true")
            .format("paimon")
            .start(location)

          val query = () => spark.sql("SELECT * FROM T ORDER BY a")

          try {
            inputData.addData((1L, date, 123), (3L, date, 456))
            stream.processAllAvailable()

            checkAnswer(
              query(),
              Row(1L, date, 123) :: Row(2L, Date.valueOf("2023-08-09"), null) :: Row(
                3L,
                date,
                456) :: Nil)
          } finally {
            stream.stop()
          }
      }
    }
  }
}
