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
import org.apache.spark.sql.streaming.StreamTest

class PaimonCDCSourceTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon CDC Source: batch write and streaming read change-log with default scan mode") {
    withTempDir {
      checkpointDir =>
        val tableName = "T"
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        spark.sql(s"""
                     |CREATE TABLE $tableName (a INT, b STRING)
                     |TBLPROPERTIES (
                     |  'primary-key'='a',
                     |  'bucket'='2',
                     |  'changelog-producer' = 'lookup')
                     |""".stripMargin)

        spark.sql(s"INSERT INTO $tableName VALUES (1, 'v_1')")
        spark.sql(s"INSERT INTO $tableName VALUES (2, 'v_2')")
        spark.sql(s"INSERT INTO $tableName VALUES (2, 'v_2_new')")

        val table = loadTable(tableName)
        val location = table.tableDataPath().toString

        val readStream = spark.readStream
          .format("paimon")
          .option("read.changelog", "true")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          readStream.processAllAvailable()
          val expertResult1 = Row("+I", 1, "v_1") :: Row("+I", 2, "v_2_new") :: Nil
          checkAnswer(currentResult(), expertResult1)

          spark.sql(s"INSERT INTO $tableName VALUES (1, 'v_1_new'), (3, 'v_3')")
          readStream.processAllAvailable()
          val expertResult2 =
            Row("+I", 1, "v_1") :: Row("-U", 1, "v_1") :: Row("+U", 1, "v_1_new") :: Row(
              "+I",
              2,
              "v_2_new") :: Row("+I", 3, "v_3") :: Nil
          checkAnswer(currentResult(), expertResult2)
        } finally {
          readStream.stop()
        }
    }
  }

  test("Paimon CDC Source: batch write and streaming read change-log with scan.snapshot-id") {
    withTempDir {
      checkpointDir =>
        val tableName = "T"
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        spark.sql(s"""
                     |CREATE TABLE $tableName (a INT, b STRING)
                     |TBLPROPERTIES (
                     |  'primary-key'='a',
                     |  'bucket'='2',
                     |  'changelog-producer' = 'lookup')
                     |""".stripMargin)

        spark.sql(s"INSERT INTO $tableName VALUES (1, 'v_1')")
        spark.sql(s"INSERT INTO $tableName VALUES (2, 'v_2')")
        spark.sql(s"INSERT INTO $tableName VALUES (2, 'v_2_new')")

        val table = loadTable(tableName)
        val location = table.tableDataPath().toString

        val readStream = spark.readStream
          .format("paimon")
          .option("read.changelog", "true")
          .option("scan.mode", "from-snapshot")
          .option("scan.snapshot-id", 1)
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          readStream.processAllAvailable()
          val expertResult1 = Row("+I", 1, "v_1") :: Row("+I", 2, "v_2") :: Row(
            "-U",
            2,
            "v_2") :: Row("+U", 2, "v_2_new") :: Nil
          checkAnswer(currentResult(), expertResult1)

          spark.sql(s"INSERT INTO $tableName VALUES (1, 'v_1_new'), (3, 'v_3')")
          readStream.processAllAvailable()
          val expertResult2 =
            Row("+I", 1, "v_1") :: Row("-U", 1, "v_1") :: Row("+U", 1, "v_1_new") :: Row(
              "+I",
              2,
              "v_2") :: Row("-U", 2, "v_2") :: Row("+U", 2, "v_2_new") :: Row("+I", 3, "v_3") :: Nil
          checkAnswer(currentResult(), expertResult2)
        } finally {
          readStream.stop()
        }
    }
  }

  test("Paimon CDC Source: streaming write and streaming read change-log") {
    withTempDirs {
      (checkpointDir1, checkpointDir2) =>
        val tableName = "T"
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        spark.sql(s"""
                     |CREATE TABLE $tableName (a INT, b STRING)
                     |TBLPROPERTIES (
                     |  'primary-key'='a',
                     |  'bucket'='2',
                     |  'changelog-producer' = 'lookup')
                     |""".stripMargin)

        val table = loadTable(tableName)
        val location = table.tableDataPath().toString

        // streaming write
        val inputData = MemoryStream[(Int, String)]
        val writeStream = inputData
          .toDS()
          .toDF("a", "b")
          .writeStream
          .option("checkpointLocation", checkpointDir1.getCanonicalPath)
          .foreachBatch {
            (batch: Dataset[Row], _: Long) =>
              batch.write.format("paimon").mode("append").save(location)
          }
          .start()

        // streaming read
        val readStream = spark.readStream
          .format("paimon")
          .option("read.changelog", "true")
          .option("scan.mode", "from-snapshot")
          .option("scan.snapshot-id", 1)
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir2.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          inputData.addData((1, "v_1"))
          writeStream.processAllAvailable()
          readStream.processAllAvailable()
          val expertResult1 = Row("+I", 1, "v_1") :: Nil
          checkAnswer(currentResult(), expertResult1)

          inputData.addData((2, "v_2"))
          writeStream.processAllAvailable()
          readStream.processAllAvailable()
          val expertResult2 = Row("+I", 1, "v_1") :: Row("+I", 2, "v_2") :: Nil
          checkAnswer(currentResult(), expertResult2)

          inputData.addData((2, "v_2_new"))
          writeStream.processAllAvailable()
          readStream.processAllAvailable()
          val expertResult3 = Row("+I", 1, "v_1") :: Row("+I", 2, "v_2") :: Row(
            "-U",
            2,
            "v_2") :: Row("+U", 2, "v_2_new") :: Nil
          checkAnswer(currentResult(), expertResult3)

          inputData.addData((1, "v_1_new"), (3, "v_3"))
          writeStream.processAllAvailable()
          readStream.processAllAvailable()
          val expertResult4 =
            Row("+I", 1, "v_1") :: Row("-U", 1, "v_1") :: Row("+U", 1, "v_1_new") :: Row(
              "+I",
              2,
              "v_2") :: Row("-U", 2, "v_2") :: Row("+U", 2, "v_2_new") :: Row("+I", 3, "v_3") :: Nil
          checkAnswer(currentResult(), expertResult4)
        } finally {
          readStream.stop()
        }
    }
  }

  test("Paimon CDC Source: streaming read change-log with audit_log system table") {
    withTable("T") {
      withTempDir {
        checkpointDir =>
          spark.sql(
            s"""
               |CREATE TABLE T (a INT, b STRING)
               |TBLPROPERTIES ('primary-key'='a','bucket'='2', 'changelog-producer' = 'lookup')
               |""".stripMargin)

          val readStream = spark.readStream
            .format("paimon")
            .table("`T$audit_log`")
            .writeStream
            .format("memory")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .queryName("mem_table")
            .outputMode("append")
            .start()

          val currentResult = () => spark.sql("SELECT * FROM mem_table")
          try {
            spark.sql(s"INSERT INTO T VALUES (1, 'v_1')")
            readStream.processAllAvailable()
            checkAnswer(currentResult(), Row("+I", 1, "v_1") :: Nil)

            spark.sql(s"INSERT INTO T VALUES (2, 'v_2')")
            readStream.processAllAvailable()
            checkAnswer(currentResult(), Row("+I", 1, "v_1") :: Row("+I", 2, "v_2") :: Nil)
          } finally {
            readStream.stop()
          }
      }
    }
  }
}
