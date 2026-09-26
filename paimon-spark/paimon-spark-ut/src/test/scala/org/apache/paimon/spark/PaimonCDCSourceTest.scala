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

import org.apache.paimon.data.GenericRow
import org.apache.paimon.disk.IOManagerImpl

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.paimon.shims.memstream.MemoryStream
import org.apache.spark.sql.streaming.StreamTest

import scala.collection.JavaConverters._

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
        val location = table.location().toString

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
        val location = table.location().toString

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
        val location = table.location().toString

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

  test("Paimon CDC Source: Spark reads exposed event metadata as a regular column") {
    withTempDirs {
      (checkpointDir, ioManagerDir) =>
        val tableName = "T"
        spark.sql(s"""
                     |CREATE TABLE $tableName (id INT, data INT, event_ts BIGINT)
                     |TBLPROPERTIES (
                     |  'primary-key'='id',
                     |  'bucket'='1',
                     |  'changelog-producer' = 'lookup',
                     |  'sequence.field' = 'event_ts',
                     |  'changelog-producer.expose-field-as-metadata' = 'event_ts',
                     |  'changelog-producer.metadata-field-prefix' = '__event__')
                     |""".stripMargin)

        val table = loadTable(tableName)
        val ioManager = new IOManagerImpl(ioManagerDir.getCanonicalPath)
        val write = table.newWrite(commitUser).withIOManager(ioManager)
        val commit = table.newCommit(commitUser)
        try {
          // Write only the physical columns, as a Flink writer would. Spark exposes the
          // generated metadata field when reading the table, but it is not an input column.
          write.write(GenericRow.of(1, 10, 50L))
          commit.commit(0, write.prepareCommit(true, 0))

          // Spark exposes the generated field as a regular column. No Flink metadata alias is
          // needed to read it.
          checkAnswer(
            spark.sql(s"SELECT id, data, event_ts, __event__event_ts FROM $tableName"),
            Row(1, 10, 50L, 50L) :: Nil)

          val location = table.location().toString
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
            checkAnswer(currentResult(), Row("+I", 1, 10, 50L, 50L) :: Nil)

            write.write(GenericRow.of(1, 20, 100L))
            commit.commit(1, write.prepareCommit(true, 1))
            readStream.processAllAvailable()
            checkAnswer(
              currentResult(),
              Row("+I", 1, 10, 50L, 50L) ::
                Row("-U", 1, 10, 50L, 100L) ::
                Row("+U", 1, 20, 100L, 100L) :: Nil)
          } finally {
            readStream.stop()
          }
        } finally {
          write.close()
          commit.close()
          ioManager.close()
        }
    }
  }

  test("Paimon CDC Source: exposed event metadata is read-only for Spark writes") {
    withTable("T") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        spark.sql("""
                    |CREATE TABLE T (id INT, data INT, event_ts BIGINT)
                    |TBLPROPERTIES (
                    |  'primary-key' = 'id',
                    |  'bucket' = '1',
                    |  'changelog-producer' = 'lookup',
                    |  'sequence.field' = 'event_ts',
                    |  'changelog-producer.expose-field-as-metadata' = 'event_ts')
                    |""".stripMargin)

        // Positional writes must not require the generated metadata field as an input column.
        spark.sql("INSERT INTO T VALUES (1, 10, 50)")

        // A self-insert reads the generated field through SELECT *, so it also verifies that a
        // readable metadata field is removed before write resolution and schema evolution.
        spark.sql("INSERT INTO T SELECT * FROM T")

        if (gteqSpark3_5) {
          // The physical extra column should still participate in merge-schema evolution, while
          // the generated metadata field must not be committed as a physical column.
          spark.sql(
            "INSERT INTO T BY NAME " +
              "SELECT 2 AS id, 20 AS data, 200L AS event_ts, 'extra' AS extra")
        }

        val physicalFieldNames = loadTable("T").copyWithLatestSchema().schema().fieldNames().asScala
        assert(!physicalFieldNames.contains("__internal__event_ts"))
        if (gteqSpark3_5) {
          assert(physicalFieldNames.contains("extra"))
        }
        assert(spark.table("T").schema.fieldNames.contains("__internal__event_ts"))
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
