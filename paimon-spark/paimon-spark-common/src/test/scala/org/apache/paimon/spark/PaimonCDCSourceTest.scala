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

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamTest

class PaimonCDCSourceTest extends PaimonSparkTestBase with StreamTest {

  test("Paimon CDC Source: default scan mode") {
    withTempDir {
      checkpointDir =>
        val tableName = "T"
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        spark.sql(s"""
                     |CREATE TABLE $tableName (a INT, b STRING)
                     |TBLPROPERTIES (
                     |  'primary-key'='a',
                     |  'write-mode'='change-log',
                     |  'bucket'='2',
                     |  'changelog-producer' = 'lookup')
                     |""".stripMargin)

        spark.sql(s"INSERT INTO $tableName VALUES (1, 'v_1')")
        spark.sql(s"INSERT INTO $tableName VALUES (2, 'v_2')")
        spark.sql(s"INSERT INTO $tableName VALUES (2, 'v_2_new')")

        val table = loadTable(tableName)
        val location = table.location().getPath

        val query = spark.readStream
          .format("paimon")
          .option("read.readChangeLog", "true")
          .load(location)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("mem_table")
          .outputMode("append")
          .start()

        val currentResult = () => spark.sql("SELECT * FROM mem_table")
        try {
          query.processAllAvailable()
          val expertResult1 = Row(1, "v_1", "+I") :: Row(2, "v_2_new", "+I") :: Nil
          checkAnswer(currentResult(), expertResult1)

          spark.sql(s"INSERT INTO $tableName VALUES (1, 'v_1_new'), (3, 'v_3')")
          query.processAllAvailable()
          val expertResult2 =
            Row(1, "v_1", "+I") :: Row(1, "v_1", "-U") :: Row(1, "v_1_new", "+U") :: Row(
              2,
              "v_2_new",
              "+I") :: Row(3, "v_3", "+I") :: Nil
          checkAnswer(currentResult(), expertResult2)
        } finally {
          query.stop()
        }
    }
  }

  test("Paimon CDC Source: from-snapshot scan mode with scan.snapshot-id") {
    withTempDir {
      checkpointDir =>
        val tableName = "T"
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        spark.sql(s"""
                     |CREATE TABLE $tableName (a INT, b STRING)
                     |TBLPROPERTIES (
                     |  'primary-key'='a',
                     |  'write-mode'='change-log',
                     |  'bucket'='2',
                     |  'changelog-producer' = 'lookup')
                     |""".stripMargin)

        spark.sql(s"INSERT INTO $tableName VALUES (1, 'v_1')")
        spark.sql(s"INSERT INTO $tableName VALUES (2, 'v_2')")
        spark.sql(s"INSERT INTO $tableName VALUES (2, 'v_2_new')")

        val table = loadTable(tableName)
        val location = table.location().getPath

        val query = spark.readStream
          .format("paimon")
          .option("read.readChangeLog", "true")
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
          query.processAllAvailable()
          val expertResult1 = Row(1, "v_1", "+I") :: Row(2, "v_2", "+I") :: Row(
            2,
            "v_2",
            "-U") :: Row(2, "v_2_new", "+U") :: Nil
          checkAnswer(currentResult(), expertResult1)

          spark.sql(s"INSERT INTO $tableName VALUES (1, 'v_1_new'), (3, 'v_3')")
          query.processAllAvailable()
          val expertResult2 =
            Row(1, "v_1", "+I") :: Row(1, "v_1", "-U") :: Row(1, "v_1_new", "+U") :: Row(
              2,
              "v_2",
              "+I") :: Row(2, "v_2", "-U") :: Row(2, "v_2_new", "+U") :: Row(3, "v_3", "+I") :: Nil
          checkAnswer(currentResult(), expertResult2)
        } finally {
          query.stop()
        }
    }
  }

}
