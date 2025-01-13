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

import org.apache.paimon.data.{BinaryString, GenericRow, Timestamp}
import org.apache.paimon.manifest.ManifestCommittable
import org.apache.paimon.spark.PaimonHiveTestBase

import org.apache.spark.sql.{DataFrame, Row}

import java.time.LocalDateTime
import java.util.Collections

class TableValuedFunctionsTest extends PaimonHiveTestBase {

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"incremental query: hasPk: $hasPk, bucket: $bucket") {
            Seq("paimon", sparkCatalogName, paimonHiveCatalogName).foreach {
              catalogName =>
                sql(s"use $catalogName")

                withTable("t") {
                  val prop = if (hasPk) {
                    s"'primary-key'='a,b', 'bucket' = '$bucket' "
                  } else if (bucket != -1) {
                    s"'bucket-key'='b', 'bucket' = '$bucket' "
                  } else {
                    "'write-only'='true'"
                  }

                  spark.sql(s"""
                               |CREATE TABLE t (a INT, b INT, c STRING)
                               |USING paimon
                               |TBLPROPERTIES ($prop)
                               |PARTITIONED BY (a)
                               |""".stripMargin)

                  spark.sql("INSERT INTO t values (1, 1, '1'), (2, 2, '2')")
                  spark.sql("INSERT INTO t VALUES (1, 3, '3'), (2, 4, '4')")
                  spark.sql("INSERT INTO t VALUES (1, 5, '5'), (1, 7, '7')")

                  checkAnswer(
                    incrementalDF("t", 0, 1).orderBy("a", "b"),
                    Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)
                  checkAnswer(
                    spark.sql(
                      "SELECT * FROM paimon_incremental_query('t', '0', '1') ORDER BY a, b"),
                    Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)

                  checkAnswer(
                    incrementalDF("t", 1, 2).orderBy("a", "b"),
                    Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)
                  checkAnswer(
                    spark.sql(
                      "SELECT * FROM paimon_incremental_query('t', '1', '2') ORDER BY a, b"),
                    Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)

                  checkAnswer(
                    incrementalDF("t", 2, 3).orderBy("a", "b"),
                    Row(1, 5, "5") :: Row(1, 7, "7") :: Nil)
                  checkAnswer(
                    spark.sql(
                      "SELECT * FROM paimon_incremental_query('t', '2', '3') ORDER BY a, b"),
                    Row(1, 5, "5") :: Row(1, 7, "7") :: Nil)

                  checkAnswer(
                    incrementalDF("t", 1, 3).orderBy("a", "b"),
                    Row(1, 3, "3") :: Row(1, 5, "5") :: Row(1, 7, "7") :: Row(2, 4, "4") :: Nil
                  )
                  checkAnswer(
                    spark.sql(
                      "SELECT * FROM paimon_incremental_query('t', '1', '3') ORDER BY a, b"),
                    Row(1, 3, "3") :: Row(1, 5, "5") :: Row(1, 7, "7") :: Row(2, 4, "4") :: Nil)
                }
            }
          }
      }
  }

  test("Table Valued Functions: paimon_incremental_between_timestamp") {
    Seq("paimon", sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        sql(s"USE $catalogName")
        val dbName = "test_tvf_db"
        withDatabase(dbName) {
          sql(s"CREATE DATABASE $dbName")
          withTable("t") {
            sql(s"USE $dbName")
            sql("CREATE TABLE t (id INT) USING paimon")

            sql("INSERT INTO t VALUES 1")
            Thread.sleep(100)
            val t1 = System.currentTimeMillis()
            sql("INSERT INTO t VALUES 2")
            Thread.sleep(100)
            val t2 = System.currentTimeMillis()
            sql("INSERT INTO t VALUES 3")
            sql("INSERT INTO t VALUES 4")
            Thread.sleep(100)
            val t3 = System.currentTimeMillis()
            sql("INSERT INTO t VALUES 5")

            checkAnswer(
              sql(
                s"SELECT * FROM paimon_incremental_between_timestamp('t', '$t1', '$t2') ORDER BY id"),
              Seq(Row(2)))
            checkAnswer(
              sql(
                s"SELECT * FROM paimon_incremental_between_timestamp('$dbName.t', '$t2', '$t3') ORDER BY id"),
              Seq(Row(3), Row(4)))
            checkAnswer(
              sql(
                s"SELECT * FROM paimon_incremental_between_timestamp('$catalogName.$dbName.t', '$t1', '$t3') ORDER BY id"),
              Seq(Row(2), Row(3), Row(4)))
          }
        }
    }
  }

  test("Table Valued Functions: paimon_incremental_to_auto_tag") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (a INT, b STRING) USING paimon
            |TBLPROPERTIES ('primary-key' = 'a', 'bucket' = '1', 'tag.automatic-creation'='watermark', 'tag.creation-period'='daily')
            |""".stripMargin)

      val table = loadTable("t")
      val write = table.newWrite(commitUser)
      val commit = table.newCommit(commitUser).ignoreEmptyCommit(false)

      write.write(GenericRow.of(1, BinaryString.fromString("a")))
      var commitMessages = write.prepareCommit(false, 0)
      commit.commit(
        new ManifestCommittable(
          0,
          utcMills("2024-12-02T10:00:00"),
          Collections.emptyMap[Integer, java.lang.Long],
          commitMessages))

      write.write(GenericRow.of(2, BinaryString.fromString("b")))
      commitMessages = write.prepareCommit(false, 1)
      commit.commit(
        new ManifestCommittable(
          1,
          utcMills("2024-12-03T10:00:00"),
          Collections.emptyMap[Integer, java.lang.Long],
          commitMessages))

      write.write(GenericRow.of(3, BinaryString.fromString("c")))
      commitMessages = write.prepareCommit(false, 2)
      commit.commit(
        new ManifestCommittable(
          2,
          utcMills("2024-12-05T10:00:00"),
          Collections.emptyMap[Integer, java.lang.Long],
          commitMessages))

      checkAnswer(
        sql(s"SELECT * FROM paimon_incremental_to_auto_tag('t', '2024-12-01') ORDER BY a"),
        Seq())
      checkAnswer(
        sql(s"SELECT * FROM paimon_incremental_to_auto_tag('t', '2024-12-02') ORDER BY a"),
        Seq(Row(2, "b")))
      checkAnswer(
        sql(s"SELECT * FROM paimon_incremental_to_auto_tag('t', '2024-12-03') ORDER BY a"),
        Seq())
      checkAnswer(
        sql(s"SELECT * FROM paimon_incremental_to_auto_tag('t', '2024-12-04') ORDER BY a"),
        Seq(Row(3, "c")))
    }
  }

  private def incrementalDF(tableIdent: String, start: Int, end: Int): DataFrame = {
    spark.read
      .format("paimon")
      .option("incremental-between", s"$start,$end")
      .table(tableIdent)
  }

  private def utcMills(timestamp: String) =
    Timestamp.fromLocalDateTime(LocalDateTime.parse(timestamp)).getMillisecond

  object GenericRow {
    def of(values: Any*): GenericRow = {
      val row = new GenericRow(values.length)
      values.zipWithIndex.foreach {
        case (value, index) =>
          row.setField(index, value)
      }
      row
    }
  }
}
