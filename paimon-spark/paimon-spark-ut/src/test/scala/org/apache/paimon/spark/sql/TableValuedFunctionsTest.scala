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

import org.apache.paimon.spark.PaimonHiveTestBase

import org.apache.spark.sql.{DataFrame, Row}

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

  private def incrementalDF(tableIdent: String, start: Int, end: Int): DataFrame = {
    spark.read
      .format("paimon")
      .option("incremental-between", s"$start,$end")
      .table(tableIdent)
  }
}
