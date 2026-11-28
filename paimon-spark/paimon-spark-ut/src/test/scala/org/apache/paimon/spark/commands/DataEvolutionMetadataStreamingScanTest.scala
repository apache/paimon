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

package org.apache.paimon.spark.commands

import org.apache.paimon.spark.{PaimonScan, PaimonSparkTestBase}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

class DataEvolutionMetadataStreamingScanTest extends PaimonSparkTestBase {

  Seq("off", "lightweight", "streaming").foreach {
    mode =>
      test(s"metadata mode $mode preserves partial columns and repeatable SELECT and LIMIT") {
        withTable("t") {
          sql(s"""
                 |CREATE TABLE t (id INT, a INT, b INT) TBLPROPERTIES (
                 | 'bucket'='-1', 'row-tracking.enabled'='true',
                 | 'data-evolution.enabled'='true', 'write-only'='true',
                 | 'data-evolution.metadata-memory-optimization.mode'='$mode')
                 |""".stripMargin)
          sql(
            "INSERT INTO t SELECT /*+ COALESCE(1) */ * FROM VALUES " +
              "(1, 10, 100), (2, 20, CAST(NULL AS INT)), (3, 30, 300) AS v(id, a, b)")
          sql("INSERT INTO t VALUES (4, 40, 400)")
          sql(
            "MERGE INTO t USING (SELECT 1 AS id, 111 AS b) s ON t.id = s.id " +
              "WHEN MATCHED THEN UPDATE SET b = s.b")
          val query = spark.sql("SELECT id, a, b, _ROW_ID FROM t")
          val scan = query.queryExecution.optimizedPlan.collectFirst {
            case relation: DataSourceV2ScanRelation => relation.scan.asInstanceOf[PaimonScan]
          }.get
          val splits = scan.inputSplits.collect { case split: DataSplit => split }
          assert(splits.nonEmpty)
          assert(splits.forall {
            split =>
              split.dataFiles().getClass.getName.contains("DataFileMetaStore$Slice") ==
                (mode == "streaming")
          })
          val expected =
            Seq(Row(1, 10, 111, 0L), Row(2, 20, null, 1L), Row(3, 30, 300, 2L), Row(4, 40, 400, 3L))
          checkAnswer(query, expected)
          checkAnswer(query, expected)
          query.cache()
          try {
            checkAnswer(query, expected)
            checkAnswer(query, expected)
          } finally {
            query.unpersist(blocking = true)
          }
          checkAnswer(query, expected)
          checkAnswer(sql("SELECT id FROM t WHERE b IS NULL"), Seq(Row(2)))
          checkAnswer(
            sql("SELECT id, b FROM t ORDER BY id LIMIT 2"),
            Seq(Row(1, 111), Row(2, null)))
          val limited = sql("SELECT id, a, b, _ROW_ID FROM t LIMIT 2").collect().toSeq
          assert(limited.size == 2 && limited.distinct.size == 2)
          assert(limited.forall(expected.contains))
        }
      }
  }

  test("streaming scan handles an empty table without requiring a sealed snapshot store") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, b INT) TBLPROPERTIES (
            | 'bucket'='-1', 'row-tracking.enabled'='true',
            | 'data-evolution.enabled'='true', 'write-only'='true',
            | 'data-evolution.metadata-memory-optimization.mode'='streaming')
            |""".stripMargin)
      checkAnswer(sql("SELECT * FROM t"), Seq.empty[Row])
      checkAnswer(sql("SELECT * FROM t LIMIT 2"), Seq.empty[Row])
    }
  }

  test("streaming scan preserves partition filtering and the full scan guard") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, b INT, p INT) PARTITIONED BY (p) TBLPROPERTIES (
            | 'bucket'='-1', 'row-tracking.enabled'='true',
            | 'data-evolution.enabled'='true', 'write-only'='true',
            | 'data-evolution.metadata-memory-optimization.mode'='streaming')
            |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 10, 1), (2, 20, 2)")
      withSparkSQLConf("spark.paimon.read.allow.fullScan" -> "false") {
        assert(
          intercept[Exception](sql("SELECT * FROM t").collect()).getMessage
            .contains("Full scan is not supported."))
        checkAnswer(sql("SELECT * FROM t WHERE p = 2"), Seq(Row(2, 20, 2)))
      }
    }
  }

  test("streaming scan propagates its replay disk limit instead of materializing") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, b INT) TBLPROPERTIES (
            | 'bucket'='-1', 'row-tracking.enabled'='true',
            | 'data-evolution.enabled'='true', 'write-only'='true',
            | 'data-evolution.metadata-memory-optimization.mode'='streaming',
            | 'data-evolution.metadata-memory-optimization.max-disk-size'='8 b')
            |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 10)")
      val failure = intercept[Exception](sql("SELECT id FROM t").collect())
      val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
      assert(
        causes.exists(
          error =>
            Option(error.getMessage)
              .exists(_.contains("Metadata store disk limit exceeded"))))
    }
  }
}
