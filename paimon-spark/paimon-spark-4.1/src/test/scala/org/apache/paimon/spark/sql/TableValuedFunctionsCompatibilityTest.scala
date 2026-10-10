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
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.table.{FullTextSearchTable, HybridSearchTable, VectorSearchTable}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.logical.Except
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * `TableValuedFunctionsTest` only runs in `paimon-spark-ut`, against the profile's newest Spark.
 * These cases run table-valued functions on a Spark 4.1 runtime, which loads `paimon-spark-common`
 * classes compiled against a newer Spark.
 */
class TableValuedFunctionsCompatibilityTest extends PaimonSparkTestBase {

  private def relationTables(df: DataFrame): Seq[org.apache.spark.sql.connector.catalog.Table] =
    df.queryExecution.analyzed.collect { case r: DataSourceV2Relation => r.table }

  test("paimon_incremental_query reads the snapshots in range") {
    withTable("t") {
      sql("CREATE TABLE t (id INT) USING paimon")
      sql("INSERT INTO t VALUES 1")
      sql("INSERT INTO t VALUES 2")
      sql("INSERT INTO t VALUES 3")

      checkAnswer(
        sql("SELECT * FROM paimon_incremental_query('t', 1, 3) ORDER BY id"),
        Seq(Row(2), Row(3)))
    }
  }

  test("paimon_incremental_query diffs the snapshots when the tags' buckets differ") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (a INT, b INT) USING paimon
            |TBLPROPERTIES ('primary-key'='a', 'bucket' = '1')
            |""".stripMargin)

      val table = loadTable("t")
      sql("INSERT INTO t VALUES (1, 11), (2, 22)")
      table.createTag("2024-01-01", 1)

      sql("ALTER TABLE t SET TBLPROPERTIES ('bucket' = '2')")
      sql("INSERT OVERWRITE t SELECT * FROM t")
      sql("INSERT INTO t VALUES (3, 33)")
      table.createTag("2024-01-03", 3)

      val df =
        sql("SELECT * FROM paimon_incremental_query('t', '2024-01-01', '2024-01-03') ORDER BY a, b")
      // The bucket change makes Paimon diff the two snapshots in Spark instead of in its scan.
      assert(df.queryExecution.analyzed.collectFirst { case e: Except => e }.isDefined)
      checkAnswer(df, Seq(Row(3, 33)))
    }
  }

  // The search functions are only analyzed: the relation is built during analysis.

  test("vector_search resolves to a vector search relation") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, v ARRAY<FLOAT>) USING paimon")
      val df = sql("SELECT id FROM vector_search('t', 'v', array(1.0f, 0.0f), 1)")
      relationTables(df) match {
        case Seq(SparkTable(_: VectorSearchTable)) =>
        case other => fail(s"Unexpected relations: $other")
      }
    }
  }

  test("hybrid_search resolves to a hybrid search relation") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, v ARRAY<FLOAT>) USING paimon")
      val df =
        sql("""
              |SELECT id FROM hybrid_search(
              |  't',
              |  array(named_struct('vector_column', 'v', 'query_vector', array(1.0f, 0.0f))),
              |  array(),
              |  1)
              |""".stripMargin)
      relationTables(df) match {
        case Seq(SparkTable(_: HybridSearchTable)) =>
        case other => fail(s"Unexpected relations: $other")
      }
    }
  }

  test("full_text_search resolves to a full-text search relation") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, content STRING) USING paimon")
      val df =
        sql("""
              |SELECT id FROM full_text_search(
              |  't',
              |  'content',
              |  '{"match":{"column":"content","terms":"paimon"}}',
              |  1)
              |""".stripMargin)
      relationTables(df) match {
        case Seq(SparkTable(_: FullTextSearchTable)) =>
        case other => fail(s"Unexpected relations: $other")
      }
    }
  }
}
