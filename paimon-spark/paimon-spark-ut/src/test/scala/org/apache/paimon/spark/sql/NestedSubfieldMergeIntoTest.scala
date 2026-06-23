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
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

/**
 * End-to-end tests for sub-field-level data evolution via Spark `MERGE INTO`: updating a single
 * sub-field of a nested struct column should write an incremental file containing only that
 * sub-field (a dotted write column like `nest.a`), aligned by row-id, while the rest of the struct
 * is read back from the original file.
 */
class NestedSubfieldMergeIntoTest extends PaimonSparkTestBase {

  import testImplicits._

  private def latestDeltaWriteCols(tableName: String): Seq[Seq[String]] = {
    val t = loadTable(tableName)
    val splits = t.newSnapshotReader().read().splits().asScala
    splits
      .flatMap(_.asInstanceOf[DataSplit].dataFiles().asScala)
      .map(f => Option(f.writeCols()).map(_.asScala.toSeq).getOrElse(Seq.empty))
      .toSeq
  }

  test("Sub-field data evolution: MERGE INTO updating one struct sub-field writes only that leaf") {
    withTable("s", "t") {
      sql(s"""
             |CREATE TABLE t (id INT, nest STRUCT<a: INT, b: STRING>) TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true',
             |  'data-evolution.nested-field.enabled' = 'true')
             |""".stripMargin)
      sql(
        "INSERT INTO t VALUES (1, named_struct('a', 10, 'b', 'x')), " +
          "(2, named_struct('a', 20, 'b', 'y'))")

      Seq((1, 100)).toDF("id", "newa").createOrReplaceTempView("s")

      sql(s"""
             |MERGE INTO t
             |USING s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.nest.a = s.newa
             |""".stripMargin).collect()

      // correctness: nest.a updated for id=1, nest.b preserved, other row untouched
      checkAnswer(
        sql("SELECT id, nest.a, nest.b FROM t ORDER BY id"),
        Seq(Row(1, 100, "x"), Row(2, 20, "y")))

      // feature engaged: the incremental file written by the merge only contains nest.a
      val deltaCols = latestDeltaWriteCols("t")
      assert(
        deltaCols.exists(cols => cols == Seq("nest.a")),
        s"expected an incremental file with writeCols == [nest.a], got: $deltaCols")
    }
  }

  test("Sub-field data evolution: updating whole struct still writes the whole column") {
    withTable("s", "t") {
      sql(s"""
             |CREATE TABLE t (id INT, nest STRUCT<a: INT, b: STRING>) TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true',
             |  'data-evolution.nested-field.enabled' = 'true')
             |""".stripMargin)
      sql("INSERT INTO t VALUES (1, named_struct('a', 10, 'b', 'x'))")

      Seq((1, 100, "z")).toDF("id", "newa", "newb").createOrReplaceTempView("s")

      sql(s"""
             |MERGE INTO t
             |USING s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.nest = named_struct('a', s.newa, 'b', s.newb)
             |""".stripMargin).collect()

      checkAnswer(sql("SELECT id, nest.a, nest.b FROM t"), Seq(Row(1, 100, "z")))
    }
  }

  test(
    "Sub-field data evolution: disabled by default, sub-field update rewrites the whole column") {
    withTable("s", "t") {
      // data-evolution.nested-field.enabled is left at its default (false)
      sql(s"""
             |CREATE TABLE t (id INT, nest STRUCT<a: INT, b: STRING>) TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true')
             |""".stripMargin)
      sql("INSERT INTO t VALUES (1, named_struct('a', 10, 'b', 'x'))")

      Seq((1, 100)).toDF("id", "newa").createOrReplaceTempView("s")

      sql(s"""
             |MERGE INTO t
             |USING s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.nest.a = s.newa
             |""".stripMargin).collect()

      // correctness still holds: nest.a updated, nest.b preserved
      checkAnswer(sql("SELECT id, nest.a, nest.b FROM t"), Seq(Row(1, 100, "x")))

      // but no sub-field incremental file is produced: the whole nest column is rewritten
      val deltaCols = latestDeltaWriteCols("t")
      assert(
        !deltaCols.exists(cols => cols.contains("nest.a")),
        s"expected no dotted (sub-field) writeCols when feature is disabled, got: $deltaCols")
    }
  }
}
