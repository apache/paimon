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

  // Guards the read path: DataEvolutionSplitRead calls leafPaths() on the planned read type,
  // which now requires recursive field order to match the schema. A reversed nested projection
  // must still read back correctly rather than tripping that check.
  test("Sub-field data evolution: reversed nested projection reads correctly") {
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

      // sub-field incremental file exists; now read the nested fields in REVERSE schema order
      assert(latestDeltaWriteCols("t").exists(cols => cols == Seq("nest.a")))
      checkAnswer(sql("SELECT nest.b, nest.a FROM t ORDER BY id"), Seq(Row("x", 100), Row("y", 20)))
      // and the whole struct plus a reversed pair together
      checkAnswer(
        sql("SELECT id, nest.b, nest.a, nest FROM t ORDER BY id"),
        Seq(Row(1, "x", 100, Row(100, "x")), Row(2, "y", 20, Row(20, "y"))))
    }
  }

  test("Sub-field data evolution: compaction merges sub-field files without changing data") {
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
      assert(latestDeltaWriteCols("t").exists(c => c == Seq("nest.a")))

      sql("CALL sys.compact(table => 't', options => 'compaction.min.file-num=2')").collect()

      // data survives the compaction and the dotted write columns are gone (single full file)
      checkAnswer(
        sql("SELECT id, nest.a, nest.b FROM t ORDER BY id"),
        Seq(Row(1, 100, "x"), Row(2, 20, "y")))
      assert(!latestDeltaWriteCols("t").exists(c => c.exists(_.contains("."))))
    }
  }

  test("Sub-field data evolution: adding a nested sub-field after sub-field files exist") {
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
      assert(latestDeltaWriteCols("t").exists(c => c == Seq("nest.a")))

      // the pre-existing sub-field files must still reconstruct against the evolved struct
      sql("ALTER TABLE t ADD COLUMN nest.c INT")
      checkAnswer(
        sql("SELECT id, nest.a, nest.b, nest.c FROM t ORDER BY id"),
        Seq(Row(1, 100, "x", null), Row(2, 20, "y", null)))
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

  test("Sub-field data evolution: disabled by default, sub-field update rewrites the whole column") {
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

  test(
    "Sub-field data evolution: sub-fields touched by separate WHEN MATCHED clauses in reverse " +
      "schema order are not swapped (regression for #8334 review)") {
    withTable("s", "t") {
      sql(s"""
             |CREATE TABLE t (id INT, nest STRUCT<a: INT, b: STRING, c: INT>) TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true',
             |  'data-evolution.nested-field.enabled' = 'true')
             |""".stripMargin)
      sql(
        "INSERT INTO t VALUES (1, named_struct('a', 10, 'b', 'x', 'c', 100)), " +
          "(2, named_struct('a', 200, 'b', 'y', 'c', 40))")

      Seq((1, 1, 999, 111), (2, 2, 222, 888))
        .toDF("id", "kind", "newc", "newa")
        .createOrReplaceTempView("s")

      // TWO separate WHEN MATCHED clauses: the first touches c, the second touches a.
      // The per-action union therefore starts in clause order [c, a], not schema order [a, c].
      sql(s"""
             |MERGE INTO t
             |USING s
             |ON t.id = s.id
             |WHEN MATCHED AND s.kind = 1 THEN UPDATE SET t.nest.c = s.newc
             |WHEN MATCHED AND s.kind = 2 THEN UPDATE SET t.nest.a = s.newa
             |""".stripMargin).collect()

      // row1: only c updated -> (10, x, 999); row2: only a updated -> (888, y, 40)
      checkAnswer(
        sql("SELECT id, nest.a, nest.b, nest.c FROM t ORDER BY id"),
        Seq(Row(1, 10, "x", 999), Row(2, 888, "y", 40)))
    }
  }

  test(
    "Sub-field data evolution: a matched row whose clause leaves its NULL struct untouched stays " +
      "NULL (regression for #8334 review)") {
    withTable("s", "t") {
      sql(s"""
             |CREATE TABLE t (id INT, nest STRUCT<a: INT, b: STRING>) TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true',
             |  'data-evolution.nested-field.enabled' = 'true')
             |""".stripMargin)
      sql(
        "INSERT INTO t VALUES (1, named_struct('a', 10, 'b', 'x')), " +
          "(2, CAST(NULL AS STRUCT<a: INT, b: STRING>))")

      // BOTH rows are matched by the source, but only row 1 satisfies the update condition,
      // so row 2 flows through the copy instruction with its NULL nest.
      Seq((1, 1, 100), (2, 2, 200)).toDF("id", "kind", "newa").createOrReplaceTempView("s")

      sql(s"""
             |MERGE INTO t
             |USING s
             |ON t.id = s.id
             |WHEN MATCHED AND s.kind = 1 THEN UPDATE SET t.nest.a = s.newa
             |""".stripMargin).collect()

      checkAnswer(
        sql("SELECT id, nest FROM t ORDER BY id"),
        Seq(Row(1, Row(100, "x")), Row(2, null)))
      checkAnswer(sql("SELECT id FROM t WHERE nest IS NULL"), Seq(Row(2)))
    }
  }

  // Note: a single clause with several assignments is normalised into schema order by Spark's
  // own assignment alignment, so this case cannot expose the ordering bug; the multi-clause test
  // above is the one that does. Kept as a plain correctness check.
  test("Sub-field data evolution: several sub-field assignments in one clause keep their values") {
    withTable("s", "t") {
      sql(s"""
             |CREATE TABLE t (id INT, nest STRUCT<a: INT, b: STRING, c: INT>) TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true',
             |  'data-evolution.nested-field.enabled' = 'true')
             |""".stripMargin)
      sql(
        "INSERT INTO t VALUES (1, named_struct('a', 10, 'b', 'x', 'c', 100)), " +
          "(2, named_struct('a', 200, 'b', 'y', 'c', 40))")

      Seq((1, 999, 111), (2, 222, 888)).toDF("id", "newc", "newa").createOrReplaceTempView("s")

      // note: SET touches c before a, i.e. in reverse of the struct's declaration order (a,b,c)
      sql(s"""
             |MERGE INTO t
             |USING s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.nest.c = s.newc, t.nest.a = s.newa
             |""".stripMargin).collect()

      checkAnswer(
        sql("SELECT id, nest.a, nest.b, nest.c FROM t ORDER BY id"),
        Seq(Row(1, 111, "x", 999), Row(2, 888, "y", 222)))
    }
  }

  // Note: a row that is not matched at all keeps its parent-struct nullness from the base file,
  // so this case cannot expose the null-guard bug; the matched-but-untouched test above is the one
  // that does. Kept as a plain correctness check.
  test("Sub-field data evolution: an unmatched row keeps its NULL struct") {
    withTable("s", "t") {
      sql(s"""
             |CREATE TABLE t (id INT, nest STRUCT<a: INT, b: STRING>) TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true',
             |  'data-evolution.nested-field.enabled' = 'true')
             |""".stripMargin)
      sql(
        "INSERT INTO t VALUES (1, named_struct('a', 10, 'b', 'x')), " +
          "(2, CAST(NULL AS STRUCT<a: INT, b: STRING>))")

      Seq((1, 100)).toDF("id", "newa").createOrReplaceTempView("s")

      sql(s"""
             |MERGE INTO t
             |USING s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.nest.a = s.newa
             |""".stripMargin).collect()

      checkAnswer(
        sql("SELECT id, nest FROM t ORDER BY id"),
        Seq(Row(1, Row(100, "x")), Row(2, null)))
      checkAnswer(sql("SELECT id FROM t WHERE nest IS NULL"), Seq(Row(2)))
    }
  }

  test(
    "Sub-field data evolution: SET on a sub-field materializes a previously-NULL struct " +
      "(regression for #8334 review)") {
    withTable("s", "t") {
      sql(s"""
             |CREATE TABLE t (id INT, nest STRUCT<a: INT, b: STRING>) TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true',
             |  'data-evolution.nested-field.enabled' = 'true')
             |""".stripMargin)
      sql("INSERT INTO t VALUES (1, CAST(NULL AS STRUCT<a: INT, b: STRING>))")

      Seq((1, 100)).toDF("id", "newa").createOrReplaceTempView("s")

      sql(s"""
             |MERGE INTO t
             |USING s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.nest.a = s.newa
             |""".stripMargin).collect()

      // An explicit sub-field assignment must materialize the struct even though the target
      // struct was NULL; the untouched sibling stays NULL.
      checkAnswer(sql("SELECT id, nest.a, nest.b FROM t"), Seq(Row(1, 100, null)))
      assert(latestDeltaWriteCols("t").exists(cols => cols == Seq("nest.a")))
    }
  }

  test(
    "Sub-field data evolution: MERGE INTO whole-struct SET on a previously-NULL struct " +
      "materializes correctly") {
    withTable("s", "t") {
      sql(s"""
             |CREATE TABLE t (id INT, nest STRUCT<a: INT, b: STRING>) TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true',
             |  'data-evolution.nested-field.enabled' = 'true')
             |""".stripMargin)
      sql("INSERT INTO t VALUES (1, CAST(NULL AS STRUCT<a: INT, b: STRING>))")

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

  test("Data evolution: deep ADD COLUMN plus an unrelated partial update stays readable") {
    withTable("s", "t") {
      // NOTE: 'data-evolution.nested-field.enabled' is deliberately NOT set. Every write here is
      // a whole-column write, so this is a plain data-evolution table.
      sql(s"""
             |CREATE TABLE t (id INT, v INT, payload STRUCT<inner: STRUCT<x: INT>>)
             |TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true')
             |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 5, named_struct('inner', named_struct('x', 10)))")

      // a nested column is added one level deeper than the existing struct
      sql("ALTER TABLE t ADD COLUMN payload.inner.y INT")

      // an unrelated partial update that only touches the top-level column "v"
      Seq((1, 6)).toDF("id", "newv").createOrReplaceTempView("s")
      sql(s"""
             |MERGE INTO t
             |USING s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.v = s.newv
             |""".stripMargin).collect()

      // the row-id group now holds two files, so the read goes through the union reader:
      // payload.inner.x comes from the original file and payload.inner.y is null-filled
      checkAnswer(
        sql("SELECT id, v, payload.inner.x, payload.inner.y FROM t"),
        Seq(Row(1, 6, 10, null)))

      // compaction over the same group must work too
      sql("CALL sys.compact(table => 't')").collect()
      checkAnswer(
        sql("SELECT id, v, payload.inner.x, payload.inner.y FROM t"),
        Seq(Row(1, 6, 10, null)))
    }
  }

  test("Data evolution: projecting a single leaf of a two-level struct reads correctly") {
    withTable("s", "t") {
      sql(s"""
             |CREATE TABLE t (id INT, v INT, payload STRUCT<inner: STRUCT<x: INT, y: INT>>)
             |TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true')
             |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 5, named_struct('inner', named_struct('x', 10, 'y', 20)))")

      // an unrelated partial update, so the read goes through the union reader
      Seq((1, 6)).toDF("id", "newv").createOrReplaceTempView("s")
      sql(s"""
             |MERGE INTO t
             |USING s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.v = s.newv
             |""".stripMargin).collect()

      // the engine prunes the read type down to a single leaf two levels deep; that is a read-side
      // projection, not a partial write, so it must not hit any write-side nesting restriction
      checkAnswer(sql("SELECT payload.inner.x FROM t"), Seq(Row(10)))
      checkAnswer(sql("SELECT id, v, payload.inner.y FROM t"), Seq(Row(1, 6, 20)))
      checkAnswer(
        sql("SELECT id, v, payload.inner.x, payload.inner.y FROM t"),
        Seq(Row(1, 6, 10, 20)))
    }
  }
}
