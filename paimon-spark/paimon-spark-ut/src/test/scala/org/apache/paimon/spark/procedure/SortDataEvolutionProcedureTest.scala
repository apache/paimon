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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class SortDataEvolutionProcedureTest extends PaimonSparkTestBase {

  test("sort preserves disjoint global index files in the same partition") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT) TBLPROPERTIES (
            |'row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'write-only'='true')
            |""".stripMargin)
      def buildIndex(): Unit = {
        sql(
          "CALL sys.create_global_index(table => 'test.T', index_column => 'id', index_type => 'bitmap')")
          .collect()
      }
      def indexes: Seq[Row] = sql(
        "SELECT file_name, row_range_start, row_range_end FROM `T$table_indexes` " +
          "WHERE index_type = 'bitmap' ORDER BY row_range_start").collect().toSeq
      sql("INSERT INTO T SELECT /*+ COALESCE(1) */ * FROM VALUES (2), (1) AS r(id)")
      buildIndex()
      sql("INSERT INTO T SELECT /*+ COALESCE(1) */ * FROM VALUES (4), (3) AS r(id)")
      buildIndex()
      val before = indexes
      assert(before.map(r => (r.getLong(1), r.getLong(2))) == Seq((0L, 1L), (2L, 3L)))
      checkAnswer(
        sql("CALL sys.sort_data_evolution(table => 'T', `range` => '[0, 1]', order_by => 'id')"),
        Row(1) :: Nil)
      assert(indexes == before.tail)
    }
  }

  test("sort complete groups within a partition and rewrite the aligned payloads") {
    withTable("T") {
      sql("""
            |CREATE TABLE T (id INT, k INT, priority INT, payload BINARY, embedding ARRAY<FLOAT>, p INT)
            |PARTITIONED BY (p) TBLPROPERTIES (
            | 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
            | 'deletion-vectors.enabled'='true', 'blob-field'='payload',
            | 'vector-field'='embedding', 'field.embedding.vector-dim'='2',
            | 'vector.file.format'='parquet', 'write-only'='true')
            |""".stripMargin)
      def insert(start: Int, end: Int, p: Int): Unit = {
        sql(s"""INSERT INTO T SELECT /*+ COALESCE(1) */
               | CAST(id AS INT), IF(id = 2, NULL, CAST(id % 2 AS INT)), IF(id = 9, 0, 1),
               | IF(id = 8, NULL, CAST(concat('blob-', id) AS BINARY)),
               | IF(id = 9, NULL, array(CAST(id AS FLOAT), CAST(-id AS FLOAT))), $p
               | FROM range($start, $end, 1, 1)
               |""".stripMargin)
      }
      insert(0, 4, 0)
      insert(4, 7, 1)
      insert(7, 10, 0)
      insert(10, 12, 0)
      sql("UPDATE T SET k = 7 WHERE id = 1 AND p = 0")
      sql("DELETE FROM T WHERE id = 3 AND p = 0")

      val columns = "id, k, priority, cast(payload AS STRING), embedding[0], embedding[1], p"
      val target = "p = 0 AND _ROW_ID BETWEEN 0 AND 9"
      def rows(query: String): Seq[Row] = sql(query).collect().toSeq
      val expected = rows(s"SELECT $columns FROM T WHERE $target ORDER BY k, priority, _ROW_ID")
      assert(expected.map(_.getInt(0)) == Seq(2, 0, 8, 9, 7, 1))
      val untouched = rows(s"SELECT _ROW_ID, $columns FROM T WHERE NOT ($target) ORDER BY _ROW_ID")
      def files = loadTable("T")
        .newSnapshotReader()
        .read()
        .dataSplits()
        .asScala
        .flatMap(s => s.dataFiles().asScala.map(f => (s.partition().getInt(0), f)))
        .toVector
      val (inside, outside) = files.partition {
        case (p, f) => p == 0 && f.nonNullFirstRowId() < 10
      }
      val oldNames = inside.map(_._2.fileName()).toSet
      assert(oldNames.exists(_.endsWith(".blob")) && oldNames.exists(_.contains(".vector.")))
      val before = loadTable("T").latestSnapshot().get().id()
      val error = intercept[Exception] {
        sql("CALL sys.sort_data_evolution('T', '[1,9]', 'k', 'p = 0')").collect()
      }
      assert(
        Iterator
          .iterate[Throwable](error)(_.getCause)
          .takeWhile(_ != null)
          .exists(e => Option(e.getMessage).exists(_.contains("complete file groups"))))
      assert(loadTable("T").latestSnapshot().get().id() == before)

      val ranges = rows(
        "SELECT start_row_id, end_row_id FROM `T$row_ranges` " +
          "WHERE partition = '{0}' AND end_row_id < 10 ORDER BY start_row_id")
      assert(ranges == Seq(Row(0L, 3L), Row(7L, 9L)))
      val start = ranges.head.getLong(0)
      val end = ranges.last.getLong(1)
      checkAnswer(
        sql(s"CALL sys.sort_data_evolution('T', '[$start,$end]', 'k,priority', 'p = 0')"),
        Row(1) :: Nil)
      assert(rows(s"SELECT $columns FROM T WHERE _ROW_ID >= 12 ORDER BY _ROW_ID") == expected)
      assert(
        rows(s"SELECT _ROW_ID, $columns FROM T WHERE _ROW_ID < 12 ORDER BY _ROW_ID") == untouched)
      val now = files.map(_._2.fileName()).toSet
      assert(now.intersect(oldNames).isEmpty)
      assert(outside.forall(f => now.contains(f._2.fileName())))
      assert(now.diff(outside.map(_._2.fileName()).toSet).exists(_.endsWith(".blob")))
      assert(now.diff(outside.map(_._2.fileName()).toSet).exists(_.contains(".vector.")))
      checkAnswer(sql("CALL sys.sort_data_evolution('T', '[0,9]', 'k', 'p = 0')"), Row(0) :: Nil)
      assert(loadTable("T").latestSnapshot().get().id() == before + 1)
    }
  }
}
