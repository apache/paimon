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

import org.apache.spark.sql.Row

abstract class DataEvolutionDeletionTestBase extends PaimonSparkTestBase {

  test("Data Evolution deletion: delete from table with deletion vectors") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, b INT, c INT)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true')
            |""".stripMargin)
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(0, 5)")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(5, 10)")
      sql("ALTER TABLE t ADD COLUMNS (d INT)")
      sql(
        "INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c, id + 100 AS d FROM range(10, 13)")

      sql("DELETE FROM t WHERE id IN (1, 4, 6, 11)")
      checkAnswer(
        sql("SELECT *, _ROW_ID FROM t ORDER BY id"),
        Seq(
          Row(0, 0, 0, null, 0L),
          Row(2, 2, 2, null, 2L),
          Row(3, 3, 3, null, 3L),
          Row(5, 5, 5, null, 5L),
          Row(7, 7, 7, null, 7L),
          Row(8, 8, 8, null, 8L),
          Row(9, 9, 9, null, 9L),
          Row(10, 10, 10, 110, 10L),
          Row(12, 12, 12, 112, 12L)
        )
      )

      sql("DELETE FROM t WHERE id IN (2, 8)")
      checkAnswer(
        sql("SELECT *, _ROW_ID FROM t ORDER BY id"),
        Seq(
          Row(0, 0, 0, null, 0L),
          Row(3, 3, 3, null, 3L),
          Row(5, 5, 5, null, 5L),
          Row(7, 7, 7, null, 7L),
          Row(9, 9, 9, null, 9L),
          Row(10, 10, 10, 110, 10L),
          Row(12, 12, 12, 112, 12L))
      )
    }
  }

  test("Data Evolution deletion: global index query skips deleted rows") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, name STRING, b INT)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true',
            |  'global-index.search-mode' = 'full',
            |  'btree-index.records-per-range' = '1000')
            |""".stripMargin)
      sql("""
            |INSERT INTO t VALUES
            |  (1, 'name-1', 10),
            |  (2, 'name-2', 20),
            |  (3, 'name-3', 30),
            |  (4, 'name-4', 40)
            |""".stripMargin)
      sql(
        "CALL sys.create_global_index(table => 'test.t', index_column => 'name', " +
          "index_type => 'btree')")

      sql("DELETE FROM t WHERE id IN (2, 4)")

      checkAnswer(
        sql("SELECT id, name, b FROM t WHERE name IN ('name-2', 'name-4') ORDER BY id"),
        Seq.empty[Row])
      checkAnswer(
        sql("SELECT id, name, b FROM t WHERE name IN ('name-1', 'name-3') ORDER BY id"),
        Seq(Row(1, "name-1", 10), Row(3, "name-3", 30)))
    }
  }

  test("Data Evolution deletion: merge update after file-level and partial deletion") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, new_b INT)")
      sql("INSERT INTO s VALUES (2, 200), (6, 600), (7, 700), (9, 900)")

      sql("""
            |CREATE TABLE t (id INT, b INT, c INT)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true')
            |""".stripMargin)
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(0, 5)")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(5, 10)")
      sql("DELETE FROM t WHERE id IN (0, 1, 2, 3, 4, 6, 9)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.b = s.new_b
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, b, c, _ROW_ID FROM t ORDER BY id"),
        Seq(Row(5, 5, 5, 5L), Row(7, 700, 7, 7L), Row(8, 8, 8, 8L)))
    }
  }

  test("Data Evolution deletion: merge update blob after deletion") {
    withTable("s", "t") {
      sql("""
            |CREATE TABLE t (id INT, b INT, picture BINARY)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true',
            |  'blob-field' = 'picture',
            |  'blob.target-file-size' = '1 b')
            |""".stripMargin)
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(1) */ id, b, picture FROM VALUES
            |  (0, 0, X'00'), (1, 1, X'01'), (2, 2, X'02'), (3, 3, X'03'), (4, 4, X'04')
            |  AS v(id, b, picture)
            |""".stripMargin)
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(1) */ id, b, picture FROM VALUES
            |  (5, 5, X'05'), (6, 6, X'06'), (7, 7, X'07'), (8, 8, X'08'), (9, 9, X'09')
            |  AS v(id, b, picture)
            |""".stripMargin)
      sql("DELETE FROM t WHERE id IN (0, 1, 2, 3, 4, 6, 9)")

      sql("CREATE TABLE s (id INT, picture BINARY)")
      sql("INSERT INTO s VALUES (2, X'22'), (6, X'66'), (7, X'4D'), (9, X'79')")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.picture = s.picture
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, b, picture, _ROW_ID FROM t ORDER BY id"),
        Seq(
          Row(5, 5, Array[Byte](5), 5L),
          Row(7, 7, Array[Byte](77), 7L),
          Row(8, 8, Array[Byte](8), 8L)))
    }
  }

  test("Data Evolution deletion: self merge skips deleted rows") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, b INT, c INT)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true')
            |""".stripMargin)
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(0, 5)")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(5, 10)")
      sql("DELETE FROM t WHERE id IN (0, 1, 2, 3, 4, 6, 9)")

      sql("""
            |MERGE INTO t
            |USING t AS source
            |ON t._ROW_ID = source._ROW_ID
            |WHEN MATCHED AND source.id IN (2, 6, 7, 9) THEN UPDATE SET t.b = source.b + 100
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, b, c, _ROW_ID FROM t ORDER BY id"),
        Seq(Row(5, 5, 5, 5L), Row(7, 107, 7, 7L), Row(8, 8, 8, 8L)))
    }
  }

  test("Data Evolution deletion: self merge delete by row id") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, b INT, c INT)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true')
            |""".stripMargin)
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(0, 5)")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(5, 10)")

      sql("""
            |MERGE INTO t
            |USING t AS source
            |ON t._ROW_ID = source._ROW_ID
            |WHEN MATCHED AND source.id IN (2, 6, 9) THEN DELETE
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, b, c, _ROW_ID FROM t ORDER BY id"),
        Seq(
          Row(0, 0, 0, 0L),
          Row(1, 1, 1, 1L),
          Row(3, 3, 3, 3L),
          Row(4, 4, 4, 4L),
          Row(5, 5, 5, 5L),
          Row(7, 7, 7, 7L),
          Row(8, 8, 8, 8L))
      )
    }
  }

  test("Data Evolution deletion: merge matched delete writes deletion vectors only") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT)")
      sql("INSERT INTO s VALUES (1), (6), (8), (99)")

      sql("""
            |CREATE TABLE t (id INT, b INT, c INT)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true')
            |""".stripMargin)
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(0, 5)")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(5, 10)")

      val fileCountBefore = sql("SELECT COUNT(*) FROM `t$files` WHERE file_path NOT LIKE '%.blob'")
        .collect()
        .head
        .getLong(0)

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN DELETE
            |""".stripMargin)

      val fileCountAfter = sql("SELECT COUNT(*) FROM `t$files` WHERE file_path NOT LIKE '%.blob'")
        .collect()
        .head
        .getLong(0)
      assert(fileCountAfter == fileCountBefore)

      checkAnswer(
        sql("SELECT id, b, c, _ROW_ID FROM t ORDER BY id"),
        Seq(
          Row(0, 0, 0, 0L),
          Row(2, 2, 2, 2L),
          Row(3, 3, 3, 3L),
          Row(4, 4, 4, 4L),
          Row(5, 5, 5, 5L),
          Row(7, 7, 7, 7L),
          Row(9, 9, 9, 9L))
      )
    }
  }

  test("Data Evolution deletion: merge matched update and delete keeps first-match semantics") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, new_b INT, op STRING)")
      sql("""
            |INSERT INTO s VALUES
            |  (1, 100, 'update'),
            |  (2, 200, 'delete'),
            |  (6, 600, 'update'),
            |  (11, 1100, 'delete')
            |""".stripMargin)

      sql("""
            |CREATE TABLE t (id INT, b INT, c INT)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true')
            |""".stripMargin)
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(0, 5)")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(5, 10)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED AND s.op = 'delete' THEN DELETE
            |WHEN MATCHED AND s.id IN (1, 2, 6) THEN UPDATE SET t.b = s.new_b
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, b, c, _ROW_ID FROM t ORDER BY id"),
        Seq(
          Row(0, 0, 0, 0L),
          Row(1, 100, 1, 1L),
          Row(3, 3, 3, 3L),
          Row(4, 4, 4, 4L),
          Row(5, 5, 5, 5L),
          Row(6, 600, 6, 6L),
          Row(7, 7, 7, 7L),
          Row(8, 8, 8, 8L),
          Row(9, 9, 9, 9L))
      )
    }
  }

  test("Data Evolution deletion: merge not matched by source delete") {
    assume(gteqSpark3_4)

    withTable("s", "t") {
      sql("CREATE TABLE s (id INT)")
      sql("INSERT INTO s VALUES (1), (3)")

      sql("""
            |CREATE TABLE t (id INT, b INT, c INT)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true')
            |""".stripMargin)
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(0, 6)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN NOT MATCHED BY SOURCE THEN DELETE
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, b, c, _ROW_ID FROM t ORDER BY id"),
        Seq(Row(1, 1, 1, 1L), Row(3, 3, 3, 3L)))
    }
  }

  test("Data Evolution deletion: merge update insert and not matched by source delete") {
    assume(gteqSpark3_4)

    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, new_b INT)")
      sql("INSERT INTO s VALUES (1, 100), (4, 400)")

      sql("""
            |CREATE TABLE t (id INT, b INT, c INT)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true')
            |""".stripMargin)
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(0, 4)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.b = s.new_b
            |WHEN NOT MATCHED THEN INSERT (id, b, c) VALUES (s.id, s.new_b, s.new_b)
            |WHEN NOT MATCHED BY SOURCE THEN DELETE
            |""".stripMargin)

      checkAnswer(sql("SELECT id, b, c FROM t ORDER BY id"), Seq(Row(1, 100, 1), Row(4, 400, 400)))
    }
  }

  test("Data Evolution deletion: repeated complex merge on table with deletion vectors") {
    assume(gteqSpark3_4)

    withTempView("s") {
      withTable("t") {
        sql("""
              |CREATE TABLE t (id INT, b INT, c INT)
              |TBLPROPERTIES (
              |  'row-tracking.enabled' = 'true',
              |  'data-evolution.enabled' = 'true',
              |  'deletion-vectors.enabled' = 'true')
              |""".stripMargin)
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(0, 5)")
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(5, 10)")
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(10, 12)")

        sql("""
              |CREATE OR REPLACE TEMP VIEW s AS
              |SELECT * FROM VALUES
              |  (1, 100, 'update'),
              |  (2, 200, 'delete'),
              |  (12, 1200, 'insert')
              |AS s(id, new_b, op)
              |""".stripMargin)
        sql("""
              |MERGE INTO t
              |USING s
              |ON t.id = s.id
              |WHEN MATCHED AND s.op = 'delete' THEN DELETE
              |WHEN MATCHED AND s.op = 'update' THEN UPDATE SET t.b = s.new_b
              |WHEN NOT MATCHED THEN INSERT (id, b, c) VALUES (s.id, s.new_b, s.new_b + 1000)
              |WHEN NOT MATCHED BY SOURCE AND t.id IN (3, 8) THEN DELETE
              |""".stripMargin)

        checkAnswer(
          sql("SELECT id, b, c FROM t ORDER BY id"),
          Seq(
            Row(0, 0, 0),
            Row(1, 100, 1),
            Row(4, 4, 4),
            Row(5, 5, 5),
            Row(6, 6, 6),
            Row(7, 7, 7),
            Row(9, 9, 9),
            Row(10, 10, 10),
            Row(11, 11, 11),
            Row(12, 1200, 2200))
        )

        sql("""
              |CREATE OR REPLACE TEMP VIEW s AS
              |SELECT * FROM VALUES
              |  (1, 101, 'update'),
              |  (2, 202, 'update'),
              |  (4, 400, 'delete'),
              |  (13, 1300, 'insert')
              |AS s(id, new_b, op)
              |""".stripMargin)
        sql("""
              |MERGE INTO t
              |USING s
              |ON t.id = s.id
              |WHEN MATCHED AND s.op = 'delete' THEN DELETE
              |WHEN MATCHED AND s.op = 'update' THEN UPDATE SET t.b = s.new_b
              |WHEN NOT MATCHED THEN INSERT (id, b, c) VALUES (s.id, s.new_b, s.new_b + 1000)
              |WHEN NOT MATCHED BY SOURCE AND t.id IN (5, 10) THEN DELETE
              |""".stripMargin)

        checkAnswer(
          sql("SELECT id, b, c FROM t ORDER BY id, b, c"),
          Seq(
            Row(0, 0, 0),
            Row(1, 101, 1),
            Row(2, 202, 1202),
            Row(6, 6, 6),
            Row(7, 7, 7),
            Row(9, 9, 9),
            Row(11, 11, 11),
            Row(12, 1200, 2200),
            Row(13, 1300, 2300))
        )
      }
    }
  }

  test("Data Evolution deletion: not matched by source delete does not trigger partial copy") {
    assume(gteqSpark3_4)

    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, new_b INT)")
      sql("INSERT INTO s VALUES (1, 100)")

      sql("""
            |CREATE TABLE t (id INT, b INT, c INT)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true')
            |""".stripMargin)
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(0, 5)")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(5, 10)")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(10, 15)")

      val thirdRangeFileCountBefore =
        sql(
          "SELECT COUNT(*) FROM `t$files` WHERE first_row_id = 10 AND file_path NOT LIKE '%.blob'")
          .collect()
          .head
          .getLong(0)

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.b = s.new_b
            |WHEN NOT MATCHED BY SOURCE AND t.id >= 10 THEN DELETE
            |""".stripMargin)

      val thirdRangeFileCountAfter =
        sql(
          "SELECT COUNT(*) FROM `t$files` WHERE first_row_id = 10 AND file_path NOT LIKE '%.blob'")
          .collect()
          .head
          .getLong(0)
      assert(thirdRangeFileCountAfter == thirdRangeFileCountBefore)

      checkAnswer(
        sql("SELECT id, b, c, _ROW_ID FROM t ORDER BY id"),
        Seq(
          Row(0, 0, 0, 0L),
          Row(1, 100, 1, 1L),
          Row(2, 2, 2, 2L),
          Row(3, 3, 3, 3L),
          Row(4, 4, 4, 4L),
          Row(5, 5, 5, 5L),
          Row(6, 6, 6, 6L),
          Row(7, 7, 7, 7L),
          Row(8, 8, 8, 8L),
          Row(9, 9, 9, 9L))
      )
    }
  }

  test("Data Evolution deletion: merge blob update and delete") {
    withTable("s", "t") {
      sql("""
            |CREATE TABLE t (id INT, b INT, picture BINARY)
            |TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'deletion-vectors.enabled' = 'true',
            |  'blob-field' = 'picture',
            |  'blob.target-file-size' = '1 b')
            |""".stripMargin)
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(1) */ id, b, picture FROM VALUES
            |  (0, 0, X'00'), (1, 1, X'01'), (2, 2, X'02'), (3, 3, X'03'), (4, 4, X'04')
            |  AS v(id, b, picture)
            |""".stripMargin)
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(1) */ id, b, picture FROM VALUES
            |  (5, 5, X'05'), (6, 6, X'06'), (7, 7, X'07'), (8, 8, X'08'), (9, 9, X'09')
            |  AS v(id, b, picture)
            |""".stripMargin)

      sql("CREATE TABLE s (id INT, picture BINARY, op STRING)")
      sql("""
            |INSERT INTO s VALUES
            |  (2, X'22', 'delete'),
            |  (6, X'66', 'update'),
            |  (7, X'4D', 'update'),
            |  (9, X'79', 'delete')
            |""".stripMargin)

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED AND s.op = 'delete' THEN DELETE
            |WHEN MATCHED THEN UPDATE SET t.picture = s.picture
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, b, picture, _ROW_ID FROM t ORDER BY id"),
        Seq(
          Row(0, 0, Array[Byte](0), 0L),
          Row(1, 1, Array[Byte](1), 1L),
          Row(3, 3, Array[Byte](3), 3L),
          Row(4, 4, Array[Byte](4), 4L),
          Row(5, 5, Array[Byte](5), 5L),
          Row(6, 6, Array[Byte](102), 6L),
          Row(7, 7, Array[Byte](77), 7L),
          Row(8, 8, Array[Byte](8), 8L)
        )
      )
    }
  }
}
