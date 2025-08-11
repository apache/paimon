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

import org.apache.paimon.Snapshot.CommitKind
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

abstract class RowLineageTestBase extends PaimonSparkTestBase {

  test("Row Lineage: read row lineage") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, data STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO t VALUES (11, 'a'), (22, 'b')")

      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t"),
        Seq(Row(11, "a", 0, 1), Row(22, "b", 1, 1))
      )
      checkAnswer(
        sql("SELECT _ROW_ID, data, _SEQUENCE_NUMBER, id FROM t"),
        Seq(Row(0, "a", 1, 11), Row(1, "b", 1, 22))
      )
    }
  }

  test("Row Lineage: compact table") {
    withTable("t") {
      sql(
        "CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'compaction.min.file-num'='2')")

      sql("INSERT INTO t VALUES (1, 1)")
      sql("INSERT INTO t VALUES (2, 2)")
      sql("INSERT INTO t VALUES (3, 3)")
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 1, 0, 1), Row(2, 2, 1, 2), Row(3, 3, 2, 3))
      )

      sql("CALL sys.compact(table => 't')")
      val table = loadTable("t")
      assert(table.snapshotManager().latestSnapshot().commitKind().equals(CommitKind.COMPACT))
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 1, 0, 1), Row(2, 2, 1, 2), Row(3, 3, 2, 3))
      )
    }
  }

  test("Row Lineage: update table") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")

      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS data FROM range(1, 4)")
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 1, 0, 1), Row(2, 2, 1, 1), Row(3, 3, 2, 1))
      )

      sql("UPDATE t SET data = 22 WHERE id = 2")
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 1, 0, 1), Row(2, 22, 1, 2), Row(3, 3, 2, 1))
      )
    }
  }

  test("Row Lineage: update table without condition") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")

      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS data FROM range(1, 4)")

      sql("UPDATE t SET data = 22")
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 22, 0, 2), Row(2, 22, 1, 2), Row(3, 22, 2, 2))
      )
    }
  }

  test("Row Lineage: merge into table") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO s VALUES (1, 11), (2, 22)")

      sql("CREATE TABLE t (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b FROM range(2, 4)")
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(2, 2, 0, 1), Row(3, 3, 1, 1))
      )

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.b = s.b
            |WHEN NOT MATCHED THEN INSERT *
            |""".stripMargin)
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 11, 2, 2), Row(2, 22, 0, 2), Row(3, 3, 1, 1))
      )
    }
  }

  test("Row Lineage: merge into table with only insert") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO s VALUES (1, 11), (2, 22)")

      sql("CREATE TABLE t (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b FROM range(2, 4)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN NOT MATCHED THEN INSERT *
            |""".stripMargin)
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 11, 2, 2), Row(2, 2, 0, 1), Row(3, 3, 1, 1))
      )
    }
  }

  test("Row Lineage: merge into table with only delete") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO s VALUES (1, 11), (2, 22)")

      sql("CREATE TABLE t (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b FROM range(2, 4)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN DELETE
            |""".stripMargin)
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(3, 3, 1, 1))
      )
    }
  }

  test("Row Lineage: merge into table with only update") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO s VALUES (1, 11), (2, 22)")

      sql("CREATE TABLE t (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b FROM range(2, 4)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET *
            |""".stripMargin)
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(2, 22, 0, 2), Row(3, 3, 1, 1))
      )
    }
  }

  test("Row Lineage: merge into table not matched by source") {
    if (gteqSpark3_4) {
      withTable("source", "target") {
        sql(
          "CREATE TABLE source (a INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
        sql(
          "INSERT INTO source VALUES (1, 100, 'c11'), (3, 300, 'c33'), (5, 500, 'c55'), (7, 700, 'c77'), (9, 900, 'c99')")

        sql(
          "CREATE TABLE target (a INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
        sql(
          "INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2'), (3, 30, 'c3'), (4, 40, 'c4'), (5, 50, 'c5')")

        sql(s"""
               |MERGE INTO target
               |USING source
               |ON target.a = source.a
               |WHEN MATCHED AND target.a = 5 THEN UPDATE SET b = source.b + target.b
               |WHEN MATCHED AND source.c > 'c2' THEN UPDATE SET *
               |WHEN NOT MATCHED AND c > 'c9' THEN INSERT (a, b, c) VALUES (a, b * 1.1, c)
               |WHEN NOT MATCHED THEN INSERT *
               |WHEN NOT MATCHED BY SOURCE AND a = 2 THEN UPDATE SET b = b * 10
               |WHEN NOT MATCHED BY SOURCE THEN DELETE
               |""".stripMargin)
        checkAnswer(
          sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM target ORDER BY a"),
          Seq(
            Row(1, 10, "c1", 0, 1),
            Row(2, 200, "c2", 1, 2),
            Row(3, 300, "c33", 2, 2),
            Row(5, 550, "c5", 4, 2),
            Row(7, 700, "c77", 9, 2),
            Row(9, 990, "c99", 10, 2))
        )
      }
    }
  }
}
