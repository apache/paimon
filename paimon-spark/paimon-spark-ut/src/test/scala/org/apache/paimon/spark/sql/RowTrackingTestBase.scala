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
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, Join, LogicalPlan, MergeRows, RepartitionByExpression, Sort}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.{CountDownLatch, TimeUnit}

abstract class RowTrackingTestBase extends PaimonSparkTestBase {

  test("Row Tracking: read row Tracking") {
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

  test("Row Tracking: compact table") {
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

  test("Row Tracking: delete table") {
    withTable("t") {
      // only enable row tracking
      sql("CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      runAndCheckAnswer()
      sql("DROP TABLE t")

      // enable row tracking and deletion vectors
      sql(
        "CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'deletion-vectors.enabled' = 'true')")
      runAndCheckAnswer()

      def runAndCheckAnswer(): Unit = {
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS data FROM range(1, 4)")
        sql("DELETE FROM t WHERE id = 2")
        checkAnswer(
          sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
          Seq(Row(1, 1, 0, 1), Row(3, 3, 2, 1))
        )
        sql("DELETE FROM t WHERE _ROW_ID = 2")
        checkAnswer(
          sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
          Seq(Row(1, 1, 0, 1))
        )
      }
    }
  }

  test("Row Tracking: update table") {
    withTable("t") {
      // only enable row tracking
      sql("CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      runAndCheckAnswer()
      sql("DROP TABLE t")

      // enable row tracking and deletion vectors
      sql(
        "CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'deletion-vectors.enabled' = 'true')")
      runAndCheckAnswer()

      def runAndCheckAnswer(): Unit = {
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

        sql("UPDATE t SET data = 222 WHERE _ROW_ID = 1")
        checkAnswer(
          sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
          Seq(Row(1, 1, 0, 1), Row(2, 222, 1, 3), Row(3, 3, 2, 1))
        )
      }
    }
  }

  test("Row Tracking: update table without condition") {
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

  test("Row Tracking: update") {
    withTable("s", "t") {
      spark.sql("CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      spark.sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS data FROM range(1, 4)")

      spark.sql("UPDATE t SET data = 22 WHERE id = 2")

      spark.sql("INSERT INTO t VALUES (4, 4), (5, 5)")

      checkAnswer(
        spark.sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t"),
        Seq(Row(1, 1, 0, 1), Row(2, 22, 1, 2), Row(3, 3, 2, 1), Row(4, 4, 3, 3), Row(5, 5, 4, 3))
      )
    }
  }

  test("Row Tracking: merge into table") {
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

  test("Row Tracking: merge into table with only insert") {
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

  test("Row Tracking: merge into table with only delete") {
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

  test("Row Tracking: merge into table with only update") {
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

  test("Data Evolution: insert into table with data-evolution") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, b INT)")
      sql("INSERT INTO s VALUES (1, 11), (2, 22)")

      sql(
        "CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(2, 4)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN NOT MATCHED THEN INSERT (id, b, c) VALUES (id, b, 11)
            |""".stripMargin)

      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 11, 11, 2, 2), Row(2, 2, 2, 0, 1), Row(3, 3, 3, 1, 1))
      )
    }
  }

  test("Data Evolution: insert into table with data-evolution partial insert") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, b INT)")
      sql("INSERT INTO s VALUES (1, 11), (2, 22)")

      sql(
        "CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(2, 4)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN NOT MATCHED THEN INSERT (id, b) VALUES (-1, b)
            |""".stripMargin)

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN NOT MATCHED THEN INSERT (b) VALUES (b)
            |""".stripMargin)

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN NOT MATCHED THEN INSERT (id, c) VALUES (3, 4)
            |""".stripMargin)

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(null, 11, null), Row(-1, 11, null), Row(2, 2, 2), Row(3, 3, 3), Row(3, null, 4))
      )
    }
  }

  test("Data Evolution: merge into table with data-evolution") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, b INT)")
      sql("INSERT INTO s VALUES (1, 11), (2, 22)")

      sql(
        "CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(2, 4)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.b = s.b
            |WHEN NOT MATCHED THEN INSERT (id, b, c) VALUES (id, b, 11)
            |""".stripMargin)
      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(3)))
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 11, 11, 2, 2), Row(2, 22, 2, 0, 2), Row(3, 3, 3, 1, 2))
      )
    }
  }

  test("Data Evolution: merge into table with data-evolution complex") {
    withTable("source", "target") {
      sql("CREATE TABLE source (a INT, b INT, c STRING)")
      sql(
        "INSERT INTO source VALUES (1, 100, 'c11'), (3, 300, 'c33'), (5, 500, 'c55'), (7, 700, 'c77'), (9, 900, 'c99')")

      sql(
        "CREATE TABLE target (a INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
      sql(
        "INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2'), (3, 30, 'c3'), (4, 40, 'c4'), (5, 50, 'c5')")

      sql(s"""
             |MERGE INTO target
             |USING source
             |ON target.a = source.a
             |WHEN MATCHED AND target.a = 5 THEN UPDATE SET b = source.b + target.b
             |WHEN MATCHED AND source.c > 'c2' THEN UPDATE SET b = source.b, c = source.c
             |WHEN NOT MATCHED AND c > 'c9' THEN INSERT (a, b, c) VALUES (a, b * 1.1, c)
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin)
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM target ORDER BY a"),
        Seq(
          Row(1, 10, "c1", 0, 2),
          Row(2, 20, "c2", 1, 2),
          Row(3, 300, "c33", 2, 2),
          Row(4, 40, "c4", 3, 2),
          Row(5, 550, "c5", 4, 2),
          Row(7, 700, "c77", 5, 2),
          Row(9, 990, "c99", 6, 2))
      )
    }
  }

  test("Data Evolution: merge into table with data-evolution on _ROW_ID") {
    withTable("source", "target") {
      sql(
        "CREATE TABLE source (a INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
      sql(
        "INSERT INTO source VALUES (1, 100, 'c11'), (3, 300, 'c33'), (5, 500, 'c55'), (7, 700, 'c77'), (9, 900, 'c99')")

      sql(
        "CREATE TABLE target (a INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
      sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2'), (3, 30, 'c3')")

      sql(s"""
             |MERGE INTO target
             |USING source
             |ON target._ROW_ID = source._ROW_ID
             |WHEN MATCHED AND target.a = 2 THEN UPDATE SET b = source.b + target.b
             |WHEN MATCHED AND source.c > 'c2' THEN UPDATE SET b = source.b, c = source.c
             |WHEN NOT MATCHED AND c > 'c9' THEN INSERT (a, b, c) VALUES (a, b * 1.1, c)
             |WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (a, b, c)
             |""".stripMargin)

      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM target ORDER BY a"),
        Seq(
          Row(1, 10, "c1", 0, 2),
          Row(2, 320, "c2", 1, 2),
          Row(3, 500, "c55", 2, 2),
          Row(7, 700, "c77", 3, 2),
          Row(9, 990, "c99", 4, 2))
      )
    }
  }

  test("Data Evolution: merge into table with data-evolution with _ROW_ID shortcut") {
    withTable("source", "target") {
      sql("CREATE TABLE source (target_ROW_ID BIGINT, b INT, c STRING)")
      sql(
        "INSERT INTO source VALUES (0, 100, 'c11'), (2, 300, 'c33'), (4, 500, 'c55'), (6, 700, 'c77'), (8, 900, 'c99')")

      sql(
        "CREATE TABLE target (a INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
      sql(
        "INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2'), (3, 30, 'c3'), (4, 40, 'c4'), (5, 50, 'c5')")

      var findSplitsPlan: LogicalPlan = null
      val latch = new CountDownLatch(1)
      val listener = new QueryExecutionListener {
        override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
          if (qe.analyzed.collectFirst { case _: Deduplicate => true }.nonEmpty) {
            latch.countDown()
            findSplitsPlan = qe.analyzed
          }
        }
        override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
          if (qe.analyzed.collectFirst { case _: Deduplicate => true }.nonEmpty) {
            latch.countDown()
            findSplitsPlan = qe.analyzed
          }
        }
      }
      spark.listenerManager.register(listener)
      sql(s"""
             |MERGE INTO target
             |USING source
             |ON target._ROW_ID = source.target_ROW_ID
             |WHEN MATCHED AND target.a = 5 THEN UPDATE SET b = source.b + target.b
             |WHEN MATCHED AND source.c > 'c2' THEN UPDATE SET b = source.b, c = source.c
             |WHEN NOT MATCHED AND c > 'c9' THEN INSERT (a, b, c) VALUES (target_ROW_ID, b * 1.1, c)
             |WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (target_ROW_ID, b, c)
             |""".stripMargin)
      assert(latch.await(10, TimeUnit.SECONDS), "await timeout")
      // Assert that no Join operator was used during
      // `org.apache.paimon.spark.commands.MergeIntoPaimonDataEvolutionTable.targetRelatedSplits`
      assert(findSplitsPlan != null && findSplitsPlan.collect { case plan: Join => plan }.isEmpty)
      spark.listenerManager.unregister(listener)

      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM target ORDER BY a"),
        Seq(
          Row(1, 10, "c1", 0, 2),
          Row(2, 20, "c2", 1, 2),
          Row(3, 300, "c33", 2, 2),
          Row(4, 40, "c4", 3, 2),
          Row(5, 550, "c5", 4, 2),
          Row(6, 700, "c77", 5, 2),
          Row(8, 990, "c99", 6, 2))
      )
    }
  }

  test("Data Evolution: merge into table with data-evolution for Self-Merge with _ROW_ID shortcut") {
    withTable("target") {
      sql(
        "CREATE TABLE target (a INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
      sql(
        "INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2'), (3, 30, 'c3'), (4, 40, 'c4'), (5, 50, 'c5')")

      var updatePlan: LogicalPlan = null
      val latch = new CountDownLatch(1)
      val listener = new QueryExecutionListener {
        override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
          if (qe.analyzed.collectFirst { case _: MergeRows => true }.nonEmpty) {
            latch.countDown()
            updatePlan = qe.analyzed
          }
        }
        override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
          if (qe.analyzed.collectFirst { case _: MergeRows => true }.nonEmpty) {
            latch.countDown()
            updatePlan = qe.analyzed
          }
        }
      }
      spark.listenerManager.register(listener)
      sql(s"""
             |MERGE INTO target
             |USING target AS source
             |ON target._ROW_ID = source._ROW_ID
             |WHEN MATCHED AND target.a = 5 THEN UPDATE SET b = source.b + target.b
             |WHEN MATCHED AND source.c > 'c2' THEN UPDATE SET b = source.b * 3,
             |c = concat(target.c, source.c)
             |""".stripMargin).collect()
      assert(latch.await(10, TimeUnit.SECONDS), "await timeout")
      // Assert no shuffle/join/sort was used in
      // 'org.apache.paimon.spark.commands.MergeIntoPaimonDataEvolutionTable.updateActionInvoke'
      assert(
        updatePlan != null &&
          updatePlan.collectFirst {
            case p: Join => p
            case p: Sort => p
            case p: RepartitionByExpression => p
          }.isEmpty,
        s"Found unexpected Join/Sort/Exchange in plan: $updatePlan"
      )
      spark.listenerManager.unregister(listener)

      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM target ORDER BY a"),
        Seq(
          Row(1, 10, "c1", 0, 2),
          Row(2, 20, "c2", 1, 2),
          Row(3, 90, "c3c3", 2, 2),
          Row(4, 120, "c4c4", 3, 2),
          Row(5, 100, "c5", 4, 2))
      )
    }
  }

  test("Data Evolution: update table throws exception") {
    withTable("t") {
      sql(
        "CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(2, 4)")
      assert(
        intercept[RuntimeException] {
          sql("UPDATE t SET b = 22")
        }.getMessage
          .contains("Update operation is not supported when data evolution is enabled yet."))
    }
  }

  test("Data Evolution: delete table throws exception") {
    withTable("t") {
      sql(
        "CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(2, 4)")
      assert(
        intercept[RuntimeException] {
          sql("DELETE FROM t WHERE id = 2")
        }.getMessage
          .contains("Delete operation is not supported when data evolution is enabled yet."))
    }
  }

  test("Row Tracking: merge into table not matched by source") {
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
            Row(7, 700, "c77", 5, 2),
            Row(9, 990, "c99", 6, 2))
        )
      }
    }
  }

  test("Data Evolution: compact fields action") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, b INT)")
      sql("INSERT INTO s VALUES (1, 11), (2, 22)")

      sql(
        "CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true', 'compaction.min.file-num'='2')")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(2, 4)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.b = s.b
            |WHEN NOT MATCHED THEN INSERT (id, b, c) VALUES (id, b, 111)
            |""".stripMargin)
      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(3)))
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 11, 111, 2, 2), Row(2, 22, 2, 0, 2), Row(3, 3, 3, 1, 2))
      )
      checkAnswer(
        sql("SELECT count(*) FROM `t$files`"),
        Seq(Row(3))
      )
      sql("CALL paimon.sys.compact(table => 't')")
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 11, 111, 2, 3), Row(2, 22, 2, 0, 3), Row(3, 3, 3, 1, 3))
      )

      checkAnswer(
        sql("SELECT count(*) FROM `t$files`"),
        Seq(Row(1))
      )
    }
  }
}
