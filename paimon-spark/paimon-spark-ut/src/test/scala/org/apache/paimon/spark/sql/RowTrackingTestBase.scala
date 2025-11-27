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

abstract class RowTrackingTestBase extends PaimonSparkTestBase {

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Row Tracking: read row Tracking, bucket: $bucketEnable") {
          withTable("t") {
            sql(
              s"CREATE TABLE t (id INT, data STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO t VALUES (11, 'a'), (11, 'b')")
            sql("INSERT INTO t VALUES (22, 'a'), (22, 'b')")

            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t"),
              Seq(Row(11, "a", 0, 1), Row(11, "b", 1, 1), Row(22, "a", 2, 2), Row(22, "b", 3, 2))
            )
            checkAnswer(
              sql("SELECT _ROW_ID, data, _SEQUENCE_NUMBER, id FROM t"),
              Seq(Row(0, "a", 1, 11), Row(1, "b", 1, 11), Row(2, "a", 2, 22), Row(3, "b", 2, 22))
            )
          }
        }
      }
  }

  test("Row Tracking: compact table") {
    withTable("t") {
      sql(
        s"CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'compaction.min.file-num'='2')")

      batchInsert(Seq((1, 1), (2, 2), (3, 3)), "t")
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

  test("Row Tracking: compact bucket table") {
    withTable("t") {
      sql(
        s"CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'compaction.min.file-num'='2' ${bucketProperties(true)})")

      batchInsert(Seq((1, 1), (2, 2), (3, 3)), "t")
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
        Seq(Row(1, 1, 3, 3), Row(2, 2, 4, 3), Row(3, 3, 5, 4))
      )
    }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Row Tracking: delete table, bucket: $bucketEnable") {
          withTable("t") {
            sql(
              s"CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")

            batchInsert(Seq((1, 1), (2, 2), (3, 3)), "t")

            sql("DELETE FROM t WHERE id = 2")
            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
              Seq(Row(1, 1, 0, 1), Row(3, 3, 2, 3))
            )
            sql("DELETE FROM t WHERE _ROW_ID = 2")
            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
              Seq(Row(1, 1, 0, 1))
            )
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Row Tracking: update table, bucket: $bucketEnable") {
          withTable("t") {
            sql(
              s"CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")

            batchInsert(Seq((1, 1), (2, 2), (3, 3)), "t")
            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
              Seq(Row(1, 1, 0, 1), Row(2, 2, 1, 2), Row(3, 3, 2, 3))
            )

            sql("UPDATE t SET data = 22 WHERE id = 2")
            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
              Seq(Row(1, 1, 0, 1), Row(2, 22, 1, 4), Row(3, 3, 2, 3))
            )

            sql("UPDATE t SET data = 222 WHERE _ROW_ID = 1")
            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
              Seq(Row(1, 1, 0, 1), Row(2, 222, 1, 5), Row(3, 3, 2, 3))
            )
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Row Tracking: update table without condition, bucket: $bucketEnable") {
          withTable("t") {
            sql(
              s"CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")

            batchInsert(Seq((1, 1), (2, 2), (3, 3)), "t")

            sql("UPDATE t SET data = 22")
            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
              Seq(Row(1, 22, 0, 4), Row(2, 22, 1, 4), Row(3, 22, 2, 4))
            )
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Row Tracking: merge into table, bucket: $bucketEnable") {
          withTable("s", "t") {
            sql(
              s"CREATE TABLE s (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO s VALUES (1, 11), (2, 22)")

            sql(
              s"CREATE TABLE t (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO t VALUES(2, 2),(4, 4)")

            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
              Seq(Row(2, 2, 0, 1), Row(4, 4, 1, 1))
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
              Seq(Row(1, 11, 2, 2), Row(2, 22, 0, 2), Row(4, 4, 1, 1))
            )
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Row Tracking: merge into table with only insert, bucket:$bucketEnable") {
          withTable("s", "t") {
            sql(
              s"CREATE TABLE s (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO s VALUES (1, 11), (2, 22)")

            sql(
              s"CREATE TABLE t (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO t VALUES(2, 2),(4, 4)")

            sql("""
                  |MERGE INTO t
                  |USING s
                  |ON t.id = s.id
                  |WHEN NOT MATCHED THEN INSERT *
                  |""".stripMargin)
            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
              Seq(Row(1, 11, 2, 2), Row(2, 2, 0, 1), Row(4, 4, 1, 1))
            )
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Row Tracking: merge into table with only delete, bucket: $bucketEnable") {
          withTable("s", "t") {
            sql(
              s"CREATE TABLE s (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO s VALUES (1, 11), (2, 22)")

            sql(
              s"CREATE TABLE t (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO t VALUES(2, 2),(4, 4)")

            sql("""
                  |MERGE INTO t
                  |USING s
                  |ON t.id = s.id
                  |WHEN MATCHED THEN DELETE
                  |""".stripMargin)
            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
              Seq(Row(4, 4, 1, 1))
            )
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Row Tracking: merge into table with only update, bucket: $bucketEnable") {
          withTable("s", "t") {
            sql(
              s"CREATE TABLE s (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO s VALUES (1, 11), (2, 22)")

            sql(
              s"CREATE TABLE t (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO t VALUES(2, 2),(4, 4)")

            sql("""
                  |MERGE INTO t
                  |USING s
                  |ON t.id = s.id
                  |WHEN MATCHED THEN UPDATE SET *
                  |""".stripMargin)
            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
              Seq(Row(2, 22, 0, 2), Row(4, 4, 1, 1))
            )
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Data Evolution: insert into table with data-evolution, bucket: $bucketEnable") {
          withTable("s", "t") {
            sql("CREATE TABLE s (id INT, b INT)")
            sql("INSERT INTO s VALUES (1, 11), (2, 22)")

            sql(
              s"CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO t VALUES(2, 2, 2),(4, 4, 4)")

            sql("""
                  |MERGE INTO t
                  |USING s
                  |ON t.id = s.id
                  |WHEN NOT MATCHED THEN INSERT (id, b, c) VALUES (id, b, 11)
                  |""".stripMargin)

            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
              Seq(Row(1, 11, 11, 2, 3), Row(2, 2, 2, 0, 1), Row(4, 4, 4, 1, 2))
            )
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(
          s"Data Evolution: insert into table with data-evolution partial insert, bucket: $bucketEnable") {
          withTable("s", "t") {
            sql("CREATE TABLE s (id INT, b INT)")
            sql("INSERT INTO s VALUES (1, 11), (2, 22)")

            sql(
              s"CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true' ${bucketProperties(bucketEnable)})")
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
              Seq(
                Row(null, 11, null),
                Row(-1, 11, null),
                Row(2, 2, 2),
                Row(3, 3, 3),
                Row(3, null, 4))
            )
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Data Evolution: merge into table with data-evolution, bucket: $bucketEnable") {
          withTable("s", "t") {
            sql("CREATE TABLE s (id INT, b INT)")
            sql("INSERT INTO s VALUES (1, 11), (2, 22)")

            sql(
              s"CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO t VALUES (2, 2, 2),(4, 4, 4)")

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
              Seq(Row(1, 11, 11, 2, 2), Row(2, 22, 2, 0, 2), Row(4, 4, 4, 1, 2))
            )
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(
          s"Data Evolution: merge into table with data-evolution complex, bucket: $bucketEnable") {
          withTable("source", "target") {
            sql("CREATE TABLE source (id INT, b INT, c STRING)")
            batchInsert(
              Seq(
                (1, 100, "c11"),
                (3, 300, "c33"),
                (5, 500, "c55"),
                (7, 700, "c77"),
                (9, 900, "c99")),
              "source")

            sql(
              s"CREATE TABLE target (id INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            batchInsert(
              Seq((1, 10, "c1"), (2, 20, "c2"), (3, 30, "c3"), (4, 40, "c4"), (5, 50, "c5")),
              "target")

            sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.id = source.id
                   |WHEN MATCHED AND target.id = 5 THEN UPDATE SET b = source.b + target.b
                   |WHEN MATCHED AND source.c > 'c2' THEN UPDATE SET b = source.b, c = source.c
                   |WHEN NOT MATCHED AND c > 'c9' THEN INSERT (id, b, c) VALUES (id, b * 1.1, c)
                   |WHEN NOT MATCHED THEN INSERT *
                   |""".stripMargin)
            checkAnswer(
              sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM target ORDER BY id"),
              Seq(
                Row(1, 10, "c1", 0, 6),
                Row(2, 20, "c2", 1, 6),
                Row(3, 300, "c33", 2, 6),
                Row(4, 40, "c4", 3, 6),
                Row(5, 550, "c5", 4, 6),
                Row(7, 700, "c77", 5, 6),
                Row(9, 990, "c99", 6, 6))
            )
          }
        }
      }
  }

  test(
    "Data Evolution: merge into bucket table with data-evolution update bucket key throw exception") {
    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, b INT)")
      sql("INSERT INTO s VALUES (1, 11), (2, 22)")

      sql(
        "CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES (" +
          "'row-tracking.enabled' = 'true', " +
          "'data-evolution.enabled' = 'true', " +
          "'bucket'='2', " +
          "'bucket-key'='id')")

      assertThrows[RuntimeException] {
        sql("""
              |MERGE INTO t
              |USING s
              |ON t.id = s.id
              |WHEN MATCHED THEN UPDATE SET t.id = s.id
              |WHEN NOT MATCHED THEN INSERT (id, b, c) VALUES (id, b, 11)
              |""".stripMargin)
      }
    }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Data Evolution: update table throws exception, bucket: $bucketEnable") {
          withTable("t") {
            sql(
              s"CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true'  ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(2, 4)")
            assert(
              intercept[RuntimeException] {
                sql("UPDATE t SET b = 22")
              }.getMessage
                .contains("Update operation is not supported when data evolution is enabled yet."))
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Data Evolution: compact table throws exception, bucket:$bucketEnable") {
          withTable("t") {
            sql(
              s"CREATE TABLE t (id INT, b INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            for (i <- 1 to 6) {
              sql(s"INSERT INTO t VALUES ($i, $i)")
            }
            assert(
              intercept[RuntimeException] {
                sql("CALL sys.compact(table => 't')")
              }.getMessage
                .contains("Compact operation is not supported when data evolution is enabled yet."))
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Data Evolution: delete table throws exception, bucket: $bucketEnable") {
          withTable("t") {
            sql(
              s"CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true' ${bucketProperties(bucketEnable)})")
            sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id AS b, id AS c FROM range(2, 4)")
            assert(
              intercept[RuntimeException] {
                sql("DELETE FROM t WHERE id = 2")
              }.getMessage
                .contains("Delete operation is not supported when data evolution is enabled yet."))
          }
        }
      }
  }

  Seq(false, true).foreach {
    bucketEnable =>
      {
        test(s"Row Tracking: merge into table not matched by source, bucket: $bucketEnable") {
          if (gteqSpark3_4) {
            withTable("source", "target") {
              sql(
                s"CREATE TABLE source (id INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")
              batchInsert(
                Seq(
                  (1, 100, "c11"),
                  (3, 300, "c33"),
                  (5, 500, "c55"),
                  (7, 700, "c77"),
                  (9, 900, "c99")),
                "source")

              sql(
                s"CREATE TABLE target (id INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true' ${bucketProperties(bucketEnable)})")
              batchInsert(
                Seq((1, 10, "c1"), (2, 20, "c2"), (3, 30, "c3"), (4, 40, "c4"), (5, 50, "c5")),
                "target")

              sql(s"""
                     |MERGE INTO target
                     |USING source
                     |ON target.id = source.id
                     |WHEN MATCHED AND target.id = 5 THEN UPDATE SET b = source.b + target.b
                     |WHEN MATCHED AND source.c > 'c2' THEN UPDATE SET *
                     |WHEN NOT MATCHED AND c > 'c9' THEN INSERT (id, b, c) VALUES (id, b * 1.1, c)
                     |WHEN NOT MATCHED THEN INSERT *
                     |WHEN NOT MATCHED BY SOURCE AND id = 2 THEN UPDATE SET b = b * 10
                     |WHEN NOT MATCHED BY SOURCE THEN DELETE
                     |""".stripMargin)
              checkAnswer(
                sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM target ORDER BY id"),
                Seq(
                  Row(1, 10, "c1", 0, 1),
                  Row(2, 200, "c2", 1, 6),
                  Row(3, 300, "c33", 2, 6),
                  Row(5, 550, "c5", 4, 6),
                  Row(7, 700, "c77", 9, 6),
                  Row(9, 990, "c99", 10, 6))
              )
            }
          }
        }
      }
  }

  def bucketProperties(enableBucket: Boolean): String = {
    if (enableBucket) {
      ",'bucket'='2','bucket-key'='id'"
    } else {
      ""
    }
  }

  def formatValue(value: Any): String = {
    if (value == null) return "NULL"

    value match {
      case b: java.lang.Boolean => if (b.booleanValue()) "TRUE" else "FALSE"
      case b: Boolean => if (b) "TRUE" else "FALSE"

      case n: Byte => n.toString
      case n: Short => n.toString
      case n: Int => n.toString
      case n: Long => n.toString
      case n: Float => n.toString
      case n: Double => n.toString
      case n: BigInt => n.toString
      case n: BigDecimal => n.bigDecimal.toPlainString

      case s: String => s"'${escapeSingleQuotes(s)}'"
      case c: Char => s"'${escapeSingleQuotes(c.toString)}'"

      case other => s"'${escapeSingleQuotes(String.valueOf(other))}'"
    }
  }

  def batchInsert(rows: Seq[Product], table: String): String = {
    rows
      .map {
        p =>
          val values = p.productIterator.toList
          val args = values.map(formatValue).mkString(", ")
          sql(s"INSERT INTO $table VALUES ($args)")
      }
      .mkString("")
  }

  private def escapeSingleQuotes(s: String): String = s.replace("'", "''")

}
