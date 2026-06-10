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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.schema.PaimonMetadataColumn

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.types.Metadata

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

class RowTrackingTest extends RowTrackingTestBase {

  import testImplicits._

  test("Row Tracking: metadata columns expose Spark preserve flags") {
    val rowIdMetadata = Metadata.fromJson(PaimonMetadataColumn.ROW_ID.metadataInJSON())
    assert(rowIdMetadata.getBoolean("__preserve_on_delete"))
    assert(rowIdMetadata.getBoolean("__preserve_on_update"))
    assert(!rowIdMetadata.getBoolean("__preserve_on_reinsert"))

    val sequenceNumberMetadata =
      Metadata.fromJson(PaimonMetadataColumn.SEQUENCE_NUMBER.metadataInJSON())
    assert(sequenceNumberMetadata.getBoolean("__preserve_on_delete"))
    assert(!sequenceNumberMetadata.getBoolean("__preserve_on_update"))
    assert(!sequenceNumberMetadata.getBoolean("__preserve_on_reinsert"))
  }

  test("Row Tracking: Spark 4.1 uses V2 copy-on-write for DML") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("s", "t") {
        sql("CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
        sql("INSERT INTO t VALUES (1, 1), (2, 2)")
        sql("INSERT INTO t VALUES (3, 3), (4, 4)")

        assertPlanContains("DELETE FROM t WHERE id = 2", "ReplaceData")
        sql("DELETE FROM t WHERE id = 2")

        assertPlanContains("UPDATE t SET data = 30 WHERE id = 3", "ReplaceData")
        sql("UPDATE t SET data = 30 WHERE id = 3")

        sql("CREATE TABLE s (id INT, data INT)")
        sql("INSERT INTO s VALUES (3, 300), (5, 500)")
        assertPlanContains(
          """
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET data = s.data
            |WHEN NOT MATCHED THEN INSERT *
            |""".stripMargin,
          "ReplaceData"
        )
        sql("""
              |MERGE INTO t
              |USING s
              |ON t.id = s.id
              |WHEN MATCHED THEN UPDATE SET data = s.data
              |WHEN NOT MATCHED THEN INSERT *
              |""".stripMargin)

        checkAnswer(
          sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
          Seq(Row(1, 1, 0, 1), Row(3, 300, 2, 5), Row(4, 4, 3, 2), Row(5, 500, 4, 5))
        )
      }
    }
  }

  test("Row Tracking: nested CHAR columns do not expose V2 row-level capability") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql("""
              |CREATE TABLE t (
              |  id INT,
              |  info STRUCT<name: CHAR(3), data: INT>
              |) TBLPROPERTIES ('row-tracking.enabled' = 'true')
              |""".stripMargin)

        assert(!SparkTable.of(loadTable("t")).isInstanceOf[SupportsRowLevelOperations])
      }
    }
  }

  test("Row Tracking: Spark 4.1 restores metadata-only delete fast path") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql("""
              |CREATE TABLE t (id INT, data INT, dt STRING)
              |PARTITIONED BY (dt)
              |TBLPROPERTIES ('row-tracking.enabled' = 'true')
              |""".stripMargin)
        sql("INSERT INTO t VALUES (1, 1, 'p1'), (2, 2, 'p1'), (3, 3, 'p2')")

        assertPlanContains("DELETE FROM t WHERE dt = 'p1'", "DeleteFromPaimonTableCommand")
        sql("DELETE FROM t WHERE dt = 'p1'")

        checkAnswer(
          sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
          Seq(Row(3, 3, "p2", 0, 1))
        )
      }
    }
  }

  test("Data Evolution: Spark 4.1 uses V2 copy-on-write for UPDATE") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql(
          "CREATE TABLE t (id INT, data INT) TBLPROPERTIES " +
            "('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
        // One file (= one row-id group) per INSERT, so the UPDATE below rewrites a group that
        // still contains carryover rows.
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ * FROM VALUES (1, 1), (2, 2)")
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ * FROM VALUES (3, 3), (4, 4)")

        assertPlanContains("UPDATE t SET data = 30 WHERE id = 3", "ReplaceData")
        sql("UPDATE t SET data = 30 WHERE id = 3")
        // Group [2, 3] is rewritten: both the updated row and the carryover row keep their
        // row ids and get the new snapshot's sequence number.
        checkAnswer(
          sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
          Seq(Row(1, 1, 0, 1), Row(2, 2, 1, 1), Row(3, 30, 2, 3), Row(4, 4, 3, 3))
        )
      }
    }
  }

  test("Data Evolution: DELETE remains unsupported") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql("""
              |CREATE TABLE t (id INT, data INT, dt STRING)
              |PARTITIONED BY (dt)
              |TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')
              |""".stripMargin)
        sql("INSERT INTO t VALUES (1, 1, 'p1'), (2, 2, 'p1'), (3, 3, 'p2')")

        assert(
          intercept[Exception] {
            sql("DELETE FROM t WHERE id = 2")
          }.getMessage
            .contains("Delete operation is not supported when data evolution is enabled yet"))
        assert(
          intercept[Exception] {
            sql("DELETE FROM t WHERE dt = 'p1'")
          }.getMessage
            .contains("Delete operation is not supported when data evolution is enabled yet"))
      }
    }
  }

  test("Data Evolution: V2 update rewrites whole row-id groups with patch files") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("s", "t") {
        sql(
          "CREATE TABLE t (id INT, data INT, name STRING) TBLPROPERTIES " +
            "('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
        sql("INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c')")

        // V1 data-evolution MERGE writes a partial-column patch file, so the row-id group now
        // spans multiple files.
        sql("CREATE TABLE s (id INT, data INT)")
        sql("INSERT INTO s VALUES (2, 20), (3, 30)")
        sql("""
              |MERGE INTO t
              |USING s
              |ON t.id = s.id
              |WHEN MATCHED THEN UPDATE SET data = s.data
              |""".stripMargin)

        // Multi-file groups must still serve __paimon_file_path: the assembled row reports one
        // member file of its row-id group (via the FileRecordIterator pass-through in
        // ForceSingleBatchReader / DataEvolutionFileReader).
        val filePaths = sql("SELECT __paimon_file_path FROM t").collect().map(_.getString(0))
        assert(filePaths.length == 3 && filePaths.forall(_ != null), filePaths.mkString(", "))

        // The predicate spans a patched column (data) and a base column (name), so the
        // matching scan must merge-read the group while also producing the file path used for
        // runtime group filtering.
        sql("UPDATE t SET name = 'bb' WHERE data = 20 AND name = 'b'")
        checkAnswer(
          sql("SELECT *, _ROW_ID FROM t ORDER BY id"),
          Seq(Row(1, 1, "a", 0), Row(2, 20, "bb", 1), Row(3, 30, "c", 2))
        )
      }
    }
  }

  test("Data Evolution: MERGE INTO keeps the V1 data-evolution command") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("s", "t") {
        sql(
          "CREATE TABLE t (id INT, data INT) TBLPROPERTIES " +
            "('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
        sql("INSERT INTO t VALUES (1, 1), (2, 2)")
        sql("CREATE TABLE s (id INT, data INT)")
        sql("INSERT INTO s VALUES (2, 200), (5, 500)")

        val mergeSql =
          """
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET data = s.data
            |WHEN NOT MATCHED THEN INSERT *
            |""".stripMargin
        val plan = explain(mergeSql)
        assert(!plan.contains("ReplaceData"), plan)
        assert(plan.contains("MergeIntoPaimonDataEvolutionTable"), plan)
        sql(mergeSql)

        checkAnswer(
          sql("SELECT *, _ROW_ID FROM t ORDER BY id"),
          Seq(Row(1, 1, 0), Row(2, 200, 1), Row(5, 500, 2))
        )
      }
    }
  }

  test("Data Evolution: V2 UPDATE works with commit.force-compact") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        // Guards the run writer's assumption that the underlying append writer never
        // restores or compacts existing files, under the config most likely to break it.
        sql(
          "CREATE TABLE t (id INT, data INT) TBLPROPERTIES " +
            "('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true', " +
            "'commit.force-compact' = 'true')")
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ * FROM VALUES (1, 1), (2, 2)")
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ * FROM VALUES (3, 3), (4, 4)")

        sql("UPDATE t SET data = 30 WHERE id = 3")
        checkAnswer(
          sql("SELECT *, _ROW_ID FROM t ORDER BY id"),
          Seq(Row(1, 1, 0), Row(2, 2, 1), Row(3, 30, 2), Row(4, 4, 3))
        )
      }
    }
  }

  test("Data Evolution: V2 UPDATE works with spillable append write buffer") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql("""
              |CREATE TABLE t (id INT, data INT, payload STRING) TBLPROPERTIES (
              |  'row-tracking.enabled' = 'true',
              |  'data-evolution.enabled' = 'true',
              |  'write-buffer-for-append' = 'true',
              |  'write-buffer-size' = '4 kb',
              |  'page-size' = '1 kb')
              |""".stripMargin)
        sql("""
              |INSERT INTO t
              |SELECT /*+ REPARTITION(1) */ CAST(id AS INT), CAST(id AS INT), repeat('x', 256)
              |FROM range(0, 20)
              |""".stripMargin)

        sql("UPDATE t SET data = data + 100 WHERE id >= 5 AND id < 15")
        checkAnswer(
          sql("SELECT id, data, _ROW_ID FROM t ORDER BY id"),
          Seq.tabulate(20)(i => Row(i, if (i >= 5 && i < 15) i + 100 else i, i))
        )
      }
    }
  }

  test("Data Evolution: partition column update is rejected") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql("""
              |CREATE TABLE t (id INT, data INT, dt STRING)
              |PARTITIONED BY (dt)
              |TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')
              |""".stripMargin)
        sql("INSERT INTO t VALUES (1, 1, 'p1'), (2, 2, 'p2')")

        assert(
          intercept[Exception] {
            sql("UPDATE t SET dt = 'p3' WHERE id = 1")
          }.getMessage
            .contains("Update to partition columns is not supported for data evolution tables"))

        // Non-partition columns of a partitioned data-evolution table stay updatable.
        sql("UPDATE t SET data = 10 WHERE id = 1")
        checkAnswer(sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, 10, "p1"), Row(2, 2, "p2")))
      }
    }
  }

  test("Data Evolution: concurrent V1 merge patch and V2 update detect row-id conflicts") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql(
          "CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES " +
            "('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ * FROM VALUES (1, 0, 0), (2, 0, 0)")
        Seq((1, 1), (2, 1)).toDF("id", "c").createOrReplaceTempView("s")

        def hasConflictMessage(e: Throwable): Boolean = {
          Iterator
            .iterate(e)(_.getCause)
            .takeWhile(_ != null)
            .exists(t => t.getMessage != null && t.getMessage.toLowerCase.contains("conflict"))
        }

        def retryOnConflict(action: => Unit): Unit = {
          var success = false
          while (!success) {
            try {
              action
              success = true
            } catch {
              case e: Exception if hasConflictMessage(e) => // retry
            }
          }
        }

        // The V1 MERGE only ADDs partial-column patch files while the V2 UPDATE rewrites whole
        // row-id groups; without the scan-snapshot row-id conflict check on the V2 commit, an
        // interleaved patch would silently survive next to the rewritten files and corrupt the
        // group, losing increments below.
        val mergeFuture = Future {
          for (_ <- 1 to 5) {
            retryOnConflict {
              sql("""
                    |MERGE INTO t
                    |USING s
                    |ON t.id = s.id
                    |WHEN MATCHED THEN UPDATE SET t.c = t.c + s.c
                    |""".stripMargin).collect()
            }
          }
        }
        val updateFuture = Future {
          for (_ <- 1 to 5) {
            retryOnConflict {
              sql("UPDATE t SET b = b + 1").collect()
            }
          }
        }
        Await.result(mergeFuture, 180.seconds)
        Await.result(updateFuture, 180.seconds)

        checkAnswer(sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, 5, 5), Row(2, 5, 5)))
      }
    }
  }

  test("Data Evolution: BLOB tables do not expose V2 row-level capability") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql(
          "CREATE TABLE t (id INT, data STRING, picture BINARY) TBLPROPERTIES " +
            "('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true', " +
            "'blob-field' = 'picture')")
        assert(!SparkTable.of(loadTable("t")).isInstanceOf[SupportsRowLevelOperations])
      }
    }
  }

  private def assertPlanContains(sqlText: String, fragment: String): Unit = {
    val plan = explain(sqlText)
    assert(plan.contains(fragment), plan)
  }

  private def explain(sqlText: String): String = {
    sql(s"EXPLAIN EXTENDED $sqlText").collect().map(_.getString(0)).mkString("\n")
  }
}
