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

import org.apache.paimon.Snapshot
import org.apache.paimon.deletionvectors.{BitmapDeletionVector, DeletionVector}
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.schema.PaimonMetadataColumn.{FILE_PATH_COLUMN, ROW_INDEX_COLUMN}
import org.apache.paimon.spark.write.{PaimonDeltaBatchWrite, PaimonDeltaWriteTaskResult, SerializedDeletionVector}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.util.Random

/**
 * Tests for the delta-based (`SupportsDelta` / `WriteDelta`) DELETE, UPDATE and MERGE INTO of
 * deletion-vector enabled unaware-bucket append tables.
 */
class DeltaRowLevelOpsTest extends PaimonSparkTestBase {

  private val v2WriteConf = "spark.paimon.write.use-v2-write" -> "true"

  private def explainContains(statement: String): String = {
    sql(s"EXPLAIN EXTENDED $statement").head().getString(0)
  }

  private def dataFilePaths(): Set[String] = {
    sql("SELECT file_path FROM `T$files`").collect().map(_.getString(0)).toSet
  }

  private def createDvAppendTable(extraProps: String = ""): Unit = {
    sql(s"""
           |CREATE TABLE T (id INT, v INT)
           |TBLPROPERTIES (
           | 'deletion-vectors.enabled' = 'true',
           | 'deletion-vectors.bitmap64' = '${Random.nextBoolean()}'
           | $extraProps
           |)
           |""".stripMargin)
  }

  test("Paimon delta ops: basic delete goes through WriteDelta") {
    withSparkSQLConf(v2WriteConf) {
      createDvAppendTable()
      // file 1
      sql("INSERT INTO T SELECT /*+ REPARTITION(1) */ id, id FROM range(1, 1001)")
      // file 2
      sql("INSERT INTO T SELECT /*+ REPARTITION(1) */ id, id FROM range(1001, 2001)")

      assert(explainContains("DELETE FROM T WHERE id <= 10").contains("WriteDelta"))

      val filesBefore = dataFilePaths()
      sql("DELETE FROM T WHERE id <= 10")
      checkAnswer(sql("SELECT COUNT(*), MIN(id) FROM T"), Row(1990, 11))
      // Delta delete marks rows in deletion vectors and must not rewrite data files.
      assert(dataFilePaths() === filesBefore)

      // Delete rows of both files, merging with the existing deletion vector of file 1.
      sql("DELETE FROM T WHERE id IN (11, 1001)")
      checkAnswer(sql("SELECT COUNT(*), MIN(id) FROM T"), Row(1988, 12))
      checkAnswer(sql("SELECT COUNT(*) FROM T WHERE id = 1001"), Row(0))
      assert(dataFilePaths() === filesBefore)

      // Delete the remaining rows.
      sql("DELETE FROM T WHERE id > 0")
      checkAnswer(sql("SELECT COUNT(*) FROM T"), Row(0))
    }
  }

  test("Paimon delta ops: delete with zero matched rows") {
    withSparkSQLConf(v2WriteConf) {
      createDvAppendTable()
      sql("INSERT INTO T VALUES (1, 1), (2, 2)")
      sql("DELETE FROM T WHERE id > 100")
      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1, 1) :: Row(2, 2) :: Nil)
    }
  }

  test("Paimon delta ops: delete on partitioned table") {
    withSparkSQLConf(v2WriteConf) {
      sql(s"""
             |CREATE TABLE T (id INT, v INT, pt STRING)
             |PARTITIONED BY (pt)
             |TBLPROPERTIES ('deletion-vectors.enabled' = 'true')
             |""".stripMargin)
      sql("INSERT INTO T VALUES (1, 1, 'a'), (2, 2, 'a'), (3, 3, 'b'), (4, 4, 'b')")

      // A row-level condition across partitions goes through WriteDelta.
      assert(explainContains("DELETE FROM T WHERE id % 2 = 0").contains("WriteDelta"))
      sql("DELETE FROM T WHERE id % 2 = 0")
      checkAnswer(sql("SELECT id, pt FROM T ORDER BY id"), Row(1, "a") :: Row(3, "b") :: Nil)

      // A partition-only (metadata only) condition falls back to the V1 command to drop whole
      // files instead of writing deletion vectors.
      assert(explainContains("DELETE FROM T WHERE pt = 'a'").contains("DeleteFromPaimonTable"))
      sql("DELETE FROM T WHERE pt = 'a'")
      checkAnswer(sql("SELECT id, pt FROM T"), Row(3, "b") :: Nil)
    }
  }

  test("Paimon delta ops: basic update goes through WriteDelta") {
    withSparkSQLConf(v2WriteConf) {
      createDvAppendTable()
      sql("INSERT INTO T SELECT /*+ REPARTITION(1) */ id, id FROM range(1, 1001)")

      assert(explainContains("UPDATE T SET v = v + 1 WHERE id <= 10").contains("WriteDelta"))

      val filesBefore = dataFilePaths()
      sql("UPDATE T SET v = v + 1 WHERE id <= 10")
      checkAnswer(sql("SELECT COUNT(*) FROM T"), Row(1000))
      checkAnswer(sql("SELECT SUM(v) FROM T"), Row(500510))
      checkAnswer(sql("SELECT v FROM T WHERE id = 1"), Row(2))
      // The old rows are marked in deletion vectors, the new versions are appended: the original
      // data files must remain untouched.
      assert(filesBefore.subsetOf(dataFilePaths()))

      // Update rows whose current version lives in the appended file, merging deletion vectors
      // with the previous update.
      sql("UPDATE T SET v = v + 1 WHERE id <= 5")
      checkAnswer(sql("SELECT SUM(v) FROM T"), Row(500515))
      checkAnswer(sql("SELECT v FROM T WHERE id = 1"), Row(3))

      // Zero matched rows.
      sql("UPDATE T SET v = 0 WHERE id > 10000")
      checkAnswer(sql("SELECT SUM(v) FROM T"), Row(500515))

      // NULL assignment.
      sql("UPDATE T SET v = NULL WHERE id = 1000")
      checkAnswer(sql("SELECT v FROM T WHERE id = 1000"), Row(null))
    }
  }

  test("Paimon delta ops: update moving rows across partitions") {
    withSparkSQLConf(v2WriteConf) {
      sql(s"""
             |CREATE TABLE T (id INT, pt STRING)
             |PARTITIONED BY (pt)
             |TBLPROPERTIES ('deletion-vectors.enabled' = 'true')
             |""".stripMargin)
      sql("INSERT INTO T VALUES (1, 'a'), (2, 'a'), (3, 'b')")

      sql("UPDATE T SET pt = 'b' WHERE id = 1")
      checkAnswer(
        sql("SELECT id, pt FROM T ORDER BY id"),
        Row(1, "b") :: Row(2, "a") :: Row(3, "b") :: Nil)
      checkAnswer(sql("SELECT COUNT(*) FROM T WHERE pt = 'a'"), Row(1))
    }
  }

  test("Paimon delta ops: merge into goes through WriteDelta") {
    withSparkSQLConf(v2WriteConf) {
      createDvAppendTable()
      sql("INSERT INTO T VALUES (1, 1), (2, 2), (3, 3), (4, 4)")

      val merge =
        """
          |MERGE INTO T
          |USING (SELECT * FROM VALUES (1, 10), (2, 20), (5, 50) AS s(id, v)) s
          |ON T.id = s.id
          |WHEN MATCHED AND s.id = 1 THEN UPDATE SET T.v = s.v
          |WHEN MATCHED AND s.id = 2 THEN DELETE
          |WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
          |""".stripMargin

      assert(explainContains(merge).contains("WriteDelta"))

      val filesBefore = dataFilePaths()
      sql(merge)
      checkAnswer(
        sql("SELECT id, v FROM T ORDER BY id"),
        Row(1, 10) :: Row(3, 3) :: Row(4, 4) :: Row(5, 50) :: Nil)
      assert(filesBefore.subsetOf(dataFilePaths()))
    }
  }

  test("Paimon delta ops: merge into with not matched by source") {
    withSparkSQLConf(v2WriteConf) {
      createDvAppendTable()
      sql("INSERT INTO T VALUES (1, 1), (2, 2), (3, 3)")

      sql("""
            |MERGE INTO T
            |USING (SELECT * FROM VALUES (1, 10) AS s(id, v)) s
            |ON T.id = s.id
            |WHEN MATCHED THEN UPDATE SET T.v = s.v
            |WHEN NOT MATCHED BY SOURCE AND T.id = 3 THEN DELETE
            |WHEN NOT MATCHED BY SOURCE THEN UPDATE SET T.v = -1
            |""".stripMargin)
      checkAnswer(sql("SELECT id, v FROM T ORDER BY id"), Row(1, 10) :: Row(2, -1) :: Nil)
    }
  }

  test("Paimon delta ops: merge into cardinality violation") {
    withSparkSQLConf(v2WriteConf) {
      createDvAppendTable()
      sql("INSERT INTO T VALUES (1, 1), (2, 2)")

      val e = intercept[Throwable] {
        sql("""
              |MERGE INTO T
              |USING (SELECT * FROM VALUES (1, 10), (1, 11) AS s(id, v)) s
              |ON T.id = s.id
              |WHEN MATCHED THEN UPDATE SET T.v = s.v
              |""".stripMargin)
      }
      def messages(t: Throwable): Seq[String] =
        Iterator.iterate(t)(_.getCause).takeWhile(_ != null).map(_.toString).toSeq
      assert(messages(e).exists(_.contains("MERGE_CARDINALITY_VIOLATION")))
    }
  }

  test("Paimon delta ops: concurrent deletion vector commits conflict") {
    withSparkSQLConf(v2WriteConf) {
      createDvAppendTable()
      sql("INSERT INTO T SELECT /*+ REPARTITION(1) */ id, id FROM range(0, 100)")

      // A delta write planned against the current snapshot, deleting row 0 of the file.
      val table = loadTable("T")
      val readSnapshotId = table.snapshotManager().latestSnapshot().id()
      val filePath = sql("SELECT __paimon_file_path FROM T LIMIT 1").head().getString(0)
      val dv = new BitmapDeletionVector()
      dv.delete(0L)
      val taskResult = PaimonDeltaWriteTaskResult(
        None,
        Array(SerializedDeletionVector(filePath, DeletionVector.serializeToBytes(dv))))

      // A concurrent DELETE commits a deletion vector for the same file in between.
      sql("DELETE FROM T WHERE id = 50")

      // Committing against the stale read snapshot must fail with a conflict instead of silently
      // merging with (or dropping) the concurrent deletion vector.
      val rowIdSchema = StructType(
        Seq(StructField(FILE_PATH_COLUMN, StringType), StructField(ROW_INDEX_COLUMN, LongType)))
      val batchWrite = new PaimonDeltaBatchWrite(
        table,
        new StructType(),
        rowIdSchema,
        Snapshot.Operation.DELETE,
        Some(readSnapshotId))
      val e = intercept[Throwable](batchWrite.commit(Array(taskResult)))
      def messages(t: Throwable): Seq[String] =
        Iterator.iterate(t)(_.getCause).takeWhile(_ != null).map(_.toString).toSeq
      assert(messages(e).exists(_.toLowerCase.contains("conflict")))

      // Only the concurrent DELETE applied; the failed commit is invisible.
      checkAnswer(sql("SELECT COUNT(*), MIN(id) FROM T"), Row(99, 0))
    }
  }

  test("Paimon delta ops: bucketed deletion-vector append table stays on V1") {
    withSparkSQLConf(v2WriteConf) {
      createDvAppendTable(", 'bucket-key' = 'id', 'bucket' = '2'")
      sql("INSERT INTO T VALUES (1, 1), (2, 2), (3, 3)")
      assert(explainContains("DELETE FROM T WHERE id = 1").contains("DeleteFromPaimonTable"))
      sql("DELETE FROM T WHERE id = 1")
      checkAnswer(sql("SELECT COUNT(*) FROM T"), Row(2))

      val updatePlan = explainContains("UPDATE T SET v = v + 1 WHERE id = 2")
      assert(!updatePlan.contains("WriteDelta") && !updatePlan.contains("ReplaceData"))
      sql("UPDATE T SET v = v + 1 WHERE id = 2")
      checkAnswer(sql("SELECT v FROM T WHERE id = 2"), Row(3))
    }
  }

  test("Paimon delta ops: append table without deletion vectors keeps copy-on-write") {
    withSparkSQLConf(v2WriteConf) {
      sql("CREATE TABLE T (id INT, v INT)")
      sql("INSERT INTO T VALUES (1, 1), (2, 2), (3, 3)")
      val plan = explainContains("DELETE FROM T WHERE id = 1")
      assert(plan.contains("ReplaceData") && !plan.contains("WriteDelta"))
      sql("DELETE FROM T WHERE id = 1")
      checkAnswer(sql("SELECT COUNT(*) FROM T"), Row(2))
    }
  }

  test("Paimon delta ops: falls back to V1 when v2 write is disabled") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "false") {
      createDvAppendTable()
      sql("INSERT INTO T VALUES (1, 1), (2, 2)")
      assert(explainContains("DELETE FROM T WHERE id = 1").contains("DeleteFromPaimonTable"))
      sql("DELETE FROM T WHERE id = 1")
      checkAnswer(sql("SELECT id FROM T"), Row(2))
    }
  }
}
