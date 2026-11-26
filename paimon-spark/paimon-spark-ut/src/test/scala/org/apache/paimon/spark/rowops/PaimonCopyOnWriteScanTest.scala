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

package org.apache.paimon.spark.rowops

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.schema.PaimonMetadataColumn.FILE_PATH_COLUMN
import org.apache.paimon.table.{FileStoreTable, FileStoreTableFactory}

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.unsafe.types.UTF8String

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

object PaimonCopyOnWriteScanTest {
  // Called by a UDF in a DML condition. The first call comes from the runtime group filter
  // subquery, so its action commits after that subquery read the table and before the
  // copy-on-write scan reloads its splits.
  private val fired = new AtomicBoolean(false)
  @volatile private var action: () => Unit = () => ()

  def arm(onFirstCall: () => Unit): Unit = {
    action = onFirstCall
    fired.set(false)
  }

  def disarm(): Unit = action = () => ()

  def hasFired: Boolean = fired.get()

  def fireOnce(): Boolean = {
    if (fired.compareAndSet(false, true)) action()
    true
  }
}

class PaimonCopyOnWriteScanTest extends PaimonSparkTestBase {

  private def createAppendTable(): Unit =
    sql("CREATE TABLE T (id INT, name STRING) TBLPROPERTIES ('bucket' = '1', 'bucket-key' = 'id')")

  private def createAppendTableWithFourFiles(): FileStoreTable = {
    createAppendTable()
    // A full compaction of an append bucket needs at least three files.
    (1 to 4).foreach(i => sql(s"INSERT INTO T VALUES ($i, 'v$i')"))
    loadTable("T")
  }

  private def fullCompact(table: FileStoreTable): Unit = {
    val builder = table.newBatchWriteBuilder()
    val write = builder.newWrite()
    val commit = builder.newCommit()
    try {
      write.compact(BinaryRow.EMPTY_ROW, 0, true)
      commit.commit(write.prepareCommit())
    } finally {
      write.close()
      commit.close()
    }
  }

  private def loadedScan(table: FileStoreTable): PaimonCopyOnWriteScan = {
    val scan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
    scan.inputSplits
    scan
  }

  private def fileNames(scan: PaimonCopyOnWriteScan): Set[String] =
    scan.scannedFiles.map(_.dataFileMeta.fileName()).toSet

  /** The runtime group filter Spark pushes into the scan: `__paimon_file_path IN (paths)`. */
  private def filePathIn(scan: PaimonCopyOnWriteScan): Array[Predicate] = {
    // Runtime filter literals carry Spark's internal string type.
    val values =
      scan.scannedFiles.map(file => Expressions.literal(UTF8String.fromString(file.filePath())))
    Array(new Predicate("IN", (Expressions.column(FILE_PATH_COLUMN) +: values).toArray[Expression]))
  }

  Seq(
    "DELETE" -> "DELETE FROM T WHERE id = 1 AND cow_commit_hook(id)",
    "UPDATE" -> "UPDATE T SET name = 'x' WHERE id = 1 AND cow_commit_hook(id)",
    // A subquery makes Spark plan the UPDATE as a union of two scans sharing this scan.
    "UPDATE with a subquery" ->
      ("UPDATE T SET name = 'x' WHERE id = 1 AND cow_commit_hook(id) " +
        "AND id IN (SELECT 1 AS id)"),
    "MERGE" -> ("MERGE INTO T USING (SELECT 1 AS id) s ON T.id = s.id AND cow_commit_hook(T.id) " +
      "WHEN MATCHED THEN DELETE")
  ).foreach {
    case (command, statement) =>
      test(
        s"copy-on-write $command conflicts with a compaction committed during its group filter") {
        spark.udf.register("cow_commit_hook", (_: Int) => PaimonCopyOnWriteScanTest.fireOnce())
        withTable("T") {
          val table = createAppendTableWithFourFiles()
          withSparkSQLConf(
            "spark.sql.optimizer.runtime.rowLevelOperationGroupFilter.enabled" -> "true") {
            assertGroupFilterPlanned(statement)
            PaimonCopyOnWriteScanTest.arm(() => fullCompact(table))
            try {
              val error = intercept[Exception](sql(statement))
              assert(
                hasMessage(error, "File deletion conflicts detected"),
                s"Unexpected error: $error")
            } finally {
              PaimonCopyOnWriteScanTest.disarm()
            }
          }
          assert(PaimonCopyOnWriteScanTest.hasFired, "The group filter never called the hook")
          val kinds =
            loadTable("T").snapshotManager().snapshots().asScala.map(_.commitKind().name).toList
          assert(kinds == Seq("APPEND", "APPEND", "APPEND", "APPEND", "COMPACT"))
          checkAnswer(sql("SELECT id, name FROM T"), (1 to 4).map(i => Row(i, s"v$i")))
        }
      }
  }

  test("copy-on-write scan reads the snapshot it was built on") {
    for (withRuntimeFilter <- Seq(false, true)) {
      withTable("T") {
        val table = createAppendTableWithFourFiles()
        val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
        val groupFilterScan = loadedScan(table)
        fullCompact(table)

        if (withRuntimeFilter) cowScan.filter(filePathIn(groupFilterScan))
        cowScan.inputSplits

        assert(fileNames(cowScan) == fileNames(groupFilterScan), s"filter=$withRuntimeFilter")
      }
    }
  }

  test("copy-on-write scan rejects a runtime filter on files of a newer snapshot") {
    withTable("T") {
      val table = createAppendTableWithFourFiles()
      val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
      fullCompact(table)
      val groupFilterScan = loadedScan(loadTable("T"))

      cowScan.filter(filePathIn(groupFilterScan))
      val error = intercept[IllegalStateException](cowScan.inputSplits)
      assert(error.getMessage.contains("concurrent commit"), error.getMessage)
    }
  }

  test("copy-on-write DELETE accepts a group filter that selects fallback branch files") {
    withTable("T") {
      sql("CREATE TABLE T (dt STRING, name STRING, amount BIGINT) PARTITIONED BY (dt)")
      sql("CALL sys.create_branch('test.T', 'fallback')")
      sql("ALTER TABLE T SET TBLPROPERTIES ('scan.fallback-branch' = 'fallback')")
      sql("INSERT INTO `T$branch_fallback` VALUES ('20240726', 'cherry', 3)")
      sql("INSERT INTO T VALUES ('20240725', 'apple', 5), ('20240725', 'banana', 7)")

      withSparkSQLConf(
        "spark.sql.optimizer.runtime.rowLevelOperationGroupFilter.enabled" -> "true") {
        assertGroupFilterPlanned("DELETE FROM T WHERE amount > 0")
        sql("DELETE FROM T WHERE amount > 0")
      }

      val kinds =
        loadTable("T").snapshotManager().snapshots().asScala.map(_.commitKind().name).toList
      assert(kinds == Seq("APPEND", "OVERWRITE"))
      // Row-level operations change the main branch only, so the fallback branch row stays.
      checkAnswer(sql("SELECT * FROM T"), Seq(Row("20240726", "cherry", 3L)))
    }
  }

  test("copy-on-write scan accepts a runtime filter on files appended after its snapshot") {
    withTable("T") {
      sql(
        "CREATE TABLE T (id INT, name STRING) TBLPROPERTIES " +
          "('bucket' = '1', 'bucket-key' = 'id', 'write-only' = 'true')")
      Seq(1, 2).foreach(i => sql(s"INSERT INTO T VALUES ($i, 'v$i')"))
      val table = loadTable("T")
      val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
      val initialFiles = fileNames(loadedScan(table))
      sql("INSERT INTO T VALUES (3, 'v3')")
      val groupFilterScan = loadedScan(loadTable("T"))

      cowScan.filter(filePathIn(groupFilterScan))
      cowScan.inputSplits

      assert(fileNames(cowScan) == initialFiles)
    }
  }

  test("copy-on-write scan rejects a runtime filter after its snapshot id was reused") {
    withTable("T") {
      val table = createAppendTableWithFourFiles()
      val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
      // Another writer, without this session's snapshot cache, rolls back and compacts, so the
      // compaction commits a different snapshot under the id this scan read.
      val other = FileStoreTableFactory.create(fileIO, table.location())
      other.rollbackTo(3)
      fullCompact(other)
      val groupFilterScan = loadedScan(FileStoreTableFactory.create(fileIO, table.location()))
      assert(other.snapshotManager().latestSnapshotId() == 4L)

      cowScan.filter(filePathIn(groupFilterScan))
      val error = intercept[IllegalStateException](cowScan.inputSplits)
      assert(error.getMessage.contains("concurrent commit"), error.getMessage)
    }
  }

  test("copy-on-write scan built on an empty table reads nothing") {
    for (withRuntimeFilter <- Seq(false, true)) {
      withTable("T") {
        createAppendTable()
        val cowScan = PaimonCopyOnWriteScan(loadTable("T"), spark.table("T").schema)
        sql("INSERT INTO T VALUES (1, 'v1')")

        if (withRuntimeFilter) cowScan.filter(filePathIn(loadedScan(loadTable("T"))))

        assert(cowScan.inputSplits.isEmpty, s"filter=$withRuntimeFilter")
      }
    }
  }

  test("copy-on-write DELETE fails when the group filter reads an older snapshot") {
    withTable("T") {
      val table = createAppendTableWithFourFiles()
      fullCompact(table)

      // The group filter reads snapshot 4, whose file of id = 1 was compacted away.
      withSparkSQLConf(
        "spark.paimon.scan.snapshot-id" -> "4",
        "spark.sql.optimizer.runtime.rowLevelOperationGroupFilter.enabled" -> "true") {
        assertGroupFilterPlanned("DELETE FROM T WHERE id = 1")
        val error = intercept[Exception](sql("DELETE FROM T WHERE id = 1"))
        assert(hasMessage(error, "cannot tell whether a commit replaced"), s"Unexpected: $error")
      }

      checkAnswer(sql("SELECT id, name FROM T"), (1 to 4).map(i => Row(i, s"v$i")))
    }
  }

  /** The copy-on-write plan of Spark 3.5+ runs the group filter as a dynamic pruning subquery. */
  private def assertGroupFilterPlanned(statement: String): Unit = {
    val plan = sql(s"EXPLAIN $statement").collect().head.getString(0)
    assert(plan.contains("dynamicpruningexpression"), plan)
  }

  private def hasMessage(throwable: Throwable, expected: String): Boolean =
    Iterator
      .iterate(throwable)(_.getCause)
      .takeWhile(_ != null)
      .exists(t => Option(t.getMessage).exists(_.contains(expected)))

  test("getInputSplits should cache dataSplits and not trigger redundant loadSplits") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, name STRING) USING PAIMON
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')
            |""".stripMargin)
      sql("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      val table = loadTable("T")
      val schema = spark.table("T").schema

      // Create COW scan and load initial splits (snapshot 1)
      val cowScan = PaimonCopyOnWriteScan(table, schema)
      cowScan.inputSplits
      assert(cowScan.dataSplits.nonEmpty, "Initial dataSplits should not be empty")

      val initialSplitCount = cowScan.dataSplits.length
      val initialScannedFiles = cowScan.scannedFiles

      // Insert more data to create a new snapshot (simulates concurrent commit)
      sql("INSERT INTO T VALUES (4, 'd'), (5, 'e')")

      // Call inputSplits again (e.g., from reportDriverMetrics) - this should NOT trigger
      // loadSplits() and should NOT overwrite dataSplits due to caching in getInputSplits
      val metrics = cowScan.reportDriverMetrics()

      // Verify dataSplits was NOT overwritten by the new snapshot
      assert(
        cowScan.dataSplits.length == initialSplitCount,
        s"getInputSplits cache should prevent overwriting dataSplits. " +
          s"Expected $initialSplitCount splits, got ${cowScan.dataSplits.length}"
      )

      // Verify scannedFiles still returns the original files
      assert(
        cowScan.scannedFiles == initialScannedFiles,
        "scannedFiles should return the same files after reportDriverMetrics()"
      )

      // Verify metrics were computed correctly based on cached dataSplits
      assert(metrics.nonEmpty, "reportDriverMetrics should return non-empty metrics")
    }
  }

  test("getInputSplits cache should preserve dataSplits when filter applied and snapshot changed") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, name STRING) USING PAIMON
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')
            |""".stripMargin)
      sql("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      val table = loadTable("T")
      val schema = spark.table("T").schema

      // Create COW scan and load initial splits (snapshot 1)
      val cowScan = PaimonCopyOnWriteScan(table, schema)
      cowScan.inputSplits
      assert(cowScan.dataSplits.nonEmpty, "Initial dataSplits should not be empty")

      val initialScannedFiles = cowScan.scannedFiles
      assert(initialScannedFiles.nonEmpty, "Initial scannedFiles should not be empty")

      // Simulate a concurrent MERGE INTO that replaces the data files in a new snapshot.
      // After this, the original files from snapshot 1 no longer exist in the latest snapshot.
      sql("""MERGE INTO T USING (SELECT 1 as id, 'x' as name) AS s
            |ON T.id = s.id
            |WHEN MATCHED THEN UPDATE SET name = s.name
            |""".stripMargin)

      // Call inputSplits again (e.g., via reportDriverMetrics). With the getInputSplits
      // cache fix, this must NOT trigger loadSplits(). Without the fix, loadSplits()
      // would read the latest snapshot and overwrite dataSplits.
      cowScan.reportDriverMetrics()

      // Key assertion: dataSplits must remain non-empty (from original snapshot)
      assert(
        cowScan.dataSplits.nonEmpty,
        "dataSplits should remain non-empty after reportDriverMetrics() even when snapshot changed"
      )

      // scannedFiles should still return the original files for correct deletedCommitMessage
      assert(
        cowScan.scannedFiles == initialScannedFiles,
        "scannedFiles should still return original files for correct COW commit"
      )
    }
  }

  test("concurrent MERGE INTO should produce correct row count") {
    import java.util.concurrent.Executors

    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, b INT, c INT) USING PAIMON")
      sql(
        "INSERT INTO s VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), " +
          "(4, 4, 4), (5, 5, 5), (6, 6, 6), (7, 7, 7), (8, 8, 8), (9, 9, 9)")

      sql("""CREATE TABLE t (id INT, b INT, c INT) USING PAIMON
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')
            |""".stripMargin)
      sql(
        "INSERT INTO t VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), " +
          "(4, 4, 4), (5, 5, 5), (6, 6, 6), (7, 7, 7), (8, 8, 8), (9, 9, 9)")

      def doMergeInto(): Unit = {
        for (i <- 1 to 9) {
          try {
            sql(s"""
                   |MERGE INTO t
                   |USING (SELECT * FROM s WHERE id = $i) AS src
                   |ON t.id = src.id
                   |WHEN MATCHED THEN
                   |UPDATE SET t.b = src.b + t.b, t.c = src.c + t.c
                   |""".stripMargin)
          } catch {
            case e: Throwable =>
              assert(
                e.getMessage.contains("Conflicts during commits") ||
                  e.getMessage.contains("Missing file"),
                s"Unexpected error: ${e.getMessage}"
              )
          }
          // Key assertion: row count must always be 9 after each MERGE INTO
          checkAnswer(sql("SELECT count(*) FROM t"), Seq(org.apache.spark.sql.Row(9)))
        }
      }

      val executor = Executors.newFixedThreadPool(2)
      val runnable = new Runnable {
        override def run(): Unit = doMergeInto()
      }

      val future1 = executor.submit(runnable)
      val future2 = executor.submit(runnable)

      future1.get()
      future2.get()

      executor.shutdown()
    }
  }
}
