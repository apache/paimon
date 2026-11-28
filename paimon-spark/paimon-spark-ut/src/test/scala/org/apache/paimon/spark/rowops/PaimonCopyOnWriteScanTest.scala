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

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.StartupMode
import org.apache.paimon.Snapshot
import org.apache.paimon.catalog.{Catalog, CatalogLoader, DelegateCatalog, Identifier}
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.fs.Path
import org.apache.paimon.options.Options
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.commands.SparkDataFileMeta
import org.apache.paimon.spark.schema.PaimonMetadataColumn.FILE_PATH_COLUMN
import org.apache.paimon.table.{CatalogEnvironment, FallbackReadFileStoreTable, FileStoreTable, FileStoreTableFactory, Instant}
import org.apache.paimon.utils.SnapshotLoader

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.unsafe.types.UTF8String

import java.util.Optional
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

  /** The latest snapshot a catalog that tracks snapshots returns. */
  @volatile var catalogLatest: Option[Snapshot] = None

  object CatalogSnapshotLoader extends SnapshotLoader {
    override def load(): Optional[Snapshot] = Optional.ofNullable(catalogLatest.orNull)
    override def rollback(instant: Instant): Unit = throw new UnsupportedOperationException
    override def copyWithBranch(branch: String): SnapshotLoader = this
  }

  /** The snapshots by id that a catalog that tracks snapshots returns. */
  @volatile var catalogSnapshots: Map[Long, Snapshot] = Map.empty

  /** The same for the other branch of a fallback branch table. */
  @volatile var otherCatalogLatest: Option[Snapshot] = None
  @volatile var otherCatalogSnapshots: Map[Long, Snapshot] = Map.empty

  object OtherSnapshotLoader extends SnapshotLoader {
    override def load(): Optional[Snapshot] = Optional.ofNullable(otherCatalogLatest.orNull)
    override def rollback(instant: Instant): Unit = throw new UnsupportedOperationException
    override def copyWithBranch(branch: String): SnapshotLoader = this
  }

  object SnapshotCatalog extends DelegateCatalog(null) {
    override def loadSnapshot(identifier: Identifier, version: String): Optional[Snapshot] = {
      val snapshots =
        if (identifier.getBranchName == null) catalogSnapshots else otherCatalogSnapshots
      Optional.ofNullable(snapshots.get(version.toLong).orNull)
    }
    override def catalogLoader(): CatalogLoader = SnapshotCatalogLoader
    override def close(): Unit = ()
  }

  object SnapshotCatalogLoader extends CatalogLoader {
    override def load(): Catalog = SnapshotCatalog
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

  private def fullCompact(
      table: FileStoreTable,
      partition: BinaryRow = BinaryRow.EMPTY_ROW): Unit = {
    val builder = table.newBatchWriteBuilder()
    val write = builder.newWrite()
    val commit = builder.newCommit()
    try {
      write.compact(partition, 0, true)
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

  private def fileName(file: SparkDataFileMeta): String = file.dataFileMeta.fileName()

  private def fileNames(scan: PaimonCopyOnWriteScan): Set[String] =
    scan.scannedFiles.map(fileName).toSet

  /** The runtime group filter Spark pushes into the scan: `__paimon_file_path IN (paths)`. */
  private def filePathIn(scan: PaimonCopyOnWriteScan): Array[Predicate] = filePathIn(
    scan.scannedFiles)

  private def filePathIn(files: Seq[SparkDataFileMeta]): Array[Predicate] = {
    // Runtime filter literals carry Spark's internal string type.
    val values = files.map(file => Expressions.literal(UTF8String.fromString(file.filePath())))
    Array(new Predicate("IN", (Expressions.column(FILE_PATH_COLUMN) +: values).toArray[Expression]))
  }

  Seq(
    "DELETE" -> "DELETE FROM T WHERE id = 1 AND cow_commit_hook(id)",
    "UPDATE" -> "UPDATE T SET name = 'x' WHERE id = 1 AND cow_commit_hook(id)",
    // A subquery makes Spark plan the UPDATE as a union whose two reads share one scan.
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
            val plan = assertGroupFilterPlanned(statement)
            if (command == "UPDATE with a subquery") assert(plan.contains("Union"), plan)
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
      // Retry loops, e.g. the concurrent MERGE tests, match the "Missing file" wording.
      assert(error.getMessage.contains("Missing files"), error.getMessage)
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

  test("copy-on-write scan with scan.file-creation-time-millis accepts a later append") {
    withTable("T") {
      sql(
        "CREATE TABLE T (id INT, name STRING) TBLPROPERTIES " +
          "('bucket' = '1', 'bucket-key' = 'id', 'write-only' = 'true')")
      Seq(1, 2).foreach(i => sql(s"INSERT INTO T VALUES ($i, 'v$i')"))
      val creationTime = Map("scan.file-creation-time-millis" -> "0").asJava
      val table = loadTable("T").copy(creationTime)
      assert(table.coreOptions().startupMode() == StartupMode.FROM_FILE_CREATION_TIME)
      val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
      val initialFiles = fileNames(loadedScan(table))
      sql("INSERT INTO T VALUES (3, 'v3')")
      val groupFilterScan = loadedScan(loadTable("T").copy(creationTime))

      cowScan.filter(filePathIn(groupFilterScan))
      cowScan.inputSplits

      assert(fileNames(cowScan) == initialFiles)
    }
  }

  test("copy-on-write scan accepts an append whose snapshot file the catalog has not published") {
    withTable("T") {
      sql(
        "CREATE TABLE T (id INT, name STRING) TBLPROPERTIES " +
          "('bucket' = '1', 'bucket-key' = 'id', 'write-only' = 'true')")
      Seq(1, 2).foreach(i => sql(s"INSERT INTO T VALUES ($i, 'v$i')"))
      val table = loadTable("T")
      // A catalog that tracks snapshots can return a commit before its snapshot file is visible.
      val environment = new CatalogEnvironment(null, null, null, null, null, null, true, false) {
        override def snapshotLoader(): SnapshotLoader =
          PaimonCopyOnWriteScanTest.CatalogSnapshotLoader
      }
      val catalogTable =
        FileStoreTableFactory.create(table.fileIO(), table.location(), table.schema(), environment)
      PaimonCopyOnWriteScanTest.catalogLatest = Some(table.latestSnapshot().get())
      try {
        val cowScan = PaimonCopyOnWriteScan(catalogTable, spark.table("T").schema)
        val initialFiles = fileNames(loadedScan(table))
        sql("INSERT INTO T VALUES (3, 'v3')")
        val appended = loadTable("T")
        val groupFilterScan = loadedScan(appended)
        val latest = appended.latestSnapshot().get()
        PaimonCopyOnWriteScanTest.catalogLatest = Some(latest)
        val snapshotFile = appended.snapshotManager().snapshotPath(latest.id())
        val hidden = new Path(snapshotFile.getParent, s"hidden-${snapshotFile.getName}")
        assert(fileIO.rename(snapshotFile, hidden))
        try {
          cowScan.filter(filePathIn(groupFilterScan))
          cowScan.inputSplits
        } finally {
          fileIO.rename(hidden, snapshotFile)
        }

        assert(fileNames(cowScan) == initialFiles)
      } finally {
        PaimonCopyOnWriteScanTest.catalogLatest = None
      }
    }
  }

  test("copy-on-write scan accepts an appended file while another partition is compacted") {
    withTable("T") {
      sql(
        "CREATE TABLE T (id INT, name STRING, p STRING) PARTITIONED BY (p) " +
          "TBLPROPERTIES ('bucket' = '1', 'bucket-key' = 'id')")
      sql("INSERT INTO T VALUES (1, 'v1', 'a')")
      (2 to 4).foreach(i => sql(s"INSERT INTO T VALUES ($i, 'v$i', 'b')"))
      val table = loadTable("T")
      val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
      val initial = loadedScan(table).scannedFiles
      // Partition a has one file, partition b three, enough for a full compaction.
      val partitionA =
        initial.groupBy(_.partition).collectFirst { case (p, fs) if fs.size == 1 => p }
      val partitionB =
        initial.groupBy(_.partition).collectFirst { case (p, fs) if fs.size == 3 => p }
      sql("INSERT INTO T VALUES (5, 'v5', 'a')")
      fullCompact(table, partitionB.get)
      val groupFilterFiles =
        loadedScan(loadTable("T")).scannedFiles.filter(_.partition == partitionA.get)
      assert(groupFilterFiles.size == 2)

      cowScan.filter(filePathIn(groupFilterFiles))
      cowScan.inputSplits

      assert(
        fileNames(cowScan) == initial.filter(_.partition == partitionA.get).map(fileName).toSet)
      assert(loadTable("T").latestSnapshot().get().commitKind().name == "COMPACT")
    }
  }

  test("copy-on-write scan rejects the output of a compaction that was rolled back") {
    withTable("T") {
      val table = createAppendTableWithFourFiles()
      val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
      fullCompact(table)
      val groupFilterScan = loadedScan(loadTable("T"))
      FileStoreTableFactory.create(fileIO, table.location()).rollbackTo(4)
      assert(loadTable("T").latestSnapshot().get().id() == 4L)

      cowScan.filter(filePathIn(groupFilterScan))
      val error = intercept[IllegalStateException](cowScan.inputSplits)
      assert(error.getMessage.contains("concurrent commit"), error.getMessage)
    }
  }

  test("copy-on-write scan accepts other branch files while the main branch is overwritten") {
    for (option <- Seq("scan.fallback-branch", "scan.primary-branch")) {
      withTable("T") {
        sql("CREATE TABLE T (dt STRING, name STRING, amount BIGINT) PARTITIONED BY (dt)")
        sql("CALL sys.create_branch('test.T', 'other')")
        sql(s"ALTER TABLE T SET TBLPROPERTIES ('$option' = 'other')")
        sql("INSERT INTO `T$branch_other` VALUES ('20240726', 'cherry', 3)")
        sql("INSERT INTO T VALUES ('20240725', 'apple', 5)")
        val table = loadTable("T")
        val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
        sql("INSERT OVERWRITE T PARTITION (dt = '20240725') VALUES ('banana', 7)")
        val other = table.asInstanceOf[FallbackReadFileStoreTable].other()
        val otherFiles = PaimonCopyOnWriteScan(other, spark.table("T").schema)
        otherFiles.inputSplits
        assert(otherFiles.scannedFiles.nonEmpty, option)

        cowScan.filter(filePathIn(otherFiles))

        assert(cowScan.inputSplits.isEmpty, option)
      }
    }
  }

  test("copy-on-write scan rejects a file both branches have after the main branch rolled back") {
    // With `recompact`, main compacts the regained files again under the id the rollback freed.
    for (recompact <- Seq(false, true)) {
      withTable("T") {
        sql(
          "CREATE TABLE T (dt STRING, name STRING, amount BIGINT) PARTITIONED BY (dt) " +
            "TBLPROPERTIES ('bucket' = '1', 'bucket-key' = 'name')")
        (1 to 3).foreach(i => sql(s"INSERT INTO T VALUES ('20240725', 'n$i', $i)"))
        // The other branch is created from a tag, so it shares the three files with main.
        sql("CALL sys.create_tag(table => 'test.T', tag => 't1', snapshot => 3)")
        sql("CALL sys.create_branch('test.T', 'other', 't1')")
        sql("ALTER TABLE T SET TBLPROPERTIES ('scan.fallback-branch' = 'other')")
        val table = loadTable("T")
        val partition = loadedScan(table).scannedFiles.head.partition
        fullCompact(table, partition)
        val cowScan = PaimonCopyOnWriteScan(loadTable("T"), spark.table("T").schema)
        FileStoreTableFactory.create(fileIO, table.location()).rollbackTo(3)
        val groupFilterScan = loadedScan(loadTable("T"))
        assert(groupFilterScan.scannedFiles.size == 3)
        if (recompact)
          fullCompact(FileStoreTableFactory.create(fileIO, table.location()), partition)

        cowScan.filter(filePathIn(groupFilterScan))
        val error = intercept[IllegalStateException](cowScan.inputSplits)
        assert(error.getMessage.contains("concurrent commit"), s"$recompact: ${error.getMessage}")
      }
    }
  }

  test("copy-on-write scan accepts other branch files the other branch compacted since") {
    withTable("T") {
      sql(
        "CREATE TABLE T (dt STRING, name STRING, amount BIGINT) PARTITIONED BY (dt) " +
          "TBLPROPERTIES ('bucket' = '1', 'bucket-key' = 'name')")
      sql("CALL sys.create_branch('test.T', 'other')")
      sql("ALTER TABLE T SET TBLPROPERTIES ('scan.fallback-branch' = 'other')")
      (1 to 3).foreach(i => sql(s"INSERT INTO `T$$branch_other` VALUES ('20240726', 'c$i', $i)"))
      sql("INSERT INTO T VALUES ('20240725', 'apple', 5)")
      val table = loadTable("T")
      val other = table.asInstanceOf[FallbackReadFileStoreTable].other()
      val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
      val otherFiles = PaimonCopyOnWriteScan(other, spark.table("T").schema)
      otherFiles.inputSplits
      assert(otherFiles.scannedFiles.size == 3)
      fullCompact(other, otherFiles.scannedFiles.head.partition)

      cowScan.filter(filePathIn(otherFiles))

      assert(cowScan.inputSplits.isEmpty)
    }
  }

  private def createFallbackTableWithThreeOtherFiles(): FallbackReadFileStoreTable = {
    sql(
      "CREATE TABLE T (dt STRING, name STRING, amount BIGINT) PARTITIONED BY (dt) " +
        "TBLPROPERTIES ('bucket' = '1', 'bucket-key' = 'name')")
    sql("CALL sys.create_branch('test.T', 'other')")
    sql("ALTER TABLE T SET TBLPROPERTIES ('scan.fallback-branch' = 'other')")
    (1 to 3).foreach(i => sql(s"INSERT INTO `T$$branch_other` VALUES ('20240726', 'c$i', $i)"))
    sql("INSERT INTO T VALUES ('20240725', 'apple', 5)")
    loadTable("T").asInstanceOf[FallbackReadFileStoreTable]
  }

  private def otherBranch(table: FileStoreTable): FileStoreTable = {
    val options = new Options()
    options.set(CoreOptions.PATH, table.location().toString)
    options.set(CoreOptions.BRANCH, "other")
    FileStoreTableFactory.create(fileIO, options)
  }

  test("copy-on-write scan accepts files the other branch committed after the scan was built") {
    withTable("T") {
      val table = createFallbackTableWithThreeOtherFiles()
      val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
      sql("INSERT INTO `T$branch_other` VALUES ('20240726', 'c4', 4)")
      fullCompact(otherBranch(table), loadedScan(table.other()).scannedFiles.head.partition)
      val otherFiles = PaimonCopyOnWriteScan(otherBranch(table), spark.table("T").schema)
      otherFiles.inputSplits
      // One file the compaction wrote, which the other branch did not have when the scan was built.
      assert(otherFiles.scannedFiles.size == 1)

      cowScan.filter(filePathIn(otherFiles))

      assert(cowScan.inputSplits.isEmpty)
    }
  }

  test("copy-on-write scan rejects later files of an other branch rolled back past its snapshot") {
    withTable("T") {
      val table = createFallbackTableWithThreeOtherFiles()
      val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
      // A branch rolled back since can hold files it shares with main, e.g. if created from a tag.
      otherBranch(table).rollbackTo(2)
      sql("INSERT INTO `T$branch_other` VALUES ('20240726', 'c4', 4)")
      val otherFiles = PaimonCopyOnWriteScan(otherBranch(table), spark.table("T").schema)
      otherFiles.inputSplits
      assert(otherFiles.scannedFiles.size == 3)

      cowScan.filter(filePathIn(otherFiles))
      val error = intercept[IllegalStateException](cowScan.inputSplits)
      assert(error.getMessage.contains("concurrent commit"), error.getMessage)
    }
  }

  test("copy-on-write scan reads the other branch from the catalog before its files exist") {
    withTable("T") {
      val fallback = createFallbackTableWithThreeOtherFiles()
      val other = otherBranch(fallback)
      val environment = new CatalogEnvironment(
        new Identifier(dbName0, "T", "other"),
        null,
        PaimonCopyOnWriteScanTest.SnapshotCatalogLoader,
        null,
        null,
        null,
        false,
        false) {
        override def snapshotLoader(): SnapshotLoader =
          PaimonCopyOnWriteScanTest.OtherSnapshotLoader
      }
      val catalogOther = FileStoreTableFactory.createWithoutFallbackBranch(
        fileIO,
        other.location(),
        other.schema(),
        new Options(Map(CoreOptions.BRANCH.key() -> "other").asJava),
        environment)
      val table = new FallbackReadFileStoreTable(fallback.wrapped(), catalogOther, true)
      PaimonCopyOnWriteScanTest.otherCatalogLatest = Some(other.snapshot(3))
      try {
        val cowScan = PaimonCopyOnWriteScan(table, spark.table("T").schema)
        // Write-only, so that the fifth file does not trigger an inline compaction.
        withSparkSQLConf("spark.paimon.write-only" -> "true") {
          Seq(4, 5).foreach(
            i => sql(s"INSERT INTO `T$$branch_other` VALUES ('20240726', 'c$i', $i)"))
        }
        val otherFiles = PaimonCopyOnWriteScan(otherBranch(fallback), spark.table("T").schema)
        otherFiles.inputSplits
        assert(otherFiles.scannedFiles.size == 5)
        // The catalog has snapshots 4 and 5, their files are not visible yet.
        val snapshots = Seq(3L, 4L, 5L).map(id => id -> otherBranch(fallback).snapshot(id)).toMap
        PaimonCopyOnWriteScanTest.otherCatalogSnapshots = snapshots
        PaimonCopyOnWriteScanTest.otherCatalogLatest = Some(snapshots(5L))
        val hidden = Seq(4L, 5L).map {
          id =>
            val file = other.snapshotManager().snapshotPath(id)
            val moved = new Path(file.getParent, s"hidden-${file.getName}")
            assert(fileIO.rename(file, moved))
            file -> moved
        }
        try {
          cowScan.filter(filePathIn(otherFiles))
          assert(cowScan.inputSplits.isEmpty)
        } finally {
          hidden.foreach { case (file, moved) => fileIO.rename(moved, file) }
        }
      } finally {
        PaimonCopyOnWriteScanTest.otherCatalogLatest = None
        PaimonCopyOnWriteScanTest.otherCatalogSnapshots = Map.empty
      }
    }
  }

  test("copy-on-write scan asks for a retry when its snapshot expired before the load") {
    withTable("T") {
      val table = createAppendTableWithFourFiles()
      // Without the catalog's manifest cache, which can still serve the expired manifests.
      val uncached = FileStoreTableFactory.create(fileIO, table.location())
      val cowScan = PaimonCopyOnWriteScan(uncached, spark.table("T").schema)
      sql("INSERT INTO T VALUES (5, 'v5')")
      sql("CALL sys.expire_snapshots(table => 'T', retain_max => 1, retain_min => 1)")
      assert(loadTable("T").snapshotManager().earliestSnapshotId() > 4L)

      val error = intercept[IllegalStateException](cowScan.inputSplits)
      assert(error.getMessage.contains("may have expired"), error.getMessage)
    }
  }

  test("copy-on-write scan reads later snapshots from the catalog before their files exist") {
    withTable("T") {
      sql(
        "CREATE TABLE T (id INT, name STRING) TBLPROPERTIES " +
          "('bucket' = '1', 'bucket-key' = 'id', 'write-only' = 'true')")
      Seq(1, 2).foreach(i => sql(s"INSERT INTO T VALUES ($i, 'v$i')"))
      val table = loadTable("T")
      val environment = new CatalogEnvironment(
        Identifier.create(dbName0, "T"),
        null,
        PaimonCopyOnWriteScanTest.SnapshotCatalogLoader,
        null,
        null,
        null,
        false,
        false) {
        override def snapshotLoader(): SnapshotLoader =
          PaimonCopyOnWriteScanTest.CatalogSnapshotLoader
      }
      val catalogTable =
        FileStoreTableFactory.create(table.fileIO(), table.location(), table.schema(), environment)
      PaimonCopyOnWriteScanTest.catalogLatest = Some(table.latestSnapshot().get())
      try {
        val cowScan = PaimonCopyOnWriteScan(catalogTable, spark.table("T").schema)
        val initialFiles = fileNames(loadedScan(table))
        Seq(3, 4).foreach(i => sql(s"INSERT INTO T VALUES ($i, 'v$i')"))
        val appended = loadTable("T")
        // The group filter also selects the files of snapshots 3 and 4.
        val groupFilterFiles = loadedScan(appended).scannedFiles
        assert(groupFilterFiles.size == initialFiles.size + 2)
        // The catalog has every snapshot, the files of snapshots 3 and 4 are not visible yet.
        val snapshots = Seq(2L, 3L, 4L).map(id => id -> appended.snapshot(id)).toMap
        PaimonCopyOnWriteScanTest.catalogSnapshots = snapshots
        PaimonCopyOnWriteScanTest.catalogLatest = Some(snapshots(4L))
        val manager = appended.snapshotManager()
        val hidden = Seq(3L, 4L).map {
          id =>
            val file = manager.snapshotPath(id)
            val moved = new Path(file.getParent, s"hidden-${file.getName}")
            assert(fileIO.rename(file, moved))
            file -> moved
        }
        try {
          cowScan.filter(filePathIn(groupFilterFiles))
          cowScan.inputSplits
        } finally {
          hidden.foreach { case (file, moved) => fileIO.rename(moved, file) }
        }

        assert(fileNames(cowScan) == initialFiles)
      } finally {
        PaimonCopyOnWriteScanTest.catalogLatest = None
        PaimonCopyOnWriteScanTest.catalogSnapshots = Map.empty
      }
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
  private def assertGroupFilterPlanned(statement: String): String = {
    val plan = sql(s"EXPLAIN $statement").collect().head.getString(0)
    assert(plan.contains("dynamicpruningexpression"), plan)
    plan
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
