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

import org.apache.paimon.blob.ManagedBlobReferenceFile
import org.apache.paimon.catalog.Identifier
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.fs.{FileStatus, Path, SeekableInputStream}
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.manifest.FileKind
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.{FileStoreTable, FileStoreTableFactory}
import org.apache.paimon.utils.{DataFilePathFactories, DateTimeUtils, TraceableFileIO}

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

class RemoveOrphanBlobsProcedureTest extends PaimonSparkTestBase {

  Seq("local", "distributed").foreach {
    mode =>
      test(s"Paimon procedure: remove unreferenced managed blob pack ($mode)") {
        createManagedBlobTable()
        spark.sql("INSERT INTO T VALUES (1, 'a', X'0102')")

        val table = loadTable("T")
        val orphan = new Path(bucketPath(table), "orphan.managed.blob")
        table.fileIO().newOutputStream(orphan, false).close()
        Thread.sleep(2000)

        val referenced = filesWithSuffix(table, ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)
          .filterNot(_.getName == orphan.getName)
        assert(referenced.nonEmpty)

        val olderThan = DateTimeUtils.formatLocalDateTime(
          DateTimeUtils.toLocalDateTime(System.currentTimeMillis()),
          3)
        spark.sql(
          s"CALL sys.remove_orphan_blobs(table => 'T', older_than => '$olderThan', mode => '$mode')")

        assert(!table.fileIO().exists(orphan))
        referenced.foreach(pack => assert(table.fileIO().exists(pack)))
      }

      test(s"Paimon procedure: skip managed blob pack gc when sidecar missing ($mode)") {
        createManagedBlobTable()
        spark.sql("INSERT INTO T VALUES (1, 'a', X'0102')")

        val table = loadTable("T")
        val orphanPack = new Path(bucketPath(table), "orphan.managed.blob")
        val orphanOther = new Path(bucketPath(table), "orphan.txt")
        table.fileIO().newOutputStream(orphanPack, false).close()
        table.fileIO().writeFile(orphanOther, "x", true)
        Thread.sleep(2000)

        val referenced = filesWithSuffix(table, ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)
          .filterNot(_.getName == orphanPack.getName)
        assert(referenced.nonEmpty)
        filesWithSuffix(table, ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)
          .foreach(table.fileIO().deleteQuietly)

        val olderThan = DateTimeUtils.formatLocalDateTime(
          DateTimeUtils.toLocalDateTime(System.currentTimeMillis()),
          3)
        spark.sql(
          s"CALL sys.remove_orphan_blobs(table => 'T', older_than => '$olderThan', mode => '$mode')")

        assert(table.fileIO().exists(orphanPack))
        assert(table.fileIO().exists(orphanOther))
        referenced.foreach(pack => assert(table.fileIO().exists(pack)))
      }
  }

  test("Paimon procedure: preserve distributed deletion parallelism") {
    createManagedBlobTable()
    spark.sql("INSERT INTO T VALUES (1, 'a', X'0102')")

    val table = loadTable("T")
    val orphan = new Path(bucketPath(table), "orphan.managed.blob")
    table.fileIO().newOutputStream(orphan, false).close()
    Thread.sleep(2000)

    withSQLConf("spark.sql.shuffle.partitions" -> "7") {
      val cleaner =
        SparkManagedBlobOrphanFilesClean(table, System.currentTimeMillis(), 2, true, spark)
      val deleted = cleaner.doClean()._1
      try {
        assert(deleted.rdd.getNumPartitions == 2, deleted.queryExecution.executedPlan)
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  test("Paimon procedure: reject non-positive parallelism") {
    createManagedBlobTable()

    val error = intercept[Exception] {
      spark.sql("CALL sys.remove_orphan_blobs(table => 'T', parallelism => 0)").collect()
    }
    assert(causeMessages(error).contains("Parallelism must be greater than 0, but was 0."))
  }

  test("Paimon procedure: release cached marks after distributed failure") {
    createManagedBlobTable()
    spark.sql("INSERT INTO T VALUES (1, 'a', X'0102')")

    val persistentBefore = spark.sparkContext.getPersistentRDDs.keySet
    val cleaner = new SparkManagedBlobOrphanFilesClean(
      loadTable("T"),
      System.currentTimeMillis(),
      2,
      true,
      spark) {
      override protected def betweenUsedCollections(): Unit =
        throw new RuntimeException("Expected failure after the first mark.")
    }

    val error = intercept[RuntimeException] {
      cleaner.doClean()
    }
    assert(error.getMessage == "Expected failure after the first mark.")
    assert(spark.sparkContext.getPersistentRDDs.keySet == persistentBefore)
  }

  test("Paimon procedure: skip cleanup when candidate identity cannot be canonicalized") {
    createManagedBlobTable()
    spark.sql("INSERT INTO T VALUES (1, 'a', X'0102')")

    val table = loadTable("T")
    val orphan = new Path(bucketPath(table), "orphan.managed.blob")
    table.fileIO().newOutputStream(orphan, false).close()
    val relativeTable = FileStoreTableFactory.create(
      new RemoveOrphanBlobsProcedureTest.RelativeListingFileIO,
      table.location(),
      table.schema())
    val cleaner = SparkManagedBlobOrphanFilesClean(relativeTable, Long.MaxValue, 2, false, spark)

    val (deleted, cached) = cleaner.doClean()
    try {
      assert(deleted.collect().map(_._1).sum == 0)
    } finally {
      cached.foreach(_.unpersist())
    }
    assert(table.fileIO().exists(orphan))
  }

  test("Paimon procedure: deduplicate shared sidecar globally in each mark pass") {
    createManagedBlobTable()
    spark.sql("INSERT INTO T VALUES (1, 'a', X'0102')")
    spark.sql("INSERT INTO T VALUES (2, 'b', X'0304')")

    val table = loadTable("T")
    val commit = table.newBatchWriteBuilder().newCommit()
    try {
      commit.compactManifests()
    } finally {
      commit.close()
    }

    val occurrences = sidecarManifestOccurrences(table)
    val (sidecar, containingManifests) = occurrences
      .find(_._2.distinct.size >= 2)
      .getOrElse(fail(s"No sidecar is shared by distinct manifests: $occurrences"))
    val parallelism = (2 to 64)
      .find {
        candidate =>
          containingManifests
            .map {
              manifest =>
                Math.floorMod(
                  (false, Identifier.DEFAULT_MAIN_BRANCH, manifest).hashCode(),
                  candidate)
            }
            .distinct
            .size >= 2
      }
      .getOrElse(fail(s"Shared manifests cannot be assigned to different partitions: $occurrences"))

    RemoveOrphanBlobsProcedureTest.resetSidecarReadCounts()
    val countingTable = FileStoreTableFactory.create(
      new RemoveOrphanBlobsProcedureTest.CountingSidecarFileIO,
      table.location(),
      table.schema())
    val cleaner =
      SparkManagedBlobOrphanFilesClean(countingTable, Long.MaxValue, parallelism, true, spark)

    val (deleted, cached) = cleaner.doClean()
    try {
      deleted.collect()
    } finally {
      cached.foreach(_.unpersist())
    }
    assert(RemoveOrphanBlobsProcedureTest.sidecarReadCount(sidecar) == 2)
  }

  private def createManagedBlobTable(): Unit = {
    spark.sql("""
                |CREATE TABLE T (id INT, name STRING, payload BINARY)
                |USING PAIMON
                |TBLPROPERTIES (
                |  'primary-key'='id',
                |  'bucket'='1',
                |  'changelog-producer'='none',
                |  'blob-field'='payload')
                |""".stripMargin)
  }

  private def bucketPath(table: FileStoreTable): Path = {
    table.store().pathFactory().bucketPath(BinaryRow.EMPTY_ROW, 0)
  }

  private def filesWithSuffix(table: FileStoreTable, suffix: String): Seq[Path] = {
    val statuses = table.fileIO().listStatus(bucketPath(table))
    if (statuses == null) {
      Seq.empty
    } else {
      statuses.map(_.getPath).filter(_.getName.endsWith(suffix))
    }
  }

  private def sidecarManifestOccurrences(table: FileStoreTable): Map[String, Seq[String]] = {
    val manifestList = table.store().manifestListFactory().create()
    val manifestFile = table.store().manifestFileFactory().create()
    val pathFactories = new DataFilePathFactories(table.store().pathFactory())
    val manifests = table
      .snapshotManager()
      .safelyGetAllSnapshots()
      .asScala
      .flatMap(snapshot => manifestList.readDataManifests(snapshot).asScala)
      .map(_.fileName())
      .distinct
    manifests
      .flatMap {
        manifest =>
          manifestFile
            .read(manifest)
            .asScala
            .filter(_.kind() == FileKind.ADD)
            .flatMap {
              entry =>
                val dataFile = pathFactories
                  .get(entry.partition(), entry.bucket())
                  .toPath(entry)
                Option(entry.file().extraFiles()).toSeq
                  .flatMap(_.asScala)
                  .filter(_.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX))
                  .map(extra => (new Path(dataFile.getParent, extra).toUri.getPath, manifest))
            }
      }
      .groupBy(_._1)
      .map { case (sidecar, values) => (sidecar, values.map(_._2).toList) }
  }

  private def causeMessages(error: Throwable): String = {
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .flatMap(e => Option(e.getMessage))
      .mkString("\n")
  }
}

private object RemoveOrphanBlobsProcedureTest {

  private val sidecarReadCounts = new ConcurrentHashMap[String, AtomicInteger]()

  private def resetSidecarReadCounts(): Unit = sidecarReadCounts.clear()

  private def sidecarReadCount(path: String): Int =
    Option(sidecarReadCounts.get(path)).map(_.get()).getOrElse(0)

  private class CountingSidecarFileIO extends LocalFileIO {

    override def newInputStream(path: Path): SeekableInputStream = {
      if (path.getName.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
        sidecarReadCounts
          .computeIfAbsent(path.toUri.getPath, (_: String) => new AtomicInteger())
          .incrementAndGet()
      }
      super.newInputStream(path)
    }
  }

  private class RelativeListingFileIO extends TraceableFileIO {

    override def listStatus(path: Path): Array[FileStatus] = {
      val statuses = super.listStatus(path)
      if (statuses == null) {
        null
      } else {
        statuses.map {
          status =>
            if (status.getPath.getName.endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)) {
              withPath(status, new Path(status.getPath.getName))
            } else {
              status
            }
        }
      }
    }

    override def getFileStatus(path: Path): FileStatus = {
      val status = super.getFileStatus(path)
      if (new File(path.toUri.getPath).isAbsolute) {
        status
      } else {
        withPath(status, path)
      }
    }

    private def withPath(status: FileStatus, path: Path): FileStatus = new FileStatus {
      override def getLen: Long = status.getLen

      override def isDir: Boolean = status.isDir

      override def getPath: Path = path

      override def getModificationTime: Long = status.getModificationTime
    }
  }
}
