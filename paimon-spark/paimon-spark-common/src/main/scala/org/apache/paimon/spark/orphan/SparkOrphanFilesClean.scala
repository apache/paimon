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

package org.apache.paimon.spark.orphan

import org.apache.paimon.{utils, Snapshot}
import org.apache.paimon.catalog.{Catalog, Identifier}
import org.apache.paimon.fs.Path
import org.apache.paimon.manifest.{ManifestEntry, ManifestFile}
import org.apache.paimon.operation.OrphanFilesClean
import org.apache.paimon.operation.OrphanFilesClean.retryReadingFiles
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.utils.SerializableConsumer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.functions.sum

import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class SparkOrphanFilesClean(
    specifiedTable: FileStoreTable,
    specifiedOlderThanMillis: Long,
    specifiedFileCleaner: SerializableConsumer[Path],
    parallelism: Int,
    @transient spark: SparkSession)
  extends OrphanFilesClean(specifiedTable, specifiedOlderThanMillis, specifiedFileCleaner)
  with SQLConfHelper
  with Logging {

  def doOrphanClean(): (Dataset[Long], Dataset[BranchAndManifestFile]) = {
    import spark.implicits._

    val branches = validBranches()
    val deletedInLocal = new AtomicLong(0)
    // snapshot and changelog files are the root of everything, so they are handled specially
    // here, and subsequently, we will not count their orphan files.
    cleanSnapshotDir(branches, (_: Path) => deletedInLocal.incrementAndGet)

    val maxBranchParallelism = Math.min(branches.size(), parallelism)
    // find snapshots using branch and find manifests(manifest, index, statistics) using snapshot
    val usedManifestFiles = spark.sparkContext
      .parallelize(branches.asScala.toSeq, maxBranchParallelism)
      .mapPartitions(_.flatMap {
        branch => safelyGetAllSnapshots(branch).asScala.map(snapshot => (branch, snapshot.toJson))
      })
      .repartition(parallelism)
      .flatMap {
        case (branch, snapshotJson) =>
          val usedFileBuffer = new ArrayBuffer[BranchAndManifestFile]()
          val usedFileConsumer =
            new Consumer[org.apache.paimon.utils.Pair[String, java.lang.Boolean]] {
              override def accept(pair: utils.Pair[String, java.lang.Boolean]): Unit = {
                usedFileBuffer.append(BranchAndManifestFile(branch, pair.getLeft, pair.getRight))
              }
            }
          val snapshot = Snapshot.fromJson(snapshotJson)
          collectWithoutDataFileWithManifestFlag(branch, snapshot, usedFileConsumer)
          usedFileBuffer
      }
      .toDS()
      .cache()

    // find all data files
    val dataFiles = usedManifestFiles
      .filter(_.isManifestFile)
      .distinct()
      .mapPartitions {
        it =>
          val branchManifests = new util.HashMap[String, ManifestFile]
          it.flatMap {
            branchAndManifestFile =>
              val manifestFile = branchManifests.computeIfAbsent(
                branchAndManifestFile.branch,
                (key: String) =>
                  specifiedTable.switchToBranch(key).store.manifestFileFactory.create)

              retryReadingFiles(
                () => manifestFile.readWithIOException(branchAndManifestFile.manifestName),
                Collections.emptyList[ManifestEntry]
              ).asScala.flatMap {
                manifestEntry =>
                  manifestEntry.fileName() +: manifestEntry.file().extraFiles().asScala
              }
          }
      }

    // union manifest and data files
    val usedFiles = usedManifestFiles
      .map(_.manifestName)
      .union(dataFiles)
      .toDF("used_name")

    // find candidate files which can be removed
    val fileDirs = listPaimonFileDirs.asScala.map(_.toUri.toString).toSeq
    val maxFileDirsParallelism = Math.min(fileDirs.size, parallelism)
    val candidates = spark.sparkContext
      .parallelize(fileDirs, maxFileDirsParallelism)
      .flatMap {
        dir =>
          tryBestListingDirs(new Path(dir)).asScala.filter(oldEnough).map {
            file => (file.getPath.getName, file.getPath.toUri.toString)
          }
      }
      .toDF("name", "path")
      .repartition(parallelism)

    // use left anti to filter files which is not used
    val deleted = candidates
      .join(usedFiles, $"name" === $"used_name", "left_anti")
      .mapPartitions {
        it =>
          var deleted = 0L
          while (it.hasNext) {
            val pathToClean = it.next().getString(1)
            specifiedFileCleaner.accept(new Path(pathToClean))
            logInfo(s"Cleaned file: $pathToClean")
            deleted += 1
          }
          logInfo(s"Total cleaned files: $deleted");
          Iterator.single(deleted)
      }
    val finalDeletedDataset = if (deletedInLocal.get() != 0) {
      deleted.union(spark.createDataset(Seq(deletedInLocal.get())))
    } else {
      deleted
    }

    (finalDeletedDataset, usedManifestFiles)
  }
}

/**
 * @param branch
 *   The branch name
 * @param manifestName
 *   The manifest file name, including manifest-list, manifest, index-manifest, statistics
 * @param isManifestFile
 *   If it is the manifest file
 */
case class BranchAndManifestFile(branch: String, manifestName: String, isManifestFile: Boolean)

object SparkOrphanFilesClean extends SQLConfHelper {
  def executeDatabaseOrphanFiles(
      catalog: Catalog,
      databaseName: String,
      tableName: String,
      olderThanMillis: Long,
      fileCleaner: SerializableConsumer[Path],
      parallelismOpt: Integer): Long = {
    val spark = SparkSession.active
    val parallelism = if (parallelismOpt == null) {
      Math.max(spark.sparkContext.defaultParallelism, conf.numShufflePartitions)
    } else {
      parallelismOpt.intValue()
    }

    val tableNames = if (tableName == null || "*" == tableName) {
      catalog.listTables(databaseName).asScala
    } else {
      tableName :: Nil
    }
    val tables = tableNames.map {
      tableName =>
        val identifier = new Identifier(databaseName, tableName)
        val table = catalog.getTable(identifier)
        assert(
          table.isInstanceOf[FileStoreTable],
          s"Only FileStoreTable supports remove-orphan-files action. The table type is '${table.getClass.getName}'.")
        table.asInstanceOf[FileStoreTable]
    }
    if (tables.isEmpty) {
      return 0
    }
    val (deleted, waitToRelease) = tables.map {
      table =>
        new SparkOrphanFilesClean(
          table,
          olderThanMillis,
          fileCleaner,
          parallelism,
          spark
        ).doOrphanClean()
    }.unzip
    try {
      val result = deleted
        .reduce((l, r) => l.union(r))
        .toDF("deleted")
        .agg(sum("deleted"))
        .head()
      assert(result.schema.size == 1, result.schema)
      if (result.isNullAt(0)) {
        // no files can be deleted
        0
      } else {
        result.getLong(0)
      }
    } finally {
      waitToRelease.foreach(_.unpersist())
    }
  }
}
