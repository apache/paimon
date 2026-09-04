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

import org.apache.paimon.catalog.{Catalog, Identifier}
import org.apache.paimon.fs.Path
import org.apache.paimon.manifest.{ManifestFile, ManifestFileMeta, ManifestList}
import org.apache.paimon.operation.{CleanOrphanFilesResult, ManagedBlobOrphanFilesClean}
import org.apache.paimon.operation.ManagedBlobOrphanFilesClean.SidecarWorkItem
import org.apache.paimon.operation.OrphanFilesClean.retryReadingFiles
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.utils.DataFilePathFactories
import org.apache.paimon.utils.FileStorePathFactory.BUCKET_PATH_PREFIX
import org.apache.paimon.utils.Preconditions

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{functions, DataFrame, Dataset, PaimonSparkSession, SparkSession}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.storage.StorageLevel

import java.util
import java.util.function.Consumer

import scala.collection.JavaConverters._
import scala.collection.mutable

case class SparkManagedBlobOrphanFilesClean(
    specifiedTable: FileStoreTable,
    specifiedOlderThanMillis: Long,
    parallelism: Int,
    dryRunPara: Boolean,
    @transient spark: SparkSession)
  extends SparkManagedBlobOrphanFilesCleanBase(specifiedTable, specifiedOlderThanMillis, dryRunPara)
  with SQLConfHelper
  with Logging {

  def doClean(): (Dataset[(Long, Long)], Seq[Dataset[_]]) = {
    import spark.implicits._

    SparkManagedBlobOrphanFilesClean.checkParallelism(parallelism)
    val cached = new mutable.ArrayBuffer[Dataset[_]]()
    try {
      val topologyBefore = snapshotTopology()
      val usedPacks = collectUsedPacksDf().persist(StorageLevel.MEMORY_AND_DISK)
      cached += usedPacks
      val skipGc = usedPacks
        .filter($"used_name" === ManagedBlobOrphanFilesClean.SKIP_MANAGED_BLOB_GC)
        .limit(1)
        .collect()
        .nonEmpty

      val fileDirs = listPaimonFileDirs.asScala.map(_.toString).toSeq
      val maxFileDirsParallelism = Math.min(Math.max(fileDirs.size, 1), parallelism)
      val candidates = spark.sparkContext
        .parallelize(fileDirs, maxFileDirsParallelism)
        .flatMap {
          dir =>
            tryBestListingDirs(new Path(dir)).asScala
              .filter(file => !file.isDir)
              .filter(oldEnough)
              .filter(
                file => ManagedBlobOrphanFilesClean.isManagedBlobPackName(file.getPath.getName))
              .map {
                file =>
                  val path = file.getPath
                  val parent = path.getParent
                  (
                    packIdentityForCandidate(path),
                    path.toString,
                    file.getLen,
                    if (parent == null) "" else parent.toString)
              }
        }
        .toDF("name", "path", "len", "dataDir")
        .dropDuplicates("name")
        .repartition(parallelism)
        .persist(StorageLevel.MEMORY_AND_DISK)
      cached += candidates
      val candidateSkipGc = candidates
        .filter($"name" === ManagedBlobOrphanFilesClean.SKIP_MANAGED_BLOB_GC)
        .limit(1)
        .collect()
        .nonEmpty
      val canonicalCandidates = candidates
        .filter($"name" =!= ManagedBlobOrphanFilesClean.SKIP_MANAGED_BLOB_GC)

      betweenUsedCollections()
      val usedPacks2 = collectUsedPacksDf().persist(StorageLevel.MEMORY_AND_DISK)
      cached += usedPacks2
      val skipGc2 = usedPacks2
        .filter($"used_name" === ManagedBlobOrphanFilesClean.SKIP_MANAGED_BLOB_GC)
        .limit(1)
        .collect()
        .nonEmpty
      val topologyAfter = snapshotTopology()
      val used1Packs =
        usedPacks.filter($"used_name" =!= ManagedBlobOrphanFilesClean.SKIP_MANAGED_BLOB_GC)
      val used2Packs =
        usedPacks2.filter($"used_name" =!= ManagedBlobOrphanFilesClean.SKIP_MANAGED_BLOB_GC)

      val topologyChanged = topologyBefore != topologyAfter
      val usedSetDifferences = used1Packs
        .toDF()
        .except(used2Packs.toDF())
        .union(used2Packs.toDF().except(used1Packs.toDF()))
      val usedSetChanged = usedSetDifferences.limit(1).collect().nonEmpty
      val frozenAbort =
        skipGc || skipGc2 || candidateSkipGc || topologyChanged || usedSetChanged

      // Freeze every abort already observed by an action, and also retain dynamic gates so a cache
      // miss that discovers a new unsafe mark cannot drop a live pack from the join.
      val abortKeys = spark
        .range(if (frozenAbort) 1L else 0L)
        .select(functions.lit(1).as("abort_key"))
        .union(abortKeyDf(usedPacks, "used_name"))
        .union(abortKeyDf(usedPacks2, "used_name"))
        .union(abortKeyDf(candidates, "name"))
        .union(usedSetDifferences
          .limit(1)
          .select(functions.lit(1).as("abort_key")))
        .distinct()
      if (frozenAbort) {
        val reason =
          if (usedSetChanged) {
            "the used pack set changed during collection"
          } else {
            "sidecars, manifests, or candidate identities cannot be trusted, or snapshot topology changed during collection"
          }
        logWarning(s"Skip managed blob pack GC for table ${table.fullName()} because $reason.")
      }

      val unused =
        canonicalCandidates.join(used2Packs.toDF(), $"name" === $"used_name", "left_anti")
      val toDelete = unused
        .withColumn("abort_key", functions.lit(1))
        .join(abortKeys, Seq("abort_key"), "left_anti")
        .drop("abort_key")

      val deleted: Dataset[(Long, Long)] = toDelete
        .repartition(parallelism, $"dataDir")
        .mapPartitions {
          it =>
            var deletedFilesCount = 0L
            var deletedFilesLenInBytes = 0L
            val dataDirs = new mutable.HashSet[String]()
            while (it.hasNext) {
              val fileInfo = it.next()
              val pathToClean = fileInfo.getString(1)
              val deletedPath = new Path(pathToClean)
              if (cleanManagedBlobFileIdempotently(deletedPath)) {
                deletedFilesLenInBytes += fileInfo.getLong(2)
                logInfo(s"Cleaned managed blob pack: $pathToClean")
                dataDirs.add(fileInfo.getString(3))
                deletedFilesCount += 1
              }
            }
            if (!dryRun) {
              val bucketDirs = dataDirs
                .filter(_.contains(BUCKET_PATH_PREFIX))
                .map(new Path(_))
              tryCleanDataDirectory(bucketDirs.asJava, partitionKeysNum + 1)
            }
            Iterator.single((deletedFilesCount, deletedFilesLenInBytes))
        }

      (deleted, cached.toSeq)
    } catch {
      case t: Throwable =>
        cached.foreach(_.unpersist())
        throw t
    }
  }

  private[procedure] def abortKeyDf(source: Dataset[_], column: String): DataFrame = {
    source
      .filter(functions.col(column) === ManagedBlobOrphanFilesClean.SKIP_MANAGED_BLOB_GC)
      .limit(1)
      .select(functions.lit(1).as("abort_key"))
  }

  private[procedure] def collectUsedPacksDf(): DataFrame = {
    import spark.implicits._
    val branches = validBranches()
    val maxBranchParallelism = Math.min(branches.size(), parallelism)
    val manifestLists = spark.sparkContext
      .parallelize(branches.asScala.toSeq, maxBranchParallelism)
      .flatMap {
        branch =>
          safelyGetAllSnapshots(branch).asScala.flatMap {
            snapshot =>
              Seq(
                snapshot.changelogManifestList(),
                snapshot.deltaManifestList(),
                snapshot.baseManifestList())
                .filter(_ != null)
                .map((branch, _))
          }
      }
      .distinct(parallelism)

    val manifests = manifestLists
      .mapPartitions {
        lists =>
          val branchManifestLists = new util.HashMap[String, ManifestList]()
          lists.flatMap {
            case (branch, listName) =>
              val manifestList = branchManifestLists.computeIfAbsent(
                branch,
                (key: String) =>
                  specifiedTable.switchToBranch(key).store.manifestListFactory.create)
              val metas = retryReadingFiles[java.util.List[ManifestFileMeta]](
                () => manifestList.readWithIOException(listName),
                null)
              if (metas == null) {
                logWarning(
                  s"Manifest list $listName is missing while collecting used managed blob packs. Skip pack GC this run.")
                Iterator.single((true, branch, listName))
              } else {
                metas.asScala.iterator.map(meta => (false, branch, meta.fileName()))
              }
          }
      }
      .distinct(parallelism)

    val sidecarWorkItems = manifests
      .mapPartitions {
        records =>
          val branchManifestFiles = new util.HashMap[String, ManifestFile]()
          val branchPathFactories = new util.HashMap[String, DataFilePathFactories]()
          records.flatMap {
            case (unsafe, _, _) if unsafe =>
              Iterator.single(
                (ManagedBlobOrphanFilesClean.SKIP_MANAGED_BLOB_GC, null: SidecarWorkItem))
            case (_, branch, manifestName) =>
              val branchTable = specifiedTable.switchToBranch(branch)
              val manifestFile = branchManifestFiles.computeIfAbsent(
                branch,
                (_: String) => branchTable.store.manifestFileFactory.create)
              val pathFactories = branchPathFactories.computeIfAbsent(
                branch,
                (_: String) => new DataFilePathFactories(branchTable.store.pathFactory))
              val entries =
                retryReadingFiles(() => manifestFile.readWithIOException(manifestName), null)
              if (entries == null) {
                logWarning(
                  s"Manifest $manifestName is missing while collecting used managed blob packs. Skip pack GC this run.")
                Iterator.single(
                  (ManagedBlobOrphanFilesClean.SKIP_MANAGED_BLOB_GC, null: SidecarWorkItem))
              } else {
                entries.asScala.iterator.flatMap {
                  entry =>
                    createSidecarWorkItemsForSpark(
                      entry,
                      pathFactories.get(entry.partition(), entry.bucket())).asScala.iterator
                      .map(workItem => (workItem.dedupIdentity(), workItem))
                }
              }
          }
      }
      .distinct(parallelism)

    val rawUsedPackNames = sidecarWorkItems
      .mapPartitions {
        records =>
          val scan = newReachabilityScan()
          records.flatMap {
            case (_, null) =>
              Iterator.single(ManagedBlobOrphanFilesClean.SKIP_MANAGED_BLOB_GC)
            case (_, workItem) =>
              val names = new util.ArrayList[String]()
              emitUsedPacksForSpark(
                workItem,
                scan,
                new Consumer[String] {
                  override def accept(name: String): Unit = names.add(name)
                })
              names.iterator().asScala
          }
      }
    distinctUsedPackNames(rawUsedPackNames)
  }

  private[procedure] def distinctUsedPackNames(raw: RDD[String]): DataFrame = {
    import spark.implicits._
    raw.distinct(parallelism).toDF("used_name")
  }

}

object SparkManagedBlobOrphanFilesClean extends SQLConfHelper {

  private def checkParallelism(parallelism: Int): Unit = {
    Preconditions.checkArgument(
      parallelism > 0,
      "Parallelism must be greater than 0, but was %s.",
      Int.box(parallelism))
  }

  def executeDatabase(
      catalog: Catalog,
      databaseName: String,
      tableName: String,
      olderThanMillis: Long,
      parallelismOpt: Integer,
      dryRun: Boolean): CleanOrphanFilesResult = {
    val spark = PaimonSparkSession.active
    val parallelism = if (parallelismOpt == null) {
      Math.max(spark.sparkContext.defaultParallelism, conf.numShufflePartitions)
    } else {
      parallelismOpt.intValue()
    }
    checkParallelism(parallelism)

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
          s"Only FileStoreTable supports remove-orphan-blobs action. The table type is '${table.getClass.getName}'.")
        table.asInstanceOf[FileStoreTable]
    }
    if (tables.isEmpty) {
      return new CleanOrphanFilesResult(0, 0)
    }
    var deletedFilesCount = 0L
    var deletedFilesLenInBytes = 0L
    tables.foreach {
      table =>
        val (tableDeleted, tableCached) = new SparkManagedBlobOrphanFilesClean(
          table,
          olderThanMillis,
          parallelism,
          dryRun,
          spark
        ).doClean()
        try {
          val result = tableDeleted
            .toDF("deletedFilesCount", "deletedFilesLenInBytes")
            .agg(functions.sum("deletedFilesCount"), functions.sum("deletedFilesLenInBytes"))
            .head()
          assert(result.schema.size == 2, result.schema)
          if (!result.isNullAt(0)) {
            deletedFilesCount += result.getLong(0)
            deletedFilesLenInBytes += result.getLong(1)
          }
        } finally {
          tableCached.foreach(_.unpersist())
        }
    }
    new CleanOrphanFilesResult(deletedFilesCount, deletedFilesLenInBytes)
  }
}
