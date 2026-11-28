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

import org.apache.paimon.CoreOptions.StartupMode
import org.apache.paimon.Snapshot
import org.apache.paimon.Snapshot.CommitKind
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.Predicate
import org.apache.paimon.rest.exceptions.NotImplementedException
import org.apache.paimon.spark.commands.SparkDataFileMeta
import org.apache.paimon.spark.commands.SparkDataFileMeta.convertToSparkDataFileMeta
import org.apache.paimon.spark.read.BaseScan
import org.apache.paimon.spark.schema.PaimonMetadataColumn.FILE_PATH_COLUMN
import org.apache.paimon.table.{FallbackReadFileStoreTable, FileStoreTable}
import org.apache.paimon.table.source.{DataSplit, ScanMode, Split}
import org.apache.paimon.table.source.snapshot.SnapshotReader
import org.apache.paimon.utils.{ExceptionUtils, SnapshotManager}

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering
import org.apache.spark.sql.sources.{Filter, In}
import org.apache.spark.sql.types.StructType

import java.io.FileNotFoundException
import java.nio.file.Paths

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Note: The [[pushedPartitionFilters]] and [[pushedDataFilters]] are intentionally set to empty
 * because file-level filtering is handled through Spark's runtime V2 filtering mechanism.
 *
 * When Spark's runtime filter is not applied (e.g., when NOT MATCHED BY SOURCE is present in
 * MergeInto), this scan will read all data from the table.
 */
case class PaimonCopyOnWriteScan(
    table: FileStoreTable,
    requiredSchema: StructType,
    pushedPartitionFilters: Seq[PartitionPredicate] = Seq.empty,
    pushedDataFilters: Seq[Predicate] = Seq.empty)
  extends BaseScan
  with SupportsRuntimeV2Filtering {

  // Track whether filter() has been called
  @volatile private var filterApplied: Boolean = false

  @volatile private var splitsLoaded: Boolean = false

  private val filteredFileNames: mutable.Set[String] = mutable.Set[String]()

  protected def getInputSplits: Array[Split] = {
    if (!splitsLoaded) {
      loadSplits()
      splitsLoaded = true
    }
    dataSplits.asInstanceOf[Array[Split]]
  }

  var dataSplits: Array[DataSplit] = Array()

  // Captured when Spark builds this scan, before the runtime group filter query is planned.
  // Every load reads it: once a concurrent commit replaced the files the group filter selected, a
  // load of the latest snapshot would match none of them, and the operation would change nothing.
  private val readSnapshot: Option[Snapshot] = Option(table.snapshotManager().latestSnapshot())

  // For a `scan.fallback-branch` or `scan.primary-branch` table, the latest snapshot of the other
  // branch at the same time. The group filter reads that branch at this snapshot or a later one.
  private val otherReadSnapshot: Option[Snapshot] = table match {
    case fallback: FallbackReadFileStoreTable => latestSnapshot(fallback.other())
    case _ => None
  }

  private case class LaterAdds(byAppend: Set[String], byOthers: Set[String])

  // The group filter query reads the table with its options. With options that select another
  // startup mode, e.g. `scan.snapshot-id` or `scan.mode` = `compacted-full`, it can read an older
  // snapshot, so a file it selected may be missing here because a later commit removed it. With
  // `scan.file-creation-time-millis` it reads part of the latest snapshot.
  private val groupFilterReadsLatest: Boolean = {
    val mode = table.coreOptions().startupMode()
    mode == StartupMode.LATEST_FULL || mode == StartupMode.LATEST ||
    mode == StartupMode.FROM_FILE_CREATION_TIME
  }

  private def loadSplits(): Unit = {
    val snapshotReader = table.newSnapshotReader()
    if (table.coreOptions().manifestDeleteFileDropStats()) {
      snapshotReader.dropStats()
    }
    if (filterApplied) {
      snapshotReader.withDataFileNameFilter(fileName => filteredFileNames.contains(fileName))
    }
    dataSplits = readSnapshot match {
      case Some(snapshot) =>
        try {
          dataSplitsOf(snapshotReader.withSnapshot(snapshot))
        } catch {
          case e: RuntimeException if causedByMissingFile(e) =>
            throw new IllegalStateException(
              s"Cannot read snapshot ${snapshot.id()} this copy-on-write scan was built on, it " +
                "may have expired while the operation was planned. Please retry.",
              e
            )
        }
      case None => Array()
    }
    if (filterApplied) {
      // A scan built on an empty table reads no file, so no change can be lost.
      readSnapshot.foreach(checkFilteredFilesLoaded)
    }
  }

  // Rows in a file the group filter selected and this scan does not read are left unchanged. When
  // the group filter reads the latest snapshot, that is fine for a file a later APPEND added, whose
  // rows the operation does not see, and for a file of the other branch of `scan.fallback-branch`
  // or `scan.primary-branch`, which this scan never reads. A file a COMPACT or OVERWRITE added can
  // hold rows of this scan's snapshot, so fail rather than report success without its changes.
  private def checkFilteredFilesLoaded(snapshot: Snapshot): Unit = {
    val loaded = dataSplits.iterator.flatMap(_.dataFiles().asScala.map(_.fileName())).toSet
    val missing = filteredFileNames.toSet -- loaded
    if (missing.isEmpty) {
      return
    }
    if (!groupFilterReadsLatest) {
      throw new IllegalStateException(
        s"The runtime group filter selected ${missing.size} data files that are not in " +
          s"snapshot ${snapshot.id()} this copy-on-write scan was built on. With startup mode " +
          s"${table.coreOptions().startupMode()} the group filter query can read an older " +
          s"snapshot, so the scan cannot tell whether a commit replaced them. " +
          s"Unset the options that select this startup mode, e.g. scan.snapshot-id, " +
          s"scan.tag-name, scan.mode or incremental-between, for row-level operations. " +
          s"Missing files: ${missing.take(10).mkString(", ")}")
    }
    val mainAdded = latestSnapshot(table).flatMap {
      latest =>
        withSnapshotLookup(table)(
          lookup => addedSince(table, Some(snapshot), latest, lookup, missing))
    }
    val unexplained = mainAdded match {
      case Some(added) =>
        val notAppended = missing -- added.byAppend
        notAppended -- otherBranchFiles(notAppended -- added.byOthers)
      case None => missing
    }
    if (unexplained.nonEmpty) {
      throw new IllegalStateException(
        s"The runtime group filter selected ${unexplained.size} data files that are neither in " +
          s"snapshot ${snapshot.id()} this copy-on-write scan was built on nor added by a later " +
          "append: a concurrent commit may have replaced files of it. Please retry. " +
          s"Missing files: ${unexplained.take(10).mkString(", ")}")
    }
  }

  // Among `names`, the data files added by the commits of `branch` after `start` (after none: from
  // the first snapshot), split by whether the commit is an APPEND. An APPEND never deletes a file
  // (it is committed as an OVERWRITE then), and the tables this scan serves have no primary key or
  // data evolution, so a file an APPEND added holds no row of `start`. None if a later snapshot
  // cannot be read, or the snapshot under the id of `start` is another commit now, i.e. a rollback
  // went past `start`: the commits undone by it are not visible any more.
  private def addedSince(
      branch: FileStoreTable,
      start: Option[Snapshot],
      latest: Snapshot,
      snapshotAt: Long => Option[Snapshot],
      names: Set[String]): Option[LaterAdds] = {
    def current(id: Long): Option[Snapshot] =
      if (id == latest.id()) Some(latest) else snapshotAt(id)
    val unchanged = start.forall(
      s =>
        latest.id() >= s.id() && current(s.id()).exists(
          c =>
            c.baseManifestList() == s.baseManifestList() &&
              c.deltaManifestList() == s.deltaManifestList()))
    if (!unchanged) {
      return None
    }
    val byAppend = mutable.Set[String]()
    val byOthers = mutable.Set[String]()
    val ids = (start.map(_.id() + 1).getOrElse(Snapshot.FIRST_SNAPSHOT_ID) to latest.id()).iterator
    while (ids.hasNext && byAppend.size + byOthers.size < names.size) {
      val delta = current(ids.next()).flatMap(
        s =>
          fileNamesIn(branch.newSnapshotReader().withSnapshot(s).withMode(ScanMode.DELTA), names)
            .map(s.commitKind() -> _))
      delta match {
        case Some((CommitKind.APPEND, files)) => byAppend ++= files
        case Some((_, files)) => byOthers ++= files
        case None => return None
      }
    }
    Some(LaterAdds(byAppend.toSet, byOthers.toSet))
  }

  // Among `names`, files of the other branch of a `scan.fallback-branch` or `scan.primary-branch`
  // table: in its snapshot when this scan was built, or added by its later commits. Callers pass
  // only names that this branch has not had since this scan's snapshot. The other branch can share
  // files with this one, e.g. if it was created from a tag, so its later commits are not trusted
  // once it was re-created or rolled back past its snapshot when this scan was built.
  private def otherBranchFiles(names: Set[String]): Set[String] = table match {
    case fallback: FallbackReadFileStoreTable if names.nonEmpty =>
      val other = fallback.other()
      val atBuild = otherReadSnapshot
        .flatMap(s => fileNamesIn(other.newSnapshotReader().withSnapshot(s), names))
        .getOrElse(Set.empty)
      val rest = names -- atBuild
      val later =
        if (rest.isEmpty) {
          Set.empty[String]
        } else {
          latestSnapshot(other)
            .flatMap(
              latest =>
                withSnapshotLookup(other)(
                  lookup => addedSince(other, otherReadSnapshot, latest, lookup, rest)))
            .map(added => added.byAppend ++ added.byOthers)
            .getOrElse(Set.empty[String])
        }
      atBuild ++ later
    case _ => Set.empty
  }

  // Among `names`, the data files the reader returns, None if a file it needs no longer exists.
  private def fileNamesIn(
      snapshotReader: SnapshotReader,
      names: Set[String]): Option[Set[String]] = {
    try {
      Some(
        dataSplitsOf(snapshotReader.dropStats().withDataFileNameFilter(names.contains)).iterator
          .flatMap(_.dataFiles().asScala.map(_.fileName()))
          .toSet)
    } catch {
      case e: RuntimeException if causedByMissingFile(e) => None
    }
  }

  private def dataSplitsOf(snapshotReader: SnapshotReader): Array[DataSplit] =
    snapshotReader.read().splits().asScala.collect { case s: DataSplit => s }.toArray

  // The latest snapshot of `branch`, from the catalog if it tracks snapshots: there the snapshot
  // file may not be visible yet. Otherwise from the file of the latest id, bypassing the cache.
  private def latestSnapshot(branch: FileStoreTable): Option[Snapshot] = {
    val loader = branch.catalogEnvironment().snapshotLoader()
    val fromCatalog =
      if (loader == null) {
        None
      } else {
        try {
          Some(Option(loader.load().orElse(null)))
        } catch {
          case _: UnsupportedOperationException => None
        }
      }
    fromCatalog.getOrElse(
      Option(branch.snapshotManager().latestSnapshotIdFromFileSystem())
        .flatMap(snapshotFile(branch, _)))
  }

  // Runs `f` with a lookup of the snapshot that holds an id of `branch` now: from the catalog if
  // it tracks snapshots, as their files may not be visible yet, otherwise from the files.
  private def withSnapshotLookup[T](
      branch: FileStoreTable)(f: (Long => Option[Snapshot]) => T): T = {
    val environment = branch.catalogEnvironment()
    if (environment.snapshotLoader() == null || environment.catalogLoader() == null) {
      f(snapshotFile(branch, _))
    } else {
      val catalog = environment.catalogLoader().load()
      try {
        f(
          id =>
            try {
              Option(catalog.loadSnapshot(environment.identifier(), id.toString).orElse(null))
            } catch {
              // A REST server without the endpoint answers 501.
              case _: UnsupportedOperationException | _: NotImplementedException =>
                snapshotFile(branch, id)
            })
      } finally {
        catalog.close()
      }
    }
  }

  // Reads the snapshot file of `branch` under `id`, None if it does not exist. It bypasses the
  // snapshot cache, which still returns a snapshot that a rollback deleted.
  private def snapshotFile(branch: FileStoreTable, id: Long): Option[Snapshot] = {
    val path = branch.snapshotManager().snapshotPath(id)
    try {
      Some(SnapshotManager.fromPath(branch.fileIO(), path))
    } catch {
      case e: RuntimeException if causedByMissingFile(e) => None
    }
  }

  private def causedByMissingFile(e: Throwable): Boolean =
    ExceptionUtils.findThrowable(e, classOf[FileNotFoundException]).isPresent

  def scannedFiles: Seq[SparkDataFileMeta] = {
    dataSplits
      .flatMap(dataSplit => convertToSparkDataFileMeta(dataSplit, dataSplit.totalBuckets()))
      .toSeq
  }

  override def filterAttributes(): Array[NamedReference] = {
    Array(Expressions.column(FILE_PATH_COLUMN))
  }

  override def filter(predicates: Array[SparkPredicate]): Unit = {
    filterApplied = true
    splitsLoaded = false
    val runtimeFilters: Array[Filter] = predicates.flatMap(PaimonUtils.filterV2ToV1)
    for (filter <- runtimeFilters) {
      filter match {
        case in: In if in.attribute.equalsIgnoreCase(FILE_PATH_COLUMN) =>
          for (value <- in.values) {
            val location = value.asInstanceOf[String]
            filteredFileNames.add(Paths.get(location).getFileName.toString)
          }
        case _ => logWarning("Unsupported runtime filter")
      }
    }
  }

}
