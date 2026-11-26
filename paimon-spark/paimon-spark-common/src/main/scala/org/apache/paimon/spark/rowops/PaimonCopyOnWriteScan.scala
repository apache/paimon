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
import org.apache.paimon.spark.commands.SparkDataFileMeta
import org.apache.paimon.spark.commands.SparkDataFileMeta.convertToSparkDataFileMeta
import org.apache.paimon.spark.read.BaseScan
import org.apache.paimon.spark.schema.PaimonMetadataColumn.FILE_PATH_COLUMN
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.{DataSplit, Split}
import org.apache.paimon.utils.SnapshotManager

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

  // The group filter query reads the table with its options. With options that select another
  // startup mode, e.g. `scan.snapshot-id` or `scan.mode` = `compacted-full`, it can read an older
  // snapshot or only part of one, so a file it selected may be missing here for another reason
  // than an append.
  private val groupFilterReadsLatest: Boolean = {
    val mode = table.coreOptions().startupMode()
    mode == StartupMode.LATEST_FULL || mode == StartupMode.LATEST
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
        snapshotReader
          .withSnapshot(snapshot)
          .read()
          .splits()
          .asScala
          .collect { case s: DataSplit => s }
          .toArray
      case None => Array()
    }
    if (filterApplied) {
      // A scan built on an empty table reads no file, so no change can be lost.
      readSnapshot.foreach(checkFilteredFilesLoaded)
    }
  }

  // Rows in a file the group filter selected and this scan does not read are left unchanged. When
  // the group filter reads the latest snapshot, that is fine for a file appended after this scan's
  // snapshot, which the operation does not see, and for a file of another branch, e.g. that of
  // `scan.fallback-branch` or `scan.primary-branch`, which this scan never reads. Otherwise fail,
  // so that the operation does not report success without its changes.
  private def checkFilteredFilesLoaded(snapshot: Snapshot): Unit = {
    val loaded = dataSplits.iterator.flatMap(_.dataFiles().asScala.map(_.fileName())).toSet
    val missing = filteredFileNames.filterNot(loaded.contains)
    if (missing.isEmpty) {
      return
    }
    val sample = missing.take(10).mkString(", ")
    if (!groupFilterReadsLatest) {
      throw new IllegalStateException(
        s"The runtime group filter selected ${missing.size} data files that are not in " +
          s"snapshot ${snapshot.id()} this copy-on-write scan was built on. With startup mode " +
          s"${table.coreOptions().startupMode()} the group filter query can read an older " +
          s"snapshot or part of one, so the scan cannot tell whether a commit replaced them. " +
          s"Unset the options that select this startup mode, e.g. scan.snapshot-id, " +
          s"scan.tag-name, scan.mode or incremental-between, for row-level operations. " +
          s"Missing files: $sample")
    }
    if (!onlyAppendedSince(snapshot)) {
      throw new IllegalStateException(
        s"The runtime group filter selected ${missing.size} data files that are not in " +
          s"snapshot ${snapshot.id()} this copy-on-write scan was built on, and a concurrent " +
          s"commit since then may have replaced files of it. Please retry. Missing files: $sample")
    }
  }

  // Whether no commit since `snapshot` removed a file of it: the snapshot file under its id is
  // still this snapshot (a rollback lets a later commit reuse the id), and every later commit is
  // an APPEND, which never deletes a file (it becomes an OVERWRITE then), or an ANALYZE.
  private def onlyAppendedSince(snapshot: Snapshot): Boolean = {
    val latestId = Option(table.snapshotManager().latestSnapshotId()).map(_.longValue())
    val sameSnapshot = snapshotFile(snapshot.id()).exists(
      current =>
        current.baseManifestList() == snapshot.baseManifestList() &&
          current.deltaManifestList() == snapshot.deltaManifestList())
    sameSnapshot && latestId.exists {
      latest =>
        latest >= snapshot.id() && (snapshot.id() + 1 to latest).forall {
          id =>
            snapshotFile(id).exists {
              later =>
                later.commitKind() == CommitKind.APPEND || later.commitKind() == CommitKind.ANALYZE
            }
        }
    }
  }

  // Reads the snapshot file under `id`, None if it does not exist. It bypasses the snapshot cache,
  // which still returns a snapshot that a rollback deleted.
  private def snapshotFile(id: Long): Option[Snapshot] = {
    val path = table.snapshotManager().snapshotPath(id)
    try {
      Some(SnapshotManager.fromPath(table.fileIO(), path))
    } catch {
      case e: RuntimeException if e.getCause.isInstanceOf[FileNotFoundException] => None
    }
  }

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
