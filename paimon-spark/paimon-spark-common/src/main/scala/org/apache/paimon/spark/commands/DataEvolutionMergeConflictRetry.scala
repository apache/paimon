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

package org.apache.paimon.spark.commands

import org.apache.paimon.Snapshot
import org.apache.paimon.Snapshot.CommitKind
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.errors.ErrorMessages
import org.apache.paimon.format.blob.BlobFileFormat.isBlobFile
import org.apache.paimon.io.{DataFileMeta, DataIncrement}
import org.apache.paimon.operation.commit.RowIdColumnConflictChecker
import org.apache.paimon.schema.TableSchema
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.{DataSplit, ScanMode}
import org.apache.paimon.types.VectorType.isVectorStoreFile
import org.apache.paimon.utils.{DataEvolutionUtils, Range, RowRangeIndex}

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/** Select complete current file groups whose MERGE expressions must be evaluated again. */
private[commands] object DataEvolutionMergeConflictRetry {

  private val LOG = LoggerFactory.getLogger(getClass)

  case class Plan(
      remainingMessages: Seq[CommitMessage],
      dataSplits: Seq[DataSplit],
      currentSplits: Seq[DataSplit],
      discardedMessages: Seq[CommitMessage])

  /** Only for files known not to have been published; never use after an uncertain commit. */
  def abortUnpublished(
      table: FileStoreTable,
      messages: Seq[CommitMessage],
      failure: Throwable): Unit = {
    if (messages.nonEmpty) {
      try {
        val commit = table.newBatchWriteBuilder().newCommit()
        try {
          commit.abort(messages.asJava)
        } finally {
          commit.close()
        }
      } catch {
        case NonFatal(cleanupFailure) =>
          if (cleanupFailure ne failure) failure.addSuppressed(cleanupFailure)
          LOG.warn("Failed to abort unpublished MERGE files.", cleanupFailure)
      }
    }
  }

  private case class FileRange(bucket: (BinaryRow, Int), range: Range)

  private def indexes(ranges: Seq[FileRange]): Map[(BinaryRow, Int), RowRangeIndex] = {
    ranges.groupBy(_.bucket).map {
      case (bucket, files) => bucket -> RowRangeIndex.create(files.map(_.range).asJava)
    }
  }

  private def intersects(index: Map[(BinaryRow, Int), RowRangeIndex], range: FileRange): Boolean = {
    index.get(range.bucket).exists(_.intersects(range.range.from, range.range.to))
  }

  def isUpdateConflict(error: Throwable): Boolean = {
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists {
      cause =>
        Option(cause.getMessage).exists(
          _.contains(ErrorMessages.DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE))
    }
  }

  def plan(
      table: FileStoreTable,
      messages: Seq[CommitMessage],
      readSnapshotId: Long,
      latestSnapshot: Snapshot,
      includeConcurrentChanges: Boolean = false): Option[Plan] = {
    plan(table, messages, readSnapshotId, latestSnapshot, includeConcurrentChanges, None)
  }

  def plan(
      table: FileStoreTable,
      messages: Seq[CommitMessage],
      readSnapshotId: Long,
      latestSnapshot: Snapshot,
      includeConcurrentChanges: Boolean,
      readFieldIds: Option[Set[Int]]): Option[Plan] = {
    if (readSnapshotId < 0 || table.coreOptions().deletionVectorsEnabled()) {
      return None
    }
    val impls = messages.collect { case message: CommitMessageImpl => message }
    if (
      impls.size != messages.size || impls.exists {
        message =>
          val increment = message.newFilesIncrement()
          !message.compactIncrement().isEmpty ||
          !increment.deletedFiles().isEmpty || !increment.changelogFiles().isEmpty ||
          !increment.newIndexFiles().isEmpty || !increment.deletedIndexFiles().isEmpty
      }
    ) {
      return None
    }
    val nextRowId = table.snapshotManager().snapshot(readSnapshotId).nextRowId()
    val stagedFiles = impls.flatMap(_.newFilesIncrement().newFiles().asScala)
    if (
      nextRowId == null || stagedFiles.isEmpty || stagedFiles.exists(
        file =>
          file.firstRowId() == null || file.firstRowId() >= nextRowId ||
            isBlobFile(file.fileName()) || isVectorStoreFile(file.fileName()))
    ) {
      return None
    }

    val partitions = impls.map(_.partition()).distinct.asJava
    val changes = (readSnapshotId + 1 to latestSnapshot.id()).flatMap {
      id =>
        val snapshot = table.snapshotManager().snapshot(id)
        if (snapshot.commitKind() == CommitKind.COMPACT) Nil
        else {
          val scan = table.store().newScan()
          if (!includeConcurrentChanges) {
            scan.withPartitionFilter(partitions)
          }
          scan
            .withSnapshot(snapshot)
            .withKind(ScanMode.DELTA)
            .plan()
            .files()
            .asScala
            .filter(
              entry =>
                entry.file().firstRowId() != null &&
                  (includeConcurrentChanges || entry.file().firstRowId() < nextRowId))
        }
    }
    val checkers = changes.groupBy(entry => (entry.partition(), entry.bucket())).map {
      case (bucket, entries) =>
        bucket -> RowIdColumnConflictChecker.fromDataFiles(
          table.schemaManager(),
          entries.map(_.file()).asJava,
          table.coreOptions().dataEvolutionNestedFieldEnabled())
    }
    val stagedRanges = impls.flatMap {
      message =>
        message.newFilesIncrement().newFiles().asScala.map {
          file =>
            (
              file.fileName(),
              FileRange((message.partition(), message.bucket()), file.nonNullRowIdRange()))
        }
    }.toMap
    var affected = impls.flatMap {
      message =>
        message
          .newFilesIncrement()
          .newFiles()
          .asScala
          .filter {
            file =>
              checkers.get((message.partition(), message.bucket())).exists(_.conflictsWith(file))
          }
          .map(_.fileName())
    }.toSet
    if (affected.isEmpty) {
      return None
    }

    val currentSplits = table
      .newSnapshotReader()
      .withSnapshot(latestSnapshot)
      .read()
      .splits()
      .asScala
      .collect { case split: DataSplit => split }
      .toSeq
    val currentRanges = currentSplits.flatMap {
      split =>
        split
          .dataFiles()
          .asScala
          .filter(file => !isBlobFile(file.fileName()) && !isVectorStoreFile(file.fileName()))
          .map(file => FileRange((split.partition(), split.bucket()), file.nonNullRowIdRange()))
    }.distinct
    // An upsert's formerly unmatched source rows can now match newly inserted rows, including
    // rows in partitions absent from the original update messages. Changes to an ON column can
    // also turn an old match into an insert. Reevaluate those target groups as well.
    val schemas = mutable.HashMap.empty[Long, TableSchema]
    val changedIndex = indexes(if (includeConcurrentChanges) {
      changes
        .filter {
          entry =>
            val file = entry.file()
            file.firstRowId() >= nextRowId || readFieldIds.forall {
              ids =>
                val schema = schemas
                  .getOrElseUpdate(file.schemaId(), table.schemaManager().schema(file.schemaId()))
                DataEvolutionUtils.fileFieldIds(schema, file).asScala.exists(id => ids.contains(id))
            }
        }
        .map(
          entry => FileRange((entry.partition(), entry.bucket()), entry.file().nonNullRowIdRange()))
    } else Nil)

    // A compacted group can include another staged file. Expand to a fixed point so that
    // replacing that file never drops its updates outside the initially conflicted range.
    var selectedRanges = Seq.empty[FileRange]
    var expanded = true
    while (expanded) {
      val affectedIndex = indexes(affected.toSeq.map(stagedRanges))
      selectedRanges = currentRanges.filter(
        range => intersects(affectedIndex, range) || intersects(changedIndex, range))
      val selectedIndex = indexes(selectedRanges)
      val selectedFiles = affected ++ stagedRanges.collect {
        case (name, range) if intersects(selectedIndex, range) => name
      }.toSet
      expanded = selectedFiles != affected
      affected = selectedFiles
    }
    val coverage = indexes(selectedRanges)
    if (
      affected.exists {
        name =>
          val range = stagedRanges(name)
          !coverage.get(range.bucket).exists(_.contains(range.range))
      }
    ) {
      return None
    }

    val splits = currentSplits.flatMap {
      split =>
        val selected = split.filterDataFile {
          file =>
            intersects(
              coverage,
              FileRange((split.partition(), split.bucket()), file.nonNullRowIdRange()))
        }
        if (selected.isPresent) Some(selected.get()) else None
    }
    def selectMessages(discarded: Boolean): Seq[CommitMessage] = impls.flatMap {
      message =>
        val files = message
          .newFilesIncrement()
          .newFiles()
          .asScala
          .filter(file => affected.contains(file.fileName()) == discarded)
        if (files.isEmpty) None
        else
          Some(
            new CommitMessageImpl(
              message.partition(),
              message.bucket(),
              message.totalBuckets(),
              new DataIncrement(
                files.asJava,
                java.util.Collections.emptyList[DataFileMeta](),
                java.util.Collections.emptyList[DataFileMeta]()),
              message.compactIncrement()
            ))
    }
    Some(Plan(selectMessages(false), splits, currentSplits, selectMessages(true)))
  }
}
