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
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.format.blob.BlobFileFormat.isBlobFile
import org.apache.paimon.io.{DataFileMeta, DataIncrement}
import org.apache.paimon.operation.commit.RowIdExistenceConflictException
import org.apache.paimon.spark.util.ScanPlanHelper
import org.apache.paimon.table.{FileStoreTable, SpecialFields}
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.VectorType.isVectorStoreFile
import org.apache.paimon.utils.{ExceptionUtils, Range, RetryWaiter}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.resolver
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable

/** Rebase staged partial-column files onto current row-id file boundaries. */
private[spark] class DataEvolutionRowIdConflictRewriter(
    table: FileStoreTable,
    targetRelation: DataSourceV2Relation)
  extends ScanPlanHelper {

  import DataEvolutionRowIdConflictRewriter._

  def rewrite(
      sparkSession: SparkSession,
      latestSnapshot: Snapshot,
      commitMessages: Seq[CommitMessage]): Option[RewriteResult] = {
    if (table.coreOptions().deletionVectorsEnabled()) {
      return None
    }

    val messageImpls = commitMessages.collect { case message: CommitMessageImpl => message }
    if (messageImpls.size != commitMessages.size) {
      return None
    }

    val stagedFiles = messageImpls.flatMap(
      message =>
        message
          .newFilesIncrement()
          .newFiles()
          .asScala
          .map(file => StagedFile(message, file)))
    val nextRowId = Option(latestSnapshot.nextRowId()).map(_.longValue()).getOrElse(return None)

    if (
      stagedFiles.exists(
        staged =>
          isDedicatedFile(staged.file) && staged.file.firstRowId() != null &&
            staged.file.firstRowId() < nextRowId)
    ) {
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
    val currentFiles = currentSplits.flatMap(
      split =>
        split
          .dataFiles()
          .asScala
          .filter(isNormalRowIdFile)
          .map(file => CurrentFile(split, file)))
    val currentExactRanges = currentFiles.map(file => rangeKey(file.split, file.file)).toSet
    val candidates = stagedFiles.filter(
      staged =>
        isRewriteCandidate(staged.file, nextRowId) &&
          !currentExactRanges.contains(rangeKey(staged.message, staged.file)))

    if (candidates.isEmpty || !rangesAreStillCovered(currentFiles, candidates)) {
      return None
    }

    val rewrittenMessages = candidates
      .groupBy(staged => staged.file.writeCols().asScala.toSeq)
      .toSeq
      .flatMap {
        case (columnNames, files) =>
          rewriteFiles(sparkSession, columnNames, files, currentSplits)
      }

    val candidateKeys = candidates.map(staged => fileKey(staged.message, staged.file)).toSet
    val remainingMessages =
      messageImpls.flatMap(message => withoutCandidates(message, candidateKeys))
    Some(RewriteResult(remainingMessages ++ rewrittenMessages, candidates.size))
  }

  private def rewriteFiles(
      sparkSession: SparkSession,
      columnNames: Seq[String],
      stagedFiles: Seq[StagedFile],
      currentSplits: Seq[DataSplit]): Seq[CommitMessage] = {
    val stagedSplits = stagedFiles.map(
      staged =>
        DataSplit
          .builder()
          .withPartition(staged.message.partition())
          .withBucket(staged.message.bucket())
          .withTotalBuckets(staged.message.totalBuckets())
          .withBucketPath(
            table
              .store()
              .pathFactory()
              .bucketPath(staged.message.partition(), staged.message.bucket())
              .toString)
          .withDataFiles(java.util.Collections.singletonList(staged.file))
          .rawConvertible(true)
          .build())
    val affectedSplits = currentSplits.flatMap(
      split => {
        val filtered = split.filterDataFile(
          file =>
            isNormalRowIdFile(file) && stagedFiles.exists(
              staged =>
                sameBucket(split, staged.message) &&
                  file.nonNullRowIdRange().hasIntersection(staged.file.nonNullRowIdRange())))
        if (filtered.isPresent) Some(filtered.get()) else None
      })
    val firstRowIds: immutable.IndexedSeq[Long] = affectedSplits
      .flatMap(_.dataFiles().asScala)
      .filter(isNormalRowIdFile)
      .map(_.firstRowId().longValue())
      .distinct
      .sorted
      .toIndexedSeq

    val relationAttributes = (targetRelation.output ++ targetRelation.metadataOutput).collect {
      case attribute: AttributeReference => attribute
    }
    def attribute(name: String): AttributeReference = {
      relationAttributes
        .find(attr => resolver(attr.name, name))
        .getOrElse(throw new RuntimeException(s"Cannot find column $name for row-id rewrite."))
    }

    val rowIdAttribute = attribute(ROW_ID_NAME)
    val readOutput = columnNames.map(attribute) :+ rowIdAttribute
    def readRows(splits: Seq[DataSplit]) = {
      val relation = createNewScanPlan(splits, targetRelation)
      val readPlan =
        SparkShimLoader.shim.copyDataSourceV2Relation(relation, relation.table, readOutput)
      createDataset(sparkSession, readPlan)
        .select((columnNames.map(quotedColumn) :+ quotedColumn(ROW_ID_NAME)): _*)
    }

    val stagedRows = readRows(stagedSplits)
    val currentRows = readRows(affectedSplits)
    // A new compacted row-id range may contain rows outside the staged file. Preserve their
    // latest values and only replace rows for which the staged update has a value.
    val mergedRows = currentRows
      .join(stagedRows.select(quotedColumn(ROW_ID_NAME)), Seq(ROW_ID_NAME), "left_anti")
      .unionByName(stagedRows)
      .select((columnNames.map(quotedColumn) :+ quotedColumn(ROW_ID_NAME)): _*)
    val firstRowIdUdf = udf((rowId: Long) => floorBinarySearch(firstRowIds, rowId))
    val rewrittenRows = mergedRows
      .withColumn(FIRST_ROW_ID_NAME, firstRowIdUdf(quotedColumn(ROW_ID_NAME)))
      .repartition(col(FIRST_ROW_ID_NAME))
      .sortWithinPartitions(FIRST_ROW_ID_NAME, ROW_ID_NAME)

    DataEvolutionPaimonWriter(table, affectedSplits)
      .writePartialFields(rewrittenRows, columnNames)
  }

  private def rangesAreStillCovered(
      currentFiles: Seq[CurrentFile],
      candidates: Seq[StagedFile]): Boolean = {
    val currentRanges = currentFiles
      .groupBy(current => bucketKey(current.split))
      .map {
        case (key, files) =>
          key -> Range.sortAndMergeOverlap(files.map(_.file.nonNullRowIdRange()).asJava, true)
      }
    candidates.forall(
      candidate => {
        val ranges = currentRanges.getOrElse(
          bucketKey(candidate.message),
          java.util.Collections.emptyList[Range]())
        candidate.file.nonNullRowIdRange().exclude(ranges).isEmpty
      })
  }

  private def withoutCandidates(
      message: CommitMessageImpl,
      candidates: Set[FileKey]): Option[CommitMessage] = {
    val increment = message.newFilesIncrement()
    val newFiles = increment
      .newFiles()
      .asScala
      .filterNot(file => candidates.contains(fileKey(message, file)))
      .asJava
    val remaining = new CommitMessageImpl(
      message.partition(),
      message.bucket(),
      message.totalBuckets(),
      new DataIncrement(
        newFiles,
        increment.deletedFiles(),
        increment.changelogFiles(),
        increment.newIndexFiles(),
        increment.deletedIndexFiles()),
      message.compactIncrement()
    )
    if (remaining.isEmpty) None else Some(remaining)
  }
}

private[spark] object DataEvolutionRowIdConflictRewriter {

  private val ROW_ID_NAME = "_ROW_ID"
  private val FIRST_ROW_ID_NAME = "_FIRST_ROW_ID"

  private case class StagedFile(message: CommitMessageImpl, file: DataFileMeta)

  private case class CurrentFile(split: DataSplit, file: DataFileMeta)

  private case class BucketKey(partition: BinaryRow, bucket: Int)

  private case class FileKey(partition: BinaryRow, bucket: Int, fileName: String)

  private case class RangeKey(partition: BinaryRow, bucket: Int, firstRowId: Long, rowCount: Long)

  case class RewriteResult(commitMessages: Seq[CommitMessage], rewrittenFileCount: Int)

  private def isRewriteCandidate(file: DataFileMeta, nextRowId: Long): Boolean = {
    isNormalRowIdFile(file) &&
    file.firstRowId() < nextRowId &&
    Option(file.writeCols()).exists(
      columns =>
        !columns.isEmpty && columns.asScala.forall(column => !SpecialFields.isSystemField(column)))
  }

  private def isNormalRowIdFile(file: DataFileMeta): Boolean = {
    file.firstRowId() != null && !isDedicatedFile(file)
  }

  private def isDedicatedFile(file: DataFileMeta): Boolean = {
    isBlobFile(file.fileName()) || isVectorStoreFile(file.fileName())
  }

  private def bucketKey(split: DataSplit): BucketKey = {
    BucketKey(split.partition(), split.bucket())
  }

  private def bucketKey(message: CommitMessage): BucketKey = {
    BucketKey(message.partition(), message.bucket())
  }

  private def sameBucket(split: DataSplit, message: CommitMessage): Boolean = {
    bucketKey(split) == bucketKey(message)
  }

  private def fileKey(message: CommitMessage, file: DataFileMeta): FileKey = {
    FileKey(message.partition(), message.bucket(), file.fileName())
  }

  private def rangeKey(split: DataSplit, file: DataFileMeta): RangeKey = {
    RangeKey(split.partition(), split.bucket(), file.firstRowId(), file.rowCount())
  }

  private def rangeKey(message: CommitMessage, file: DataFileMeta): RangeKey = {
    RangeKey(message.partition(), message.bucket(), file.firstRowId(), file.rowCount())
  }

  private def quotedColumn(name: String) = {
    col("`" + name.replace("`", "``") + "`")
  }

  private def floorBinarySearch(firstRowIds: immutable.IndexedSeq[Long], rowId: Long): Long = {
    val index =
      java.util.Collections.binarySearch(firstRowIds.map(Long.box).asJava, Long.box(rowId))
    if (index >= 0) {
      firstRowIds(index)
    } else {
      val insertionPoint = -index - 1
      if (insertionPoint == 0) {
        throw new IllegalArgumentException(
          s"Row ID $rowId is less than the first current row ID boundary.")
      }
      firstRowIds(insertionPoint - 1)
    }
  }
}

private[spark] object DataEvolutionRowIdConflictCommitter {

  private val LOG = LoggerFactory.getLogger(getClass)

  def commit(
      sparkSession: SparkSession,
      table: FileStoreTable,
      targetRelation: DataSourceV2Relation,
      writer: PaimonSparkWriter,
      updateMessages: Seq[CommitMessage],
      otherMessages: Seq[CommitMessage],
      readSnapshotId: Long,
      operation: Snapshot.Operation): Unit = {
    var currentUpdateMessages = updateMessages
    var retryCount = 0
    val startMillis = System.currentTimeMillis()
    val options = table.coreOptions()
    val retryWaiter = new RetryWaiter(options.commitMinRetryWait(), options.commitMaxRetryWait())
    val rewriter = new DataEvolutionRowIdConflictRewriter(table, targetRelation)

    val latestBeforeCommit = table.snapshotManager().latestSnapshot()
    if (latestBeforeCommit != null && latestBeforeCommit.id() != readSnapshotId) {
      rewriter.rewrite(sparkSession, latestBeforeCommit, currentUpdateMessages).foreach {
        result =>
          currentUpdateMessages = result.commitMessages
          logRewrite(table, latestBeforeCommit, result)
      }
    }

    while (true) {
      try {
        writer.commit(currentUpdateMessages ++ otherMessages, operation)
        return
      } catch {
        case conflict: RuntimeException if isRowIdExistenceConflict(conflict) =>
          val elapsedBeforeRewrite = System.currentTimeMillis() - startMillis
          if (
            elapsedBeforeRewrite > options.commitTimeout() ||
            retryCount >= options.commitMaxRetries()
          ) {
            throw conflict
          }

          val latestSnapshot = table.snapshotManager().latestSnapshot()
          if (latestSnapshot == null) {
            throw conflict
          }

          val rewriteResult =
            try {
              rewriter.rewrite(sparkSession, latestSnapshot, currentUpdateMessages)
            } catch {
              case rewriteError: RuntimeException =>
                throw new RuntimeException(
                  s"${conflict.getMessage} ${rewriteError.getMessage}",
                  conflict)
            }
          if (rewriteResult.isEmpty) {
            throw conflict
          }

          currentUpdateMessages = rewriteResult.get.commitMessages
          val elapsedMillis = System.currentTimeMillis() - startMillis
          if (elapsedMillis > options.commitTimeout()) {
            throw conflict
          }

          logRewrite(table, latestSnapshot, rewriteResult.get)
          retryWaiter.retryWait(retryCount)
          retryCount += 1
      }
    }
  }

  private def isRowIdExistenceConflict(error: Throwable): Boolean = {
    ExceptionUtils.findThrowable(error, classOf[RowIdExistenceConflictException]).isPresent
  }

  private def logRewrite(
      table: FileStoreTable,
      snapshot: Snapshot,
      result: DataEvolutionRowIdConflictRewriter.RewriteResult): Unit = {
    LOG.info(
      "Rewrote {} stale row-id file(s) against snapshot {} before committing to table {}.",
      Int.box(result.rewrittenFileCount),
      Long.box(snapshot.id()),
      table.name()
    )
  }
}
