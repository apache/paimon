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
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement}
import org.apache.paimon.spark.util.ScanPlanHelper
import org.apache.paimon.table.{FileStoreTable, SpecialFields}
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.{DataSplit, IncrementalSplit}
import org.apache.paimon.types.VectorType.isVectorStoreFile
import org.apache.paimon.utils.Range

import org.apache.spark.sql.{functions, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.resolver
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import java.util.{Collections, List => JList, Optional => JOptional}

import scala.collection.JavaConverters._

/** Rebases MERGE-compatible partial-column files onto staged compact output boundaries. */
class DataEvolutionCompactMergeConflictRewriter(
    table: FileStoreTable,
    targetRelation: DataSourceV2Relation)
  extends ScanPlanHelper {

  import DataEvolutionCompactMergeConflictRewriter._

  def rewrite(
      sparkSession: SparkSession,
      baseSnapshot: Snapshot,
      latestSnapshot: Snapshot,
      compactMessages: JList[CommitMessage]): JOptional[JList[CommitMessage]] = {
    if (
      table.coreOptions().deletionVectorsEnabled() ||
      latestSnapshot.schemaId() != baseSnapshot.schemaId() ||
      latestSnapshot.id() <= baseSnapshot.id()
    ) {
      return JOptional.empty()
    }

    val messageImpls = compactMessages.asScala.collect {
      case message: CommitMessageImpl => message
    }
    if (messageImpls.size != compactMessages.size()) {
      return JOptional.empty()
    }

    val targets = messageImpls.flatMap(
      message =>
        normalRowIdFiles(message.compactIncrement().compactAfter().asScala)
          .map(file => CompactTarget(message, file)))
    if (targets.isEmpty) {
      return JOptional.empty()
    }

    // Snapshot.operation is optional and Python MERGE currently does not persist it. Validate
    // the portable partial-column file contract below instead.
    val additions = table
      .newSnapshotReader()
      .withSnapshot(latestSnapshot)
      .readIncrementalDiff(baseSnapshot)
      .splits()
      .asScala
      .collect { case split: IncrementalSplit => split }
      .flatMap(
        split =>
          split
            .afterFiles()
            .asScala
            .map(file => AddedFile(split.partition(), split.bucket(), file)))

    val overlappingAdditions = additions.filter(addition => targets.exists(_.intersects(addition)))
    if (overlappingAdditions.isEmpty) {
      return JOptional.empty()
    }
    if (overlappingAdditions.exists(addition => !isRegularPartialFile(addition.file))) {
      return JOptional.empty()
    }
    if (overlappingAdditions.exists(addition => targets.count(_.contains(addition)) != 1)) {
      return JOptional.empty()
    }

    val targetRewrites = targets.flatMap {
      target =>
        val files = overlappingAdditions.filter(target.contains)
        if (files.nonEmpty && files.exists(file => file.file.nonNullRowIdRange() != target.range)) {
          val updatedFields = table
            .rowType()
            .getFieldNames
            .asScala
            .filter(name => files.exists(_.file.writeCols().contains(name)))
            .toSeq
          if (updatedFields.isEmpty) {
            return JOptional.empty()
          }
          Some(TargetRewrite(target, files.toSeq, updatedFields))
        } else {
          None
        }
    }
    if (targetRewrites.isEmpty) {
      return JOptional.empty()
    }

    val currentSplits = table
      .newSnapshotReader()
      .withSnapshot(latestSnapshot)
      .read()
      .splits()
      .asScala
      .collect { case split: DataSplit => split }
      .toSeq

    val rewrittenMessages = targetRewrites
      .groupBy(_.updatedFields)
      .toSeq
      .flatMap {
        case (updatedFields, rewrites) =>
          rewriteFiles(sparkSession, updatedFields, rewrites.toSeq, currentSplits)
      }

    JOptional.of((messageImpls ++ rewrittenMessages).map(_.asInstanceOf[CommitMessage]).asJava)
  }

  private def rewriteFiles(
      sparkSession: SparkSession,
      updatedFields: Seq[String],
      rewrites: Seq[TargetRewrite],
      currentSplits: Seq[DataSplit]): Seq[CommitMessageImpl] = {
    val relevantSplits = currentSplits.flatMap {
      split =>
        val filtered = split.filterDataFile(
          file =>
            isNormalRowIdFile(file) && rewrites.exists(
              rewrite =>
                rewrite.target.sameBucket(split.partition(), split.bucket()) &&
                  rewrite.target.range.hasIntersection(file.nonNullRowIdRange())))
        if (filtered.isPresent) Some(filtered.get()) else None
    }

    val relationAttributes = (targetRelation.output ++ targetRelation.metadataOutput).collect {
      case attribute: AttributeReference => attribute
    }
    def attribute(name: String): AttributeReference = {
      relationAttributes
        .find(attr => resolver(attr.name, name))
        .getOrElse(throw new RuntimeException(s"Cannot find column $name for compact rebase."))
    }

    val rowIdAttribute = attribute(ROW_ID_NAME)
    val readOutput = updatedFields.map(attribute) :+ rowIdAttribute
    val relation = createNewScanPlan(relevantSplits, targetRelation)
    val readPlan =
      SparkShimLoader.shim.copyDataSourceV2Relation(relation, relation.table, readOutput)
    val targetRanges = rewrites.map(_.target.range).toArray
    val rowIdFilter = targetRanges
      .map(range => col(ROW_ID_NAME).between(range.from, range.to))
      .reduce(_ or _)
    val firstRowId = udf(
      (rowId: Long) =>
        targetRanges
          .find(range => range.from <= rowId && rowId <= range.to)
          .map(_.from)
          .getOrElse(
            throw new IllegalArgumentException(s"Row ID $rowId is outside staged compact ranges.")))
    val rewrittenRows = createDataset(sparkSession, readPlan)
      .select((updatedFields.map(quotedColumn) :+ quotedColumn(ROW_ID_NAME)): _*)
      .filter(rowIdFilter)
      .withColumn(FIRST_ROW_ID_NAME, firstRowId(quotedColumn(ROW_ID_NAME)))
      .repartition(col(FIRST_ROW_ID_NAME))
      .sortWithinPartitions(FIRST_ROW_ID_NAME, ROW_ID_NAME)

    val targetSplits = rewrites.map {
      rewrite =>
        val target = rewrite.target
        DataSplit
          .builder()
          .withPartition(target.message.partition())
          .withBucket(target.message.bucket())
          .withTotalBuckets(target.message.totalBuckets())
          .withBucketPath(
            table
              .store()
              .pathFactory()
              .bucketPath(target.message.partition(), target.message.bucket())
              .toString)
          .withDataFiles(Collections.singletonList(target.file))
          .rawConvertible(true)
          .build()
    }

    val written = DataEvolutionPaimonWriter(table, targetSplits).writePartialFields(
      rewrittenRows,
      updatedFields)
    written.map {
      case message: CommitMessageImpl =>
        val newFiles = normalRowIdFiles(message.newFilesIncrement().newFiles().asScala)
        if (newFiles.size != message.newFilesIncrement().newFiles().size()) {
          throw new UnsupportedOperationException(
            "Compact MERGE conflict rebase does not support dedicated files.")
        }
        val target = rewrites
          .find(
            rewrite =>
              rewrite.target.sameBucket(message.partition(), message.bucket()) &&
                newFiles.forall(_.nonNullRowIdRange() == rewrite.target.range))
          .getOrElse(throw new IllegalStateException(
            s"Cannot match rebased files $newFiles to a staged compact range."))
        new CommitMessageImpl(
          message.partition(),
          message.bucket(),
          message.totalBuckets(),
          DataIncrement.emptyIncrement(),
          new CompactIncrement(
            target.mergeFiles.map(_.file).asJava,
            newFiles.asJava,
            Collections.emptyList())
        )
      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported compact MERGE conflict commit message: $other")
    }
  }

}

private object DataEvolutionCompactMergeConflictRewriter {

  private val ROW_ID_NAME = "_ROW_ID"
  private val FIRST_ROW_ID_NAME = "_FIRST_ROW_ID"

  private case class AddedFile(partition: BinaryRow, bucket: Int, file: DataFileMeta)

  private case class CompactTarget(message: CommitMessageImpl, file: DataFileMeta) {

    val range: Range = file.nonNullRowIdRange()

    def sameBucket(partition: BinaryRow, bucket: Int): Boolean = {
      message.partition() == partition && message.bucket() == bucket
    }

    def intersects(added: AddedFile): Boolean = {
      sameBucket(added.partition, added.bucket) &&
      added.file.firstRowId() != null &&
      range.hasIntersection(added.file.nonNullRowIdRange())
    }

    def contains(added: AddedFile): Boolean = {
      sameBucket(added.partition, added.bucket) && containsRange(added.file.nonNullRowIdRange())
    }

    def containsRange(other: Range): Boolean = {
      range.from <= other.from && other.to <= range.to
    }
  }

  private case class TargetRewrite(
      target: CompactTarget,
      mergeFiles: Seq[AddedFile],
      updatedFields: Seq[String])

  private def normalRowIdFiles(files: Iterable[DataFileMeta]): Seq[DataFileMeta] = {
    files.filter(isNormalRowIdFile).toSeq
  }

  private def isNormalRowIdFile(file: DataFileMeta): Boolean = {
    file.firstRowId() != null && !isBlobFile(file.fileName()) && !isVectorStoreFile(file.fileName())
  }

  private def isRegularPartialFile(file: DataFileMeta): Boolean = {
    isNormalRowIdFile(file) &&
    file.writeCols() != null &&
    !file.writeCols().isEmpty &&
    file.writeCols().asScala.forall(column => !SpecialFields.isSystemField(column))
  }

  private def quotedColumn(name: String) = {
    functions.col("`" + name.replace("`", "``") + "`")
  }
}
