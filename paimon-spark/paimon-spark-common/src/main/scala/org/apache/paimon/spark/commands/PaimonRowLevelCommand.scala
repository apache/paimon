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

import org.apache.paimon.CoreOptions
import org.apache.paimon.deletionvectors.{Bitmap64DeletionVector, BitmapDeletionVector, DeletionVector}
import org.apache.paimon.fs.Path
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement}
import org.apache.paimon.operation.DataEvolutionSplitRead
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.commands.SparkDataFileMeta.convertToSparkDataFileMeta
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.schema.PaimonMetadataColumn._
import org.apache.paimon.spark.util.ScanPlanHelper
import org.apache.paimon.table.BucketMode
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.col

import java.util.Collections
import java.util.function.Function

import scala.collection.JavaConverters._

/** Helper trait for paimon row level command, like delete, update, merge into. */
trait PaimonRowLevelCommand
  extends PaimonLeafRunnableCommand
  with WithFileStoreTable
  with ExpressionHelper
  with ScanPlanHelper
  with SQLConfHelper {

  lazy val writer: PaimonSparkWriter = {
    if (table.primaryKeys().isEmpty && table.bucketMode() == BucketMode.HASH_FIXED) {
      // todo: fix out why fix bucket non-pk table need write only
      /**
       * Writer without compaction, note that some operations may generate Deletion Vectors, and
       * writing may occur at the same time as generating deletion vectors. If compaction occurs at
       * this time, it will cause the file that deletion vectors are working on to no longer exist,
       * resulting in an error.
       *
       * For example: Update bucketed append table with deletion vectors enabled
       */
      PaimonSparkWriter(table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true")))
    } else {
      PaimonSparkWriter(table)
    }
  }

  protected def findCandidateDataSplits(
      condition: Expression,
      output: Seq[Attribute]): Seq[DataSplit] = {
    // low level snapshot reader, it can not be affected by 'scan.mode'
    val snapshotReader = table.newSnapshotReader()
    // dropStats after filter push down
    if (table.coreOptions().manifestDeleteFileDropStats()) {
      snapshotReader.dropStats()
    }
    if (condition != TrueLiteral) {
      val filter =
        convertConditionToPaimonPredicate(condition, output, rowType, ignorePartialFailure = true)
      filter.foreach(snapshotReader.withFilter)
    }

    snapshotReader.read().splits().asScala.collect { case s: DataSplit => s }.toSeq
  }

  protected def findTouchedFiles(
      candidateDataSplits: Seq[DataSplit],
      condition: Expression,
      relation: DataSourceV2Relation,
      sparkSession: SparkSession): Array[String] = {
    for (split <- candidateDataSplits) {
      if (!split.rawConvertible()) {
        throw new IllegalArgumentException(
          "Only compacted table can generate touched files, please use 'COMPACT' procedure first.");
      }
    }

    val filteredRelation = createNewScanPlan(candidateDataSplits, relation, Some(condition))
    findTouchedFiles(createDataset(sparkSession, filteredRelation), sparkSession)
  }

  protected def findTouchedFiles(
      dataset: Dataset[Row],
      sparkSession: SparkSession,
      identifier: String = FILE_PATH_COLUMN): Array[String] = {
    import sparkSession.implicits._
    dataset
      .select(identifier)
      .distinct()
      .as[String]
      .collect()
  }

  protected def extractFilesAndCreateNewScan(
      filePaths: Array[String],
      filePathToMeta: Map[String, SparkDataFileMeta],
      relation: DataSourceV2Relation): (Array[SparkDataFileMeta], LogicalPlan) = {
    val files = filePaths.map(
      file => filePathToMeta.getOrElse(file, throw new RuntimeException(s"Missing file: $file")))
    val touchedDataSplits =
      SparkDataFileMeta.convertToDataSplits(files, rawConvertible = true, fileStore.pathFactory())
    val newRelation = createNewScanPlan(touchedDataSplits, relation)
    (files, newRelation)
  }

  /** Notice that, the key is a file path, not just the file name. */
  protected def candidateFileMap(
      candidateDataSplits: Seq[DataSplit]): Map[String, SparkDataFileMeta] = {
    val totalBuckets = coreOptions.bucket()
    val candidateDataFiles = candidateDataSplits
      .flatMap(dataSplit => convertToSparkDataFileMeta(dataSplit, totalBuckets))
    candidateDataFiles
      .map(file => (file.filePath(), file))
      .toMap
  }

  protected def collectDeletionVectors(
      candidateDataSplits: Seq[DataSplit],
      dataFilePathToMeta: Map[String, SparkDataFileMeta],
      condition: Expression,
      relation: DataSourceV2Relation,
      sparkSession: SparkSession,
      dataEvolutionEnabled: Boolean = false): Dataset[SparkDeletionVector] = {
    val filteredRelation =
      createNewScanPlan(candidateDataSplits, relation, Some(condition))
    val dataset = createDataset(sparkSession, filteredRelation)
    val deletionTargets =
      if (dataEvolutionEnabled) {
        dataEvolutionDeletionTargets(candidateDataSplits, dataset, sparkSession)
      } else {
        pathAndIndexDeletionTargets(dataset, sparkSession)
      }
    buildDeletionVectors(dataFilePathToMeta, deletionTargets, sparkSession)
  }

  /**
   * Collect deletion targets for data evolution tables: find all [AnchorFile, Deletion position]
   * pairs. For deletion positions, we simply use `recordRowId - anchorFileRangeStart` to get the
   * offset, instead of calculating returned positions.
   */
  private def dataEvolutionDeletionTargets(
      candidateDataSplits: Seq[DataSplit],
      dataset: Dataset[Row],
      sparkSession: SparkSession): Dataset[PaimonRowLevelCommand.DeletionTarget] = {
    import sparkSession.implicits._

    val anchorRanges = dataEvolutionAnchorRanges(candidateDataSplits)
    val broadcastAnchorRanges = sparkSession.sparkContext.broadcast(anchorRanges)

    dataset
      .select(col(ROW_ID_COLUMN))
      .as[Long]
      .map {
        rowId =>
          val (filePath, rowIndex) =
            PaimonRowLevelCommand.rowIdToPathAndIndex(rowId, broadcastAnchorRanges.value)
          PaimonRowLevelCommand.DeletionTarget(filePath, rowIndex)
      }
  }

  protected def collectDeletionVectors(
      dataFilePathToMeta: Map[String, SparkDataFileMeta],
      dataset: Dataset[Row],
      sparkSession: SparkSession): Dataset[SparkDeletionVector] = {
    buildDeletionVectors(
      dataFilePathToMeta,
      pathAndIndexDeletionTargets(dataset, sparkSession),
      sparkSession)
  }

  protected def collectDataEvolutionDeletionVectors(
      candidateDataSplits: Seq[DataSplit],
      dataFilePathToMeta: Map[String, SparkDataFileMeta],
      dataset: Dataset[Row],
      sparkSession: SparkSession): Dataset[SparkDeletionVector] = {
    buildDeletionVectors(
      dataFilePathToMeta,
      dataEvolutionDeletionTargets(candidateDataSplits, dataset, sparkSession),
      sparkSession)
  }

  private def pathAndIndexDeletionTargets(
      dataset: Dataset[Row],
      sparkSession: SparkSession): Dataset[PaimonRowLevelCommand.DeletionTarget] = {
    import sparkSession.implicits._
    dataset
      .select(PATH_AND_INDEX_META_COLUMNS.map(col): _*)
      .as[(String, Long)]
      .map {
        case (filePath, rowIndex) =>
          PaimonRowLevelCommand.DeletionTarget(filePath, rowIndex)
      }
  }

  private def buildDeletionVectors(
      dataFilePathToMeta: Map[String, SparkDataFileMeta],
      deletionTargets: Dataset[PaimonRowLevelCommand.DeletionTarget],
      sparkSession: SparkSession): Dataset[SparkDeletionVector] = {
    import sparkSession.implicits._
    // convert to a serializable map
    val dataFileNameToPartitionAndBucket = dataFilePathToMeta.map {
      case (k, v) =>
        new Path(k).getName -> (v.bucketPath, v.partition, v.bucket, k)
    }

    val my_table = table
    val dvBitmap64 = my_table.coreOptions().deletionVectorBitmap64()
    deletionTargets
      .groupByKey(_.filePath)
      .mapGroups {
        (filePath, iter) =>
          val dv =
            if (dvBitmap64) new Bitmap64DeletionVector() else new BitmapDeletionVector()
          while (iter.hasNext) {
            dv.delete(iter.next().rowIndex)
          }

          val (bucketPath, partition, bucket, dataFilePath) =
            dataFileNameToPartitionAndBucket.apply(new Path(filePath).getName)
          SparkDeletionVector(
            bucketPath,
            SerializationUtils.serializeBinaryRow(partition),
            bucket,
            dataFilePath,
            DeletionVector.serializeToBytes(dv)
          )
      }
  }

  private def dataEvolutionAnchorRanges(
      candidateDataSplits: Seq[DataSplit]): Array[PaimonRowLevelCommand.AnchorRange] = {
    val identity =
      new Function[DataFileMeta, DataFileMeta] {
        override def apply(file: DataFileMeta): DataFileMeta = file
      }

    candidateDataSplits
      .flatMap {
        split =>
          DataEvolutionSplitRead.mergeRangesAndSort(split.dataFiles()).asScala.map {
            group =>
              val anchor = retrieveAnchorFile(group, identity)
              val range = anchor.nonNullRowIdRange()
              val anchorFilePath =
                if (anchor.externalPath().isPresent) {
                  anchor.externalPath().get()
                } else {
                  split.bucketPath() + "/" + anchor.fileName()
                }
              PaimonRowLevelCommand.AnchorRange(range.from, range.to, anchorFilePath)
          }
      }
      .sortBy(_.from)
      .toArray
  }

  protected def buildDeletedCommitMessage(
      deletedFiles: Array[SparkDataFileMeta]): Seq[CommitMessage] = {
    deletedFiles
      .groupBy(f => (f.partition, f.bucket))
      .map {
        case ((partition, bucket), files) =>
          val deletedDataFileMetas = files.map(_.dataFileMeta).toList.asJava

          new CommitMessageImpl(
            partition,
            bucket,
            files.head.totalBuckets,
            new DataIncrement(
              Collections.emptyList[DataFileMeta],
              deletedDataFileMetas,
              Collections.emptyList[DataFileMeta]),
            new CompactIncrement(
              Collections.emptyList[DataFileMeta],
              Collections.emptyList[DataFileMeta],
              Collections.emptyList[DataFileMeta])
          )
      }
      .toSeq
  }
}

object PaimonRowLevelCommand {

  /** A data-evolution anchor file and its covered row-id range. */
  final private[commands] case class AnchorRange(from: Long, to: Long, filePath: String)

  /** A deletion target represented by data file path and local row index. */
  final private[commands] case class DeletionTarget(filePath: String, rowIndex: Long)

  /** Binary search to lookup anchor file & range for each row id. */
  private[commands] def rowIdToPathAndIndex(
      rowId: Long,
      anchorRanges: Array[AnchorRange]): (String, Long) = {
    var low = 0
    var high = anchorRanges.length - 1
    var candidate = -1

    while (low <= high) {
      val mid = (low + high) >>> 1
      if (anchorRanges(mid).from <= rowId) {
        candidate = mid
        low = mid + 1
      } else {
        high = mid - 1
      }
    }

    if (candidate < 0 || rowId > anchorRanges(candidate).to) {
      throw new IllegalStateException(
        s"Cannot find data-evolution deletion-vector anchor range for row id $rowId.")
    }

    val anchor = anchorRanges(candidate)
    (anchor.filePath, rowId - anchor.from)
  }
}
