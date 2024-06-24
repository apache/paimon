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

import org.apache.paimon.deletionvectors.{BitmapDeletionVector, DeletionVector}
import org.apache.paimon.fs.Path
import org.apache.paimon.index.IndexFileMeta
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement, IndexIncrement}
import org.apache.paimon.manifest.IndexManifestEntry
import org.apache.paimon.spark.{PaimonSplitScan, SparkFilterConverter}
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.commands.SparkDataFileMeta.convertToSparkDataFileMeta
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.spark.schema.PaimonMetadataColumn._
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Filter => FilterLogicalNode, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.{AlwaysTrue, And, EqualNullSafe, Filter}

import java.net.URI
import java.util.Collections

import scala.collection.JavaConverters._

/** Helper trait for all paimon commands. */
trait PaimonCommand extends WithFileStoreTable with ExpressionHelper {

  /**
   * For the 'INSERT OVERWRITE' semantics of SQL, Spark DataSourceV2 will call the `truncate`
   * methods where the `AlwaysTrue` Filter is used.
   */
  def isTruncate(filter: Filter): Boolean = {
    val filters = splitConjunctiveFilters(filter)
    filters.length == 1 && filters.head.isInstanceOf[AlwaysTrue]
  }

  /**
   * For the 'INSERT OVERWRITE T PARTITION (partitionVal, ...)' semantics of SQL, Spark will
   * transform `partitionVal`s to EqualNullSafe Filters.
   */
  def convertFilterToMap(filter: Filter, partitionRowType: RowType): Map[String, String] = {
    val converter = new SparkFilterConverter(partitionRowType)
    splitConjunctiveFilters(filter).map {
      case EqualNullSafe(attribute, value) =>
        if (isNestedFilterInValue(value)) {
          throw new RuntimeException(
            s"Not support the complex partition value in EqualNullSafe when run `INSERT OVERWRITE`.")
        } else {
          (attribute, converter.convertLiteral(attribute, value).toString)
        }
      case _ =>
        throw new RuntimeException(
          s"Only EqualNullSafe should be used when run `INSERT OVERWRITE`.")
    }.toMap
  }

  private def splitConjunctiveFilters(filter: Filter): Seq[Filter] = {
    filter match {
      case And(filter1, filter2) =>
        splitConjunctiveFilters(filter1) ++ splitConjunctiveFilters(filter2)
      case other => other :: Nil
    }
  }

  private def isNestedFilterInValue(value: Any): Boolean = {
    value.isInstanceOf[Filter]
  }

  /** Gets a relative path against the table path. */
  protected def relativePath(absolutePath: String): String = {
    val location = table.location().toUri
    location.relativize(new URI(absolutePath)).toString
  }

  protected def findCandidateDataSplits(
      condition: Expression,
      output: Seq[Attribute]): Seq[DataSplit] = {
    // low level snapshot reader, it can not be affected by 'scan.mode'
    val snapshotReader = table.newSnapshotReader()
    if (condition == TrueLiteral) {
      val filter =
        convertConditionToPaimonPredicate(condition, output, rowType, ignoreFailure = true)
      filter.foreach(snapshotReader.withFilter)
    }

    snapshotReader.read().splits().asScala.collect { case s: DataSplit => s }
  }

  protected def findTouchedFiles(
      candidateDataSplits: Seq[DataSplit],
      condition: Expression,
      relation: DataSourceV2Relation,
      sparkSession: SparkSession): Array[String] = {
    import sparkSession.implicits._

    for (split <- candidateDataSplits) {
      if (!split.rawConvertible()) {
        throw new IllegalArgumentException(
          "Only compacted table can generate touched files, please use 'COMPACT' procedure first.");
      }
    }

    val metadataCols = Seq(FILE_PATH)
    val filteredRelation = createNewScanPlan(candidateDataSplits, condition, relation, metadataCols)
    createDataset(sparkSession, filteredRelation)
      .select(FILE_PATH_COLUMN)
      .distinct()
      .as[String]
      .collect()
      .map(relativePath)
  }

  /** Notice that, the key is a relative path, not just the file name. */
  protected def candidateFileMap(
      candidateDataSplits: Seq[DataSplit]): Map[String, SparkDataFileMeta] = {
    val totalBuckets = coreOptions.bucket()
    val candidateDataFiles = candidateDataSplits
      .flatMap(dataSplit => convertToSparkDataFileMeta(dataSplit, totalBuckets))
    val fileStorePathFactory = fileStore.pathFactory()
    candidateDataFiles
      .map(file => (file.relativePath(fileStorePathFactory), file))
      .toMap
  }

  protected def getDeletedIndexFiles(
      dataFilePathToMeta: Map[String, SparkDataFileMeta],
      newDeletionVectors: Dataset[SparkDeletionVectors]
  ): Seq[IndexManifestEntry] = {
    val ts1 = System.currentTimeMillis()
    val deletionFiles = dataFilePathToMeta.flatMap {
      case (relativePath, sdf) =>
        sdf.deletionFile match {
          case Some(deletionFile) =>
            Some((relativePath, deletionFile))
          case None => None
        }
    }
    val dvIndexFileMaintainer = fileStore
      .newIndexFileHandler()
      .createDVIndexFileMaintainer(deletionFiles.asJava)
    val ts2 = System.currentTimeMillis()
    logInfo(s"initialize dv file maintainer in ${ts2 - ts1} ms.")

    val pathFactory = fileStore.pathFactory()
    val touchedDataFileAndDeletionFiles = newDeletionVectors
      .collect()
      .flatMap {
        sdv =>
          val relativePaths = sdv.relativePaths(pathFactory)
          relativePaths.flatMap {
            relativePath =>
              dataFilePathToMeta(relativePath).deletionFile match {
                case Some(deletionFile) => Some(relativePath, deletionFile)
                case _ => None
              }
          }
      }
      .toMap
    val ts3 = System.currentTimeMillis()
    logInfo(s"Collect the touched deletion files in ${ts3 - ts2} ms.")

    dvIndexFileMaintainer.notifyDeletionFiles(touchedDataFileAndDeletionFiles.asJava)
    val ts4 = System.currentTimeMillis()
    logInfo(s"Notify the touched deletion files in ${ts4 - ts3} ms.")

    val entries = dvIndexFileMaintainer.writeUnchangedDeletionVector().asScala
    val ts5 = System.currentTimeMillis()
    logInfo(s"Write the unchanged deletion vectors in ${ts5 - ts4} ms.")
    entries
  }
  protected def collectDeletionVectors(
      candidateDataSplits: Seq[DataSplit],
      dataFilePathToMeta: Map[String, SparkDataFileMeta],
      condition: Expression,
      relation: DataSourceV2Relation,
      sparkSession: SparkSession): Dataset[SparkDeletionVectors] = {
    import sparkSession.implicits._

    val dataFileAndDeletionFile = dataFilePathToMeta.mapValues(_.toSparkDeletionFile).toArray
    val metadataCols = Seq(FILE_PATH, ROW_INDEX)
    val filteredRelation = createNewScanPlan(candidateDataSplits, condition, relation, metadataCols)

    val store = table.store()
    val fileIO = table.fileIO()
    val location = table.location
    createDataset(sparkSession, filteredRelation)
      .select(FILE_PATH_COLUMN, ROW_INDEX_COLUMN)
      .as[(String, Long)]
      .groupByKey(_._1)
      .mapGroups {
        case (filePath, iter) =>
          val fileNameToDeletionFile = dataFileAndDeletionFile.toMap
          val dv = new BitmapDeletionVector()
          while (iter.hasNext) {
            dv.delete(iter.next()._2)
          }

          val relativeFilePath = location.toUri.relativize(new URI(filePath)).toString
          val sparkDeletionFile = fileNameToDeletionFile(relativeFilePath)
          sparkDeletionFile.deletionFile match {
            case Some(deletionFile) =>
              dv.merge(DeletionVector.read(fileIO, deletionFile))
            case None =>
          }

          val pathFactory = store.pathFactory()
          val partitionAndBucket = pathFactory
            .relativePartitionAndBucketPath(sparkDeletionFile.partition, sparkDeletionFile.bucket)
            .toString

          SparkDeletionVectors(
            partitionAndBucket,
            SerializationUtils.serializeBinaryRow(sparkDeletionFile.partition),
            sparkDeletionFile.bucket,
            Seq((new Path(filePath).getName, dv.serializeToBytes()))
          )
      }
  }

  private def createNewScanPlan(
      candidateDataSplits: Seq[DataSplit],
      condition: Expression,
      relation: DataSourceV2Relation,
      metadataCols: Seq[PaimonMetadataColumn]): LogicalPlan = {
    val metadataProj = metadataCols.map(_.toAttribute)
    val newRelation = relation.copy(output = relation.output ++ metadataProj)
    val scan = PaimonSplitScan(table, candidateDataSplits.toArray, metadataCols)
    Project(
      metadataProj,
      FilterLogicalNode(
        condition,
        Compatibility.createDataSourceV2ScanRelation(newRelation, scan, newRelation.output)))

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
            new DataIncrement(
              Collections.emptyList[DataFileMeta],
              deletedDataFileMetas,
              Collections.emptyList[DataFileMeta]),
            new CompactIncrement(
              Collections.emptyList[DataFileMeta],
              Collections.emptyList[DataFileMeta],
              Collections.emptyList[DataFileMeta]),
            new IndexIncrement(Collections.emptyList[IndexFileMeta])
          )
      }
      .toSeq
  }
}
