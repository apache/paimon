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
import org.apache.paimon.index.IndexFileMeta
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement, IndexIncrement}
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.commands.SparkDataFileMeta.convertToSparkDataFileMeta
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.spark.schema.PaimonMetadataColumn._
import org.apache.paimon.table.{BucketMode, FileStoreTable, KnownSplitsTable}
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Filter => FilterLogicalNode, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources._

import java.net.URI
import java.util.Collections

import scala.collection.JavaConverters._

/** Helper trait for all paimon commands. */
trait PaimonCommand extends WithFileStoreTable with ExpressionHelper with SQLConfHelper {

  lazy val dvSafeWriter: PaimonSparkWriter = {
    if (table.primaryKeys().isEmpty && table.bucketMode() == BucketMode.HASH_FIXED) {

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

    val filteredRelation = createNewScanPlan(candidateDataSplits, condition, relation)
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
      .map(relativePath)
  }

  protected def createNewScanPlan(
      candidateDataSplits: Seq[DataSplit],
      condition: Expression,
      relation: DataSourceV2Relation,
      metadataColumns: Seq[PaimonMetadataColumn]): LogicalPlan = {
    val newRelation = createNewScanPlan(candidateDataSplits, condition, relation)
    val resolvedMetadataColumns = metadataColumns.map {
      col =>
        val attr = newRelation.resolve(col.name :: Nil, conf.resolver)
        assert(attr.isDefined)
        attr.get
    }
    Project(relation.output ++ resolvedMetadataColumns, newRelation)
  }

  protected def createNewScanPlan(
      candidateDataSplits: Seq[DataSplit],
      condition: Expression,
      relation: DataSourceV2Relation): LogicalPlan = {
    val newRelation = createNewRelation(candidateDataSplits, relation)
    FilterLogicalNode(condition, newRelation)
  }

  protected def createNewRelation(
      filePaths: Array[String],
      filePathToMeta: Map[String, SparkDataFileMeta],
      relation: DataSourceV2Relation): (Array[SparkDataFileMeta], DataSourceV2Relation) = {
    val files = filePaths.map(
      file => filePathToMeta.getOrElse(file, throw new RuntimeException(s"Missing file: $file")))
    val touchedDataSplits =
      SparkDataFileMeta.convertToDataSplits(files, rawConvertible = true, fileStore.pathFactory())
    val newRelation = createNewRelation(touchedDataSplits, relation)
    (files, newRelation)
  }

  protected def createNewRelation(
      splits: Seq[DataSplit],
      relation: DataSourceV2Relation): DataSourceV2Relation = {
    assert(relation.table.isInstanceOf[SparkTable])
    val sparkTable = relation.table.asInstanceOf[SparkTable]
    assert(sparkTable.table.isInstanceOf[FileStoreTable])
    val knownSplitsTable =
      KnownSplitsTable.create(sparkTable.table.asInstanceOf[FileStoreTable], splits.toArray)
    // We re-plan the relation to skip analyze phase, so we should append all
    // metadata columns manually and let Spark do column pruning during optimization.
    relation.copy(
      table = relation.table.asInstanceOf[SparkTable].copy(table = knownSplitsTable),
      output = relation.output ++ sparkTable.metadataColumns.map(
        _.asInstanceOf[PaimonMetadataColumn].toAttribute)
    )
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

  protected def collectDeletionVectors(
      candidateDataSplits: Seq[DataSplit],
      dataFilePathToMeta: Map[String, SparkDataFileMeta],
      condition: Expression,
      relation: DataSourceV2Relation,
      sparkSession: SparkSession): Dataset[SparkDeletionVector] = {
    val filteredRelation = createNewScanPlan(candidateDataSplits, condition, relation)
    val dataWithMetadataColumns = createDataset(sparkSession, filteredRelation)
    collectDeletionVectors(dataFilePathToMeta, dataWithMetadataColumns, sparkSession)
  }

  protected def collectDeletionVectors(
      dataFilePathToMeta: Map[String, SparkDataFileMeta],
      dataWithMetadataColumns: Dataset[Row],
      sparkSession: SparkSession): Dataset[SparkDeletionVector] = {
    import sparkSession.implicits._

    val resolver = sparkSession.sessionState.conf.resolver
    Seq(FILE_PATH_COLUMN, ROW_INDEX_COLUMN).foreach {
      metadata =>
        dataWithMetadataColumns.schema
          .find(field => resolver(field.name, metadata))
          .orElse(throw new RuntimeException(
            "This input dataset doesn't contains the required metadata columns: __paimon_file_path and __paimon_row_index."))
    }

    val dataFileToPartitionAndBucket =
      dataFilePathToMeta.mapValues(meta => (meta.partition, meta.bucket)).toArray

    val my_table = table
    val location = my_table.location
    val dvBitmap64 = my_table.coreOptions().deletionVectorBitmap64()
    dataWithMetadataColumns
      .select(FILE_PATH_COLUMN, ROW_INDEX_COLUMN)
      .as[(String, Long)]
      .groupByKey(_._1)
      .mapGroups {
        (filePath, iter) =>
          val dv =
            if (dvBitmap64) new Bitmap64DeletionVector() else new BitmapDeletionVector()
          while (iter.hasNext) {
            dv.delete(iter.next()._2)
          }

          val relativeFilePath = location.toUri.relativize(new URI(filePath)).toString
          val (partition, bucket) = dataFileToPartitionAndBucket.toMap.apply(relativeFilePath)
          val pathFactory = my_table.store().pathFactory()
          val relativeBucketPath = pathFactory
            .relativeBucketPath(partition, bucket)
            .toString

          SparkDeletionVector(
            relativeBucketPath,
            SerializationUtils.serializeBinaryRow(partition),
            bucket,
            new Path(filePath).getName,
            DeletionVector.serializeToBytes(dv)
          )
      }
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
              Collections.emptyList[DataFileMeta]),
            new IndexIncrement(Collections.emptyList[IndexFileMeta])
          )
      }
      .toSeq
  }
}
