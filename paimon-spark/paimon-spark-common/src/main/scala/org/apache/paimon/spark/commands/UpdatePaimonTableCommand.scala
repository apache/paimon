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

import org.apache.paimon.index.IndexFileMeta
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement, IndexIncrement}
import org.apache.paimon.spark.PaimonSplitScan
import org.apache.paimon.spark.catalyst.analysis.AssignmentAlignmentHelper
import org.apache.paimon.spark.commands.SparkDataFileMeta.convertToSparkDataFileMeta
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.schema.SparkSystemColumns.ROW_KIND_COL
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.RowKind

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.Utils.createDataset
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, If}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Filter, Project, SupportsSubquery}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.functions.{input_file_name, lit}

import java.net.URI
import java.util.Collections

import scala.collection.JavaConverters._

case class UpdatePaimonTableCommand(
    relation: DataSourceV2Relation,
    override val table: FileStoreTable,
    condition: Expression,
    assignments: Seq[Assignment])
  extends PaimonLeafRunnableCommand
  with PaimonCommand
  with AssignmentAlignmentHelper
  with SupportsSubquery {

  private lazy val writer = PaimonSparkWriter(table)

  private lazy val updateExpressions = {
    generateAlignedExpressions(relation.output, assignments).zip(relation.output).map {
      case (expr, attr) => Alias(expr, attr.name)()
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val commitMessages = if (withPrimaryKeys) {
      performUpdateForPkTable(sparkSession)
    } else {
      performUpdateForNonPkTable(sparkSession)
    }
    writer.commit(commitMessages)

    Seq.empty[Row]
  }

  /** Update for table with primary keys */
  private def performUpdateForPkTable(sparkSession: SparkSession): Seq[CommitMessage] = {
    val updatedPlan = Project(updateExpressions, Filter(condition, relation))
    val df = createDataset(sparkSession, updatedPlan)
      .withColumn(ROW_KIND_COL, lit(RowKind.UPDATE_AFTER.toByteValue))
    writer.write(df)
  }

  /** Update for table without primary keys */
  private def performUpdateForNonPkTable(sparkSession: SparkSession): Seq[CommitMessage] = {
    // Step1: the candidate data splits which are filtered by Paimon Predicate.
    val candidateDataSplits = findCandidateDataSplits()

    val commitMessages = if (candidateDataSplits.isEmpty) {
      // no data spilt need to be rewrote
      logDebug("No file need to rerote. It's an empty Commit.")
      Seq.empty[CommitMessage]
    } else {
      import sparkSession.implicits._

      // Step2: extract out the exactly files, which must contain record to be updated.
      val scan = PaimonSplitScan(table, candidateDataSplits.toArray)
      val filteredRelation =
        Filter(condition, DataSourceV2ScanRelation(relation, scan, relation.output))
      val touchedFilePaths = createDataset(sparkSession, filteredRelation)
        .select(input_file_name())
        .distinct()
        .as[String]
        .collect()
        .map(relativePath)

      // Step3: build a new list of data splits which compose of those files.
      // Those are expected to be the smallest range of data files that need to be rewritten.
      val totalBuckets = table.coreOptions().bucket()
      val candidateDataFiles = candidateDataSplits
        .flatMap(dataSplit => convertToSparkDataFileMeta(dataSplit, totalBuckets))
      val fileStorePathFactory = table.store().pathFactory()
      val fileNameToMeta =
        candidateDataFiles
          .map(file => (file.relativePath(fileStorePathFactory), file))
          .toMap
      val touchedFiles: Array[SparkDataFileMeta] = touchedFilePaths.map {
        file => fileNameToMeta.getOrElse(file, throw new RuntimeException(s"Missing file: $file"))
      }
      val touchedDataSplits = SparkDataFileMeta.convertToDataSplits(touchedFiles)

      // Step4: build a dataframe that contains the unchanged and updated data, and write out them.
      val columns = updateExpressions.zip(relation.output).map {
        case (update, origin) =>
          val updated = if (condition == TrueLiteral) {
            update
          } else {
            If(condition, update, origin)
          }
          new Column(updated).as(origin.name, origin.metadata)
      }
      val toUpdateScanRelation = DataSourceV2ScanRelation(
        relation,
        PaimonSplitScan(table, touchedDataSplits),
        relation.output)
      val data = createDataset(sparkSession, toUpdateScanRelation).select(columns: _*)
      val addCommitMessage = writer.write(data)

      // Step5: convert the files that need to be wrote to commit message.
      val deletedCommitMessage = touchedFiles
        .groupBy(f => (f.partition, f.bucket))
        .map {
          case ((partition, bucket), files) =>
            val bb = files.map(_.dataFileMeta).toList.asJava
            val newFilesIncrement = new DataIncrement(
              Collections.emptyList[DataFileMeta],
              bb,
              Collections.emptyList[DataFileMeta])
            buildCommitMessage(
              new CommitMessageImpl(partition, bucket, newFilesIncrement, null, null))
        }
        .toSeq

      addCommitMessage ++ deletedCommitMessage
    }
    commitMessages
  }

  private def findCandidateDataSplits(): Seq[DataSplit] = {
    val snapshotReader = table.newSnapshotReader()
    if (condition == TrueLiteral) {
      val filter = convertConditionToPaimonPredicate(condition, relation.output, rowType)
      snapshotReader.withFilter(filter)
    }

    snapshotReader.read().splits().asScala.collect { case s: DataSplit => s }
  }

  /** Gets a relative path against the table path. */
  private def relativePath(absolutePath: String): String = {
    val location = table.location().toUri
    location.relativize(new URI(absolutePath)).toString
  }

  private def buildCommitMessage(o: CommitMessageImpl): CommitMessage = {
    new CommitMessageImpl(
      o.partition,
      o.bucket,
      o.newFilesIncrement,
      new CompactIncrement(
        Collections.emptyList[DataFileMeta],
        Collections.emptyList[DataFileMeta],
        Collections.emptyList[DataFileMeta]),
      new IndexIncrement(Collections.emptyList[IndexFileMeta]));
  }
}
