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

import org.apache.paimon.spark.catalyst.analysis.AssignmentAlignmentHelper
import org.apache.paimon.spark.schema.PaimonMetadataColumn.{ROW_ID_COLUMN, SEQUENCE_NUMBER_COLUMN}
import org.apache.paimon.spark.schema.SparkSystemColumns.ROW_KIND_COL
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.RowKind

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, If, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.{col, lit}

case class UpdatePaimonTableCommand(
    relation: DataSourceV2Relation,
    override val table: FileStoreTable,
    condition: Expression,
    assignments: Seq[Assignment])
  extends PaimonRowLevelCommand
  with AssignmentAlignmentHelper
  with SupportsSubquery {

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
    val readSnapshot = table.snapshotManager().latestSnapshot()
    // Step1: the candidate data splits which are filtered by Paimon Predicate.
    val candidateDataSplits = findCandidateDataSplits(condition, relation.output)
    val dataFilePathToMeta = candidateFileMap(candidateDataSplits)

    if (candidateDataSplits.isEmpty) {
      // no data spilt need to be rewritten
      logDebug("No file need to rewrote. It's an empty Commit.")
      Seq.empty[CommitMessage]
    } else {
      if (deletionVectorsEnabled) {
        // Step2: collect all the deletion vectors that marks the deleted rows.
        val deletionVectors = collectDeletionVectors(
          candidateDataSplits,
          dataFilePathToMeta,
          condition,
          relation,
          sparkSession)

        deletionVectors.cache()
        try {
          // Step3: write these updated data
          val touchedDataSplits =
            deletionVectors.collect().map(SparkDeletionVector.toDataSplit(_, dataFilePathToMeta))
          val addCommitMessage = writeOnlyUpdatedData(sparkSession, touchedDataSplits)

          // Step4: write these deletion vectors.
          val indexCommitMsg = writer.persistDeletionVectors(deletionVectors, readSnapshot)

          addCommitMessage ++ indexCommitMsg
        } finally {
          deletionVectors.unpersist()
        }
      } else {
        // Step2: extract out the exactly files, which must have at least one record to delete.
        val touchedFilePaths =
          findTouchedFiles(candidateDataSplits, condition, relation, sparkSession)

        // Step3: the smallest range of data files that need to be rewritten.
        val (touchedFiles, touchedFileRelation) =
          extractFilesAndCreateNewScan(touchedFilePaths, dataFilePathToMeta, relation)

        // Step4: build a dataframe that contains the unchanged and updated data, and write out them.
        val addCommitMessage = writeUpdatedAndUnchangedData(sparkSession, touchedFileRelation)

        // Step5: convert the deleted files that need to be written to commit message.
        val deletedCommitMessage = buildDeletedCommitMessage(touchedFiles)

        addCommitMessage ++ deletedCommitMessage
      }
    }
  }

  private def writeOnlyUpdatedData(
      sparkSession: SparkSession,
      touchedDataSplits: Array[DataSplit]): Seq[CommitMessage] = {
    val updateColumns = getUpdateColumns(sparkSession)

    val toUpdateScanRelation = createNewScanPlan(touchedDataSplits, relation, Some(condition))
    val data = createDataset(sparkSession, toUpdateScanRelation).select(updateColumns: _*)
    writer.withRowTracking().write(data)
  }

  private def writeUpdatedAndUnchangedData(
      sparkSession: SparkSession,
      toUpdateScanRelation: LogicalPlan): Seq[CommitMessage] = {
    val updateColumns = getUpdateColumns(sparkSession)

    val data = createDataset(sparkSession, toUpdateScanRelation).select(updateColumns: _*)
    writer.withRowTracking().write(data)
  }

  private def getUpdateColumns(sparkSession: SparkSession): Seq[Column] = {
    var updateColumns = updateExpressions.zip(relation.output).map {
      case (_, origin) if origin.name == ROW_ID_COLUMN => rowIdCol
      case (_, origin) if origin.name == SEQUENCE_NUMBER_COLUMN => sequenceNumberCol(sparkSession)
      case (update, origin) =>
        val updated = optimizedIf(condition, update, origin)
        toColumn(updated).as(origin.name, origin.metadata)
    }

    if (coreOptions.rowTrackingEnabled()) {
      val outputSet = relation.outputSet
      if (!outputSet.exists(_.name == ROW_ID_COLUMN)) {
        updateColumns ++= Seq(rowIdCol)
      }
      if (!outputSet.exists(_.name == SEQUENCE_NUMBER_COLUMN)) {
        updateColumns ++= Seq(sequenceNumberCol(sparkSession))
      }
    }

    updateColumns
  }

  private def rowIdCol = col(ROW_ID_COLUMN)

  private def sequenceNumberCol(sparkSession: SparkSession) = toColumn(
    optimizedIf(condition, Literal(null), toExpression(sparkSession, col(SEQUENCE_NUMBER_COLUMN))))
    .as(SEQUENCE_NUMBER_COLUMN)

  private def optimizedIf(
      predicate: Expression,
      trueValue: Expression,
      falseValue: Expression): Expression = {
    if (predicate == TrueLiteral) {
      trueValue
    } else if (predicate == FalseLiteral) {
      falseValue
    } else {
      If(predicate, trueValue, falseValue)
    }
  }
}
