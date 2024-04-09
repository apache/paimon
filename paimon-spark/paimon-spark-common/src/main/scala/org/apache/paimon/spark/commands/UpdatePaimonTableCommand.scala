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

import org.apache.paimon.spark.PaimonSplitScan
import org.apache.paimon.spark.catalyst.analysis.AssignmentAlignmentHelper
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.schema.SparkSystemColumns.ROW_KIND_COL
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.types.RowKind

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, If}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Filter, Project, SupportsSubquery}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.functions.lit

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
    val candidateDataSplits = findCandidateDataSplits(condition, relation.output)
    val fileNameToMeta = candidateFileMap(candidateDataSplits)

    val commitMessages = if (candidateDataSplits.isEmpty) {
      // no data spilt need to be rewrote
      logDebug("No file need to rerote. It's an empty Commit.")
      Seq.empty[CommitMessage]
    } else {
      // Step2: extract out the exactly files, which must have at least one record to delete.
      val touchedFilePaths =
        findTouchedFiles(candidateDataSplits, condition, relation, sparkSession)

      // Step3: the smallest range of data files that need to be rewritten.
      val touchedFiles = touchedFilePaths.map {
        file => fileNameToMeta.getOrElse(file, throw new RuntimeException(s"Missing file: $file"))
      }

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
      val touchedDataSplits = SparkDataFileMeta.convertToDataSplits(touchedFiles)
      val toUpdateScanRelation = DataSourceV2ScanRelation(
        relation,
        PaimonSplitScan(table, touchedDataSplits),
        relation.output)
      val data = createDataset(sparkSession, toUpdateScanRelation).select(columns: _*)
      val addCommitMessage = writer.write(data)

      // Step5: convert the deleted files that need to be wrote to commit message.
      val deletedCommitMessage = buildDeletedCommitMessage(touchedFiles)

      addCommitMessage ++ deletedCommitMessage
    }
    commitMessages
  }

}
