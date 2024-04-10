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
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.predicate.PartitionKeyVisitor
import org.apache.paimon.spark.PaimonSplitScan
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.schema.SparkSystemColumns.ROW_KIND_COL
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage}
import org.apache.paimon.types.RowKind
import org.apache.paimon.utils.RowDataPartitionComputer

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Not}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Filter, SupportsSubquery}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.lit

import java.util.UUID

import scala.collection.JavaConverters._

case class DeleteFromPaimonTableCommand(
    relation: DataSourceV2Relation,
    override val table: FileStoreTable,
    condition: Expression)
  extends PaimonLeafRunnableCommand
  with PaimonCommand
  with ExpressionHelper
  with SupportsSubquery {

  private lazy val writer = PaimonSparkWriter(table)

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val commit = table.store.newCommit(UUID.randomUUID.toString)
    if (condition == null || condition == TrueLiteral) {
      commit.truncateTable(BatchWriteBuilder.COMMIT_IDENTIFIER)
    } else {
      val (partitionCondition, otherCondition) = splitPruePartitionAndOtherPredicates(
        condition,
        table.partitionKeys().asScala,
        sparkSession.sessionState.conf.resolver)

      val visitor =
        new PartitionKeyVisitor(table.partitionKeys, table.newReadBuilder.newScan.listPartitions)

      val (partitionPredicate, candidatePartitions) = if (partitionCondition.isEmpty) {
        (None, List.empty[BinaryRow])
      } else {
        val predicate = convertConditionToPaimonPredicate(
          partitionCondition.reduce(And),
          relation.output,
          rowType,
          ignoreFailure = true)
        (predicate, predicate.map(_.visit(visitor).asScala.toList).getOrElse(List.empty[BinaryRow]))
      }

      // We have to scan table if the following three requirements are met:
      // 1) non-partitioned table;
      // 2) no partition predicate;
      // 3) all the data in candidate partitions should be deleted;
      val forceDeleteByRows =
        otherCondition.nonEmpty || partitionPredicate.isEmpty || !visitor.isAccurate

      if (forceDeleteByRows) {
        val commitMessages = if (withPrimaryKeys) {
          performDeleteForPkTable(sparkSession)
        } else {
          performDeleteForNonPkTable(sparkSession)
        }
        writer.commit(commitMessages)
      } else {
        val rowDataPartitionComputer = new RowDataPartitionComputer(
          CoreOptions.PARTITION_DEFAULT_NAME.defaultValue,
          table.schema().logicalPartitionType(),
          table.partitionKeys.asScala.toArray
        )
        commit.dropPartitions(
          candidatePartitions.map {
            partition => rowDataPartitionComputer.generatePartValues(partition).asScala.asJava
          }.asJava,
          BatchWriteBuilder.COMMIT_IDENTIFIER)
      }
    }

    Seq.empty[Row]
  }

  def performDeleteForPkTable(sparkSession: SparkSession): Seq[CommitMessage] = {
    val df = createDataset(sparkSession, Filter(condition, relation))
      .withColumn(ROW_KIND_COL, lit(RowKind.DELETE.toByteValue))
    writer.write(df)
  }

  def performDeleteForNonPkTable(sparkSession: SparkSession): Seq[CommitMessage] = {
    // Step1: the candidate data splits which are filtered by Paimon Predicate.
    val candidateDataSplits = findCandidateDataSplits(condition, relation.output)
    val fileNameToMeta = candidateFileMap(candidateDataSplits)

    // Step2: extract out the exactly files, which must have at least one record to be updated.
    val touchedFilePaths = findTouchedFiles(candidateDataSplits, condition, relation, sparkSession)

    // Step3: the smallest range of data files that need to be rewritten.
    val touchedFiles = touchedFilePaths.map {
      file => fileNameToMeta.getOrElse(file, throw new RuntimeException(s"Missing file: $file"))
    }

    // Step4: build a dataframe that contains the unchanged data, and write out them.
    val touchedDataSplits = SparkDataFileMeta.convertToDataSplits(touchedFiles)
    val toRewriteScanRelation = Filter(
      Not(condition),
      Compatibility.createDataSourceV2ScanRelation(
        relation,
        PaimonSplitScan(table, touchedDataSplits),
        relation.output))
    val data = createDataset(sparkSession, toRewriteScanRelation)
    val addCommitMessage = writer.write(data)

    // Step5: convert the deleted files that need to be wrote to commit message.
    val deletedCommitMessage = buildDeletedCommitMessage(touchedFiles)

    addCommitMessage ++ deletedCommitMessage
  }

}
