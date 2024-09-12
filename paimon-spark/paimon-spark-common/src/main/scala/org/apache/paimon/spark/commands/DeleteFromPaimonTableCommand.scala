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

import org.apache.paimon.CoreOptions.MergeEngine
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.schema.SparkSystemColumns.ROW_KIND_COL
import org.apache.paimon.spark.util.SQLHelper
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage}
import org.apache.paimon.types.RowKind
import org.apache.paimon.utils.InternalRowPartitionComputer

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
  with SupportsSubquery
  with SQLHelper {

  private lazy val writer = PaimonSparkWriter(table)

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val commit = fileStore.newCommit(UUID.randomUUID.toString)
    if (condition == null || condition == TrueLiteral) {
      commit.truncateTable(BatchWriteBuilder.COMMIT_IDENTIFIER)
    } else {
      val (partitionCondition, otherCondition) = splitPruePartitionAndOtherPredicates(
        condition,
        table.partitionKeys().asScala,
        sparkSession.sessionState.conf.resolver)

      val partitionPredicate = if (partitionCondition.isEmpty) {
        None
      } else {
        convertConditionToPaimonPredicate(
          partitionCondition.reduce(And),
          relation.output,
          table.schema.logicalPartitionType(),
          ignoreFailure = true)
      }

      if (
        otherCondition.isEmpty && partitionPredicate.nonEmpty && !table
          .coreOptions()
          .deleteForceProduceChangelog()
      ) {
        val matchedPartitions =
          table.newSnapshotReader().withPartitionFilter(partitionPredicate.get).partitions().asScala
        val rowDataPartitionComputer = new InternalRowPartitionComputer(
          table.coreOptions().partitionDefaultName(),
          table.schema().logicalPartitionType(),
          table.partitionKeys.asScala.toArray
        )
        val dropPartitions = matchedPartitions.map {
          partition => rowDataPartitionComputer.generatePartValues(partition).asScala.asJava
        }
        if (dropPartitions.nonEmpty) {
          commit.dropPartitions(dropPartitions.asJava, BatchWriteBuilder.COMMIT_IDENTIFIER)
        } else {
          writer.commit(Seq.empty)
        }
      } else {
        val commitMessages = if (usePrimaryKeyDelete()) {
          performPrimaryKeyDelete(sparkSession)
        } else {
          performNonPrimaryKeyDelete(sparkSession)
        }
        writer.commit(commitMessages)
      }
    }

    Seq.empty[Row]
  }

  def usePrimaryKeyDelete(): Boolean = {
    withPrimaryKeys && table.coreOptions().mergeEngine() == MergeEngine.DEDUPLICATE
  }

  def performPrimaryKeyDelete(sparkSession: SparkSession): Seq[CommitMessage] = {
    val df = createDataset(sparkSession, Filter(condition, relation))
      .withColumn(ROW_KIND_COL, lit(RowKind.DELETE.toByteValue))
    writer.write(df)
  }

  def performNonPrimaryKeyDelete(sparkSession: SparkSession): Seq[CommitMessage] = {
    // Step1: the candidate data splits which are filtered by Paimon Predicate.
    val candidateDataSplits = findCandidateDataSplits(condition, relation.output)
    val dataFilePathToMeta = candidateFileMap(candidateDataSplits)

    if (deletionVectorsEnabled) {
      // Step2: collect all the deletion vectors that marks the deleted rows.
      val deletionVectors = collectDeletionVectors(
        candidateDataSplits,
        dataFilePathToMeta,
        condition,
        relation,
        sparkSession)

      // Step3: update the touched deletion vectors and index files
      writer.persistDeletionVectors(deletionVectors)
    } else {
      // Step2: extract out the exactly files, which must have at least one record to be updated.
      val touchedFilePaths =
        findTouchedFiles(candidateDataSplits, condition, relation, sparkSession)

      // Step3: the smallest range of data files that need to be rewritten.
      val (touchedFiles, newRelation) =
        createNewRelation(touchedFilePaths, dataFilePathToMeta, relation)

      // Step4: build a dataframe that contains the unchanged data, and write out them.
      val toRewriteScanRelation = Filter(Not(condition), newRelation)
      val data = createDataset(sparkSession, toRewriteScanRelation)

      // only write new files, should have no compaction
      val addCommitMessage = writer.writeOnly().write(data)

      // Step5: convert the deleted files that need to be wrote to commit message.
      val deletedCommitMessage = buildDeletedCommitMessage(touchedFiles)

      addCommitMessage ++ deletedCommitMessage
    }
  }

}
