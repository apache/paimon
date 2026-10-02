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

import org.apache.paimon.errors.ErrorMessages
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.schema.PaimonMetadataColumn.ROW_ID_COLUMN
import org.apache.paimon.spark.util.OptionUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, EqualTo, Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Filter, Project, SupportsSubquery}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.paimon.shims.SparkShimLoader

/** V1 UPDATE command for data-evolution tables, implemented through partial-column MERGE. */
case class UpdatePaimonDataEvolutionTableCommand(
    relation: DataSourceV2Relation,
    v2Table: SparkTable,
    condition: Expression,
    alignedExpressions: Seq[(Expression, Attribute)])
  extends PaimonLeafRunnableCommand
  with SupportsSubquery
  with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val maxAttempts = math.max(1, OptionUtils.dataEvolutionUpdateConflictRetryMaxAttempts())
    val retryWaitMs = math.max(0L, OptionUtils.dataEvolutionUpdateConflictRetryWaitMs())
    val canRetry = deterministicUpdate
    var attempt = 1

    while (attempt < maxAttempts) {
      try {
        return runOnce(sparkSession)
      } catch {
        case e: RuntimeException if canRetry && isDataEvolutionUpdateConflict(e) =>
          val nextAttempt = attempt + 1
          logInfo(
            s"Retry Spark V1 UPDATE for data-evolution table after concurrent update conflict " +
              s"(next attempt $nextAttempt/$maxAttempts).")
          sleepBeforeRetry(retryWaitMs)
          attempt += 1
      }
    }

    runOnce(sparkSession)
  }

  private def runOnce(sparkSession: SparkSession): Seq[Row] = {
    val (updateTable, updateRelation) =
      MergeIntoPaimonDataEvolutionTable.withMatchedUpdateScanOptions(v2Table, relation)
    val targetRowId = rowIdAttribute(updateRelation)
    val matchedActionCondition = conditionAsMatchedAction
    val sourceTable = updatedRowIdSource(
      updateTable,
      updateRelation,
      targetRowId,
      filterSource = !matchedActionCondition)
    val sourceRowId = sourceTable.output.head.asInstanceOf[AttributeReference]

    val matchedCondition = EqualTo(targetRowId, sourceRowId)
    val updateAction = SparkShimLoader.shim.createUpdateAction(
      if (matchedActionCondition) Some(condition) else None,
      alignedExpressions.map { case (expression, attribute) => Assignment(attribute, expression) })

    MergeIntoPaimonDataEvolutionTable(
      updateTable,
      updateRelation,
      sourceTable,
      matchedCondition,
      Seq(updateAction),
      Nil,
      Nil,
      allowPinnedSelfMergeShortcut = true).run(sparkSession)
  }

  private def deterministicUpdate: Boolean = {
    condition.deterministic && alignedExpressions.forall {
      case (expression, _) =>
        expression.deterministic
    }
  }

  private def sleepBeforeRetry(retryWaitMs: Long): Unit = {
    if (retryWaitMs > 0) {
      try {
        Thread.sleep(retryWaitMs)
      } catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new RuntimeException(
            "Interrupted while retrying data-evolution UPDATE conflict.",
            e)
      }
    }
  }

  private def isDataEvolutionUpdateConflict(e: Throwable): Boolean = {
    var current = e
    while (current != null) {
      val message = current.getMessage
      if (
        message != null &&
        message.contains(ErrorMessages.DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE)
      ) {
        return true
      }
      current = current.getCause
    }
    false
  }

  /**
   * Whether the WHERE condition is carried as the WHEN MATCHED condition of the self-merge instead
   * of as a Filter on the source side.
   *
   * With the condition on the action, the source is `Project(PaimonRelation)` and
   * [[MergeIntoPaimonDataEvolutionTable]] takes its self-merge shortcut: one scan of the files the
   * condition can touch (pruned by file statistics), no self-join, no shuffle and no sort. Rows
   * that fail the condition are copied through unchanged, which is exactly `UPDATE ... WHERE`.
   *
   * The Filter shape is kept when the condition cannot be evaluated inside `MergeRows`:
   *   - a subquery can only be planned on a regular scan;
   *   - attributes that do not belong to the relation (e.g. the read-side CHAR padding Project the
   *     analyzer inserts on top of it) would be unresolved in the merge plan;
   *   - a condition without column references (a constant, or e.g. `rand() < 0.1`) gains nothing
   *     from file pruning, and a constant-false one would rewrite every file as a no-op.
   */
  private def conditionAsMatchedAction: Boolean = {
    if (condition == TrueLiteral || SubqueryExpression.hasSubquery(condition)) {
      return false
    }
    val relationAttributes = relation.output ++ relation.metadataOutput
    val references = condition.references.toSeq
    references.nonEmpty &&
    references.forall(attr => relationAttributes.exists(_.exprId == attr.exprId))
  }

  private def updatedRowIdSource(
      updateTable: SparkTable,
      updateRelation: DataSourceV2Relation,
      targetRowId: AttributeReference,
      filterSource: Boolean): Project = {
    val conditionReferences =
      if (filterSource) {
        condition.references.toSeq.collect { case attr: AttributeReference => attr }
      } else {
        Seq.empty
      }
    val readOutput = deduplicateByExprId(conditionReferences :+ targetRowId)
    val sourceScan =
      SparkShimLoader.shim.copyDataSourceV2Relation(updateRelation, updateTable, readOutput)
    // When the condition stays on the source side, keep the Filter visible: the data-evolution
    // MERGE command uses a self-merge shortcut for Project(PaimonRelation), and that shortcut has
    // no place for a source Filter, so it would update every row.
    val filteredSource =
      if (filterSource && condition != TrueLiteral) Filter(condition, sourceScan) else sourceScan

    Project(Seq(Alias(targetRowId, ROW_ID_COLUMN)()), filteredSource)
  }

  private def rowIdAttribute(relation: DataSourceV2Relation): AttributeReference = {
    (relation.output ++ relation.metadataOutput)
      .collectFirst {
        case attr: AttributeReference if attr.name == ROW_ID_COLUMN => attr
      }
      .getOrElse(throw new RuntimeException(
        s"Cannot find $ROW_ID_COLUMN metadata column for data-evolution UPDATE."))
  }

  private def deduplicateByExprId(attributes: Seq[AttributeReference]): Seq[AttributeReference] = {
    attributes
      .foldLeft(Seq.empty[AttributeReference]) {
        case (deduplicated, attr) if deduplicated.exists(_.exprId == attr.exprId) => deduplicated
        case (deduplicated, attr) => deduplicated :+ attr
      }
  }
}
