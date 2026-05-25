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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.commands.{MergeIntoPaimonDataEvolutionTable, MergeIntoPaimonTable}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.JavaConverters._

trait PaimonMergeIntoBase
  extends Rule[LogicalPlan]
  with RowLevelHelper
  with ExpressionHelper
  with AssignmentAlignmentHelper
  with MergeSchemaEvolutionHelper {

  val spark: SparkSession

  override val operation: RowLevelOp = MergeInto

  def apply(plan: LogicalPlan): LogicalPlan = {
    // Spark 4.1 marks the plan analyzed before postHoc runs, so `transformDown` is needed to
    // bypass `resolveOperators`'s short-circuit. Pure append-only tables on 4.1+ are handled
    // earlier by `Spark41MergeIntoRewrite` and never reach here.
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case merge: MergeIntoTable
            if merge.resolved && PaimonRelation.isPaimonTable(merge.targetTable) =>
          val relation = PaimonRelation.getPaimonRelation(merge.targetTable)
          var v2Table = relation.table.asInstanceOf[SparkTable]

          checkPaimonTable(v2Table.getTable)
          checkCondition(merge.mergeCondition)
          (merge.matchedActions ++ merge.notMatchedActions)
            .flatMap(_.condition)
            .foreach(checkCondition)

          val primaryKeys = v2Table.getTable.primaryKeys().asScala.toSeq
          if (primaryKeys.nonEmpty) {
            val updateActions = merge.matchedActions.collect { case a: UpdateAction => a }
            checkUpdateActionValidity(
              AttributeSet(relation.output),
              merge.mergeCondition,
              updateActions,
              primaryKeys)
          }

          // Commit schema changes before alignment so the aligned plan sees new columns.
          val (resolvedMerge, targetOutput) =
            evolveTargetIfNeeded(merge, relation, v2Table, spark, resolveNotMatchedBySourceActions)
              .map { case (m, r, t) => v2Table = t; (m, r.output) }
              .getOrElse((merge, relation.output))

          val aligned = alignMergeIntoTable(resolvedMerge, targetOutput)

          if (!shouldFallbackToV1MergeInto(aligned)) {
            aligned
          } else {
            buildV1Command(v2Table, resolvedMerge, aligned)
          }
      }
    }
  }

  private def buildV1Command(
      v2Table: SparkTable,
      resolvedMerge: MergeIntoTable,
      aligned: MergeIntoTable): LogicalPlan = {
    val notMatchedBySource = resolveNotMatchedBySourceActions(aligned)
    if (v2Table.coreOptions.dataEvolutionEnabled()) {
      MergeIntoPaimonDataEvolutionTable(
        v2Table,
        resolvedMerge.targetTable,
        resolvedMerge.sourceTable,
        resolvedMerge.mergeCondition,
        aligned.matchedActions,
        aligned.notMatchedActions,
        notMatchedBySource
      )
    } else {
      MergeIntoPaimonTable(
        v2Table,
        resolvedMerge.targetTable,
        resolvedMerge.sourceTable,
        resolvedMerge.mergeCondition,
        aligned.matchedActions,
        aligned.notMatchedActions,
        notMatchedBySource
      )
    }
  }

  private def checkCondition(condition: Expression): Unit = {
    if (!condition.resolved) {
      throw new RuntimeException(s"Condition $condition should have been resolved.")
    }
    if (SubqueryExpression.hasSubquery(condition)) {
      throw new RuntimeException(s"Condition $condition with subquery can't be supported.")
    }
  }

  /** This check will avoid to update the primary key columns */
  private def checkUpdateActionValidity(
      targetOutput: AttributeSet,
      mergeCondition: Expression,
      actions: Seq[UpdateAction],
      primaryKeys: Seq[String]): Unit = {
    // Check whether there are enough `EqualTo` expressions related to all the primary keys.
    lazy val isMergeConditionValid = {
      val mergeExpressions = splitConjunctivePredicates(mergeCondition)
      primaryKeys.forall {
        primaryKey => isUpdateExpressionToPrimaryKey(targetOutput, mergeExpressions, primaryKey)
      }
    }

    // Check whether there are an update expression related to any primary key.
    def isUpdateActionValid(action: UpdateAction): Boolean = {
      validUpdateAssignment(targetOutput, primaryKeys, action.assignments)
    }

    val valid = isMergeConditionValid || actions.forall(isUpdateActionValid)
    if (!valid) {
      throw new RuntimeException("Can't update the primary key column in update clause.")
    }
  }

  def resolveNotMatchedBySourceActions(merge: MergeIntoTable): Seq[MergeAction]

  def alignMergeIntoTable(m: MergeIntoTable, targetOutput: Seq[Attribute]): MergeIntoTable
}
