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

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.commands.MergeIntoPaimonTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

trait PaimonMergeIntoBase
  extends Rule[LogicalPlan]
  with RowLevelHelper
  with ExpressionHelper
  with AssignmentAlignmentHelper {

  val spark: SparkSession

  override val operation: RowLevelOp = MergeInto

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case merge: MergeIntoTable
          if merge.resolved && PaimonRelation.isPaimonTable(merge.targetTable) =>
        val relation = PaimonRelation.getPaimonRelation(merge.targetTable)
        val v2Table = relation.table.asInstanceOf[SparkTable]
        val targetOutput = relation.output

        checkPaimonTable(v2Table.getTable)
        checkCondition(merge.mergeCondition)
        merge.matchedActions.flatMap(_.condition).foreach(checkCondition)
        merge.notMatchedActions.flatMap(_.condition).foreach(checkCondition)

        val updateActions = merge.matchedActions.collect { case a: UpdateAction => a }
        val primaryKeys = v2Table.properties().get(CoreOptions.PRIMARY_KEY.key).split(",")
        checkUpdateActionValidity(
          AttributeSet(targetOutput),
          merge.mergeCondition,
          updateActions,
          primaryKeys)

        val alignedMatchedActions =
          merge.matchedActions.map(checkAndAlignActionAssignment(_, targetOutput))
        val alignedNotMatchedActions =
          merge.notMatchedActions.map(checkAndAlignActionAssignment(_, targetOutput))
        val alignedNotMatchedBySourceActions = resolveNotMatchedBySourceActions(merge, targetOutput)

        MergeIntoPaimonTable(
          v2Table,
          merge.targetTable,
          merge.sourceTable,
          merge.mergeCondition,
          alignedMatchedActions,
          alignedNotMatchedActions,
          alignedNotMatchedBySourceActions
        )
    }
  }

  def resolveNotMatchedBySourceActions(
      merge: MergeIntoTable,
      targetOutput: Seq[AttributeReference]): Seq[MergeAction]

  def buildMergeIntoPaimonTable(
      v2Table: SparkTable,
      merge: MergeIntoTable,
      alignedMatchedActions: Seq[MergeAction],
      alignedNotMatchedActions: Seq[MergeAction],
      alignedNotMatchedBySourceActions: Seq[MergeAction]): MergeIntoPaimonTable

  protected def checkAndAlignActionAssignment(
      action: MergeAction,
      targetOutput: Seq[AttributeReference]): MergeAction = {
    action match {
      case d @ DeleteAction(_) => d
      case u @ UpdateAction(_, assignments) =>
        u.copy(assignments = alignAssignments(targetOutput, assignments))

      case i @ InsertAction(_, assignments) =>
        if (assignments.length != targetOutput.length) {
          throw new RuntimeException("Can't align the table's columns in insert clause.")
        }
        i.copy(assignments = alignAssignments(targetOutput, assignments))

      case _: UpdateStarAction =>
        throw new RuntimeException(s"UpdateStarAction should not be here.")

      case _: InsertStarAction =>
        throw new RuntimeException(s"InsertStarAction should not be here.")

      case _ =>
        throw new RuntimeException(s"Can't recognize this action: $action")
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
}
