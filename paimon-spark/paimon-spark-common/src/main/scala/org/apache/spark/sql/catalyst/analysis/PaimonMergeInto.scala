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
package org.apache.spark.sql.catalyst.analysis

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.commands.MergeIntoPaimonTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.PaimonRelation
import org.apache.spark.sql.catalyst.analysis.expressions.ExpressionHelper
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, InsertStarAction, LogicalPlan, MergeAction, MergeIntoTable, UpdateAction, UpdateStarAction}
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.mutable

/** A post-hoc resolution rule for MergeInto. */
case class PaimonMergeInto(spark: SparkSession)
  extends Rule[LogicalPlan]
  with RowLevelHelper
  with ExpressionHelper {

  override val operation: RowLevelOp = MergeInto

  private val resolver: Resolver = spark.sessionState.conf.resolver

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case merge: MergeIntoTable
          if merge.resolved && PaimonRelation.isPaimonTable(merge.targetTable) =>
        val relation = PaimonRelation.getPaimonRelation(merge.targetTable)
        val v2Table = relation.table.asInstanceOf[SparkTable]
        val targetOutput = relation.output

        checkPaimonTable(v2Table)
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
          merge.matchedActions.map(checkAndAlignActionAssigment(_, targetOutput))
        val alignedNotMatchedActions =
          merge.notMatchedActions.map(checkAndAlignActionAssigment(_, targetOutput))

        MergeIntoPaimonTable(
          v2Table,
          merge.targetTable,
          merge.sourceTable,
          merge.mergeCondition,
          alignedMatchedActions,
          alignedNotMatchedActions)
    }
  }

  private def checkAndAlignActionAssigment(
      action: MergeAction,
      targetOutput: Seq[AttributeReference]): MergeAction = {
    action match {
      case d @ DeleteAction(_) => d
      case u @ UpdateAction(_, assignments) =>
        val attrNameAndUpdateExpr = checkAndConvertAssignments(assignments, targetOutput)

        val newAssignments = targetOutput.map {
          field =>
            val fieldAndExpr = attrNameAndUpdateExpr.find(a => resolver(field.name, a._1))
            if (fieldAndExpr.isEmpty) {
              Assignment(field, field)
            } else {
              Assignment(field, castIfNeeded(fieldAndExpr.get._2, field.dataType))
            }
        }
        u.copy(assignments = newAssignments)

      case i @ InsertAction(_, assignments) =>
        val attrNameAndUpdateExpr = checkAndConvertAssignments(assignments, targetOutput)
        if (assignments.length != targetOutput.length) {
          throw new RuntimeException("Can't align the table's columns in insert clause.")
        }

        val newAssignments = targetOutput.map {
          field =>
            val fieldAndExpr = attrNameAndUpdateExpr.find(a => resolver(field.name, a._1))
            if (fieldAndExpr.isEmpty) {
              throw new RuntimeException(s"Can't find the expression for ${field.name}.")
            } else {
              Assignment(field, castIfNeeded(fieldAndExpr.get._2, field.dataType))
            }
        }
        i.copy(assignments = newAssignments)

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

  private def checkAndConvertAssignments(
      assignments: Seq[Assignment],
      targetOutput: Seq[AttributeReference]): Seq[(String, Expression)] = {
    val columnToAssign = mutable.HashMap.empty[String, Int]
    val pairs = assignments.map {
      assignment =>
        assignment.key match {
          case a: AttributeReference =>
            if (!targetOutput.exists(attr => resolver(attr.name, a.name))) {
              throw new RuntimeException(
                s"Ths key of assignment doesn't belong to the target table, $assignment")
            }
            columnToAssign.put(a.name, columnToAssign.getOrElse(a.name, 0) + 1)
          case _ =>
            throw new RuntimeException(
              s"Only primitive type is supported in update/insert clause, $assignment")
        }
        (assignment.key.asInstanceOf[AttributeReference].name, assignment.value)
    }

    val duplicatedColumns = columnToAssign.filter(_._2 > 1).keys
    if (duplicatedColumns.nonEmpty) {
      val partOfMsg = duplicatedColumns.mkString(",")
      throw new RuntimeException(
        s"Can't update/insert the same column ($partOfMsg) multiple times.")
    }

    pairs
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
