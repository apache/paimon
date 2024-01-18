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

import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, InsertStarAction, LogicalPlan, MergeAction, MergeIntoTable, UpdateAction, UpdateStarAction}

trait PaimonMergeIntoResolverBase extends ExpressionHelper {

  def apply(merge: MergeIntoTable, spark: SparkSession): LogicalPlan = {
    val target = merge.targetTable
    val source = merge.sourceTable
    assert(target.resolved, "Target should have been resolved here.")
    assert(source.resolved, "Source should have been resolved here.")

    val condition = merge.mergeCondition
    val matched = merge.matchedActions
    val notMatched = merge.notMatchedActions

    val resolve: (Expression, LogicalPlan) => Expression = resolveExpression(spark)

    def resolveMergeAction(action: MergeAction): MergeAction = {
      action match {
        case DeleteAction(condition) =>
          val resolvedCond = condition.map(resolve(_, merge))
          DeleteAction(resolvedCond)
        case UpdateAction(condition, assignments) =>
          val resolvedCond = condition.map(resolve(_, merge))
          val resolvedAssignments = assignments.map {
            assignment =>
              assignment.copy(
                key = resolve(assignment.key, merge),
                value = resolve(assignment.value, merge))
          }
          UpdateAction(resolvedCond, resolvedAssignments)
        case UpdateStarAction(condition) =>
          val resolvedCond = condition.map(resolve(_, merge))
          val resolvedAssignments = target.output.map {
            attr => Assignment(attr, resolve(UnresolvedAttribute.quotedString(attr.name), source))
          }
          UpdateAction(resolvedCond, resolvedAssignments)
        case InsertAction(condition, assignments) =>
          val resolvedCond = condition.map(resolve(_, source))
          val resolvedAssignments = assignments.map {
            assignment =>
              assignment.copy(
                key = resolve(assignment.key, source),
                value = resolve(assignment.value, source))
          }
          InsertAction(resolvedCond, resolvedAssignments)
        case InsertStarAction(condition) =>
          val resolvedCond = condition.map(resolve(_, source))
          val resolvedAssignments = target.output.map {
            attr => Assignment(attr, resolve(UnresolvedAttribute.quotedString(attr.name), source))
          }
          InsertAction(resolvedCond, resolvedAssignments)
        case _ =>
          throw new RuntimeException(s"Can't recognize this action: $action")
      }
    }

    val resolvedCond = resolve(condition, merge)
    val resolvedMatched: Seq[MergeAction] = matched.map(resolveMergeAction)
    val resolvedNotMatched: Seq[MergeAction] = notMatched.map(resolveMergeAction)
    val resolvedNotMatchedBySource: Seq[MergeAction] =
      resolveNotMatchedBySourceActions(merge, target, source, resolve)

    build(merge, resolvedCond, resolvedMatched, resolvedNotMatched, resolvedNotMatchedBySource)
  }

  def resolveNotMatchedBySourceActions(
      merge: MergeIntoTable,
      target: LogicalPlan,
      source: LogicalPlan,
      resolve: (Expression, LogicalPlan) => Expression): Seq[MergeAction]

  def build(
      merge: MergeIntoTable,
      resolvedCond: Expression,
      resolvedMatched: Seq[MergeAction],
      resolvedNotMatched: Seq[MergeAction],
      resolvedNotMatchedBySource: Seq[MergeAction]): MergeIntoTable
}
