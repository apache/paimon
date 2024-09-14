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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, LogicalPlan, MergeAction, MergeIntoTable, Project, UpdateAction, UpdateStarAction}

/** Resolve all the expressions for MergeInto. */
object PaimonMergeIntoResolver extends PaimonMergeIntoResolverBase {

  def resolveNotMatchedBySourceActions(
      merge: MergeIntoTable,
      resolve: (Expression, LogicalPlan) => Expression): Seq[MergeAction] = {
    merge.notMatchedBySourceActions.map {
      case DeleteAction(condition) =>
        // The condition must be from the target table
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, TARGET_ONLY))
        DeleteAction(resolvedCond)
      case UpdateAction(condition, assignments) =>
        // The condition and value must be from the target table
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, TARGET_ONLY))
        val resolvedAssignments = resolveAssignments(resolve, assignments, merge, TARGET_ONLY)
        UpdateAction(resolvedCond, resolvedAssignments)
      case action =>
        throw new RuntimeException(s"Can't recognize this action: $action")
    }
  }

  def build(
      merge: MergeIntoTable,
      resolvedCond: Expression,
      resolvedMatched: Seq[MergeAction],
      resolvedNotMatched: Seq[MergeAction],
      resolvedNotMatchedBySource: Seq[MergeAction]): MergeIntoTable = {
    merge.copy(
      mergeCondition = resolvedCond,
      matchedActions = resolvedMatched,
      notMatchedActions = resolvedNotMatched,
      notMatchedBySourceActions = resolvedNotMatchedBySource)
  }

}
