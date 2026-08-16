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
import org.apache.paimon.spark.util.OptionUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.paimon.shims.SparkShimLoader

object PaimonMergeIntoResolver extends ExpressionHelper {

  def apply(merge: MergeIntoTable, spark: SparkSession): LogicalPlan = {
    val target = merge.targetTable
    val source = merge.sourceTable
    assert(target.resolved, "Target should have been resolved here.")
    assert(source.resolved, "Source should have been resolved here.")

    val resolve: (Expression, LogicalPlan) => Expression = resolveExpression(spark)

    val resolvedCond = resolveCondition(resolve, merge.mergeCondition, merge, ALL)
    val resolvedMatched = resolveMatchedByTargetActions(merge, resolve, spark)
    val resolvedNotMatched = resolveNotMatchedByTargetActions(merge, resolve, spark)
    val resolvedNotMatchedBySource = resolveNotMatchedBySourceActions(merge, resolve)

    SparkShimLoader.shim.createMergeIntoTable(
      merge.targetTable,
      merge.sourceTable,
      resolvedCond,
      resolvedMatched,
      resolvedNotMatched,
      resolvedNotMatchedBySource,
      withSchemaEvolution = false
    )
  }

  private def resolveMatchedByTargetActions(
      merge: MergeIntoTable,
      resolve: (Expression, LogicalPlan) => Expression,
      spark: SparkSession): Seq[MergeAction] = {
    merge.matchedActions.map {
      case DeleteAction(condition) =>
        // The condition can be from both target and source tables
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, ALL))
        DeleteAction(resolvedCond)
      case PaimonUpdateAction(condition, assignments) =>
        // The condition and value can be from both target and source tables
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, ALL))
        val resolvedAssignments = resolveAssignments(resolve, assignments, merge, ALL)
        SparkShimLoader.shim.createUpdateAction(resolvedCond, resolvedAssignments)
      case UpdateStarAction(condition) =>
        // The condition can be from both target and source tables, but the value must be from the source table
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, ALL))
        val assignments = expandStarAssignments(merge, spark)
        // Tag so merge-schema can distinguish `UPDATE *` from a fully-listed explicit clause.
        PaimonMergeActionTags.markFromStar(
          SparkShimLoader.shim.createUpdateAction(resolvedCond, assignments))
      case action =>
        throw new RuntimeException(s"Can't recognize this action: $action")
    }
  }

  private def resolveNotMatchedByTargetActions(
      merge: MergeIntoTable,
      resolve: (Expression, LogicalPlan) => Expression,
      spark: SparkSession): Seq[MergeAction] = {
    merge.notMatchedActions.map {
      case InsertAction(condition, assignments) =>
        // The condition and value must be from the source table
        val resolvedCond =
          condition.map(resolveCondition(resolve, _, merge, SOURCE_ONLY))
        val resolvedAssignments =
          resolveAssignments(resolve, assignments, merge, SOURCE_ONLY)
        SparkShimLoader.shim.createInsertAction(resolvedCond, resolvedAssignments)
      case InsertStarAction(condition) =>
        // The condition and value must be from the source table
        val resolvedCond =
          condition.map(resolveCondition(resolve, _, merge, SOURCE_ONLY))
        val assignments = expandStarAssignments(merge, spark)
        PaimonMergeActionTags.markFromStar(
          SparkShimLoader.shim.createInsertAction(resolvedCond, assignments))
      case action =>
        throw new RuntimeException(s"Can't recognize this action: $action")
    }
  }

  private def resolveNotMatchedBySourceActions(
      merge: MergeIntoTable,
      resolve: (Expression, LogicalPlan) => Expression): Seq[MergeAction] = {
    SparkShimLoader.shim.notMatchedBySourceActions(merge).map {
      case DeleteAction(condition) =>
        // The condition must be from the target table
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, TARGET_ONLY))
        DeleteAction(resolvedCond)
      case PaimonUpdateAction(condition, assignments) =>
        // The condition and value must be from the target table
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, TARGET_ONLY))
        val resolvedAssignments = resolveAssignments(resolve, assignments, merge, TARGET_ONLY)
        SparkShimLoader.shim.createUpdateAction(resolvedCond, resolvedAssignments)
      case action =>
        throw new RuntimeException(s"Can't recognize this action: $action")
    }
  }

  /**
   * Expand `UPDATE *` / `INSERT *` to source-driven assignments. Missing target columns are filled
   * later during alignment; source-only top-level columns are dropped here, then re-appended by
   * [[MergeSchemaEvolutionHelper]] when merge-schema is enabled.
   */
  private def expandStarAssignments(merge: MergeIntoTable, spark: SparkSession): Seq[Assignment] = {
    val resolver: Resolver = spark.sessionState.analyzer.resolver
    def findTarget(name: String): Option[Attribute] = {
      merge.targetTable.output.find(targetAttr => resolver(name, targetAttr.name))
    }

    merge.sourceTable.output.flatMap {
      sourceAttr =>
        findTarget(sourceAttr.name).map(targetAttr => Assignment(targetAttr, sourceAttr))
    }
  }

  sealed private trait ResolvedWith
  private case object ALL extends ResolvedWith
  private case object SOURCE_ONLY extends ResolvedWith
  private case object TARGET_ONLY extends ResolvedWith

  private def resolveCondition(
      resolve: (Expression, LogicalPlan) => Expression,
      condition: Expression,
      mergeInto: MergeIntoTable,
      resolvedWith: ResolvedWith): Expression = {
    resolvedWith match {
      case ALL => resolve(condition, mergeInto)
      case SOURCE_ONLY => resolve(condition, Project(Nil, mergeInto.sourceTable))
      case TARGET_ONLY => resolve(condition, Project(Nil, mergeInto.targetTable))
    }
  }

  private def resolveAssignments(
      resolve: (Expression, LogicalPlan) => Expression,
      assignments: Seq[Assignment],
      mergeInto: MergeIntoTable,
      resolvedWith: ResolvedWith): Seq[Assignment] = {
    assignments.map {
      assign =>
        // Merge-schema: fall back to source so a key naming a not-yet-existing target column
        // surfaces as a source-bound Attribute for [[MergeSchemaEvolutionHelper]] to detect.
        // Strict mode lets the target-attempt failure propagate as "column not found".
        val resolvedKey =
          if (OptionUtils.writeMergeSchemaEnabled()) {
            try resolve(assign.key, Project(Nil, mergeInto.targetTable))
            catch {
              case scala.util.control.NonFatal(_) =>
                resolve(assign.key, Project(Nil, mergeInto.sourceTable))
            }
          } else {
            resolve(assign.key, Project(Nil, mergeInto.targetTable))
          }
        val resolvedValue = resolvedWith match {
          case ALL => resolve(assign.value, mergeInto)
          case SOURCE_ONLY => resolve(assign.value, Project(Nil, mergeInto.sourceTable))
          case TARGET_ONLY => resolve(assign.value, Project(Nil, mergeInto.targetTable))
        }
        Assignment(resolvedKey, resolvedValue)
    }
  }
}
