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

import org.apache.paimon.spark.catalyst.analysis.AssignmentAlignmentHelper

import org.apache.spark.sql.catalyst.expressions.{Alias, EqualNullSafe, Expression, If, Literal, MetadataAttribute, Not, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, Assignment, Filter, LogicalPlan, Project, ReplaceData, Union, UpdateTable}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.WRITE_WITH_METADATA_OPERATION
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2Table}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Spark 4.1-only Resolution-batch rule that rewrites UPDATE plans targeting pure append-only Paimon
 * tables (see [[PureAppendOnlyScope]] for the scope) into the V2 `ReplaceData` plan — the same form
 * Spark's built-in `RewriteUpdateTable` produces for `SupportsRowLevelOperations` tables.
 *
 * Why this rule exists: Spark 4.1 moved `RewriteUpdateTable` into the main Resolution batch AND
 * implemented its `apply` with `plan resolveOperators { ... }`. `AnalysisHelper.resolveOperators*`
 * short-circuits on already-`analyzed` plans, so by the time the rewrite would run the append-only
 * `UpdateTable` node has transitioned to `analyzed=true` and the rewrite silently skips. The
 * `UpdateTable` then falls through to the physical planner which rejects it with
 * `UNSUPPORTED_FEATURE.TABLE_OPERATION`.
 *
 * Firing this rule in the Resolution batch via `transformDown` guarded by
 * `AnalysisHelper.allowInvokingTransformsInAnalyzer` intercepts the plan before the analyzed flag
 * traps Spark's own rule. The body is a near-verbatim transcription of
 * `RewriteUpdateTable.buildReplaceDataPlan` / `buildReplaceDataWithUnionPlan` so the result goes
 * through Paimon's V2 row-level write path (`PaimonSparkCopyOnWriteOperation` ->
 * `PaimonV2WriteBuilder` -> `PaimonBatchWrite`) exactly like Spark would have produced. The class
 * lives in `org.apache.spark.sql.catalyst.analysis` so it can reference the package-private
 * `RowLevelOperationTable` / `ReplaceData` types and the protected helpers on
 * `RewriteRowLevelCommand`.
 *
 * One subtlety: Spark's `RewriteUpdateTable` guards on `u.aligned`, which is set by its
 * `ResolveAssignments` rule running earlier in the Resolution batch. Because we fire before that
 * alignment has taken effect, `u.aligned` is always `false` for us. The rule therefore mixes in
 * Paimon's `AssignmentAlignmentHelper` and aligns assignments itself before invoking
 * `buildReplaceDataPlan`, which expects one assignment per target data column.
 *
 * Tables with row-tracking / data-evolution / deletion-vectors still route through Spark's V2 path
 * (which handles them correctly). Primary-key tables fall under Paimon's existing postHoc rule.
 * DELETE is handled by [[Spark41DeleteMetadataRestore]]; MERGE by [[Spark41MergeIntoRewrite]].
 */
object Spark41UpdateTableRewrite
  extends RewriteRowLevelCommand
  with AssignmentAlignmentHelper
  with PureAppendOnlyScope {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (org.apache.spark.SPARK_VERSION < "4.1") return plan
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case u @ UpdateTable(aliasedTable, assignments, cond)
            if u.resolved && u.rewritable && targetsPureAppendOnly(aliasedTable) =>
          EliminateSubqueryAliases(aliasedTable) match {
            case r @ ExtractV2Table(tbl: SupportsRowLevelOperations) =>
              val table = buildOperationTable(tbl, UPDATE, CaseInsensitiveStringMap.empty())
              val updateCond = cond.getOrElse(TrueLiteral)
              // Spark's `RewriteUpdateTable` relies on `ResolveAssignments` having aligned
              // `u.assignments` to the full target output first, but that rule fires later in
              // the Resolution batch than ours, so `u.aligned` is still `false` when we see the
              // plan. Align manually with Paimon's `AssignmentAlignmentHelper` (same helper
              // the postHoc `PaimonUpdateTable` rule uses for its V1 fallback) before building
              // the `ReplaceData` plan, which expects one assignment per target data column.
              val alignedAssignments = alignAssignments(r.output, assignments)
              if (SubqueryExpression.hasSubquery(updateCond)) {
                buildReplaceDataWithUnionPlan(r, table, alignedAssignments, updateCond)
              } else {
                buildReplaceDataPlan(r, table, alignedAssignments, updateCond)
              }
            case _ =>
              u
          }
      }
    }
  }

  /* ------------------------------------------------------------------------------------------- *
   * Near-verbatim replicas of `RewriteUpdateTable`'s private `buildReplaceDataPlan` /
   * `buildReplaceDataWithUnionPlan` / `buildReplaceDataUpdateProjection`. Kept in lockstep with
   * the Spark 4.1.1 implementation so the produced `ReplaceData` shape matches Spark's and reuses
   * Paimon's existing V2 write path.
   * ------------------------------------------------------------------------------------------- */

  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): ReplaceData = {
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)
    val updatedAndRemainingRowsPlan =
      buildReplaceDataUpdateProjection(readRelation, assignments, cond)
    val writeRelation = relation.copy(table = operationTable)
    val query = addOperationColumn(WRITE_WITH_METADATA_OPERATION, updatedAndRemainingRowsPlan)
    val projections = buildReplaceDataProjections(query, relation.output, metadataAttrs)
    ReplaceData(writeRelation, cond, query, relation, projections, Some(cond))
  }

  private def buildReplaceDataWithUnionPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): ReplaceData = {
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)

    val matchedRowsPlan = Filter(cond, readRelation)
    val updatedRowsPlan = buildReplaceDataUpdateProjection(matchedRowsPlan, assignments)

    val remainingRowFilter = Not(EqualNullSafe(cond, TrueLiteral))
    val remainingRowsPlan = Filter(remainingRowFilter, readRelation)

    val updatedAndRemainingRowsPlan = Union(updatedRowsPlan, remainingRowsPlan)

    val writeRelation = relation.copy(table = operationTable)
    val query = addOperationColumn(WRITE_WITH_METADATA_OPERATION, updatedAndRemainingRowsPlan)
    val projections = buildReplaceDataProjections(query, relation.output, metadataAttrs)
    ReplaceData(writeRelation, cond, query, relation, projections, Some(cond))
  }

  /** Assumes assignments are already aligned with the table output. */
  private def buildReplaceDataUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      cond: Expression = TrueLiteral): LogicalPlan = {
    val assignedValues = assignments.map(_.value)
    val updatedValues = plan.output.zipWithIndex.map {
      case (attr, index) =>
        if (index < assignments.size) {
          val assignedExpr = assignedValues(index)
          val updatedValue = If(cond, assignedExpr, attr)
          Alias(updatedValue, attr.name)()
        } else {
          assert(MetadataAttribute.isValid(attr.metadata))
          if (MetadataAttribute.isPreservedOnUpdate(attr)) {
            attr
          } else {
            val updatedValue = If(cond, Literal(null, attr.dataType), attr)
            Alias(updatedValue, attr.name)(explicitMetadata = Some(attr.metadata))
          }
        }
    }
    Project(updatedValues, plan)
  }
}
