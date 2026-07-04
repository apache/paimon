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

import org.apache.paimon.spark.catalyst.analysis.PaimonAssignmentUtils

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
 * Spark 4.1-only Resolution-batch rule that rewrites UPDATE on pure append-only Paimon tables (see
 * [[PureAppendOnlyScope]]) into a V2 `ReplaceData` plan, mirroring Spark's built-in
 * `RewriteUpdateTable`.
 *
 * In Spark 4.1, `RewriteUpdateTable` runs in the Resolution batch via `resolveOperators`, which
 * short-circuits on `analyzed=true` plans — by the time it would fire, the `UpdateTable` is already
 * marked analyzed and silently skipped, so the planner rejects it with
 * `UNSUPPORTED_FEATURE.TABLE_OPERATION`. We intercept via `transformDown` under
 * `allowInvokingTransformsInAnalyzer` and inline `buildReplaceDataPlan` /
 * `buildReplaceDataWithUnionPlan`. The class sits in `org.apache.spark.sql.catalyst.analysis` to
 * reach the package-private `RowLevelOperationTable` / `ReplaceData` types and the protected
 * helpers on `RewriteRowLevelCommand`.
 *
 * We fire before `ResolveAssignments`, so `u.aligned` is `false`; the rule pre-aligns via
 * `PaimonAssignmentUtils.alignUpdateAssignments` before building the plan.
 *
 * Row-tracking-only tables use the same V2 copy-on-write rewrite. PK / DE / DV tables go through
 * the postHoc V1 rule because they do not expose `SupportsRowLevelOperations`. DELETE is handled by
 * [[Spark41DeleteMetadataRestore]]; MERGE by [[Spark41MergeIntoRewrite]].
 */
object Spark41UpdateTableRewrite extends RewriteRowLevelCommand with PureAppendOnlyScope {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (org.apache.spark.SPARK_VERSION < "4.1") return plan
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case u @ UpdateTable(aliasedTable, assignments, cond)
            if u.resolved && u.rewritable && targetsV2CopyOnWriteTable(aliasedTable) =>
          EliminateSubqueryAliases(aliasedTable) match {
            case r @ ExtractV2Table(tbl: SupportsRowLevelOperations) =>
              val table = buildOperationTable(tbl, UPDATE, CaseInsensitiveStringMap.empty())
              val updateCond = cond.getOrElse(TrueLiteral)
              // `ResolveAssignments` fires later in the batch, so `u.aligned` is still false.
              // Pre-align via the same utility the postHoc V1 fallback uses.
              val alignedAssignments = PaimonAssignmentUtils.alignUpdateAssignments(
                r.output,
                assignments,
                fromStar = false,
                mergeSchemaEnabled = false)
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

  // Mirrors Spark 4.1.1 `RewriteUpdateTable.{buildReplaceDataPlan, buildReplaceDataWithUnionPlan,
  // buildReplaceDataUpdateProjection}`.
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
