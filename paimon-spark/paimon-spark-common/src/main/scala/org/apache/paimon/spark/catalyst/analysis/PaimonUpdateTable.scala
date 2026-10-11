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
import org.apache.paimon.spark.commands.{UpdatePaimonDataEvolutionTableCommand, UpdatePaimonTableCommand}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, ExprId, OuterReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.JavaConverters._

object PaimonUpdateTable extends Rule[LogicalPlan] with RowLevelHelper with ExpressionHelper {

  override val operation: RowLevelOp = Update

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Spark 4.1 marks the plan analyzed before postHoc runs, so `resolveOperators` would
    // short-circuit. Use `transformDown` under `allowInvokingTransformsInAnalyzer` instead.
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case u @ UpdateTable(PaimonRelation(table), assignments, condition) if u.resolved =>
          checkPaimonTable(table.getTable)

          table.getTable match {
            case paimonTable: FileStoreTable =>
              val relation = PaimonRelation.getPaimonRelation(u.table)

              val primaryKeys = paimonTable.primaryKeys().asScala.toSeq
              if (!validUpdateAssignment(u.table.outputSet, primaryKeys, assignments)) {
                throw new RuntimeException("Can't update the primary key column.")
              }

              // Align against `u.table.output`: for CHAR/VARCHAR columns the analyzer adds a
              // `readSidePadding` Project whose output has different exprIds than `relation`, and
              // the parsed assignment keys reference the Project's attributes. Order matches
              // `relation.output` 1:1, so the subsequent zip stays correct.
              val alignedAssignments = PaimonAssignmentUtils.alignUpdateAssignments(
                u.table.output,
                assignments,
                fromStar = false,
                mergeSchemaEnabled = false)
              val alignedUpdateTable = u.copy(assignments = alignedAssignments)

              // The V1 commands plan on `relation` directly, without that Project, so the aligned
              // values and the condition must reference `relation.output`: an attribute of the
              // padding Project (an untouched CHAR column, or a CHAR column read by the condition
              // or an assignment value) would otherwise be unresolvable there. Reads still go
              // through read-side padding, which the analyzer re-applies on top of `relation`.
              val toRelationAttributes = relationAttributeRewriter(u.table.output, relation.output)
              val alignedExpressions =
                alignedAssignments.map(a => toRelationAttributes(a.value)).zip(relation.output)
              val v1Condition = toRelationAttributes(condition.getOrElse(TrueLiteral))
              val dataEvolutionEnabled = paimonTable.coreOptions().dataEvolutionEnabled()

              if (dataEvolutionEnabled) {
                // The rewritten files keep the original row ids, which are derived from the
                // file's firstRowId per partition; moving a row to another partition would need
                // re-assigned row ids (delete + insert semantics).
                val partitionKeys = paimonTable.partitionKeys().asScala.toSeq
                if (!validUpdateAssignment(u.table.outputSet, partitionKeys, assignments)) {
                  throw new RuntimeException(
                    "Update to partition columns is not supported for data evolution tables.")
                }
              }

              if (!shouldFallbackToV1Update(table, alignedUpdateTable)) {
                if (dataEvolutionEnabled) {
                  // Data-evolution tables do not currently expose Spark V2 row-level operations.
                  // Keep this guard in case capability rules change; this branch intentionally
                  // implements only V1 data-evolution UPDATE.
                  throw new RuntimeException(
                    "Update operation is not supported when data evolution is enabled yet.")
                }
                alignedUpdateTable
              } else {
                if (dataEvolutionEnabled) {
                  UpdatePaimonDataEvolutionTableCommand(
                    relation,
                    table,
                    v1Condition,
                    alignedExpressions)
                } else {
                  UpdatePaimonTableCommand(relation, paimonTable, v1Condition, alignedExpressions)
                }
              }

            case _ =>
              throw new RuntimeException("Update Operation is only supported for FileStoreTable.")
          }
      }
    }
  }

  /**
   * Rewrites references to the attributes of `from` into the positionally matching attributes of
   * `to`. Both come from the same table, so they line up 1:1; attributes that already share an
   * exprId (every non-CHAR column) are left alone.
   */
  private def relationAttributeRewriter(
      from: Seq[Attribute],
      to: Seq[Attribute]): Expression => Expression = {
    require(
      from.size == to.size,
      s"UPDATE table output ${from.mkString(", ")} does not line up with relation output " +
        s"${to.mkString(", ")}.")
    val mapping: Map[ExprId, Attribute] = from
      .zip(to)
      .collect { case (f, t) if f.exprId != t.exprId => f.exprId -> t }
      .toMap
    if (mapping.isEmpty) {
      identity
    } else {
      expression =>
        expression.transform {
          case attr: AttributeReference if mapping.contains(attr.exprId) => mapping(attr.exprId)
          case subquery: SubqueryExpression =>
            // A correlated subquery refers to the outer row through OuterReference.
            subquery.withNewPlan(subquery.plan.transformAllExpressions {
              case OuterReference(attr) if mapping.contains(attr.exprId) =>
                OuterReference(mapping(attr.exprId))
            })
        }
    }
  }
}
