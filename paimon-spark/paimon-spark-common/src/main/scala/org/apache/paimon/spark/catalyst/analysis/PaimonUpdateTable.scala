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

import org.apache.paimon.spark.commands.UpdatePaimonTableCommand
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, Assignment, LogicalPlan, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.JavaConverters._

object PaimonUpdateTable
  extends Rule[LogicalPlan]
  with RowLevelHelper
  with AssignmentAlignmentHelper {

  override val operation: RowLevelOp = Update

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Spark 4.1 moved RewriteUpdateTable from the "DML rewrite" batch into the main Resolution
    // batch, which marks the logical plan as analyzed before the Post-Hoc Resolution batch runs.
    // `plan.resolveOperators` then short-circuits on the already-analyzed UPDATE node, leaving the
    // plan for Spark's physical planner to reject with "Table does not support UPDATE TABLE". Use
    // `transformDown` (which unconditionally visits every node) guarded by
    // `AnalysisHelper.allowInvokingTransformsInAnalyzer` so the in-analyzer assertion does not
    // trip. The pattern guard keeps the rewrite restricted to fully resolved plans.
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

              if (paimonTable.coreOptions().dataEvolutionEnabled()) {
                throw new RuntimeException(
                  "Update operation is not supported when data evolution is enabled yet.")
              }

              val alignedExpressions =
                generateAlignedExpressions(relation.output, assignments).zip(relation.output)

              val alignedAssignments = alignedExpressions.map {
                case (expression, field) => Assignment(field, expression)
              }

              val alignedUpdateTable = u.copy(assignments = alignedAssignments)

              if (!shouldFallbackToV1Update(table, alignedUpdateTable)) {
                alignedUpdateTable
              } else {
                UpdatePaimonTableCommand(
                  relation,
                  paimonTable,
                  condition.getOrElse(TrueLiteral),
                  alignedExpressions)
              }

            case _ =>
              throw new RuntimeException("Update Operation is only supported for FileStoreTable.")
          }
      }
    }
  }
}
