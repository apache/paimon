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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.analysis.{MergeSchemaEvolutionHelper, PaimonAssignmentUtils, PaimonRelation}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan, MergeIntoTable, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule

/** Re-enter Spark's native rewrite with fresh, aligned commands that are not marked analyzed. */
object Spark42RowLevelCommandRewrite
  extends Rule[LogicalPlan]
  with PureAppendOnlyScope
  with MergeSchemaEvolutionHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case u: UpdateTable if u.resolved && u.rewritable && eligible(u.table) =>
          val assignments = PaimonAssignmentUtils.alignUpdateAssignments(
            u.table.output,
            u.assignments,
            fromStar = false,
            mergeSchemaEnabled = false)
          RewriteUpdateTable(u.copy(assignments = assignments))

        case m: MergeIntoTable
            if m.resolved && m.rewritable && !m.withSchemaEvolution && eligible(m.targetTable) =>
          val relation = PaimonRelation.getPaimonRelation(m.targetTable)
          val table = relation.table.asInstanceOf[SparkTable]
          val evolved = evolveTargetIfNeeded(
            m,
            relation,
            table,
            SparkSession.active,
            _.notMatchedBySourceActions)
            .map(_._1)
            .getOrElse(m)
          val aligned = alignAllMergeActions(evolved, evolved.targetTable.output)
          RewriteMergeIntoTable(aligned.copy())
      }
    }
  }

  private def eligible(table: LogicalPlan): Boolean =
    targetsV2CopyOnWriteTable(table) || targetsV2DeltaTable(table)
}
