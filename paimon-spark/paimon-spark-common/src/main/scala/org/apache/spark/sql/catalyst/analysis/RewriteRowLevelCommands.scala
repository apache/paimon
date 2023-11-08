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

import org.apache.paimon.CoreOptions.MERGE_ENGINE
import org.apache.paimon.options.Options
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.table.Table

import org.apache.spark.sql.{AnalysisException, Delete, RowLevelOp, Update}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.SupportsDelete
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import java.util

object RewriteRowLevelCommands extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case d @ DeleteFromTable(ResolvesToPaimonTable(table), condition) =>
      validateRowLevelOp(Delete, table.getTable, Option.empty)
      if (canDeleteWhere(d, table, condition)) {
        d
      } else {
        DeleteFromPaimonTableCommand(d)
      }

    case u @ UpdateTable(ResolvesToPaimonTable(table), assignments, _) =>
      validateRowLevelOp(Update, table.getTable, Option.apply(assignments))
      UpdatePaimonTableCommand(u)
  }

  private object ResolvesToPaimonTable {
    def unapply(plan: LogicalPlan): Option[SparkTable] =
      EliminateSubqueryAliases(plan) match {
        case DataSourceV2Relation(table: SparkTable, _, _, _, _) => Some(table)
        case _ => None
      }
  }

  private def validateRowLevelOp(
      op: RowLevelOp,
      table: Table,
      assignments: Option[Seq[Assignment]]): Unit = {
    val options = Options.fromMap(table.options)
    val primaryKeys = table.primaryKeys()
    if (primaryKeys.isEmpty) {
      throw new UnsupportedOperationException(
        s"table ${table.getClass.getName} can not support $op, because there is no primary key.")
    }

    if (op.equals(Update) && isPrimaryKeyUpdate(primaryKeys, assignments.get)) {
      throw new UnsupportedOperationException(s"$op to primary keys is not supported.")
    }

    val mergeEngine = options.get(MERGE_ENGINE)
    if (!op.supportedMergeEngine.contains(mergeEngine)) {
      throw new UnsupportedOperationException(
        s"merge engine $mergeEngine can not support $op, currently only ${op.supportedMergeEngine
            .mkString(", ")} can support $op.")
    }
  }

  private def canDeleteWhere(
      d: DeleteFromTable,
      table: SparkTable,
      condition: Expression): Boolean = {
    table match {
      case t: SupportsDelete if !SubqueryExpression.hasSubquery(condition) =>
        // fail if any filter cannot be converted.
        // correctness depends on removing all matching data.
        val filters = DataSourceStrategy
          .normalizeExprs(Seq(condition), d.output)
          .flatMap(splitConjunctivePredicates(_).map {
            f =>
              DataSourceStrategy
                .translateFilter(f, supportNestedPredicatePushdown = true)
                .getOrElse(throw new AnalysisException(s"Exec update failed:" +
                  s" cannot translate expression to source filter: $f"))
          })
          .toArray
        t.canDeleteWhere(filters)
      case _ => false
    }
  }

  private def isPrimaryKeyUpdate(
      primaryKeys: util.List[String],
      assignments: Seq[Assignment]): Boolean = {
    assignments.exists(
      a => {
        a.key match {
          case attr: Attribute => primaryKeys.contains(attr.name)
          case _ => false
        }
      })
  }

}
