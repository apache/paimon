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
package org.apache.spark.sql.catalyst.optimizer

import org.apache.paimon.CoreOptions.{MERGE_ENGINE, MergeEngine}
import org.apache.paimon.options.Options
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.table.Table

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromPaimonTableCommand, DeleteFromTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.SupportsDelete
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

object RewriteRowLeverCommands extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case d @ DeleteFromTable(r: DataSourceV2Relation, condition) =>
      validateDeletable(r.table.asInstanceOf[SparkTable].getTable)
      if (canDeleteWhere(r, condition)) {
        d
      } else {
        DeleteFromPaimonTableCommand(r, condition)
      }
  }

  private def validateDeletable(table: Table): Boolean = {
    val options = Options.fromMap(table.options)
    if (table.primaryKeys().isEmpty) {
      throw new UnsupportedOperationException(
        String.format(
          "table '%s' can not support delete, because there is no primary key.",
          table.getClass.getName))
    }
    if (!options.get(MERGE_ENGINE).equals(MergeEngine.DEDUPLICATE)) {
      throw new UnsupportedOperationException(
        String.format(
          "merge engine '%s' can not support delete, currently only %s can support delete.",
          options.get(MERGE_ENGINE),
          MergeEngine.DEDUPLICATE))
    }
    true
  }

  private def canDeleteWhere(relation: DataSourceV2Relation, condition: Expression): Boolean = {
    relation.table match {
      case t: SupportsDelete if !SubqueryExpression.hasSubquery(condition) =>
        // fail if any filter cannot be converted.
        // correctness depends on removing all matching data.
        val filters = DataSourceStrategy
          .normalizeExprs(Seq(condition), relation.output)
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

}
