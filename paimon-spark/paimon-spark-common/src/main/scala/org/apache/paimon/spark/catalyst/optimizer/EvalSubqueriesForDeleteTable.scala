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

package org.apache.paimon.spark.catalyst.optimizer

import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.commands.DeleteFromPaimonTableCommand

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{execution, SparkSession}
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Expression, In, InSubquery, Literal, ScalarSubquery, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ExecSubqueryExpression, QueryExecution}
import org.apache.spark.sql.types.BooleanType

import scala.collection.JavaConverters._

/**
 * For those delete conditions with subqueries that only contain partition columns, we can eval them
 * in advance. So that when running [[DeleteFromPaimonTableCommand]], we can directly call
 * dropPartitions to achieve fast deletion.
 *
 * Note: this rule must be placed before [[MergePaimonScalarSubqueries]], because
 * [[MergePaimonScalarSubqueries]] will merge subqueries.
 */
object EvalSubqueriesForDeleteTable extends Rule[LogicalPlan] with ExpressionHelper with Logging {

  lazy val spark: SparkSession = SparkSession.active
  lazy val resolver: Resolver = spark.sessionState.conf.resolver

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown {
      case d @ DeleteFromPaimonTableCommand(_, table, condition)
          if SubqueryExpression.hasSubquery(condition) &&
            isPredicatePartitionColumnsOnly(condition, table.partitionKeys().asScala, resolver) =>
        try {
          d.copy(condition = evalSubquery(condition))
        } catch {
          case e: Throwable =>
            logInfo(s"Applying EvalSubqueriesForDeleteTable rule failed for: ${e.getMessage}")
            d
        }
    }
  }

  private def evalSubquery(condition: Expression): Expression = {
    condition.transformDown {
      case InSubquery(values, listQuery) =>
        val expr = if (values.length == 1) {
          values.head
        } else {
          throw new RuntimeException("InSubquery with multi-values are not supported")
        }
        if (listQuery.isCorrelated) {
          throw new RuntimeException("Correlated InSubquery is not supported")
        }

        val executedPlan = QueryExecution.prepareExecutedPlan(spark, listQuery.plan)
        val physicalSubquery = execution.InSubqueryExec(
          expr,
          execution.SubqueryExec(s"subquery#${listQuery.exprId.id}", executedPlan),
          listQuery.exprId)
        evalPhysicalSubquery(physicalSubquery)

        physicalSubquery.values() match {
          case Some(l) if l.length > 0 => In(expr, l.map(Literal(_, expr.dataType)))
          case _ => Literal(false, BooleanType)
        }

      case s: ScalarSubquery =>
        if (s.isCorrelated) {
          throw new RuntimeException("Correlated ScalarSubquery is not supported")
        }

        val executedPlan = QueryExecution.prepareExecutedPlan(spark, s.plan)
        val physicalSubquery = execution.ScalarSubquery(
          execution.SubqueryExec
            .createForScalarSubquery(s"scalar-subquery#${s.exprId.id}", executedPlan),
          s.exprId)
        evalPhysicalSubquery(physicalSubquery)

        Literal(physicalSubquery.eval(), s.dataType)

      case _: SubqueryExpression =>
        throw new RuntimeException("Only support InSubquery and ScalarSubquery")
    }
  }

  // Evaluate physicalSubquery in a bottom-up way.
  private def evalPhysicalSubquery(subquery: ExecSubqueryExpression): Unit = {
    subquery.plan.foreachUp {
      plan =>
        plan.expressions.foreach(_.foreachUp {
          case s: ExecSubqueryExpression => evalPhysicalSubquery(s)
          case _ =>
        })
    }
    subquery.updateResult()
  }
}
