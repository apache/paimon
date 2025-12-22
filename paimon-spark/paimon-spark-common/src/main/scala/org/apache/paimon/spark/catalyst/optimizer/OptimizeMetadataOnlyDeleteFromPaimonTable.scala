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

import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.catalyst.plans.logical.TruncatePaimonTableWithFilter
import org.apache.paimon.spark.commands.DeleteFromPaimonTableCommand
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{execution, PaimonSparkSession, SparkSession}
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Expression, In, InSubquery, Literal, ScalarSubquery, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ExecSubqueryExpression
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.BooleanType

import scala.collection.JavaConverters._

/**
 * Similar to spark `OptimizeMetadataOnlyDeleteFromTable`. The reasons why Paimon Table does not
 * inherit `SupportsDeleteV2` are as follows:
 *
 * <p>1. It needs to support both V1 delete and V2 delete simultaneously.
 *
 * <p>2. This rule can optimize partition filters that contain subqueries.
 *
 * <p>Note: this rule must be placed before [[MergePaimonScalarSubqueries]], because
 * [[MergePaimonScalarSubqueries]] will merge subqueries.
 */
object OptimizeMetadataOnlyDeleteFromPaimonTable
  extends Rule[LogicalPlan]
  with ExpressionHelper
  with Logging {

  lazy val spark: SparkSession = PaimonSparkSession.active
  lazy val resolver: Resolver = spark.sessionState.conf.resolver

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case d @ DeleteFromPaimonTableCommand(r: DataSourceV2Relation, table, condition) =>
        if (isTruncateTable(condition)) {
          TruncatePaimonTableWithFilter(table, None)
        } else if (isTruncatePartition(table, condition)) {
          tryConvertToPartitionPredicate(r, table, condition) match {
            case Some(p) => TruncatePaimonTableWithFilter(table, Some(p))
            case _ => d
          }
        } else {
          d
        }
    }
  }

  def isMetadataOnlyDelete(table: FileStoreTable, condition: Expression): Boolean = {
    isTruncateTable(condition) || isTruncatePartition(table, condition)
  }

  private def isTruncateTable(condition: Expression): Boolean = {
    condition == null || condition == TrueLiteral
  }

  private def isTruncatePartition(table: FileStoreTable, condition: Expression): Boolean = {
    val partitionKeys = table.partitionKeys().asScala.toSeq

    partitionKeys.nonEmpty &&
    !table.coreOptions().deleteForceProduceChangelog() &&
    isPredicatePartitionColumnsOnly(condition, partitionKeys, resolver)
  }

  private def tryConvertToPartitionPredicate(
      relation: DataSourceV2Relation,
      table: FileStoreTable,
      condition: Expression): Option[PartitionPredicate] = {
    try {
      val partitionRowType = table.schema().logicalPartitionType()
      // For those delete conditions with subqueries that only contain partition columns, we can eval them in advance.
      val finalCondiction = if (SubqueryExpression.hasSubquery(condition)) {
        evalSubquery(condition)
      } else {
        condition
      }
      convertConditionToPaimonPredicate(finalCondiction, relation.output, partitionRowType) match {
        case Some(p) => Some(PartitionPredicate.fromPredicate(partitionRowType, p))
        case None => None
      }
    } catch {
      case _: Throwable => None
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

        val executedPlan =
          SparkShimLoader.shim.classicApi.prepareExecutedPlan(spark, listQuery.plan)
        val physicalSubquery = execution.InSubqueryExec(
          expr,
          execution.SubqueryExec(s"subquery#${listQuery.exprId.id}", executedPlan),
          listQuery.exprId)
        evalPhysicalSubquery(physicalSubquery)

        physicalSubquery.values() match {
          case Some(l) if l.length > 0 => In(expr, l.map(Literal(_, expr.dataType)).toSeq)
          case _ => Literal(false, BooleanType)
        }

      case s: ScalarSubquery =>
        if (s.isCorrelated) {
          throw new RuntimeException("Correlated ScalarSubquery is not supported")
        }

        val executedPlan = SparkShimLoader.shim.classicApi.prepareExecutedPlan(spark, s.plan)
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
