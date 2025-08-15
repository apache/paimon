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

package org.apache.paimon.spark.catalyst.analysis.expressions

import org.apache.paimon.predicate.{Predicate, PredicateBuilder}
import org.apache.paimon.spark.{SparkFilterConverter, SparkV2FilterConverter}
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.write.PaimonWriteBuilder
import org.apache.paimon.types.RowType

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.PaimonUtils.{normalizeExprs, translateFilterV2}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, Cast, Expression, GetStructField, Literal, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.sources.{AlwaysTrue, And => SourceAnd, EqualNullSafe, EqualTo, Filter => SourceFilter}
import org.apache.spark.sql.types.{DataType, NullType}

/** An expression helper. */
trait ExpressionHelper extends ExpressionHelperBase {

  def convertConditionToPaimonPredicate(
      condition: Expression,
      output: Seq[Attribute],
      rowType: RowType,
      ignorePartialFailure: Boolean = false): Option[Predicate] = {
    val converter = SparkV2FilterConverter(rowType)
    val sparkPredicates = normalizeExprs(Seq(condition), output)
      .flatMap(splitConjunctivePredicates(_).flatMap {
        f =>
          val predicate =
            try {
              translateFilterV2(f)
            } catch {
              case _: Throwable =>
                None
            }
          if (predicate.isEmpty && !ignorePartialFailure) {
            throw new RuntimeException(s"Cannot translate expression to predicate: $f")
          }
          predicate
      })

    val predicates = sparkPredicates.flatMap(converter.convert(_, ignorePartialFailure))
    if (predicates.isEmpty) {
      None
    } else {
      Some(PredicateBuilder.and(predicates: _*))
    }
  }

  def resolveFilter(
      spark: SparkSession,
      relation: DataSourceV2Relation,
      conditionSql: String): Expression = {
    val unResolvedExpression = spark.sessionState.sqlParser.parseExpression(conditionSql)
    val filter = Filter(unResolvedExpression, relation)
    spark.sessionState.analyzer.executeAndCheck(filter, new QueryPlanningTracker) match {
      case filter: Filter =>
        try {
          ConstantFolding.apply(filter).asInstanceOf[Filter].condition
        } catch {
          case _: Throwable => filter.condition
        }
      case _ =>
        throw new RuntimeException(
          s"Could not resolve expression $conditionSql in relation: $relation")
    }
  }
}

trait ExpressionHelperBase extends PredicateHelper {

  import ExpressionHelper._

  /**
   * For the 'INSERT OVERWRITE' semantics of SQL, Spark DataSourceV2 will call the `truncate`
   * methods where the `AlwaysTrue` Filter is used.
   */
  def isTruncate(filter: SourceFilter): Boolean = {
    val filters = splitConjunctiveFilters(filter)
    filters.length == 1 && filters.head.isInstanceOf[AlwaysTrue]
  }

  /** See [[ PaimonWriteBuilder#failIfCanNotOverwrite]] */
  def convertPartitionFilterToMap(
      filter: SourceFilter,
      partitionRowType: RowType): Map[String, String] = {
    // todo: replace it with SparkV2FilterConverter when we drop Spark3.2
    val converter = new SparkFilterConverter(partitionRowType)
    splitConjunctiveFilters(filter).map {
      case EqualNullSafe(attribute, value) =>
        (attribute, converter.convertString(attribute, value))
      case EqualTo(attribute, value) =>
        (attribute, converter.convertString(attribute, value))
      case _ =>
        // Should not happen
        throw new RuntimeException(
          s"Only support Overwrite filters with Equal and EqualNullSafe, but got: $filter")
    }.toMap
  }

  private def splitConjunctiveFilters(filter: SourceFilter): Seq[SourceFilter] = {
    filter match {
      case SourceAnd(filter1, filter2) =>
        splitConjunctiveFilters(filter1) ++ splitConjunctiveFilters(filter2)
      case other => other :: Nil
    }
  }

  def toColumn(expr: Expression): Column = {
    SparkShimLoader.shim.classicApi.column(expr)
  }

  def toExpression(spark: SparkSession, col: Column): Expression = {
    SparkShimLoader.shim.classicApi.expression(spark, col)
  }

  protected def resolveExpression(
      spark: SparkSession)(expr: Expression, plan: LogicalPlan): Expression = {
    if (expr.resolved) {
      expr
    } else {
      val newPlan = FakeLogicalPlan(Seq(expr), plan.children)
      spark.sessionState.analyzer.execute(newPlan) match {
        case FakeLogicalPlan(resolvedExpr, _) =>
          resolvedExpr.foreach {
            expr =>
              if (!expr.resolved) {
                throw new RuntimeException(s"cannot resolve ${expr.sql} from $plan")
              }
          }
          resolvedExpr.head
        case _ =>
          throw new RuntimeException(s"Could not resolve expression $expr in plan: $plan")
      }
    }
  }

  protected def resolveExpressions(
      spark: SparkSession)(exprs: Seq[Expression], plan: LogicalPlan): Seq[Expression] = {
    val newPlan = FakeLogicalPlan(exprs, plan.children)
    spark.sessionState.analyzer.execute(newPlan) match {
      case FakeLogicalPlan(resolvedExpr, _) =>
        resolvedExpr
      case _ =>
        throw new RuntimeException(s"Could not resolve expressions $exprs in plan: $plan")
    }
  }

  /**
   * Get the parts of the expression that are only relevant to the plan, and then compose the new
   * expression.
   */
  protected def getExpressionOnlyRelated(
      expression: Expression,
      plan: LogicalPlan): Option[Expression] = {
    val expressions = splitConjunctivePredicates(expression).filter {
      expression => canEvaluate(expression, plan) && canEvaluateWithinJoin(expression)
    }
    expressions.reduceOption(And)
  }

  protected def castIfNeeded(fromExpression: Expression, toDataType: DataType): Expression = {
    fromExpression match {
      case Literal(null, NullType) => Literal(null, toDataType)
      case _ =>
        val fromDataType = fromExpression.dataType
        if (!Cast.canCast(fromDataType, toDataType)) {
          throw new RuntimeException(s"Can't cast from $fromDataType to $toDataType.")
        }
        if (DataType.equalsIgnoreCaseAndNullability(fromDataType, toDataType)) {
          fromExpression
        } else {
          Compatibility.cast(fromExpression, toDataType, Option(SQLConf.get.sessionLocalTimeZone))
        }
    }
  }

  protected def toRefSeq(expr: Expression): Seq[String] = expr match {
    case attr: Attribute =>
      Seq(attr.name)
    case GetStructField(child, _, Some(name)) =>
      toRefSeq(child) :+ name
    case Alias(child, _) =>
      toRefSeq(child)
    case other =>
      throw new UnsupportedOperationException(
        s"Unsupported update expression: $other, only support update with PrimitiveType and StructType.")
  }

  def splitPruePartitionAndOtherPredicates(
      condition: Expression,
      partitionColumns: Seq[String],
      resolver: Resolver): (Seq[Expression], Seq[Expression]) = {
    splitConjunctivePredicates(condition)
      .partition {
        isPredicatePartitionColumnsOnly(_, partitionColumns, resolver) && !SubqueryExpression
          .hasSubquery(condition)
      }
  }

  def isPredicatePartitionColumnsOnly(
      condition: Expression,
      partitionColumns: Seq[String],
      resolver: Resolver
  ): Boolean = {
    condition.references.nonEmpty &&
    condition.references.forall(r => partitionColumns.exists(resolver(r.name, _)))
  }

  /**
   * A valid predicate should meet two requirements: 1) This predicate only contains partition
   * columns. 2) This predicate doesn't contain subquery.
   */
  def isValidPredicate(
      spark: SparkSession,
      expr: Expression,
      partitionCols: Array[String]): Boolean = {
    val resolver = spark.sessionState.analyzer.resolver
    splitConjunctivePredicates(expr).forall(
      e =>
        isPredicatePartitionColumnsOnly(e, partitionCols, resolver) &&
          !SubqueryExpression.hasSubquery(expr))
  }
}

object ExpressionHelper {

  case class FakeLogicalPlan(exprs: Seq[Expression], children: Seq[LogicalPlan])
    extends LogicalPlan {
    override def output: Seq[Attribute] = Nil

    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[LogicalPlan]): FakeLogicalPlan = copy(children = newChildren)
  }

}
