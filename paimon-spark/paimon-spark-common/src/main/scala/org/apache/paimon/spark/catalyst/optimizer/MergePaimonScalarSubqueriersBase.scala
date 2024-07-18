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

import org.apache.paimon.spark.PaimonScan
import org.apache.paimon.spark.util.CTERelationRefUtils

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, CreateNamedStruct, Expression, ExprId, GetStructField, LeafExpression, Literal, NamedExpression, PredicateHelper, ScalarSubquery, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, Filter, Join, LogicalPlan, Project, Subquery, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{SCALAR_SUBQUERY, SCALAR_SUBQUERY_REFERENCE, TreePattern}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 * Most codes are copied from [[org.apache.spark.sql.catalyst.optimizer.MergeScalarSubqueries]].
 *
 * That merge scalar subqueries for DataSource V2 can't be achieved on Spark Side, due lack of the
 * unified interface which can determine whether two [[DataSourceV2ScanRelation]]s can be merged and
 * reused. So we extend the [[tryMergePlans]] method to check and merge
 * [[DataSourceV2ScanRelation]]s, thus we can merge scalar subqueries for paimon.
 */
trait MergePaimonScalarSubqueriersBase extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Subquery reuse needs to be enabled for this optimization.
      case _ if !conf.getConf(SQLConf.SUBQUERY_REUSE_ENABLED) && !containPaimonScan(plan) => plan

      // This rule does a whole plan traversal, no need to run on subqueries.
      case _: Subquery => plan

      // Plans with CTEs are not supported for now.
      case _: WithCTE => plan

      case _ => extractCommonScalarSubqueries(plan)
    }
  }

  private def containPaimonScan(plan: LogicalPlan): Boolean = {
    plan.find {
      case r: DataSourceV2ScanRelation => r.scan.isInstanceOf[PaimonScan]
      case _ => false
    }.isDefined
  }

  /**
   * An item in the cache of merged scalar subqueries.
   *
   * @param attributes
   *   Attributes that form the struct scalar return value of a merged subquery.
   * @param plan
   *   The plan of a merged scalar subquery.
   * @param merged
   *   A flag to identify if this item is the result of merging subqueries. Please note that
   *   `attributes.size == 1` doesn't always mean that the plan is not merged as there can be
   *   subqueries that are different ([[checkIdenticalPlans]] is false) due to an extra [[Project]]
   *   node in one of them. In that case `attributes.size` remains 1 after merging, but the merged
   *   flag becomes true.
   */
  case class Header(attributes: Seq[Attribute], plan: LogicalPlan, merged: Boolean)

  private def extractCommonScalarSubqueries(plan: LogicalPlan) = {
    val cache = ArrayBuffer.empty[Header]
    val planWithReferences = insertReferences(plan, cache)
    cache.zipWithIndex.foreach {
      case (header, i) =>
        cache(i) = cache(i).copy(plan = if (header.merged) {
          CTERelationDef(
            createProject(header.attributes, removeReferences(header.plan, cache)),
            underSubquery = true)
        } else {
          removeReferences(header.plan, cache)
        })
    }
    val newPlan = removeReferences(planWithReferences, cache)
    val subqueryCTEs = cache.filter(_.merged).map(_.plan.asInstanceOf[CTERelationDef])
    if (subqueryCTEs.nonEmpty) {
      WithCTE(newPlan, subqueryCTEs)
    } else {
      newPlan
    }
  }

  // First traversal builds up the cache and inserts `ScalarSubqueryReference`s to the plan.
  private def insertReferences(plan: LogicalPlan, cache: ArrayBuffer[Header]): LogicalPlan = {
    plan.transformUpWithSubqueries {
      case n =>
        n.transformExpressionsUpWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY)) {
          case s: ScalarSubquery if !s.isCorrelated && s.deterministic =>
            val (subqueryIndex, headerIndex) = cacheSubquery(s.plan, cache)
            ScalarSubqueryReference(subqueryIndex, headerIndex, s.dataType, s.exprId)
        }
    }
  }

  // Caching returns the index of the subquery in the cache and the index of scalar member in the
  // "Header".
  private def cacheSubquery(plan: LogicalPlan, cache: ArrayBuffer[Header]): (Int, Int) = {
    val output = plan.output.head
    cache.zipWithIndex
      .collectFirst(Function.unlift {
        case (header, subqueryIndex) =>
          checkIdenticalPlans(plan, header.plan)
            .map {
              outputMap =>
                val mappedOutput = mapAttributes(output, outputMap)
                val headerIndex = header.attributes.indexWhere(_.exprId == mappedOutput.exprId)
                subqueryIndex -> headerIndex
            }
            .orElse(tryMergePlans(plan, header.plan).map {
              case (mergedPlan, outputMap) =>
                val mappedOutput = mapAttributes(output, outputMap)
                var headerIndex = header.attributes.indexWhere(_.exprId == mappedOutput.exprId)
                val newHeaderAttributes = if (headerIndex == -1) {
                  headerIndex = header.attributes.size
                  header.attributes :+ mappedOutput
                } else {
                  header.attributes
                }
                cache(subqueryIndex) = Header(newHeaderAttributes, mergedPlan, merged = true)
                subqueryIndex -> headerIndex
            })
      })
      .getOrElse {
        cache += Header(Seq(output), plan, merged = false)
        cache.length - 1 -> 0
      }
  }

  // If 2 plans are identical return the attribute mapping from the new to the cached version.
  protected def checkIdenticalPlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan): Option[AttributeMap[Attribute]] = {
    if (newPlan.canonicalized == cachedPlan.canonicalized) {
      Some(AttributeMap(newPlan.output.zip(cachedPlan.output)))
    } else {
      None
    }
  }

  // Recursively traverse down and try merging 2 plans. If merge is possible then return the merged
  // plan with the attribute mapping from the new to the merged version.
  // Please note that merging arbitrary plans can be complicated, the current version supports only
  // some of the most important nodes.
  private def tryMergePlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan): Option[(LogicalPlan, AttributeMap[Attribute])] = {
    checkIdenticalPlans(newPlan, cachedPlan)
      .map(cachedPlan -> _)
      .orElse((newPlan, cachedPlan) match {
        case (np: Project, cp: Project) =>
          tryMergePlans(np.child, cp.child).map {
            case (mergedChild, outputMap) =>
              val (mergedProjectList, newOutputMap) =
                mergeNamedExpressions(np.projectList, outputMap, cp.projectList)
              val mergedPlan = Project(mergedProjectList, mergedChild)
              mergedPlan -> newOutputMap
          }
        case (np, cp: Project) =>
          tryMergePlans(np, cp.child).map {
            case (mergedChild, outputMap) =>
              val (mergedProjectList, newOutputMap) =
                mergeNamedExpressions(np.output, outputMap, cp.projectList)
              val mergedPlan = Project(mergedProjectList, mergedChild)
              mergedPlan -> newOutputMap
          }
        case (np: Project, cp) =>
          tryMergePlans(np.child, cp).map {
            case (mergedChild, outputMap) =>
              val (mergedProjectList, newOutputMap) =
                mergeNamedExpressions(np.projectList, outputMap, cp.output)
              val mergedPlan = Project(mergedProjectList, mergedChild)
              mergedPlan -> newOutputMap
          }
        case (np: Aggregate, cp: Aggregate) if supportedAggregateMerge(np, cp) =>
          tryMergePlans(np.child, cp.child).flatMap {
            case (mergedChild, outputMap) =>
              val mappedNewGroupingExpression =
                np.groupingExpressions.map(mapAttributes(_, outputMap))
              // Order of grouping expression does matter as merging different grouping orders can
              // introduce "extra" shuffles/sorts that might not present in all of the original
              // subqueries.
              if (
                mappedNewGroupingExpression.map(_.canonicalized) ==
                  cp.groupingExpressions.map(_.canonicalized)
              ) {
                val (mergedAggregateExpressions, newOutputMap) =
                  mergeNamedExpressions(np.aggregateExpressions, outputMap, cp.aggregateExpressions)
                val mergedPlan =
                  Aggregate(cp.groupingExpressions, mergedAggregateExpressions, mergedChild)
                Some(mergedPlan -> newOutputMap)
              } else {
                None
              }
          }

        case (np: Filter, cp: Filter) =>
          tryMergePlans(np.child, cp.child).flatMap {
            case (mergedChild, outputMap) =>
              val mappedNewCondition = mapAttributes(np.condition, outputMap)
              // Comparing the canonicalized form is required to ignore different forms of the same
              // expression.
              if (mappedNewCondition.canonicalized == cp.condition.canonicalized) {
                val mergedPlan = cp.withNewChildren(Seq(mergedChild))
                Some(mergedPlan -> outputMap)
              } else {
                None
              }
          }

        case (np: Join, cp: Join) if np.joinType == cp.joinType && np.hint == cp.hint =>
          tryMergePlans(np.left, cp.left).flatMap {
            case (mergedLeft, leftOutputMap) =>
              tryMergePlans(np.right, cp.right).flatMap {
                case (mergedRight, rightOutputMap) =>
                  val outputMap = leftOutputMap ++ rightOutputMap
                  val mappedNewCondition = np.condition.map(mapAttributes(_, outputMap))
                  // Comparing the canonicalized form is required to ignore different forms of the same
                  // expression and `AttributeReference.quailifier`s in `cp.condition`.
                  if (
                    mappedNewCondition.map(_.canonicalized) == cp.condition.map(_.canonicalized)
                  ) {
                    val mergedPlan = cp.withNewChildren(Seq(mergedLeft, mergedRight))
                    Some(mergedPlan -> outputMap)
                  } else {
                    None
                  }
              }
          }
        case (
              newV2ScanRelation: DataSourceV2ScanRelation,
              cachedV2ScanRelation: DataSourceV2ScanRelation) =>
          tryMergeDataSourceV2ScanRelation(newV2ScanRelation, cachedV2ScanRelation)

        // Otherwise merging is not possible.
        case _ => None
      })
  }

  def tryMergeDataSourceV2ScanRelation(
      newV2ScanRelation: DataSourceV2ScanRelation,
      cachedV2ScanRelation: DataSourceV2ScanRelation)
      : Option[(LogicalPlan, AttributeMap[Attribute])]

  protected def samePartitioning(
      newPartitioning: Option[Seq[Expression]],
      cachedPartitioning: Option[Seq[Expression]],
      outputAttrMap: AttributeMap[Attribute]): Boolean = {
    val mappedNewPartitioning = newPartitioning.map(_.map(mapAttributes(_, outputAttrMap)))
    mappedNewPartitioning.map(_.map(_.canonicalized)) == cachedPartitioning.map(
      _.map(_.canonicalized))
  }

  protected def mergePaimonScan(scan1: PaimonScan, scan2: PaimonScan): Option[PaimonScan] = {
    if (
      scan1.table == scan2.table &&
      scan1.filters == scan2.filters &&
      scan1.pushDownLimit == scan2.pushDownLimit
    ) {

      if (scan1.requiredSchema == scan2.requiredSchema) {
        Some(scan2)
      } else {
        val mergedRequiredSchema = StructType(
          (scan2.requiredSchema.fields.toSet ++ scan1.requiredSchema.fields.toSet).toSeq)
        Some(scan2.copy(requiredSchema = mergedRequiredSchema))
      }
    } else {
      None
    }
  }

  private def createProject(attributes: Seq[Attribute], plan: LogicalPlan): Project = {
    Project(
      Seq(
        Alias(
          CreateNamedStruct(attributes.flatMap(a => Seq(Literal(a.name), a))),
          "mergedValue")()),
      plan)
  }

  protected def mapAttributes[T <: Expression](expr: T, outputMap: AttributeMap[Attribute]): T = {
    expr.transform { case a: Attribute => outputMap.getOrElse(a, a) }.asInstanceOf[T]
  }

  // Applies `outputMap` attribute mapping on attributes of `newExpressions` and merges them into
  // `cachedExpressions`. Returns the merged expressions and the attribute mapping from the new to
  // the merged version that can be propagated up during merging nodes.
  private def mergeNamedExpressions(
      newExpressions: Seq[NamedExpression],
      outputMap: AttributeMap[Attribute],
      cachedExpressions: Seq[NamedExpression]) = {
    val mergedExpressions = ArrayBuffer[NamedExpression](cachedExpressions: _*)
    val newOutputMap = AttributeMap(newExpressions.map {
      ne =>
        val mapped = mapAttributes(ne, outputMap)
        val withoutAlias = mapped match {
          case Alias(child, _) => child
          case e => e
        }
        ne.toAttribute -> mergedExpressions
          .find {
            case Alias(child, _) => child.semanticEquals(withoutAlias)
            case e => e.semanticEquals(withoutAlias)
          }
          .getOrElse {
            mergedExpressions += mapped
            mapped
          }
          .toAttribute
    })
    (mergedExpressions.toSeq, newOutputMap)
  }

  // Only allow aggregates of the same implementation because merging different implementations
  // could cause performance regression.
  private def supportedAggregateMerge(newPlan: Aggregate, cachedPlan: Aggregate) = {
    val newPlanAggregateExpressions = newPlan.aggregateExpressions.flatMap(_.collect {
      case a: AggregateExpression => a
    })
    val cachedPlanAggregateExpressions = cachedPlan.aggregateExpressions.flatMap(_.collect {
      case a: AggregateExpression => a
    })
    val newPlanSupportsHashAggregate = Aggregate.supportsHashAggregate(
      newPlanAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))
    val cachedPlanSupportsHashAggregate = Aggregate.supportsHashAggregate(
      cachedPlanAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))
    newPlanSupportsHashAggregate && cachedPlanSupportsHashAggregate ||
    newPlanSupportsHashAggregate == cachedPlanSupportsHashAggregate && {
      val newPlanSupportsObjectHashAggregate =
        Aggregate.supportsObjectHashAggregate(newPlanAggregateExpressions)
      val cachedPlanSupportsObjectHashAggregate =
        Aggregate.supportsObjectHashAggregate(cachedPlanAggregateExpressions)
      newPlanSupportsObjectHashAggregate && cachedPlanSupportsObjectHashAggregate ||
      newPlanSupportsObjectHashAggregate == cachedPlanSupportsObjectHashAggregate
    }
  }

  // Second traversal replaces `ScalarSubqueryReference`s to either
  // `GetStructField(ScalarSubquery(CTERelationRef to the merged plan)` if the plan is merged from
  // multiple subqueries or `ScalarSubquery(original plan)` if it isn't.
  private def removeReferences(plan: LogicalPlan, cache: ArrayBuffer[Header]) = {
    plan.transformUpWithSubqueries {
      case n =>
        n.transformExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY_REFERENCE)) {
          case ssr: ScalarSubqueryReference =>
            val header = cache(ssr.subqueryIndex)
            if (header.merged) {
              val subqueryCTE = header.plan.asInstanceOf[CTERelationDef]
              GetStructField(
                createScalarSubquery(
                  CTERelationRefUtils.createCTERelationRef(
                    subqueryCTE.id,
                    _resolved = true,
                    subqueryCTE.output),
                  ssr.exprId),
                ssr.headerIndex)
            } else {
              createScalarSubquery(header.plan, ssr.exprId)
            }
        }
    }
  }

  protected def createScalarSubquery(plan: LogicalPlan, exprId: ExprId): ScalarSubquery

}

/** Temporal reference to a subquery. */
case class ScalarSubqueryReference(
    subqueryIndex: Int,
    headerIndex: Int,
    dataType: DataType,
    exprId: ExprId)
  extends LeafExpression
  with Unevaluable {
  override def nullable: Boolean = true

  final override val nodePatterns: Seq[TreePattern] = Seq(SCALAR_SUBQUERY_REFERENCE)

  override def stringArgs: Iterator[Any] = Iterator(subqueryIndex, headerIndex, dataType, exprId.id)
}
