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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, CreateNamedStruct, Expression, ExprId, GetStructField, LeafExpression, Literal, NamedExpression, PredicateHelper, ScalarSubquery, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, Filter, Join, LogicalPlan, Project, Subquery, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{SCALAR_SUBQUERY, SCALAR_SUBQUERY_REFERENCE, TreePattern}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 * Spark 4.1 shim for MergePaimonScalarSubqueriesBase.
 *
 * In Spark 4.1, CTERelationDef gained a 5th parameter (maxDepth). The base trait in
 * paimon-spark-common was compiled against Spark 4.0.2's 4-parameter CTERelationDef, causing
 * NoSuchMethodError at runtime. This shim recompiles the entire trait against Spark 4.1.1.
 */
trait MergePaimonScalarSubqueriesBase extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Subquery reuse needs to be enabled for this optimization.
      case _ if !conf.getConf(SQLConf.SUBQUERY_REUSE_ENABLED) && !existsPaimonScan(plan) => plan

      // This rule does a whole plan traversal, no need to run on subqueries.
      case _: Subquery => plan

      // Plans with CTEs are not supported for now.
      case _: WithCTE => plan

      case _ => extractCommonScalarSubqueries(plan)
    }
  }

  private def existsPaimonScan(plan: LogicalPlan): Boolean = {
    plan.find {
      case r: DataSourceV2ScanRelation => r.scan.isInstanceOf[PaimonScan]
      case _ => false
    }.isDefined
  }

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
      WithCTE(newPlan, subqueryCTEs.toSeq)
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

  protected def checkIdenticalPlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan): Option[AttributeMap[Attribute]] = {
    if (newPlan.canonicalized == cachedPlan.canonicalized) {
      Some(AttributeMap(newPlan.output.zip(cachedPlan.output)))
    } else {
      None
    }
  }

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
    if (scan1 == scan2) {
      Some(scan2)
    } else if (scan1 == scan2.copy(requiredSchema = scan1.requiredSchema)) {
      val mergedRequiredSchema = StructType(
        (scan2.requiredSchema.fields.toSet ++ scan1.requiredSchema.fields.toSet).toArray)
      Some(scan2.copy(requiredSchema = mergedRequiredSchema))
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

  private def supportedAggregateMerge(newPlan: Aggregate, cachedPlan: Aggregate): Boolean = {
    val aggregateExpressionsSeq = Seq(newPlan, cachedPlan).map {
      plan => plan.aggregateExpressions.flatMap(_.collect { case a: AggregateExpression => a })
    }
    val groupByExpressionSeq = Seq(newPlan, cachedPlan).map(_.groupingExpressions)

    val Seq(newPlanSupportsHashAggregate, cachedPlanSupportsHashAggregate) =
      aggregateExpressionsSeq.zip(groupByExpressionSeq).map {
        case (aggregateExpressions, groupByExpressions) =>
          SparkShimLoader.shim.supportsHashAggregate(
            aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes),
            groupByExpressions)
      }

    newPlanSupportsHashAggregate && cachedPlanSupportsHashAggregate ||
    newPlanSupportsHashAggregate == cachedPlanSupportsHashAggregate && {
      val Seq(newPlanSupportsObjectHashAggregate, cachedPlanSupportsObjectHashAggregate) =
        aggregateExpressionsSeq.zip(groupByExpressionSeq).map {
          case (aggregateExpressions, groupByExpressions: Seq[Expression]) =>
            SparkShimLoader.shim
              .supportsObjectHashAggregate(aggregateExpressions, groupByExpressions)
        }
      newPlanSupportsObjectHashAggregate && cachedPlanSupportsObjectHashAggregate ||
      newPlanSupportsObjectHashAggregate == cachedPlanSupportsObjectHashAggregate
    }
  }

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
                  SparkShimLoader.shim.createCTERelationRef(
                    subqueryCTE.id,
                    resolved = true,
                    subqueryCTE.output,
                    isStreaming = subqueryCTE.isStreaming),
                  ssr.exprId),
                ssr.headerIndex
              )
            } else {
              createScalarSubquery(header.plan, ssr.exprId)
            }
        }
    }
  }

  protected def createScalarSubquery(plan: LogicalPlan, exprId: ExprId): ScalarSubquery

}
