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

import org.apache.paimon.spark.SparkConnectorOptions
import org.apache.paimon.spark.catalyst.plans.logical.LateralVectorSearch
import org.apache.paimon.spark.util.OptionUtils

import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, CTERelationRef, GlobalLimit, HintInfo, Join, LogicalPlan, Repartition, RepartitionOperation, ResolvedHint, UnaryNode, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule

/** Restores parallelism lost by a global limit before executing a lateral vector search. */
object RepartitionLateralVectorSearchInput extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val cteDefinitions = plan
      .collect { case withCTE: WithCTE => withCTE.cteDefs }
      .flatten
      .map(definition => definition.id -> definition.child)
      .toMap

    plan.transformUp {
      case lateralVectorSearch: LateralVectorSearch
          if hasUnrepartitionedGlobalLimit(lateralVectorSearch.left, cteDefinitions, Set.empty) =>
        lateralVectorSearch.copy(
          left = Repartition(parallelism, shuffle = true, lateralVectorSearch.left))
    }
  }

  private def parallelism: Int = {
    val value =
      OptionUtils
        .getOptionString(SparkConnectorOptions.VECTOR_SEARCH_LATERAL_JOIN_PARALLELISM)
        .toInt
    require(
      value > 0,
      s"spark.paimon.${SparkConnectorOptions.VECTOR_SEARCH_LATERAL_JOIN_PARALLELISM.key()} " +
        s"must be positive, but got $value")
    value
  }

  private def hasUnrepartitionedGlobalLimit(
      plan: LogicalPlan,
      cteDefinitions: Map[Long, LogicalPlan],
      visitedCTEs: Set[Long]): Boolean = plan match {
    case repartition: RepartitionOperation if repartition.shuffle => false
    case repartition: RepartitionOperation =>
      hasUnrepartitionedGlobalLimit(repartition.child, cteDefinitions, visitedCTEs)
    case _: GlobalLimit => true
    case reference: CTERelationRef if !visitedCTEs.contains(reference.cteId) =>
      cteDefinitions
        .get(reference.cteId)
        .exists(hasUnrepartitionedGlobalLimit(_, cteDefinitions, visitedCTEs + reference.cteId))
    case join: Join
        if hasBroadcastHint(join.hint.rightHint) || hasResolvedBroadcastHint(join.right) =>
      hasUnrepartitionedGlobalLimit(join.left, cteDefinitions, visitedCTEs)
    case join: Join
        if hasBroadcastHint(join.hint.leftHint) || hasResolvedBroadcastHint(join.left) =>
      hasUnrepartitionedGlobalLimit(join.right, cteDefinitions, visitedCTEs)
    case unary: UnaryNode =>
      hasUnrepartitionedGlobalLimit(unary.child, cteDefinitions, visitedCTEs)
    case _ => false
  }

  private def hasBroadcastHint(hint: Option[HintInfo]): Boolean = {
    hint.flatMap(_.strategy).contains(BROADCAST)
  }

  private def hasResolvedBroadcastHint(plan: LogicalPlan): Boolean = {
    plan.exists {
      case hint: ResolvedHint => hasBroadcastHint(Some(hint.hints))
      case _ => false
    }
  }
}
