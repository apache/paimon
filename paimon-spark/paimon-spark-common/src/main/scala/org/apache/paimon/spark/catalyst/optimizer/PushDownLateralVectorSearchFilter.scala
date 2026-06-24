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

import org.apache.paimon.spark.catalyst.plans.logical.{LateralVectorSearch, PaimonTableValuedFunctions}

import org.apache.spark.sql.catalyst.expressions.{And, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/** Pushes filters on the query side below lateral vector search. */
object PushDownLateralVectorSearchFilter extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case filter @ Filter(condition, lvs: LateralVectorSearch) =>
      val predicates = splitConjunctivePredicates(condition)
      val (pushDownToLeft, otherPredicates) = predicates.partition {
        predicate => predicate.deterministic && predicate.references.subsetOf(lvs.child.outputSet)
      }
      val (pushDownToSearch, stayUp) = otherPredicates.partition {
        predicate =>
          predicate.deterministic &&
          predicate.references.nonEmpty &&
          predicate.references.subsetOf(lvs.searchFilterOutputSet) &&
          PaimonTableValuedFunctions
            .convertLateralVectorSearchFilters(
              lvs.innerTable,
              lvs.vectorSearchOutput,
              lvs.projectList,
              lvs.projectOutput,
              Seq(predicate))
            .isDefined
      }

      if (pushDownToLeft.isEmpty && pushDownToSearch.isEmpty) {
        filter
      } else {
        val lvsWithPushedLeft = if (pushDownToLeft.isEmpty) {
          lvs
        } else {
          lvs.copy(left = Filter(buildBalancedPredicate(pushDownToLeft, And), lvs.child))
        }
        val newLateralVectorSearch = if (pushDownToSearch.isEmpty) {
          lvsWithPushedLeft
        } else {
          lvsWithPushedLeft.copy(
            searchFilters = lvsWithPushedLeft.searchFilters ++ pushDownToSearch)
        }
        if (stayUp.isEmpty) {
          newLateralVectorSearch
        } else {
          Filter(buildBalancedPredicate(stayUp, And), newLateralVectorSearch)
        }
      }
  }
}
