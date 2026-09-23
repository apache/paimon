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

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.catalyst.optimizer.{PaimonArrayContains, PaimonArraysOverlap, PushDownArrayPredicates}

import org.apache.spark.sql.catalyst.expressions.{ArrayContains, ArraysOverlap, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

abstract class ArrayPredicatePushDownVersionTestBase extends PaimonSparkTestBase {

  protected def supportsArrayPredicatePushDown: Boolean

  test("register array predicate pushdown only for supported Spark versions") {
    val registered = spark.sessionState.optimizer.batches
      .flatMap(_.rules)
      .exists(_.getClass == PushDownArrayPredicates.getClass)

    assert(registered == supportsArrayPredicatePushDown)
  }

  test("rewrite array predicates only for supported Spark versions") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, tags ARRAY<STRING>)")

      Seq(
        "SELECT * FROM t WHERE array_contains(tags, 'red')",
        "SELECT * FROM t WHERE arrays_overlap(tags, array('red', 'blue'))"
      ).foreach {
        query =>
          val optimizedPlan = sql(query).queryExecution.optimizedPlan
          assert(
            containsPaimonArrayPredicate(optimizedPlan) == supportsArrayPredicatePushDown,
            optimizedPlan.toString())
          assert(
            containsSparkArrayPredicate(optimizedPlan) != supportsArrayPredicatePushDown,
            optimizedPlan.toString())
      }
    }
  }

  private def containsPaimonArrayPredicate(plan: LogicalPlan): Boolean =
    containsExpression(
      plan,
      {
        case _: PaimonArrayContains | _: PaimonArraysOverlap => true
        case _ => false
      })

  private def containsSparkArrayPredicate(plan: LogicalPlan): Boolean =
    containsExpression(
      plan,
      {
        case _: ArrayContains | _: ArraysOverlap => true
        case _ => false
      })

  private def containsExpression(plan: LogicalPlan, predicate: Expression => Boolean): Boolean =
    plan.find(_.expressions.exists(_.find(predicate).isDefined)).isDefined
}
