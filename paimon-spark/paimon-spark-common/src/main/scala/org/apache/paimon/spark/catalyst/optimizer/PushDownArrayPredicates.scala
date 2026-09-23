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

import org.apache.paimon.spark.catalyst.analysis.PaimonRelation

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{ArrayContains, ArraysOverlap, BinaryComparison, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/** Makes literal array predicates visible to Spark's V2 predicate translator for Paimon scans. */
object PushDownArrayPredicates extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case filter @ Filter(condition, child) if PaimonRelation.isPaimonTable(child) =>
      filter.copy(condition = condition.transformDown {
        case ArrayContains(array, literal: Literal)
            if array.references.nonEmpty &&
              array.references.subsetOf(child.outputSet) &&
              literal.value != null =>
          PaimonArrayContains(array, literal)

        case ArraysOverlap(array, literal: Literal)
            if array.references.nonEmpty && array.references.subsetOf(child.outputSet) =>
          PaimonArraysOverlap(array, literal)

        case ArraysOverlap(literal: Literal, array)
            if array.references.nonEmpty && array.references.subsetOf(child.outputSet) =>
          PaimonArraysOverlap(array, literal)
      })
  }
}

/**
 * Spark's V2 expression builder translates [[BinaryComparison]] generically, but does not know
 * [[ArrayContains]]. This comparison-shaped wrapper keeps Spark's original evaluation semantics for
 * the residual filter while exposing an array membership marker to the connector.
 */
private[spark] case class PaimonArrayContains(left: Expression, right: Expression)
  extends BinaryComparison {

  private def original: ArrayContains = ArrayContains(left, right)

  // Use a V2 predicate name understood by Spark's SQL renderer. The Paimon converter
  // distinguishes string and array CONTAINS predicates from the transform output type.
  override def symbol: String = "CONTAINS"

  override def nullable: Boolean = original.nullable

  override def checkInputDataTypes(): TypeCheckResult = original.checkInputDataTypes()

  override protected def nullSafeEval(leftValue: Any, rightValue: Any): Any =
    original.nullSafeEval(leftValue, rightValue)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    original.doGenCode(ctx, ev)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): PaimonArrayContains = copy(left = newLeft, right = newRight)
}

/** Comparison-shaped V2 bridge for Spark's [[ArraysOverlap]] expression. */
private[spark] case class PaimonArraysOverlap(left: Expression, right: Expression)
  extends BinaryComparison {

  private def original: ArraysOverlap = ArraysOverlap(left, right)

  override def symbol: String = "CONTAINS"

  override def nullable: Boolean = original.nullable

  override def checkInputDataTypes(): TypeCheckResult = original.checkInputDataTypes()

  override protected def nullSafeEval(leftValue: Any, rightValue: Any): Any =
    original.nullSafeEval(leftValue, rightValue)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    original.doGenCode(ctx, ev)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): PaimonArraysOverlap = copy(left = newLeft, right = newRight)
}
