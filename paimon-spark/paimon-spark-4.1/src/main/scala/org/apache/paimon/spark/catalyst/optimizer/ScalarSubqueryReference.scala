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

import org.apache.spark.sql.catalyst.expressions.{ExprId, LeafExpression, Unevaluable}
import org.apache.spark.sql.catalyst.trees.TreePattern.{SCALAR_SUBQUERY_REFERENCE, TreePattern}
import org.apache.spark.sql.types.DataType

/**
 * Spark 4.1 shim for ScalarSubqueryReference.
 *
 * In Spark 4.0.2, Unevaluable extends FoldableUnevaluable. In Spark 4.1.1, FoldableUnevaluable was
 * removed. The base class in paimon-spark-common was compiled against 4.0.2 and its bytecode
 * references FoldableUnevaluable, causing ClassNotFoundException at runtime. This shim redefines
 * the class against Spark 4.1.1's Unevaluable.
 */
case class ScalarSubqueryReference(
    subqueryIndex: Int,
    headerIndex: Int,
    dataType: DataType,
    exprId: ExprId)
  extends LeafExpression
  with Unevaluable {
  override def nullable: Boolean = true

  final override val nodePatterns: Seq[TreePattern] = Seq(SCALAR_SUBQUERY_REFERENCE)

  override def stringArgs: Iterator[Any] =
    Iterator(subqueryIndex, headerIndex, dataType, exprId.id)
}
