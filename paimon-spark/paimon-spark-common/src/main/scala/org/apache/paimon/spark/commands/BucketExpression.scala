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

package org.apache.paimon.spark.commands

import org.apache.paimon.spark.catalog.functions.PaimonFunctions
import org.apache.paimon.spark.catalog.functions.PaimonFunctions.BUCKET

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow => SparkInternalRow}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * The reason for adding it is that the current spark_catalog cannot access v2 functions, which
 * results in the inability to recognize the `bucket()` function in write, see
 * https://github.com/apache/spark/pull/50495, once it is fixed, remove this function.
 *
 * @param _children
 *   arg0: bucket number, arg1..argn bucket key
 */
case class FixedBucketExpression(_children: Seq[Expression])
  extends Expression
  with CodegenFallback {

  val function: ScalarFunction[Int] = {
    val inputType = StructType(_children.zipWithIndex.map {
      case (exp, pos) => StructField(s"_$pos", exp.dataType, exp.nullable)
    })

    PaimonFunctions.load(BUCKET).bind(inputType).asInstanceOf[ScalarFunction[Int]]
  }

  private lazy val reusedRow = new SpecificInternalRow(function.inputTypes())

  override def nullable: Boolean = function.isResultNullable

  override def eval(input: SparkInternalRow): Int = {
    var i = 0
    while (i < children.length) {
      val expr = children(i)
      reusedRow.update(i, expr.eval(input))
      i += 1
    }

    function.produceResult(reusedRow)
  }

  override def dataType: DataType = function.resultType()

  override def children: Seq[Expression] = _children

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(_children = newChildren)
  }

  override def canEqual(that: Any): Boolean = false
}

object BucketExpression {

  val FIXED_BUCKET = "fixed_bucket"
  val supportedFnNames: Seq[String] = Seq(FIXED_BUCKET)

  private type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)

  def getFunctionInjection(fnName: String): FunctionDescription = {
    val (info, builder) = fnName match {
      case FIXED_BUCKET =>
        FunctionRegistryBase.build[FixedBucketExpression](fnName, since = None)
      case _ =>
        throw new Exception(s"Function $fnName isn't a supported scalar function.")
    }
    val ident = FunctionIdentifier(fnName)
    (ident, info, builder)
  }

  def quote(columnName: String): String = s"`${columnName.replace("`", "``")}`"
}
