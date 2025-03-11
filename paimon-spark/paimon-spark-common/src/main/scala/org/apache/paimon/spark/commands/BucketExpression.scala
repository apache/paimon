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

import org.apache.paimon.data.serializer.InternalRowSerializer
import org.apache.paimon.spark.SparkInternalRowWrapper
import org.apache.paimon.spark.SparkTypeUtils.toPaimonType
import org.apache.paimon.table.sink.KeyAndBucketExtractor.{bucket, bucketKeyHashCode}
import org.apache.paimon.types.{RowKind, RowType}

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow => SparkInternalRow}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/** @param _children arg0: bucket number, arg1..argn bucket key */
case class FixedBucketExpression(_children: Seq[Expression])
  extends Expression
  with CodegenFallback {

  private lazy val (bucketKeyRowType: RowType, bucketKeyStructType: StructType) = {
    val (originalTypes, paimonTypes) = _children.tail.map {
      expr =>
        (StructField(expr.prettyName, expr.dataType, nullable = true), toPaimonType(expr.dataType))
    }.unzip

    (
      RowType.of(paimonTypes: _*),
      StructType(originalTypes)
    )
  }

  private lazy val numberBuckets = _children.head.asInstanceOf[Literal].value.asInstanceOf[Int]
  private lazy val serializer = new InternalRowSerializer(bucketKeyRowType)
  private lazy val wrapper =
    new SparkInternalRowWrapper(-1, bucketKeyStructType, bucketKeyStructType.fields.length)

  override def nullable: Boolean = false

  override def eval(input: SparkInternalRow): Int = {
    val bucketKeyValues = _children.tail.map(_.eval(input))
    bucket(
      bucketKeyHashCode(
        serializer.toBinaryRow(wrapper.replace(SparkInternalRow.fromSeq(bucketKeyValues)))),
      numberBuckets)
  }

  override def dataType: DataType = DataTypes.IntegerType

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

}
