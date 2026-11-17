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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.data.GenericRow
import org.apache.paimon.spark.{SparkInternalRowWrapper, SparkTypeUtils}
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.types.{ArrayType, BlobType, DataField, DataType => PaimonType, MapType, MultisetType, RowType, TimestampType}
import org.apache.paimon.utils.{GeneratedColumnRow, InternalRowUtils}

import org.apache.spark.sql.catalyst.{InternalRow => CatalystRow}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, LeafExpression, Literal, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

import scala.collection.JavaConverters._

/** Shares default normalization and expression evaluation with the storage writer. */
case class PaimonGeneratedColumn(
    override val children: Seq[Expression],
    sources: Seq[DataField],
    field: DataField)
  extends Expression
  with CodegenFallback {

  @transient private lazy val sourceType = new RowType(sources.asJava)
  @transient private lazy val generator =
    new GeneratedColumnRow(new RowType((sources :+ field).asJava), sourceType)
  @transient private lazy val input = new GenericInternalRow(sources.size)
  @transient private lazy val wrapper = new SparkInternalRowWrapper(
    StructType(children.zipWithIndex.map {
      case (child, index) => StructField(sources(index).name(), child.dataType, child.nullable)
    }),
    sources.size)
  @transient private lazy val result = new GenericRow(1)
  @transient private lazy val sparkResult = SparkInternalRow.create(RowType.of(field))

  override def dataType: DataType = SparkTypeUtils.fromPaimonType(field.`type`())

  override def nullable: Boolean = true

  override def eval(row: CatalystRow): Any = {
    children.indices.foreach(i => input.update(i, children(i).eval(row)))
    val generated = generator.replaceRow(wrapper.replace(input))
    result.setField(0, InternalRowUtils.get(generated, sources.size, field.`type`()))
    sparkResult.replace(result).get(0, dataType)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}

object PaimonGeneratedColumn {

  /** Store SQL-standard string literals independently of Spark's version-specific SQL printer. */
  def expressionSql(expression: Expression): String = {
    expression.transformUp {
      case literal: Literal if literal.dataType == StringType => SqlStringLiteral(literal)
    }.sql
  }

  private case class SqlStringLiteral(literal: Literal) extends LeafExpression with Unevaluable {
    override def dataType: DataType = literal.dataType
    override def nullable: Boolean = literal.nullable
    override def sql: String =
      if (literal.value == null) "NULL" else "'" + literal.value.toString.replace("'", "''") + "'"
  }

  def validateResultType(field: DataField): Unit = {
    def representable(dataType: PaimonType): Boolean = dataType match {
      case timestamp: TimestampType => timestamp.getPrecision <= 6
      case _: BlobType => false
      case array: ArrayType => representable(array.getElementType)
      case map: MapType => representable(map.getKeyType) && representable(map.getValueType)
      case multiset: MultisetType => representable(multiset.getElementType)
      case row: RowType => row.getFields.asScala.forall(f => representable(f.`type`()))
      case _ => true
    }
    require(
      representable(field.`type`()),
      s"Spark cannot losslessly write generated column '${field.name()}' with BLOB results or TIMESTAMP precision greater than 6."
    )
  }
}
