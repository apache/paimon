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

package org.apache.paimon.spark.util

import org.apache.paimon.data.{BinaryString, Decimal, Timestamp}
import org.apache.paimon.predicate._
import org.apache.paimon.spark.{PaimonImplicits, SparkTypeUtils}
import org.apache.paimon.spark.util.shim.TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType
import org.apache.paimon.types.{DecimalType, RowType}
import org.apache.paimon.types.DataTypeRoot._

import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils}
import org.apache.spark.sql.connector.expressions.{Cast, Expression, GeneralScalarExpression, Literal, NamedReference}
import org.apache.spark.sql.types.{ArrayType => SparkArrayType, DataType => SparkDataType}

import scala.collection.JavaConverters._

object SparkExpressionConverter {

  import PaimonImplicits._

  // Supported general scalar transform names
  private val CONCAT = "CONCAT"
  private val UPPER = "UPPER"
  private val LOWER = "LOWER"
  private val SUBSTRING = "SUBSTRING"
  private val TRIM = "TRIM"
  private val LTRIM = "LTRIM"
  private val RTRIM = "RTRIM"

  /** Convert Spark [[Expression]] to Paimon [[Transform]], return None if not supported. */
  def toPaimonTransform(exp: Expression, rowType: RowType): Option[Transform] = {

    def convertChildren(children: Seq[Expression]) = {
      val converted = children.map {
        case n: NamedReference => Some(toPaimonFieldRef(n, rowType))
        case l: Literal[_] => Some(toPaimonLiteral(l))
        case _ => None
      }
      if (converted.exists(_.isEmpty)) {
        None
      } else {
        Some(converted.map(_.get).asJava)
      }
    }

    exp match {
      case n: NamedReference => toPaimonFieldTransform(n, rowType)
      case s: GeneralScalarExpression =>
        s.name() match {
          case CONCAT => convertChildren(s.children()).map(i => new ConcatTransform(i))
          case UPPER => convertChildren(s.children()).map(i => new UpperTransform(i))
          case LOWER => convertChildren(s.children()).map(i => new LowerTransform(i))
          case SUBSTRING => convertChildren(s.children()).map(i => new SubstringTransform(i))
          case TRIM =>
            convertChildren(s.children()).map(i => new TrimTransform(i, TrimTransform.Flag.BOTH))
          case LTRIM =>
            convertChildren(s.children()).map(i => new TrimTransform(i, TrimTransform.Flag.LEADING))
          case RTRIM =>
            convertChildren(s.children()).map(
              i => new TrimTransform(i, TrimTransform.Flag.TRAILING))
          case _ => None
        }
      case c: Cast =>
        c.expression() match {
          case n: NamedReference =>
            CastTransform.tryCreate(
              toPaimonFieldRef(n, rowType),
              SparkTypeUtils.toPaimonType(c.dataType()))
          case _ => None
        }
      case _ => None
    }
  }

  /** Convert Spark [[Literal]] to Paimon literal. */
  def toPaimonLiteral(literal: Literal[_]): Object = {
    if (literal == null) {
      return null
    }

    if (literal.children().nonEmpty) {
      throw new UnsupportedOperationException(s"Convert value: $literal is unsupported.")
    }

    toPaimonLiteral(literal.value(), literal.dataType())
  }

  /** Convert a Spark ARRAY [[Literal]] to Paimon element literals. */
  def toPaimonArrayLiteral(literal: Literal[_]): Seq[Object] = {
    literal.dataType() match {
      case SparkArrayType(elementType, _) =>
        val array = literal.value().asInstanceOf[ArrayData]
        (0 until array.numElements()).map {
          i =>
            if (array.isNullAt(i)) {
              null
            } else {
              toPaimonLiteral(array.get(i, elementType), elementType)
            }
        }
      case _ =>
        throw new UnsupportedOperationException(s"Convert value: $literal is unsupported.")
    }
  }

  private def toPaimonLiteral(value: Any, sparkDataType: SparkDataType): Object = {
    val dataType = SparkTypeUtils.toPaimonType(sparkDataType)
    dataType.getTypeRoot match {
      case BOOLEAN | BIGINT | DOUBLE | TINYINT | SMALLINT | INTEGER | FLOAT | DATE =>
        value.asInstanceOf[AnyRef]
      case VARCHAR =>
        BinaryString.fromString(value.toString)
      case DECIMAL =>
        val decimalType = dataType.asInstanceOf[DecimalType]
        val precision = decimalType.getPrecision
        val scale = decimalType.getScale
        Decimal.fromBigDecimal(
          value.asInstanceOf[org.apache.spark.sql.types.Decimal].toJavaBigDecimal,
          precision,
          scale)
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        Timestamp.fromMicros(value.asInstanceOf[Long])
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        if (treatPaimonTimestampTypeAsSparkTimestampType()) {
          Timestamp.fromSQLTimestamp(DateTimeUtils.toJavaTimestamp(value.asInstanceOf[Long]))
        } else {
          Timestamp.fromMicros(value.asInstanceOf[Long])
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"Convert value: $value to datatype: $dataType is unsupported.")
    }
  }

  /**
   * A reference is either a top-level column or a path down into row-typed ones. Anything the path
   * cannot descend - a field inside an array or a map, a name the schema does not hold - yields
   * None, leaving the predicate for Spark to evaluate after the scan.
   */
  private def toPaimonFieldTransform(ref: NamedReference, rowType: RowType): Option[Transform] = {
    val parts = ref.fieldNames()
    val index = rowType.getFieldIndex(parts.head)
    if (index == -1) {
      return None
    }
    val root = rowType.getField(parts.head)
    val rootRef = new FieldRef(index, root.name(), root.`type`())
    if (parts.length == 1) {
      return Some(new FieldTransform(rootRef))
    }

    // Keep the components Spark gave us: they are the transform's identity, and joining them
    // would lose the boundaries of a name that itself contains a dot.
    val path = new java.util.ArrayList[String](parts.length - 1)
    var current = root.`type`()
    parts.tail.foreach {
      part =>
        current match {
          case nested: RowType =>
            val position = nested.getFieldIndex(part)
            if (position == -1) {
              return None
            }
            path.add(part)
            current = nested.getTypeAt(position)
          case _ => return None
        }
    }
    Some(new NestedFieldTransform(rootRef, path))
  }

  private def toPaimonFieldRef(ref: NamedReference, rowType: RowType): FieldRef = {
    val fieldName = toFieldName(ref)
    val f = rowType.getField(fieldName)
    // Note: here should use fieldIndex instead of fieldId
    val index = rowType.getFieldIndex(fieldName)
    if (index == -1) {
      throw new UnsupportedOperationException(s"Nested field '$fieldName' is unsupported.")
    }
    new FieldRef(index, f.name(), f.`type`())
  }

  private def toFieldName(ref: NamedReference): String = ref.fieldNames().mkString(".")
}
