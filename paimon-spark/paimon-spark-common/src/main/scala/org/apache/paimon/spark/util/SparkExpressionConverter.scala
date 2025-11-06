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
import org.apache.paimon.predicate.{ConcatTransform, FieldRef, FieldTransform, Transform, UpperTransform}
import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.spark.util.shim.TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType
import org.apache.paimon.types.{DecimalType, RowType}
import org.apache.paimon.types.DataTypeRoot._

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, Literal, NamedReference}

import scala.collection.JavaConverters._

object SparkExpressionConverter {

  // Supported transform names
  private val CONCAT = "CONCAT"
  private val UPPER = "UPPER"

  /** Convert Spark [[Expression]] to Paimon [[Transform]], return None if not supported. */
  def toPaimonTransform(exp: Expression, rowType: RowType): Option[Transform] = {
    exp match {
      case n: NamedReference => Some(new FieldTransform(toPaimonFieldRef(n, rowType)))
      case s: GeneralScalarExpression =>
        s.name() match {
          case CONCAT =>
            val inputs = exp.children().map {
              case n: NamedReference => toPaimonFieldRef(n, rowType)
              case l: Literal[_] => toPaimonLiteral(l)
              case _ => return None
            }
            Some(new ConcatTransform(inputs.toList.asJava))
          case UPPER =>
            val inputs = exp.children().map {
              case n: NamedReference => toPaimonFieldRef(n, rowType)
              case l: Literal[_] => toPaimonLiteral(l)
              case _ => return None
            }
            Some(new UpperTransform(inputs.toList.asJava))
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

    val dataType = SparkTypeUtils.toPaimonType(literal.dataType())
    val value = literal.value()
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
