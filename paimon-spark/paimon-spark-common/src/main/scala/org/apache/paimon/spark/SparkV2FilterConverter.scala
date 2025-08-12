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

package org.apache.paimon.spark

import org.apache.paimon.data.{BinaryString, Decimal, Timestamp}
import org.apache.paimon.predicate.{Predicate, PredicateBuilder}
import org.apache.paimon.spark.util.shim.TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType
import org.apache.paimon.types.{DataTypeRoot, DecimalType, RowType}
import org.apache.paimon.types.DataTypeRoot._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.{Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{And, Not, Or, Predicate => SparkPredicate}

import scala.collection.JavaConverters._

/** Conversion from [[SparkPredicate]] to [[Predicate]]. */
case class SparkV2FilterConverter(rowType: RowType) {

  import org.apache.paimon.spark.SparkV2FilterConverter._

  val builder = new PredicateBuilder(rowType)

  def convert(sparkPredicate: SparkPredicate, ignoreFailure: Boolean = true): Option[Predicate] = {
    try {
      Some(convert(sparkPredicate))
    } catch {
      case _ if ignoreFailure => None
      case e: Throwable => throw e
    }
  }

  private def convert(sparkPredicate: SparkPredicate): Predicate = {
    sparkPredicate.name() match {
      case EQUAL_TO =>
        BinaryPredicate.unapply(sparkPredicate) match {
          case Some((fieldName, literal)) =>
            // TODO deal with isNaN
            val index = fieldIndex(fieldName)
            builder.equal(index, convertLiteral(index, literal))
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case EQUAL_NULL_SAFE =>
        BinaryPredicate.unapply(sparkPredicate) match {
          case Some((fieldName, literal)) =>
            val index = fieldIndex(fieldName)
            if (literal == null) {
              builder.isNull(index)
            } else {
              builder.equal(index, convertLiteral(index, literal))
            }
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case GREATER_THAN =>
        BinaryPredicate.unapply(sparkPredicate) match {
          case Some((fieldName, literal)) =>
            val index = fieldIndex(fieldName)
            builder.greaterThan(index, convertLiteral(index, literal))
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case GREATER_THAN_OR_EQUAL =>
        BinaryPredicate.unapply(sparkPredicate) match {
          case Some((fieldName, literal)) =>
            val index = fieldIndex(fieldName)
            builder.greaterOrEqual(index, convertLiteral(index, literal))
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case LESS_THAN =>
        BinaryPredicate.unapply(sparkPredicate) match {
          case Some((fieldName, literal)) =>
            val index = fieldIndex(fieldName)
            builder.lessThan(index, convertLiteral(index, literal))
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case LESS_THAN_OR_EQUAL =>
        BinaryPredicate.unapply(sparkPredicate) match {
          case Some((fieldName, literal)) =>
            val index = fieldIndex(fieldName)
            builder.lessOrEqual(index, convertLiteral(index, literal))
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case IN =>
        MultiPredicate.unapply(sparkPredicate) match {
          case Some((fieldName, literals)) =>
            val index = fieldIndex(fieldName)
            literals.map(convertLiteral(index, _)).toList.asJava
            builder.in(index, literals.map(convertLiteral(index, _)).toList.asJava)
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case IS_NULL =>
        UnaryPredicate.unapply(sparkPredicate) match {
          case Some(fieldName) =>
            builder.isNull(fieldIndex(fieldName))
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case IS_NOT_NULL =>
        UnaryPredicate.unapply(sparkPredicate) match {
          case Some(fieldName) =>
            builder.isNotNull(fieldIndex(fieldName))
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case AND =>
        val and = sparkPredicate.asInstanceOf[And]
        PredicateBuilder.and(convert(and.left), convert(and.right()))

      case OR =>
        val or = sparkPredicate.asInstanceOf[Or]
        PredicateBuilder.or(convert(or.left), convert(or.right()))

      case NOT =>
        val not = sparkPredicate.asInstanceOf[Not]
        val negate = convert(not.child()).negate()
        if (negate.isPresent) {
          negate.get()
        } else {
          throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case STRING_START_WITH =>
        BinaryPredicate.unapply(sparkPredicate) match {
          case Some((fieldName, literal)) =>
            val index = fieldIndex(fieldName)
            builder.startsWith(index, convertLiteral(index, literal))
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case STRING_END_WITH =>
        BinaryPredicate.unapply(sparkPredicate) match {
          case Some((fieldName, literal)) =>
            val index = fieldIndex(fieldName)
            builder.endsWith(index, convertLiteral(index, literal))
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case STRING_CONTAINS =>
        BinaryPredicate.unapply(sparkPredicate) match {
          case Some((fieldName, literal)) =>
            val index = fieldIndex(fieldName)
            builder.contains(index, convertLiteral(index, literal))
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      // TODO: AlwaysTrue, AlwaysFalse
      case _ => throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
    }
  }

  private def fieldIndex(fieldName: String): Int = {
    val index = rowType.getFieldIndex(fieldName)
    // TODO: support nested field
    if (index == -1) {
      throw new UnsupportedOperationException(s"Nested field '$fieldName' is unsupported.")
    }
    index
  }

  private def convertLiteral(index: Int, value: Any): AnyRef = {
    if (value == null) {
      return null
    }

    val dataType = rowType.getTypeAt(index)
    dataType.getTypeRoot match {
      case BOOLEAN | BIGINT | DOUBLE | TINYINT | SMALLINT | INTEGER | FLOAT | DATE =>
        value.asInstanceOf[AnyRef]
      case DataTypeRoot.VARCHAR =>
        BinaryString.fromString(value.toString)
      case DataTypeRoot.DECIMAL =>
        val decimalType = dataType.asInstanceOf[DecimalType]
        val precision = decimalType.getPrecision
        val scale = decimalType.getScale
        Decimal.fromBigDecimal(
          value.asInstanceOf[org.apache.spark.sql.types.Decimal].toJavaBigDecimal,
          precision,
          scale)
      case DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        Timestamp.fromMicros(value.asInstanceOf[Long])
      case DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
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
}

object SparkV2FilterConverter extends Logging {

  private val EQUAL_TO = "="
  private val EQUAL_NULL_SAFE = "<=>"
  private val GREATER_THAN = ">"
  private val GREATER_THAN_OR_EQUAL = ">="
  private val LESS_THAN = "<"
  private val LESS_THAN_OR_EQUAL = "<="
  private val IN = "IN"
  private val IS_NULL = "IS_NULL"
  private val IS_NOT_NULL = "IS_NOT_NULL"
  private val AND = "AND"
  private val OR = "OR"
  private val NOT = "NOT"
  private val STRING_START_WITH = "STARTS_WITH"
  private val STRING_END_WITH = "ENDS_WITH"
  private val STRING_CONTAINS = "CONTAINS"

  private object UnaryPredicate {
    def unapply(sparkPredicate: SparkPredicate): Option[String] = {
      sparkPredicate.children() match {
        case Array(n: NamedReference) => Some(toFieldName(n))
        case _ => None
      }
    }
  }

  private object BinaryPredicate {
    def unapply(sparkPredicate: SparkPredicate): Option[(String, Any)] = {
      sparkPredicate.children() match {
        case Array(l: NamedReference, r: Literal[_]) => Some((toFieldName(l), r.value))
        case Array(l: Literal[_], r: NamedReference) => Some((toFieldName(r), l.value))
        case _ => None
      }
    }
  }

  private object MultiPredicate {
    def unapply(sparkPredicate: SparkPredicate): Option[(String, Array[Any])] = {
      sparkPredicate.children() match {
        case Array(first: NamedReference, rest @ _*)
            if rest.nonEmpty && rest.forall(_.isInstanceOf[Literal[_]]) =>
          Some(toFieldName(first), rest.map(_.asInstanceOf[Literal[_]].value).toArray)
        case _ => None
      }
    }
  }

  private def toFieldName(ref: NamedReference): String = ref.fieldNames().mkString(".")

  def isSupportedRuntimeFilter(
      sparkPredicate: SparkPredicate,
      partitionKeys: Seq[String]): Boolean = {
    sparkPredicate.name() match {
      case IN =>
        MultiPredicate.unapply(sparkPredicate) match {
          case Some((fieldName, _)) => partitionKeys.contains(fieldName)
          case _ =>
            logWarning(s"Convert $sparkPredicate is unsupported.")
            false
        }
      case _ => false
    }
  }
}
