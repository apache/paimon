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
import org.apache.spark.sql.connector.expressions.{Cast, Expression, Extract, GeneralScalarExpression, Literal, NamedReference}
import org.apache.spark.sql.types.{ArrayType => SparkArrayType, DataType => SparkDataType}

import scala.collection.JavaConverters._

object SparkExpressionConverter {

  import PaimonImplicits._

  // Supported general scalar transform names
  private val CONCAT = "CONCAT"
  private val UPPER = "UPPER"
  private val LOWER = "LOWER"
  private val SUBSTRING = "SUBSTRING"
  private val CHAR_LENGTH = "CHAR_LENGTH"
  private val BIT_LENGTH = "BIT_LENGTH"
  private val TRANSLATE = "TRANSLATE"
  private val OVERLAY = "OVERLAY"
  private val LPAD = "LPAD"
  private val RPAD = "RPAD"
  private val TRIM = "TRIM"
  private val LTRIM = "LTRIM"
  private val RTRIM = "RTRIM"
  private val DATE_ADD = "DATE_ADD"
  private val DATE_DIFF = "DATE_DIFF"
  private val TRUNC = "TRUNC"

  // Spark encodes dayofweek and weekday as arithmetic over an ISO EXTRACT(DAY_OF_WEEK).
  private val ADD = "+"
  private val SUBTRACT = "-"
  private val REMAINDER = "%"

  // Supported fields of the EXTRACT expression
  private val EXTRACT_YEAR = "YEAR"
  private val EXTRACT_MONTH = "MONTH"
  private val EXTRACT_DAY = "DAY"
  private val EXTRACT_HOUR = "HOUR"
  private val EXTRACT_MINUTE = "MINUTE"
  private val EXTRACT_SECOND = "SECOND"
  private val EXTRACT_QUARTER = "QUARTER"
  private val EXTRACT_DAY_OF_WEEK = "DAY_OF_WEEK"
  private val EXTRACT_DAY_OF_YEAR = "DAY_OF_YEAR"
  private val EXTRACT_WEEK = "WEEK"
  private val EXTRACT_YEAR_OF_WEEK = "YEAR_OF_WEEK"

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

    def literalEquals(exp: Expression, value: Int): Boolean = exp match {
      case l: Literal[_] =>
        l.value() match {
          case i: Int => i == value
          case _ => false
        }
      case _ => false
    }

    def dayOfWeekField(exp: Expression): Option[FieldRef] = {
      if (org.apache.spark.SPARK_VERSION < "3.4") {
        None
      } else {
        exp match {
          case extract: Extract if extract.field() == EXTRACT_DAY_OF_WEEK =>
            extract.source() match {
              case n: NamedReference => Some(toPaimonFieldRef(n, rowType))
              case _ => None
            }
          case _ => None
        }
      }
    }

    def sparkDayOfWeek(children: Seq[Expression]): Option[Transform] = children match {
      case Seq(remainder: GeneralScalarExpression, one)
          if literalEquals(one, 1) &&
            remainder.name() == REMAINDER =>
        remainder.children().toSeq match {
          case Seq(extract, seven) if literalEquals(seven, 7) =>
            dayOfWeekField(extract).flatMap(DayOfWeekTransform.tryCreate)
          case _ => None
        }
      case _ => None
    }

    def sparkWeekday(children: Seq[Expression]): Option[Transform] = children match {
      case Seq(extract, one) if literalEquals(one, 1) =>
        dayOfWeekField(extract).flatMap(WeekdayTransform.tryCreate)
      case _ => None
    }

    exp match {
      case n: NamedReference => toPaimonFieldTransform(n, rowType)
      case s: GeneralScalarExpression =>
        s.name() match {
          case CONCAT => convertChildren(s.children()).map(i => new ConcatTransform(i))
          case UPPER => convertChildren(s.children()).map(i => new UpperTransform(i))
          case LOWER => convertChildren(s.children()).map(i => new LowerTransform(i))
          case SUBSTRING => convertChildren(s.children()).map(i => new SubstringTransform(i))
          case CHAR_LENGTH => convertChildren(s.children()).map(i => new LengthTransform(i))
          case BIT_LENGTH => convertChildren(s.children()).map(i => new BitLengthTransform(i))
          case TRANSLATE => convertChildren(s.children()).map(i => new TranslateTransform(i))
          case OVERLAY => convertChildren(s.children()).map(i => new OverlayTransform(i))
          case LPAD =>
            convertChildren(s.children()).map(i => new PadTransform(i, PadTransform.Direction.LEFT))
          case RPAD =>
            convertChildren(s.children()).map(
              i => new PadTransform(i, PadTransform.Direction.RIGHT))
          case TRIM =>
            convertChildren(s.children()).map(i => new TrimTransform(i, TrimTransform.Flag.BOTH))
          case LTRIM =>
            convertChildren(s.children()).map(i => new TrimTransform(i, TrimTransform.Flag.LEADING))
          case RTRIM =>
            convertChildren(s.children()).map(
              i => new TrimTransform(i, TrimTransform.Flag.TRAILING))
          case DATE_ADD => convertChildren(s.children()).map(i => new DateAddTransform(i))
          case DATE_DIFF => convertChildren(s.children()).map(i => new DateDiffTransform(i))
          case TRUNC => convertChildren(s.children()).map(i => new DateTruncTransform(i))
          case ADD => sparkDayOfWeek(s.children())
          case SUBTRACT => sparkWeekday(s.children())
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
      // The connector `Extract` expression was added in Spark 3.4 and does not exist on
      // Spark 3.2/3.3 runtimes, so its type test must stay behind this version gate to avoid
      // a NoClassDefFoundError when linking the class there.
      case e if org.apache.spark.SPARK_VERSION >= "3.4" =>
        e match {
          case extract: Extract =>
            extract.source() match {
              case n: NamedReference =>
                val fieldRef = toPaimonFieldRef(n, rowType)
                if (
                  fieldRef.`type`().getTypeRoot == TIMESTAMP_WITHOUT_TIME_ZONE &&
                  treatPaimonTimestampTypeAsSparkTimestampType()
                ) {
                  // Legacy mapping exposes this Paimon type as Spark TIMESTAMP, whose extract
                  // semantics depend on the Spark session time zone.
                  None
                } else {
                  extract.field() match {
                    case EXTRACT_YEAR => YearTransform.tryCreate(fieldRef)
                    case EXTRACT_MONTH => MonthTransform.tryCreate(fieldRef)
                    case EXTRACT_DAY => DayTransform.tryCreate(fieldRef)
                    case EXTRACT_HOUR => HourTransform.tryCreate(fieldRef)
                    case EXTRACT_MINUTE => MinuteTransform.tryCreate(fieldRef)
                    case EXTRACT_SECOND => SecondTransform.tryCreate(fieldRef)
                    case EXTRACT_QUARTER => QuarterTransform.tryCreate(fieldRef)
                    case EXTRACT_DAY_OF_WEEK => IsoDayOfWeekTransform.tryCreate(fieldRef)
                    case EXTRACT_DAY_OF_YEAR => DayOfYearTransform.tryCreate(fieldRef)
                    case EXTRACT_WEEK => WeekTransform.tryCreate(fieldRef)
                    case EXTRACT_YEAR_OF_WEEK => YearOfWeekTransform.tryCreate(fieldRef)
                    case _ => None
                  }
                }
              case _ => None
            }
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
