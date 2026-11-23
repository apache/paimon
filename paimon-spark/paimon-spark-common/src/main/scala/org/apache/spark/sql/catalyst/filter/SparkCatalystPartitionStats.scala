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

package org.apache.spark.sql.catalyst.filter

import org.apache.paimon.data.{InternalArray, InternalRow => PaimonInternalRow}
import org.apache.paimon.predicate.{Predicate => PaimonPredicate}
import org.apache.paimon.spark.SparkV2FilterConverter
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.types.RowType

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BasePredicate, BinaryComparison, BoundReference, Contains, EndsWith, Expression, GenericInternalRow, In, InSet, IsNotNull, IsNull, Literal, Not, Or, Predicate, StartsWith}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType, IntegerType, LongType, ShortType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.control.NonFatal

/**
 * Conservative statistics evaluator for a Catalyst partition predicate. Instances are thread-local.
 *
 * Direct field predicates reuse Paimon's statistics functions. Other expressions are evaluated with
 * Spark semantics only if statistics prove a finite input domain: a constant non-null value, NULL,
 * or both. We enumerate the Cartesian product, not just the min/max rows, since statistics contain
 * no cross-column correlation. A bounded superset of the real partitions is sufficient: we may
 * prune only when none of its rows satisfies the original expression.
 *
 * No CAST is applied to interval endpoints. In particular, even digit-only STRING endpoints do not
 * prove a numeric interval or a fixed string length for the values between them. Merging manifests
 * across different values of a CAST-referenced field therefore limits this optimization. Merging a
 * single value across other partition columns does not. Native direct-field predicates can still
 * prune non-singleton intervals.
 */
final private[filter] class SparkCatalystPartitionStats(
    filter: Expression,
    partitionType: RowType,
    schema: StructType) {

  // A work bound, not a correctness assumption. Larger domains are left to ordinary scanning.
  private val maxCombinations = 64
  private val root = new StatisticsPredicate(filter)

  def test(
      rowCount: Long,
      min: PaimonInternalRow,
      max: PaimonInternalRow,
      nullCounts: InternalArray): Boolean = {
    if (
      rowCount <= 0 || min == null || max == null || nullCounts == null ||
      min.getFieldCount < partitionType.getFieldCount ||
      max.getFieldCount < partitionType.getFieldCount ||
      nullCounts.size() < partitionType.getFieldCount
    ) {
      true
    } else {
      root.mayMatch(new Statistics(rowCount, min, max, nullCounts)).getOrElse(true)
    }
  }

  private case class Field(index: Int, dataType: DataType)

  private class Statistics(
      val rowCount: Long,
      val min: PaimonInternalRow,
      val max: PaimonInternalRow,
      val nullCounts: InternalArray) {

    private val sparkMin = SparkInternalRow.create(partitionType).replace(min)
    private val sparkMax = SparkInternalRow.create(partitionType).replace(max)

    def nullCount(field: Field): Option[Long] =
      if (nullCounts.isNullAt(field.index)) None else Some(nullCounts.getLong(field.index))

    def minimum(field: Field): Any = sparkMin.get(field.index, field.dataType)
    def maximum(field: Field): Any = sparkMax.get(field.index, field.dataType)

    def valid(field: Field): Boolean = {
      if (!supportedType(field.dataType)) {
        return false
      }
      val count = nullCount(field)
      val lower = minimum(field)
      val upper = maximum(field)
      if (count.exists(n => n < 0 || n > rowCount) || (lower == null) != (upper == null)) {
        false
      } else if (count.contains(rowCount)) {
        lower == null && upper == null
      } else if (lower == null) {
        // Unknown bounds: let Paimon's native leaf predicates handle them conservatively.
        true
      } else {
        TypeUtils.getInterpretedOrdering(field.dataType).lteq(lower, upper)
      }
    }

    def domain(field: Field): Option[Seq[Any]] = {
      nullCount(field).flatMap {
        count =>
          if (count == rowCount) {
            Some(Seq(null))
          } else {
            val lower = minimum(field)
            val upper = maximum(field)
            if (
              lower != null && upper != null &&
              TypeUtils.getInterpretedOrdering(field.dataType).equiv(lower, upper)
            ) {
              Some(if (count == 0) Seq(lower) else Seq(lower, null))
            } else {
              None
            }
          }
      }
    }
  }

  private class StatisticsPredicate(expression: Expression) {
    private val attributes = expression.references.toSeq
    private val fields = attributes.flatMap {
      attribute =>
        schema
          .getFieldIndex(attribute.name)
          .filter(index => schema(index).dataType == attribute.dataType)
          .map(index => Field(index, attribute.dataType))
    }
    private val eligible =
      expression.resolved && expression.deterministic && fields.size == attributes.size &&
        expression.dataType == BooleanType

    // Do not introduce planning-time execution of UDFs or other non-foldable expressions.
    private val finiteEvaluationAllowed = eligible && expression.transform {
      case a: Attribute => Literal.default(a.dataType)
    }.foldable

    private lazy val exactPredicate: BasePredicate = Predicate.create(expression.transform {
      case a: Attribute =>
        val index = schema.fieldIndex(a.name)
        BoundReference(index, schema(index).dataType, schema(index).nullable)
    })

    private val nativePredicate: Option[PaimonPredicate] = {
      if (eligible && nativeLeaf(expression) && nativeLiteralsSafe(expression)) {
        try {
          PaimonUtils
            .translateFilterV2(expression)
            .flatMap(SparkV2FilterConverter(partitionType).convert(_))
        } catch {
          case NonFatal(_) => None
        }
      } else {
        None
      }
    }

    private val children: Option[(Boolean, StatisticsPredicate, StatisticsPredicate)] =
      expression match {
        case And(left, right) =>
          Some((true, new StatisticsPredicate(left), new StatisticsPredicate(right)))
        case Or(left, right) =>
          Some((false, new StatisticsPredicate(left), new StatisticsPredicate(right)))
        // SQL three-valued De Morgan laws are safe. Never negate a may-match result itself.
        case Not(And(left, right)) =>
          Some((false, new StatisticsPredicate(Not(left)), new StatisticsPredicate(Not(right))))
        case Not(Or(left, right)) =>
          Some((true, new StatisticsPredicate(Not(left)), new StatisticsPredicate(Not(right))))
        case _ => None
      }
    private val doubleNegation = expression match {
      case Not(Not(child)) => Some(new StatisticsPredicate(child))
      case _ => None
    }

    // None also covers expressions whose evaluation/error behavior cannot be proven from stats.
    // In an AND, do not discard this uncertainty just because another child cannot match.
    // This is local conservatism, not an error guarantee across independently pushed predicates.
    def mayMatch(statistics: Statistics): Option[Boolean] = {
      if (!eligible || !fields.forall(statistics.valid)) {
        return None
      }
      finiteMatch(statistics)
        .orElse {
          nativePredicate.map {
            predicate =>
              predicate.test(
                statistics.rowCount,
                statistics.min,
                statistics.max,
                statistics.nullCounts)
          }
        }
        .orElse {
          children.flatMap {
            case (isAnd, left, right) =>
              val l = left.mayMatch(statistics)
              val r = right.mayMatch(statistics)
              for (a <- l; b <- r) yield if (isAnd) a && b else a || b
          }
        }
        .orElse(doubleNegation.flatMap(_.mayMatch(statistics)))
    }

    private def finiteMatch(statistics: Statistics): Option[Boolean] = {
      if (!finiteEvaluationAllowed) {
        return None
      }
      val domains = fields.map(statistics.domain)
      if (domains.exists(_.isEmpty)) {
        return None
      }
      val values = domains.map(_.get)
      // Avoid product overflow and exponential planning work before enumerating any rows.
      var combinations = 1
      values.foreach {
        domain =>
          if (combinations > maxCombinations / domain.size) {
            return None
          }
          combinations *= domain.size
      }
      val row = new GenericInternalRow(schema.length)
      def matches(index: Int): Boolean = {
        if (index == fields.size) {
          exactPredicate.eval(row)
        } else {
          values(index).exists {
            value =>
              row.update(fields(index).index, value)
              matches(index + 1)
          }
        }
      }
      Some(matches(0))
    }
  }

  private def supportedType(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | StringType | DateType =>
      true
    case _: DecimalType => true
    // FLOAT/DOUBLE: NaN ordering and signed zero need a separate proof; BINARY ordering differs
    // between Spark and Paimon. TIMESTAMP may lose sub-microsecond precision or involve a
    // time-zone conversion. Do not delegate those comparisons or infer singleton domains.
    case _ => false
  }

  private def nativeLeaf(expression: Expression): Boolean = expression match {
    case comparison: BinaryComparison =>
      (comparison.left, comparison.right) match {
        case (_: Attribute, _: Literal) | (_: Literal, _: Attribute) => true
        case _ => false
      }
    case In(_: Attribute, list) => list.forall(_.isInstanceOf[Literal])
    case InSet(_: Attribute, _) => true
    case IsNull(_: Attribute) | IsNotNull(_: Attribute) => true
    case StartsWith(_: Attribute, _: Literal) | EndsWith(_: Attribute, _: Literal) |
        Contains(_: Attribute, _: Literal) =>
      true
    case Not(child) => nativeLeaf(child)
    // In particular, never convert a Spark CAST to Paimon's CastExecutors for this optimization.
    case _ => false
  }

  private def nativeLiteralsSafe(expression: Expression): Boolean = {
    // SparkExpressionConverter converts UTF8String literals through a Java String. Malformed
    // UTF-8 would be replaced during that round trip and could move a literal outside the range.
    def lossless(value: Any): Boolean = value match {
      case string: UTF8String => UTF8String.fromString(string.toString) == string
      case _ => true
    }
    !expression.exists {
      case literal: Literal => !lossless(literal.value)
      case InSet(_, set) => !set.forall(lossless)
      case _ => false
    }
  }
}
