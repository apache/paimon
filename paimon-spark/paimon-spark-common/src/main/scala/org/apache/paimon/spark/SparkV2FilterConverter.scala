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

import org.apache.paimon.predicate.{FieldTransform, Predicate, PredicateBuilder, Transform}
import org.apache.paimon.spark.util.SparkExpressionConverter.{toPaimonLiteral, toPaimonTransform}
import org.apache.paimon.types.RowType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.connector.expressions.Literal
import org.apache.spark.sql.connector.expressions.filter.{And, Not, Or, Predicate => SparkPredicate}

import scala.collection.JavaConverters._

/** Conversion from [[SparkPredicate]] to [[Predicate]]. */
case class SparkV2FilterConverter(rowType: RowType) extends Logging {

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
        sparkPredicate match {
          case BinaryPredicate(transform, literal) =>
            // TODO deal with isNaN
            builder.equal(transform, literal)
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case EQUAL_NULL_SAFE =>
        sparkPredicate match {
          case BinaryPredicate(transform, literal) =>
            if (literal == null) {
              builder.isNull(transform)
            } else {
              builder.equal(transform, literal)
            }
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case GREATER_THAN =>
        sparkPredicate match {
          case BinaryPredicate(transform, literal) =>
            builder.greaterThan(transform, literal)
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case GREATER_THAN_OR_EQUAL =>
        sparkPredicate match {
          case BinaryPredicate((transform, literal)) =>
            builder.greaterOrEqual(transform, literal)
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case LESS_THAN =>
        sparkPredicate match {
          case BinaryPredicate(transform, literal) =>
            builder.lessThan(transform, literal)
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case LESS_THAN_OR_EQUAL =>
        sparkPredicate match {
          case BinaryPredicate(transform, literal) =>
            builder.lessOrEqual(transform, literal)
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case IN =>
        sparkPredicate match {
          case MultiPredicate(transform, literals) =>
            builder.in(transform, literals.toList.asJava)
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case IS_NULL =>
        sparkPredicate match {
          case UnaryPredicate(transform) =>
            builder.isNull(transform)
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case IS_NOT_NULL =>
        sparkPredicate match {
          case UnaryPredicate(transform) =>
            builder.isNotNull(transform)
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
        sparkPredicate match {
          case BinaryPredicate(transform, literal) =>
            builder.startsWith(transform, literal)
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case STRING_END_WITH =>
        sparkPredicate match {
          case BinaryPredicate(transform, literal) =>
            builder.endsWith(transform, literal)
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      case STRING_CONTAINS =>
        sparkPredicate match {
          case BinaryPredicate(transform, literal) =>
            builder.contains(transform, literal)
          case _ =>
            throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
        }

      // TODO: AlwaysTrue, AlwaysFalse
      case _ => throw new UnsupportedOperationException(s"Convert $sparkPredicate is unsupported.")
    }
  }

  private object UnaryPredicate {
    def unapply(sparkPredicate: SparkPredicate): Option[Transform] = {
      sparkPredicate.children() match {
        case Array(e: Expression) => toPaimonTransform(e, rowType)
        case _ => None
      }
    }
  }

  private object BinaryPredicate {
    def unapply(sparkPredicate: SparkPredicate): Option[(Transform, Object)] = {
      sparkPredicate.children() match {
        case Array(e: Expression, r: Literal[_]) =>
          toPaimonTransform(e, rowType) match {
            case Some(transform) => Some(transform, toPaimonLiteral(r))
            case _ => None
          }
        case _ => None
      }
    }
  }

  private object MultiPredicate {
    def unapply(sparkPredicate: SparkPredicate): Option[(Transform, Seq[Object])] = {
      sparkPredicate.children() match {
        case Array(e: Expression, rest @ _*)
            if rest.nonEmpty && rest.forall(_.isInstanceOf[Literal[_]]) =>
          val literals = rest.map(_.asInstanceOf[Literal[_]])
          if (literals.forall(_.dataType() == literals.head.dataType())) {
            toPaimonTransform(e, rowType) match {
              case Some(transform) => Some(transform, literals.map(toPaimonLiteral))
              case _ => None
            }
          } else {
            None
          }
        case _ => None
      }
    }
  }

  def isSupportedRuntimeFilter(
      sparkPredicate: SparkPredicate,
      partitionKeys: Seq[String]): Boolean = {
    sparkPredicate.name() match {
      case IN =>
        sparkPredicate match {
          case MultiPredicate(transform: FieldTransform, _) =>
            partitionKeys.contains(transform.fieldRef().name())
          case _ =>
            logWarning(s"Convert $sparkPredicate is unsupported.")
            false
        }
      case _ => false
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

}
