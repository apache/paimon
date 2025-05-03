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

package org.apache.paimon.spark.catalyst.analysis.expressions

import org.apache.paimon.predicate.{Predicate, PredicateBuilder}
import org.apache.paimon.spark.SparkFilterConverter
import org.apache.paimon.spark.write.SparkWriteBuilder
import org.apache.paimon.types.RowType

import org.apache.spark.sql.PaimonUtils.{normalizeExprs, translateFilter}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.sources.{AlwaysTrue, And => SourceAnd, EqualNullSafe, EqualTo, Filter => SourceFilter}

trait ExpressionHelper extends ExpressionHelperBase {

  def convertConditionToPaimonPredicate(
      condition: Expression,
      output: Seq[Attribute],
      rowType: RowType,
      ignorePartialFailure: Boolean = false): Option[Predicate] = {
    val converter = new SparkFilterConverter(rowType)
    val filters = normalizeExprs(Seq(condition), output)
      .flatMap(splitConjunctivePredicates(_).flatMap {
        f =>
          val filter = translateFilter(f, supportNestedPredicatePushdown = true)
          if (filter.isEmpty && !ignorePartialFailure) {
            throw new RuntimeException(
              "Exec update failed:" +
                s" cannot translate expression to source filter: $f")
          }
          filter
      })

    val predicates = filters.map(converter.convert(_, ignorePartialFailure)).filter(_ != null)
    if (predicates.isEmpty) {
      None
    } else {
      Some(PredicateBuilder.and(predicates: _*))
    }
  }

  /**
   * For the 'INSERT OVERWRITE' semantics of SQL, Spark DataSourceV2 will call the `truncate`
   * methods where the `AlwaysTrue` Filter is used.
   */
  def isTruncate(filter: SourceFilter): Boolean = {
    val filters = splitConjunctiveFilters(filter)
    filters.length == 1 && filters.head.isInstanceOf[AlwaysTrue]
  }

  /** See [[ SparkWriteBuilder#failIfCanNotOverwrite]] */
  def convertPartitionFilterToMap(
      filter: SourceFilter,
      partitionRowType: RowType): Map[String, String] = {
    // todo: replace it with SparkV2FilterConverter when we drop Spark3.2
    val converter = new SparkFilterConverter(partitionRowType)
    splitConjunctiveFilters(filter).map {
      case EqualNullSafe(attribute, value) =>
        (attribute, converter.convertString(attribute, value))
      case EqualTo(attribute, value) =>
        (attribute, converter.convertString(attribute, value))
      case _ =>
        // Should not happen
        throw new RuntimeException(
          s"Only support Overwrite filters with Equal and EqualNullSafe, but got: $filter")
    }.toMap
  }

  private def splitConjunctiveFilters(filter: SourceFilter): Seq[SourceFilter] = {
    filter match {
      case SourceAnd(filter1, filter2) =>
        splitConjunctiveFilters(filter1) ++ splitConjunctiveFilters(filter2)
      case other => other :: Nil
    }
  }
}
