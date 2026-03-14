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
import org.apache.paimon.types.RowType

import org.apache.spark.sql.PaimonUtils.{normalizeExprs, translateFilter}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

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
}
