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

import org.apache.paimon.predicate.Predicate
import org.apache.paimon.types.RowType

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, SimplifyCasts}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.trees.TreePattern.CURRENT_LIKE
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/** This wrapper is only used in java code, e.g. Procedure. */
object ExpressionUtils extends ExpressionHelper {

  def convertPolicyFilter(
      spark: SparkSession,
      relation: DataSourceV2Relation,
      rowType: RowType,
      filterSql: String): Predicate = {
    val parsed = spark.sessionState.sqlParser.parseExpression(filterSql)
    val analyzed = spark.sessionState.analyzer.execute(Filter(parsed, relation))
    spark.sessionState.analyzer.checkAnalysis(analyzed)
    val filter = analyzed.asInstanceOf[Filter]
    val condition = filter.condition
    // Query-context values must not be frozen into a persistent authorization rule.
    require(
      !condition.containsPattern(CURRENT_LIKE),
      "filter_sql cannot contain expressions that depend on the current query context."
    )

    val normalized = SimplifyCasts(ConstantFolding(filter)).asInstanceOf[Filter].condition
    // A stored policy has no engine-side residual filter, so every conjunct must convert.
    convertConditionToPaimonPredicate(
      normalized,
      relation.output,
      rowType,
      ignorePartialFailure = false)
      .getOrElse(
        throw new IllegalArgumentException("Cannot fully convert filter_sql to a predicate."))
  }
}
