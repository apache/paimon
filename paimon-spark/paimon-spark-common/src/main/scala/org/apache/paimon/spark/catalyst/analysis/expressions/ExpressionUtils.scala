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

import org.apache.paimon.predicate._
import org.apache.paimon.spark.util.shim.TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType
import org.apache.paimon.types.{DataTypeChecks, DataTypeFamily, LocalZonedTimestampType, RowType, TimestampType}
import org.apache.paimon.types.DataTypeRoot.{DOUBLE, FLOAT}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Lower, Upper}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, SimplifyCasts}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.trees.TreePattern.CURRENT_LIKE
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import scala.collection.JavaConverters._

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
    // Fold constant LOWER/UPPER calls in Spark before rejecting case conversion on column values.
    // Storage transforms use Locale.ROOT and cannot encode Spark's locale and collation rules.
    require(
      !normalized.exists {
        case _: Lower | _: Upper => true
        case _ => false
      },
      "filter_sql does not support LOWER or UPPER on non-constant expressions."
    )
    // A stored policy has no engine-side residual filter, so every conjunct must convert.
    convertConditionToPaimonPredicate(
      normalized,
      relation.output,
      rowType,
      ignorePartialFailure = false)
      .map(normalizePolicyPredicate)
      .getOrElse(
        throw new IllegalArgumentException("Cannot fully convert filter_sql to a predicate."))
  }

  /** Adapt storage comparison semantics only for SQL-authored policies. */
  private def normalizePolicyPredicate(predicate: Predicate): Predicate = predicate match {
    case compound: CompoundPredicate =>
      new CompoundPredicate(
        compound.function(),
        compound.children().asScala.map(normalizePolicyPredicate).asJava)
    case leaf: LeafPredicate
        if leaf.transform().outputType().is(DataTypeFamily.TIMESTAMP) &&
          !leaf.literals().isEmpty =>
      // Legacy mapping interprets local times in the JVM zone, including DST gap adjustments.
      // Use the effective mapping: Spark 3.2/3.3 always enable it. Null checks need no adaptation.
      require(
        !treatPaimonTimestampTypeAsSparkTimestampType(),
        "filter_sql cannot compare timestamp values with legacy timestamp mapping. " +
          "Use Spark 3.4 or later with spark.paimon.legacy-timestamp-mapping.enabled=false."
      )
      if (DataTypeChecks.getPrecision(leaf.transform().outputType()) <= 6) {
        leaf
      } else {
        val field = leaf.transform() match {
          case transform: FieldTransform => transform.fieldRef()
          case _ =>
            // CastTransform accepts top-level field references, not nested transforms.
            throw new IllegalArgumentException(
              "filter_sql cannot compare nested timestamp fields with precision greater than 6.")
        }
        val timestampType = field.`type`() match {
          case timestamp: TimestampType => new TimestampType(timestamp.isNullable, 6)
          case timestamp: LocalZonedTimestampType =>
            new LocalZonedTimestampType(timestamp.isNullable, 6)
        }
        // Spark sees .123456789 as .123456, so truncate the stored value before comparison.
        // Keep the timestamp type unchanged apart from precision to avoid time-zone conversion.
        LeafPredicate.of(new CastTransform(field, timestampType), leaf.function(), leaf.literals())
      }
    case leaf: LeafPredicate
        if Set(FLOAT, DOUBLE).contains(leaf.transform().outputType().getTypeRoot) =>
      def isZero(value: AnyRef): Boolean = value match {
        case number: java.lang.Number => number.doubleValue() == 0.0d
        case _ => false
      }

      val literals = leaf.literals().asScala
      if (!literals.exists(isZero)) {
        leaf
      } else {
        // Spark equates signed zeros; storage predicates order -0.0 before +0.0.
        val zeros: Seq[AnyRef] = if (leaf.transform().outputType().getTypeRoot == FLOAT) {
          Seq(Float.box(0.0f), Float.box(-0.0f))
        } else {
          Seq(Double.box(0.0d), Double.box(-0.0d))
        }
        val positiveZero = zeros.head
        val negativeZero = zeros.last
        // Equality and membership must include or exclude both zero encodings.
        val expanded = literals.filterNot(isZero) ++ zeros
        def replace(function: LeafFunction, values: Seq[AnyRef]): Predicate =
          LeafPredicate.of(leaf.transform(), function, values.asJava)

        leaf.function() match {
          case _: Equal | _: In => replace(In.INSTANCE, expanded.toSeq)
          case _: NotEqual | _: NotIn => replace(NotIn.INSTANCE, expanded.toSeq)
          // In storage ordering, x < -0 excludes both zeros; x <= +0 includes both.
          case _: LessThan | _: GreaterOrEqual =>
            replace(leaf.function(), Seq(negativeZero))
          case _: LessOrEqual | _: GreaterThan =>
            replace(leaf.function(), Seq(positiveZero))
          case _: Between | _: NotBetween =>
            // Expand inclusive endpoints, including ranges merged by PredicateBuilder.
            replace(
              leaf.function(),
              Seq(
                if (isZero(literals.head)) negativeZero else literals.head,
                if (isZero(literals(1))) positiveZero else literals(1)))
          case _ => leaf
        }
      }
    case _ => predicate
  }
}
