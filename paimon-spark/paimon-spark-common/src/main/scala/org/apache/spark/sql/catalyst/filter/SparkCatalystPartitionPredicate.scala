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

import org.apache.paimon.data.{BinaryRow, InternalArray, InternalRow => PaimonInternalRow}
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.types.RowType

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BasePredicate, BoundReference, Expression, Predicate, PythonUDF, SubqueryExpression}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import scala.util.control.NonFatal

/**
 * A [[PartitionPredicate]] that applies Spark Catalyst partition filters to a partition
 * [[BinaryRow]].
 */
case class SparkCatalystPartitionPredicate(
    partitionFilter: Expression,
    partitionRowType: RowType
) extends PartitionPredicate {

  private val partitionSchema: StructType = CharVarcharUtils.replaceCharVarcharWithStringInSchema(
    SparkTypeUtils.fromPaimonRowType(partitionRowType))
  // Expression copies and lazy evaluators can consult SQLConf again (e.g. empty IN semantics).
  // Preserve the query context across asynchronous scans and serialization, for both test paths.
  private val evaluationSettings: Map[String, String] = SQLConf.get.getAllConfs
  // SQLConf supports concurrent reads. Populate it once per predicate, then keep it read-only;
  // only the mutable expression evaluators need thread-local instances.
  @transient private lazy val evaluationConf: SQLConf = {
    val conf = new SQLConf
    evaluationSettings.foreach { case (key, value) => conf.setConfString(key, value) }
    conf
  }
  @transient private lazy val predicateThreadLocal: ThreadLocal[BasePredicate] =
    ThreadLocal.withInitial(
      () => new StructExpressionFilters(partitionFilter, partitionSchema).toPredicate)

  @transient private lazy val statisticsThreadLocal: ThreadLocal[SparkCatalystPartitionStats] =
    ThreadLocal.withInitial(
      () => new SparkCatalystPartitionStats(partitionFilter, partitionRowType, partitionSchema))

  override def test(partition: BinaryRow): Boolean = {
    SQLConf.withExistingConf(evaluationConf) {
      val predicate = predicateThreadLocal.get()
      predicate.eval(SparkInternalRow.create(partitionRowType).replace(partition))
    }
  }

  override def test(
      rowCount: Long,
      minValues: PaimonInternalRow,
      maxValues: PaimonInternalRow,
      nullCounts: InternalArray): Boolean = {
    try {
      SQLConf.withExistingConf(evaluationConf) {
        statisticsThreadLocal.get().test(rowCount, minValues, maxValues, nullCounts)
      }
    } catch {
      // An evaluation failure is unknown, never proof that no partition matches. Keep it for
      // ordinary scanning; independent pushed predicates may still prune the same manifest.
      case NonFatal(_) => true
    }
  }

  override def toString: String = partitionFilter.toString()

  private class StructExpressionFilters(filter: Expression, schema: StructType) {
    def toPredicate: BasePredicate = {
      Predicate.create(filter.transform { case a: Attribute => toRef(a.name).get })
    }

    // Finds a filter attribute in the schema and converts it to a `BoundReference`
    private def toRef(attr: String): Option[BoundReference] = {
      // The names have been normalized and case sensitivity is not a concern here.
      schema.getFieldIndex(attr).map {
        index =>
          val field = schema(index)
          BoundReference(index, field.dataType, field.nullable)
      }
    }
  }
}

object SparkCatalystPartitionPredicate {

  def apply(
      partitionFilters: Seq[Expression],
      partitionRowType: RowType): SparkCatalystPartitionPredicate = {
    assert(partitionFilters.nonEmpty, "partitionFilters is empty")
    new SparkCatalystPartitionPredicate(
      partitionFilters.sortBy(_.references.size).reduce(And),
      partitionRowType)
  }

  /** Extracts supported partition filters from the given filters. */
  def extractSupportedPartitionFilters(
      filters: Seq[Expression],
      partitionRowType: RowType): Seq[Expression] = {
    val partitionSchema: StructType = SparkTypeUtils.fromPaimonRowType(partitionRowType)
    val (deterministicFilters, _) = filters.partition(_.deterministic)
    val (partitionFilters, _) =
      DataSourceUtils.getPartitionFiltersAndDataFilters(partitionSchema, deterministicFilters)
    partitionFilters.filter {
      f =>
        // Python UDFs might exist because this rule is applied before ``ExtractPythonUDFs``.
        !SubqueryExpression.hasSubquery(f) && !f.exists(_.isInstanceOf[PythonUDF])
    }
  }
}
