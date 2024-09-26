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

package org.apache.paimon.spark.aggregate

import org.apache.paimon.manifest.PartitionEntry
import org.apache.paimon.table.{DataTable, Table}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation, CountStar}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

class LocalAggregator(table: Table) {
  private var aggFuncEvaluator: Seq[AggFuncEvaluator[_]] = _

  private def initialize(aggregation: Aggregation): Unit = {
    aggFuncEvaluator = aggregation.aggregateExpressions().map {
      case _: CountStar => new CountStarEvaluator()
      case _ => throw new UnsupportedOperationException()
    }
  }

  private def supportAggregateFunction(func: AggregateFunc): Boolean = {
    func match {
      case _: CountStar => true
      case _ => false
    }
  }

  def supportAggregation(aggregation: Aggregation): Boolean = {
    if (
      !table.isInstanceOf[DataTable] ||
      !table.primaryKeys.isEmpty
    ) {
      return false
    }
    if (table.asInstanceOf[DataTable].coreOptions.deletionVectorsEnabled) {
      return false
    }

    if (
      aggregation.groupByExpressions().nonEmpty ||
      aggregation.aggregateExpressions().isEmpty ||
      aggregation.aggregateExpressions().exists(!supportAggregateFunction(_))
    ) {
      return false
    }

    initialize(aggregation)
    true
  }

  def update(partitionEntry: PartitionEntry): Unit = {
    assert(aggFuncEvaluator != null)
    aggFuncEvaluator.foreach(_.update(partitionEntry))
  }

  def result(): Array[InternalRow] = {
    assert(aggFuncEvaluator != null)
    Array(InternalRow.fromSeq(aggFuncEvaluator.map(_.result())))
  }

  def resultSchema(): StructType = {
    assert(aggFuncEvaluator != null)
    val fields = aggFuncEvaluator.zipWithIndex.map {
      case (evaluator, i) =>
        // Note that, Spark will re-assign the attribute name to original name,
        // so here we just return an arbitrary name
        StructField(s"${evaluator.prettyName}_$i", evaluator.resultType)
    }
    StructType.apply(fields)
  }
}

trait AggFuncEvaluator[T] {
  def update(partitionEntry: PartitionEntry): Unit
  def result(): T
  def resultType: DataType
  def prettyName: String
}

class CountStarEvaluator extends AggFuncEvaluator[Long] {
  private var _result: Long = 0L

  override def update(partitionEntry: PartitionEntry): Unit = {
    _result += partitionEntry.recordCount()
  }

  override def result(): Long = _result

  override def resultType: DataType = LongType

  override def prettyName: String = "count_star"
}
