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

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.table.{DataTable, Table}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.utils.{InternalRowUtils, ProjectedRow}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.connector.expressions.{Expression, NamedReference}
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation, CountStar}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

import scala.collection.mutable

class LocalAggregator(table: Table) {
  private val partitionType = SparkTypeUtils.toPartitionType(table)
  private val groupByEvaluatorMap = new mutable.HashMap[InternalRow, Seq[AggFuncEvaluator[_]]]()
  private var requiredGroupByType: Seq[DataType] = _
  private var requiredGroupByIndexMapping: Seq[Int] = _
  private var aggFuncEvaluatorGetter: () => Seq[AggFuncEvaluator[_]] = _
  private var isInitialized = false

  private def initialize(aggregation: Aggregation): Unit = {
    aggFuncEvaluatorGetter = () =>
      aggregation.aggregateExpressions().map {
        case _: CountStar => new CountStarEvaluator()
        case _ => throw new UnsupportedOperationException()
      }

    requiredGroupByType = aggregation.groupByExpressions().map {
      case r: NamedReference =>
        SparkTypeUtils.fromPaimonType(partitionType.getField(r.fieldNames().head).`type`())
    }

    requiredGroupByIndexMapping = aggregation.groupByExpressions().map {
      case r: NamedReference =>
        partitionType.getFieldIndex(r.fieldNames().head)
    }

    isInitialized = true
  }

  private def supportAggregateFunction(func: AggregateFunc): Boolean = {
    func match {
      case _: CountStar => true
      case _ => false
    }
  }

  private def supportGroupByExpressions(exprs: Array[Expression]): Boolean = {
    // Support empty group by keys or group by partition column
    exprs.forall {
      case r: NamedReference =>
        r.fieldNames.length == 1 && table.partitionKeys().contains(r.fieldNames().head)
      case _ => false
    }
  }

  def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!table.isInstanceOf[DataTable]) {
      return false
    }

    if (
      !supportGroupByExpressions(aggregation.groupByExpressions()) ||
      aggregation.aggregateExpressions().isEmpty ||
      aggregation.aggregateExpressions().exists(!supportAggregateFunction(_))
    ) {
      return false
    }

    initialize(aggregation)
    true
  }

  private def requiredGroupByRow(partitionRow: BinaryRow): InternalRow = {
    val projectedRow =
      ProjectedRow.from(requiredGroupByIndexMapping.toArray).replaceRow(partitionRow)
    // `ProjectedRow` does not support `hashCode`, so do a deep copy
    val genericRow = InternalRowUtils.copyInternalRow(projectedRow, partitionType)
    SparkInternalRow.create(partitionType).replace(genericRow)
  }

  def update(dataSplit: DataSplit): Unit = {
    assert(isInitialized)
    val groupByRow = requiredGroupByRow(dataSplit.partition())
    val aggFuncEvaluator =
      groupByEvaluatorMap.getOrElseUpdate(groupByRow, aggFuncEvaluatorGetter())
    aggFuncEvaluator.foreach(_.update(dataSplit))
  }

  def result(): Array[InternalRow] = {
    assert(isInitialized)
    if (groupByEvaluatorMap.isEmpty && requiredGroupByType.isEmpty) {
      // Always return one row for global aggregate
      Array(InternalRow.fromSeq(aggFuncEvaluatorGetter().map(_.result())))
    } else {
      groupByEvaluatorMap.map {
        case (partitionRow, aggFuncEvaluator) =>
          new JoinedRow(partitionRow, InternalRow.fromSeq(aggFuncEvaluator.map(_.result())))
      }.toArray
    }
  }

  def resultSchema(): StructType = {
    assert(isInitialized)
    // Always put the group by keys before the aggregate function result
    val groupByFields = requiredGroupByType.zipWithIndex.map {
      case (dt, i) =>
        StructField(s"groupby_$i", dt)
    }
    val aggResultFields = aggFuncEvaluatorGetter().zipWithIndex.map {
      case (evaluator, i) =>
        // Note that, Spark will re-assign the attribute name to original name,
        // so here we just return an arbitrary name
        StructField(s"${evaluator.prettyName}_$i", evaluator.resultType)
    }
    StructType.apply(groupByFields ++ aggResultFields)
  }
}

trait AggFuncEvaluator[T] {
  def update(dataSplit: DataSplit): Unit
  def result(): T
  def resultType: DataType
  def prettyName: String
}

class CountStarEvaluator extends AggFuncEvaluator[Long] {
  private var _result: Long = 0L

  override def update(dataSplit: DataSplit): Unit = {
    _result += dataSplit.mergedRowCount()
  }

  override def result(): Long = _result

  override def resultType: DataType = LongType

  override def prettyName: String = "count_star"
}
