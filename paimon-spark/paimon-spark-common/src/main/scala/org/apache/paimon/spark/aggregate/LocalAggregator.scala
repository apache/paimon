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
import org.apache.paimon.stats.SimpleStatsEvolutions
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.ProjectedRow

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, CountStar, Max, Min}
import org.apache.spark.sql.execution.datasources.v2.V2ColumnUtils
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.mutable

class LocalAggregator(table: FileStoreTable) {
  private val rowType = table.rowType()
  private val partitionType = SparkTypeUtils.toPartitionType(table)
  private val groupByEvaluatorMap = new mutable.HashMap[InternalRow, Seq[AggFuncEvaluator[_]]]()
  private var requiredGroupByType: Seq[DataType] = _
  private var requiredGroupByIndexMapping: Array[Int] = _
  private var requiredGroupByPaimonType: RowType = _
  private var aggFuncEvaluatorGetter: () => Seq[AggFuncEvaluator[_]] = _
  private var isInitialized = false
  private lazy val simpleStatsEvolutions = {
    val schemaManager = table.schemaManager()
    new SimpleStatsEvolutions(sid => schemaManager.schema(sid).fields(), table.schema().id())
  }

  def initialize(aggregation: Aggregation): Unit = {
    aggFuncEvaluatorGetter = () =>
      aggregation.aggregateExpressions().map {
        case _: CountStar => new CountStarEvaluator()
        case min: Min if V2ColumnUtils.extractV2Column(min.column).isDefined =>
          val fieldName = V2ColumnUtils.extractV2Column(min.column).get
          MinEvaluator(
            rowType.getFieldIndex(fieldName),
            rowType.getField(fieldName),
            simpleStatsEvolutions)
        case max: Max if V2ColumnUtils.extractV2Column(max.column).isDefined =>
          val fieldName = V2ColumnUtils.extractV2Column(max.column).get
          MaxEvaluator(
            rowType.getFieldIndex(fieldName),
            rowType.getField(fieldName),
            simpleStatsEvolutions)
        case _ =>
          throw new UnsupportedOperationException()
      }

    requiredGroupByType = aggregation.groupByExpressions().map {
      case r: NamedReference =>
        SparkTypeUtils.fromPaimonType(partitionType.getField(r.fieldNames().head).`type`())
    }

    requiredGroupByIndexMapping = aggregation.groupByExpressions().map {
      case r: NamedReference =>
        partitionType.getFieldIndex(r.fieldNames().head)
    }

    requiredGroupByPaimonType = partitionType.project(requiredGroupByIndexMapping)

    isInitialized = true
  }

  private def requiredGroupByRow(partitionRow: BinaryRow): InternalRow = {
    val projectedRow = ProjectedRow.from(requiredGroupByIndexMapping).replaceRow(partitionRow)
    SparkInternalRow.create(requiredGroupByPaimonType).replace(projectedRow)
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
