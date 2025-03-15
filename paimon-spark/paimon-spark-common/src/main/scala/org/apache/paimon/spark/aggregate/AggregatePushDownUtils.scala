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

import org.apache.paimon.table.Table
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types._

import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation, CountStar, Max, Min}
import org.apache.spark.sql.execution.datasources.v2.V2ColumnUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

object AggregatePushDownUtils {

  def canPushdownAggregation(
      table: Table,
      aggregation: Aggregation,
      dataSplits: Seq[DataSplit]): Boolean = {

    var hasMinMax = false
    val minmaxColumns = mutable.HashSet.empty[String]
    var hasCount = false

    def getDataFieldForCol(colName: String): DataField = {
      table.rowType.getField(colName)
    }

    def isPartitionCol(colName: String) = {
      table.partitionKeys.contains(colName)
    }

    def processMinOrMax(agg: AggregateFunc): Boolean = {
      val columnName = agg match {
        case max: Max if V2ColumnUtils.extractV2Column(max.column).isDefined =>
          V2ColumnUtils.extractV2Column(max.column).get
        case min: Min if V2ColumnUtils.extractV2Column(min.column).isDefined =>
          V2ColumnUtils.extractV2Column(min.column).get
        case _ => return false
      }

      val dataField = getDataFieldForCol(columnName)

      dataField.`type`() match {
        // not push down complex type
        // not push down Timestamp because INT96 sort order is undefined,
        // Parquet doesn't return statistics for INT96
        // not push down Parquet Binary because min/max could be truncated
        // (https://issues.apache.org/jira/browse/PARQUET-1685), Parquet Binary
        // could be Spark StringType, BinaryType or DecimalType.
        // not push down for ORC with same reason.
        case _: BooleanType | _: TinyIntType | _: SmallIntType | _: IntType | _: BigIntType |
            _: FloatType | _: DoubleType | _: DateType =>
          minmaxColumns.add(columnName)
          hasMinMax = true
          true
        case _ =>
          false
      }
    }

    aggregation.groupByExpressions.map(V2ColumnUtils.extractV2Column).foreach {
      colName =>
        // don't push down if the group by columns are not the same as the partition columns (orders
        // doesn't matter because reorder can be done at data source layer)
        if (colName.isEmpty || !isPartitionCol(colName.get)) return false
    }

    aggregation.aggregateExpressions.foreach {
      case max: Max =>
        if (!processMinOrMax(max)) return false
      case min: Min =>
        if (!processMinOrMax(min)) return false
      case _: CountStar =>
        hasCount = true
      case _ =>
        return false
    }

    if (hasMinMax) {
      dataSplits.forall {
        dataSplit =>
          dataSplit.dataFiles().asScala.forall {
            dataFile =>
              // It means there are all column statistics when valueStatsCols == null
              dataFile.valueStatsCols() == null ||
              minmaxColumns.forall(dataFile.valueStatsCols().contains)
          }
      }
    } else if (hasCount) {
      dataSplits.forall(_.mergedRowCountAvailable())
    } else {
      true
    }
  }

  def hasMinMaxAggregation(aggregation: Aggregation): Boolean = {
    var hasMinMax = false;
    aggregation.aggregateExpressions().foreach {
      case _: Min | _: Max =>
        hasMinMax = true
      case _ =>
    }
    hasMinMax
  }

}
