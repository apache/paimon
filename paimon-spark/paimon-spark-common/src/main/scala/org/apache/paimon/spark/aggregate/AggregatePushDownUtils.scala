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

import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.{DataSplit, ReadBuilder, Split}
import org.apache.paimon.table.source.PushDownUtils.minmaxAvailable
import org.apache.paimon.types._

import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, CountStar, Max, Min}
import org.apache.spark.sql.execution.datasources.v2.V2ColumnUtils.extractV2Column

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.postfixOps

object AggregatePushDownUtils {

  def tryPushdownAggregation(
      table: FileStoreTable,
      aggregation: Aggregation,
      readBuilder: ReadBuilder): Option[LocalAggregator] = {
    val options = table.coreOptions()
    val rowType = table.rowType
    val partitionKeys = table.partitionKeys()

    aggregation.groupByExpressions.map(extractV2Column).foreach {
      colName =>
        // don't push down if the group by columns are not the same as the partition columns (orders
        // doesn't matter because reorder can be done at data source layer)
        if (colName.isEmpty || !partitionKeys.contains(colName.get)) return None
    }

    val splits = extractMinMaxColumns(rowType, aggregation) match {
      case Some(columns) =>
        if (columns.isEmpty) {
          generateSplits(readBuilder.dropStats())
        } else {
          if (options.deletionVectorsEnabled() || !table.primaryKeys().isEmpty) {
            return None
          }
          val splits = generateSplits(readBuilder)
          if (!splits.forall(minmaxAvailable(_, columns.asJava))) {
            return None
          }
          splits
        }
      case None => return None
    }

    if (!splits.forall(_.isInstanceOf[DataSplit])) {
      return None
    }
    val dataSplits = splits.map(_.asInstanceOf[DataSplit])

    if (!dataSplits.forall(_.mergedRowCountAvailable())) {
      return None
    }

    val aggregator = new LocalAggregator(table)
    aggregator.initialize(aggregation)
    dataSplits.foreach(aggregator.update)
    Option(aggregator)
  }

  private def generateSplits(readBuilder: ReadBuilder): mutable.Seq[Split] = {
    readBuilder.newScan().plan().splits().asScala
  }

  private def extractMinMaxColumns(
      rowType: RowType,
      aggregation: Aggregation): Option[Set[String]] = {
    val columns = mutable.HashSet.empty[String]
    aggregation.aggregateExpressions.foreach {
      case e if e.isInstanceOf[Min] || e.isInstanceOf[Max] =>
        extractMinMaxColumn(rowType, e) match {
          case Some(colName) => columns.add(colName)
          case None => return None
        }
      case _: CountStar =>
      case _ => return None
    }
    Option(columns.toSet)
  }

  private def extractMinMaxColumn(rowType: RowType, minOrMax: Expression): Option[String] = {
    val column = minOrMax match {
      case min: Min => min.column()
      case max: Max => max.column()
    }
    val extractColumn = extractV2Column(column)
    if (extractColumn.isEmpty) {
      return None
    }

    val columnName = extractColumn.get
    val dataType = rowType.getField(columnName).`type`()
    if (minmaxAvailable(dataType)) {
      Option(columnName)
    } else {
      None
    }
  }
}
