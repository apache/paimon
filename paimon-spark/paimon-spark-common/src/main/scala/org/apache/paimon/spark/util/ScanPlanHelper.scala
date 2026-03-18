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

package org.apache.paimon.spark.util

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.schema.PaimonMetadataColumn.{PATH_AND_INDEX_META_COLUMNS, ROW_TRACKING_META_COLUMNS}
import org.apache.paimon.table.{InnerTable, KnownSplitsTable}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.col

trait ScanPlanHelper extends SQLConfHelper {

  /** Create a new scan plan from a relation with the given data splits, condition(optional). */
  def createNewScanPlan(
      dataSplits: Seq[DataSplit],
      relation: DataSourceV2Relation,
      condition: Option[Expression]): LogicalPlan = {
    val newRelation = createNewScanPlan(dataSplits, relation)
    condition match {
      case Some(c) if c != TrueLiteral => Filter(c, newRelation)
      case _ => newRelation
    }
  }

  def createNewScanPlan(
      dataSplits: Seq[DataSplit],
      relation: DataSourceV2Relation): DataSourceV2Relation = {
    relation.table match {
      case sparkTable @ SparkTable(table: InnerTable) =>
        val knownSplitsTable = KnownSplitsTable.create(table, dataSplits.toArray)
        relation.copy(table = sparkTable.copy(table = knownSplitsTable))
      case _ => throw new RuntimeException()
    }
  }

  def selectWithDvMeta(data: DataFrame): DataFrame = {
    selectWithAdditionalCols(data, PATH_AND_INDEX_META_COLUMNS)
  }

  def selectWithRowTracking(data: DataFrame): DataFrame = {
    selectWithAdditionalCols(data, ROW_TRACKING_META_COLUMNS)
  }

  private def selectWithAdditionalCols(data: DataFrame, additionalCols: Seq[String]): DataFrame = {
    val dataColNames = data.schema.names
    val mergedColNames = dataColNames ++ additionalCols.filterNot(dataColNames.contains)
    data.select(mergedColNames.map(col): _*)
  }
}

/** This wrapper is only used in java code, e.g. Procedure. */
object ScanPlanHelper extends ScanPlanHelper {
  def createNewScanPlan(
      dataSplits: Array[DataSplit],
      relation: DataSourceV2Relation): LogicalPlan = {
    ScanPlanHelper.createNewScanPlan(dataSplits.toSeq, relation)
  }
}
