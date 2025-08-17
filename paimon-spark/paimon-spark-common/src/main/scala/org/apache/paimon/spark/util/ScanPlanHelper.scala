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
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.{InnerTable, KnownSplitsTable}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Filter => FilterLogicalNode, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

trait ScanPlanHelper extends SQLConfHelper {

  /**
   * Create a new scan plan from a relation with the given data splits, condition(optional) and
   * metadata columns(optional).
   */
  def createNewScanPlan(
      dataSplits: Seq[DataSplit],
      relation: DataSourceV2Relation,
      condition: Option[Expression] = None,
      metadataColumns: Option[Seq[PaimonMetadataColumn]] = None): LogicalPlan = {
    val newRelation = createNewRelation(dataSplits, relation)

    val filteredNewRelation = condition match {
      case Some(c) if c != TrueLiteral => FilterLogicalNode(c, newRelation)
      case _ => newRelation
    }

    metadataColumns match {
      case Some(cols) =>
        val resolvedMetadataColumns = cols.map {
          col =>
            val attr = filteredNewRelation.resolve(col.name :: Nil, conf.resolver)
            assert(attr.isDefined)
            attr.get
        }
        Project(relation.output ++ resolvedMetadataColumns, filteredNewRelation)
      case None => filteredNewRelation
    }
  }

  private def createNewRelation(
      splits: Seq[DataSplit],
      relation: DataSourceV2Relation): DataSourceV2Relation = {
    assert(relation.table.isInstanceOf[SparkTable])
    val sparkTable = relation.table.asInstanceOf[SparkTable]
    assert(sparkTable.table.isInstanceOf[InnerTable])
    val knownSplitsTable =
      KnownSplitsTable.create(sparkTable.table.asInstanceOf[InnerTable], splits.toArray)
    val outputNames = relation.outputSet.map(_.name)

    def isOutputColumn(colName: String) = {
      val resolve = conf.resolver
      outputNames.exists(resolve(colName, _))
    }

    val appendMetaColumns = sparkTable.metadataColumns
      .map(_.asInstanceOf[PaimonMetadataColumn].toAttribute)
      .filter(col => isOutputColumn(col.name))

    // We re-plan the relation to skip analyze phase, so we should append needed
    // metadata columns manually and let Spark do column pruning during optimization.
    relation.copy(
      table = relation.table.asInstanceOf[SparkTable].copy(table = knownSplitsTable),
      output = relation.output ++ appendMetaColumns
    )
  }
}
