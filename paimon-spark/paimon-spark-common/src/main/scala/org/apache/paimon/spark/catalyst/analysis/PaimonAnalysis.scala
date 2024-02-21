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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.analysis.PaimonRelation.isPaimonTable
import org.apache.paimon.spark.commands.{PaimonAnalyzeTableColumnCommand, PaimonDynamicPartitionOverwriteCommand, PaimonTruncateTableCommand}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class PaimonAnalysis(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {

    case o @ PaimonDynamicPartitionOverwrite(r, d) if o.resolved =>
      PaimonDynamicPartitionOverwriteCommand(r, d, o.query, o.writeOptions, o.isByName)

    case merge: MergeIntoTable if isPaimonTable(merge.targetTable) && merge.childrenResolved =>
      PaimonMergeIntoResolver(merge, session)
  }

}

case class PaimonPostHocResolutionRules(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case t @ TruncateTable(PaimonRelation(table)) if t.resolved =>
        PaimonTruncateTableCommand(table, Map.empty)

      case a @ AnalyzeTable(
            ResolvedTable(catalog, identifier, table: SparkTable, _),
            partitionSpec,
            noScan) if a.resolved =>
        if (partitionSpec.nonEmpty) {
          throw new UnsupportedOperationException("Analyze table partition is not supported")
        } else if (noScan) {
          throw new IllegalArgumentException("NOSCAN is ineffective with paimon")
        } else {
          PaimonAnalyzeTableColumnCommand(
            catalog,
            identifier,
            table,
            Option.apply(Seq()),
            allColumns = false)
        }

      case a @ AnalyzeColumn(
            ResolvedTable(catalog, identifier, table: SparkTable, _),
            columnNames,
            allColumns) if a.resolved =>
        PaimonAnalyzeTableColumnCommand(catalog, identifier, table, columnNames, allColumns)

      case _ => plan
    }
  }
}

object PaimonDynamicPartitionOverwrite {
  def unapply(o: OverwritePartitionsDynamic): Option[(DataSourceV2Relation, FileStoreTable)] = {
    if (o.query.resolved) {
      o.table match {
        case r: DataSourceV2Relation if r.table.isInstanceOf[SparkTable] =>
          Some((r, r.table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]))
        case _ => None
      }
    } else {
      None
    }
  }
}
