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

package org.apache.paimon.spark

import org.apache.paimon.spark.write.PaimonWriteRequirement
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.{PaimonUtils, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, Table, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/** Routes Spark 3.2-3.4 table-as-select writes through the normal write planning rules. */
object PaimonTableAsSelectWrite {

  /** Returns None when Spark should handle the write with its own failure cleanup. */
  def writeIfSupported(
      spark: SparkSession,
      catalog: TableCatalog,
      table: Table,
      ident: Identifier,
      query: LogicalPlan,
      options: CaseInsensitiveStringMap): Option[Seq[InternalRow]] = {
    PaimonUtils.withFailureCleanup {
      unwrapTable(table) match {
        case target: SparkTable if target.useV2Write =>
          // PaimonAnalysis prepares ordinary SparkTable relations. Staged tables delegate their
          // writer but need their legacy distribution prepared here, using the unwrapped table.
          val writeQuery = table match {
            case _: RollbackStagedTable =>
              PaimonWriteRequirement.prepareQuery(
                target.getTable.asInstanceOf[FileStoreTable],
                query)
            case _ => query
          }
          val relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
          val command =
            AppendData.byPosition(relation, writeQuery, options.asCaseSensitiveMap().asScala.toMap)
          PaimonUtils.createDataset(spark, command).collect()
          table match {
            case staged: StagedTable => staged.commitStagedChanges()
            case _ =>
          }
          Some(Nil)
        case _ => None
      }
    } {
      table match {
        case staged: StagedTable => staged.abortStagedChanges()
        case _ => catalog.dropTable(ident)
      }
    }
  }

  private def unwrapTable(table: Table): Table = table match {
    case staged: RollbackStagedTable => unwrapTable(staged.wrappedTable())
    case _ => table
  }
}
