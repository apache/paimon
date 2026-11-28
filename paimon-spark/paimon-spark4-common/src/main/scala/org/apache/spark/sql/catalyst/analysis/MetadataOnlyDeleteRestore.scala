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

package org.apache.spark.sql.catalyst.analysis

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.optimizer.OptimizeMetadataOnlyDeleteFromPaimonTable
import org.apache.paimon.spark.commands.DeleteFromPaimonTableCommand
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReplaceData, WriteDelta}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Turns a metadata-only DELETE that Spark's `RewriteDeleteFromTable` has already rewritten into a
 * `ReplaceData` (copy-on-write) or `WriteDelta` (unaware-bucket deletion-vector append table) back
 * into [[DeleteFromPaimonTableCommand]], which `OptimizeMetadataOnlyDeleteFromPaimonTable` folds
 * into a truncate of the matched partitions.
 */
trait MetadataOnlyDeleteRestore extends PureAppendOnlyScope {

  /** Matches such a `ReplaceData` or `WriteDelta` and gives the command to replace it with. */
  protected object RestoredMetadataOnlyDelete {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
      case rd: ReplaceData
          if isMetadataOnlyDelete(
            rd.table,
            rd.originalTable,
            rd.condition,
            targetsV2CopyOnWriteTable) =>
        Some(toDeleteCommand(rd.originalTable, rd.condition))
      case wd: WriteDelta
          if isMetadataOnlyDelete(wd.table, wd.originalTable, wd.condition, targetsV2DeltaTable) =>
        Some(toDeleteCommand(wd.originalTable, wd.condition))
      case _ => None
    }
  }

  private def isMetadataOnlyDelete(
      writeTable: LogicalPlan,
      originalTable: LogicalPlan,
      condition: Expression,
      inScope: LogicalPlan => Boolean): Boolean = {
    val writeIsDelete = writeTable match {
      case r: DataSourceV2Relation =>
        r.table match {
          case op: RowLevelOperationTable => op.operation.command() == DELETE
          case _ => false
        }
      case _ => false
    }
    writeIsDelete && (originalTable match {
      case r: DataSourceV2Relation if inScope(r) =>
        r.table match {
          case spk: SparkTable =>
            spk.getTable match {
              case fs: FileStoreTable =>
                OptimizeMetadataOnlyDeleteFromPaimonTable.isMetadataOnlyDelete(fs, condition)
              case _ => false
            }
          case _ => false
        }
      case _ => false
    })
  }

  private def toDeleteCommand(originalTable: LogicalPlan, condition: Expression): LogicalPlan = {
    val relation = originalTable.asInstanceOf[DataSourceV2Relation]
    val table = relation.table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]
    DeleteFromPaimonTableCommand(relation, table, condition)
  }
}
