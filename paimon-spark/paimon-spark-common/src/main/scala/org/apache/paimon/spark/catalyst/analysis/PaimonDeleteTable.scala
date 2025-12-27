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
import org.apache.paimon.spark.catalyst.optimizer.OptimizeMetadataOnlyDeleteFromPaimonTable
import org.apache.paimon.spark.commands.DeleteFromPaimonTableCommand
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

object PaimonDeleteTable extends Rule[LogicalPlan] with RowLevelHelper {

  /** Determines if DataSourceV2 delete is not supported for the given table. */
  private def shouldFallbackToV1Delete(table: SparkTable, condition: Expression): Boolean = {
    val baseTable = table.getTable
    org.apache.spark.SPARK_VERSION < "3.5" ||
    !baseTable.isInstanceOf[FileStoreTable] ||
    !baseTable.primaryKeys().isEmpty ||
    !table.useV2Write ||
    table.coreOptions.deletionVectorsEnabled() ||
    table.coreOptions.rowTrackingEnabled() ||
    table.coreOptions.dataEvolutionEnabled() ||
    OptimizeMetadataOnlyDeleteFromPaimonTable.isMetadataOnlyDelete(
      baseTable.asInstanceOf[FileStoreTable],
      condition)
  }

  override val operation: RowLevelOp = Delete

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case d @ DeleteFromTable(PaimonRelation(table), condition)
          if d.resolved && shouldFallbackToV1Delete(table, condition) =>
        checkPaimonTable(table.getTable)

        table.getTable match {
          case paimonTable: FileStoreTable =>
            val relation = PaimonRelation.getPaimonRelation(d.table)
            if (paimonTable.coreOptions().dataEvolutionEnabled()) {
              throw new RuntimeException(
                "Delete operation is not supported when data evolution is enabled yet.")
            }
            DeleteFromPaimonTableCommand(relation, paimonTable, condition)

          case _ =>
            throw new RuntimeException("Delete Operation is only supported for FileStoreTable.")
        }
    }
  }
}
