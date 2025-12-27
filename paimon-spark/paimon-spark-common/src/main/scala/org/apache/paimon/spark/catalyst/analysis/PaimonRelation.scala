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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, ResolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import scala.util.control.NonFatal

/** An analysis helper */
object PaimonRelation extends Logging {

  def unapply(plan: LogicalPlan): Option[SparkTable] =
    EliminateSubqueryAliases(plan) match {
      case Project(_, d: DataSourceV2Relation) if d.table.isInstanceOf[SparkTable] =>
        Some(d.table.asInstanceOf[SparkTable])
      case d: DataSourceV2Relation if d.table.isInstanceOf[SparkTable] =>
        Some(d.table.asInstanceOf[SparkTable])
      case d: DataSourceV2Relation if isRowLevelOpTable(d.table) =>
        Some(getTableFromRowLevelOpTable(d.table))
      case ResolvedTable(_, _, table: SparkTable, _) => Some(table)
      case _ => None
    }

  def isPaimonTable(plan: LogicalPlan): Boolean = {
    try {
      PaimonRelation.unapply(plan).nonEmpty
    } catch {
      case NonFatal(e) =>
        logWarning("Can't check if this plan is a paimon table", e)
        false
    }
  }

  def getPaimonRelation(plan: LogicalPlan): DataSourceV2Relation = {
    EliminateSubqueryAliases(plan) match {
      case Project(_, d: DataSourceV2Relation) if d.table.isInstanceOf[SparkTable] => d
      case d: DataSourceV2Relation if d.table.isInstanceOf[SparkTable] => d
      case d: DataSourceV2Relation if isRowLevelOpTable(d.table) =>
        d.copy(table = getTableFromRowLevelOpTable(d.table))
      case _ => throw new RuntimeException(s"It's not a paimon table, $plan")
    }
  }

  private def isRowLevelOpTable(table: Table) = {
    if (table.getClass.getName == "org.apache.spark.sql.connector.write.RowLevelOperationTable") {
      val clazz = Class.forName("org.apache.spark.sql.connector.write.RowLevelOperationTable")
      val method = clazz.getMethod("table")
      val innerTable = method.invoke(table).asInstanceOf[Table]
      innerTable.isInstanceOf[SparkTable]
    } else {
      false
    }
  }

  private def getTableFromRowLevelOpTable(table: Table) = {
    if (table.getClass.getName == "org.apache.spark.sql.connector.write.RowLevelOperationTable") {
      val clazz = Class.forName("org.apache.spark.sql.connector.write.RowLevelOperationTable")
      val method = clazz.getMethod("table")
      val innerTable = method.invoke(table).asInstanceOf[Table]
      innerTable.asInstanceOf[SparkTable]
    } else {
      table.asInstanceOf[SparkTable]
    }
  }
}
