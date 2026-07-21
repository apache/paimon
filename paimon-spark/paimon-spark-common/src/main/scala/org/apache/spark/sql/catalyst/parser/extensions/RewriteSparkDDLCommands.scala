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

package org.apache.spark.sql.catalyst.parser.extensions

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.plans.logical.PaimonDropPartitions
import org.apache.paimon.spark.format.PaimonFormatTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.{DropPartitions, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog, TableCatalog}

import scala.util.Try

case class RewriteSparkDDLCommands(spark: SparkSession)
  extends Rule[LogicalPlan]
  with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {

    // Rewrite before Spark resolves the partition specs so both Paimon data tables and Format
    // Tables with catalog-managed partitions can handle partial DROP specs themselves. Format
    // Tables using filesystem partition discovery are not matched and keep Spark's own resolution
    // plus the explicit unsupported error at execution. Classification loads the table once through
    // this rule's own catalog manager (not SparkSession.active), so it resolves against the session
    // the command actually runs in.
    case DropPartitions(table, parts, ifExists, purge) if isPaimonDropTarget(table) =>
      PaimonDropPartitions(table, parts, ifExists, purge)
  }

  private def isPaimonDropTarget(plan: LogicalPlan): Boolean = {
    EliminateSubqueryAliases(plan) match {
      case UnresolvedTable(CatalogAndIdentifier(catalog: TableCatalog, ident), _, _) =>
        Try(catalog.loadTable(ident))
          .map {
            case _: SparkTable => true
            case formatTable: PaimonFormatTable => formatTable.hasCatalogManagedPartitions
            case _ => false
          }
          .getOrElse(false)
      case _ => false
    }
  }
}
