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
import org.apache.paimon.spark.execution.PaimonFormatTablePartitionDdlExec
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

    // A new member was added to CreatePaimonView since spark4.0,
    // unapply pattern matching is not used here to ensure compatibility across multiple spark versions.
    //
    // Both Paimon data tables and Format Tables with catalog-managed partitions route DROP
    // PARTITION through PaimonDropPartitions (the latter resolve partial specs through the
    // partition gateway, so they must bypass Spark's strict partition-spec resolution too). A
    // single extractor loads the table once and classifies it, avoiding a second
    // catalog.loadTable per analyzer iteration. Format Tables using filesystem partition
    // discovery are not matched and keep Spark's own resolution plus the explicit unsupported
    // error at execution.
    case DropPartitions(UnresolvedPaimonDropTarget(aliasedTable), parts, ifExists, purge) =>
      PaimonDropPartitions(aliasedTable, parts, ifExists, purge)
  }

  private object UnresolvedPaimonDropTarget {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] = {
      EliminateSubqueryAliases(plan) match {
        case UnresolvedTable(multipartIdentifier, _, _)
            if isPaimonDropTarget(multipartIdentifier) =>
          Some(plan)
        case _ => None
      }
    }
  }

  private def isPaimonDropTarget(multipartIdentifier: Seq[String]): Boolean = {
    multipartIdentifier match {
      case CatalogAndIdentifier(catalog: TableCatalog, ident) =>
        Try(catalog.loadTable(ident))
          .map {
            case _: SparkTable => true
            case formatTable: PaimonFormatTable =>
              PaimonFormatTablePartitionDdlExec.usesCatalogManagedPartitions(formatTable)
            case _ => false
          }
          .getOrElse(false)
      case _ => false
    }
  }
}
