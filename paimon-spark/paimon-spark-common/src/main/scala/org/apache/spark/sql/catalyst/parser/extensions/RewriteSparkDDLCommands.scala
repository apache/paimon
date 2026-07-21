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

import org.apache.paimon.spark.catalyst.plans.logical.PaimonDropPartitions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{DropPartitions, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog}

case class RewriteSparkDDLCommands(spark: SparkSession)
  extends Rule[LogicalPlan]
  with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {

    // A new member was added to CreatePaimonView since spark4.0,
    // unapply pattern matching is not used here to ensure compatibility across multiple spark versions.
    //
    // Rewrite before Spark resolves the partition specs so both Paimon data tables and Format
    // Tables with catalog-managed partitions can handle partial DROP specs themselves. Format
    // Tables using filesystem partition discovery are not matched (see UnresolvedPaimonRelation)
    // and keep Spark's own resolution plus the explicit unsupported error at execution.
    case DropPartitions(UnresolvedPaimonRelation(aliasedTable), parts, ifExists, purge) =>
      PaimonDropPartitions(aliasedTable, parts, ifExists, purge)
  }
}
