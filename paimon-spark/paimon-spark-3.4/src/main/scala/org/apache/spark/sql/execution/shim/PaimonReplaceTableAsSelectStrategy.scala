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

package org.apache.spark.sql.execution.shim

import org.apache.paimon.Snapshot
import org.apache.paimon.spark.catalog.SparkBaseCatalog
import org.apache.paimon.spark.write.PaimonWriteOptions

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReplaceTable, ReplaceTableAsSelect, TableSpec}
import org.apache.spark.sql.connector.catalog.{Identifier, StagingTableCatalog, Table, TableCatalog}
import org.apache.spark.sql.execution.{PaimonTableAsSelectHelper, SparkPlan}
import org.apache.spark.sql.execution.PaimonTableAsSelectHelper._
import org.apache.spark.sql.execution.datasources.v2.{AtomicReplaceTableAsSelectExec, ReplaceTableAsSelectExec}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

case class PaimonReplaceTableAsSelectStrategy(spark: SparkSession)
  extends Strategy
  with PaimonTableAsSelectHelper {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ReplaceTableAsSelect(
          ResolvedIdentifier(catalog: SparkBaseCatalog, ident),
          parts,
          query,
          tableSpec: TableSpec,
          options,
          orCreate,
          analyzedQuery) if supportsCatalog(catalog, tableSpec) =>
      assert(analyzedQuery.isDefined)
      // For V1 saveAsTable + overwrite on an existing table, rewrite to
      // OverwriteByExpression to preserve table definition.
      if (isV1SaveAsTableOverwrite) {
        val overwrite =
          rewriteToOverwrite(spark, catalog, ident, analyzedQuery.get, options)
        if (overwrite.isDefined) {
          val qe = spark.sessionState.executePlan(overwrite.get)
          return qe.sparkPlan :: Nil
        }
      }

      val (tableOptions, writeOptions) =
        splitTableAndWriteOptions(options)
      val qualifiedSpec = qualifyTableSpec(tableSpec, tableOptions)
      val operation =
        if (orCreate) Snapshot.Operation.CREATE_OR_REPLACE_TABLE_AS_SELECT
        else Snapshot.Operation.REPLACE_TABLE_AS_SELECT
      val writeOpts = new CaseInsensitiveStringMap(
        (writeOptions + (PaimonWriteOptions.OPERATION_OPTION -> operation.name())).asJava)
      val pinnedQuery =
        pinSnapshotInQuery(catalog, ident, analyzedQuery.get)
      if (canAtomicReplace(catalog, ident, qualifiedSpec, parts)) {
        AtomicReplaceTableAsSelectExec(
          catalog.asInstanceOf[StagingTableCatalog],
          ident,
          parts,
          pinnedQuery,
          planLater(query),
          qualifiedSpec,
          writeOpts,
          orCreate = orCreate,
          invalidateCache
        ) :: Nil
      } else {
        ReplaceTableAsSelectExec(
          catalog,
          ident,
          parts,
          pinnedQuery,
          planLater(query),
          qualifiedSpec,
          writeOpts,
          orCreate = orCreate,
          invalidateCache
        ) :: Nil
      }
    case _ => Nil
  }

  private def invalidateCache(tableCatalog: TableCatalog, table: Table, ident: Identifier): Unit = {
    tableCatalog.invalidateTable(ident)
  }
}

case class PaimonReplaceTableStrategy(spark: SparkSession)
  extends Strategy
  with PaimonTableAsSelectHelper {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case replace @ ReplaceTable(
          ResolvedIdentifier(catalog: SparkBaseCatalog, ident),
          schemaOrColumns,
          parts,
          tableSpec: TableSpec,
          orCreate) if supportsCatalog(catalog, tableSpec) =>
      val columns =
        SparkShimLoader.shim.toReplaceTableColumns(
          replace.tableSchema,
          schemaOrColumns,
          catalog,
          ident)
      val qualifiedSpec = qualifyTableSpec(tableSpec, Map.empty)
      if (canAtomicReplace(catalog, ident, qualifiedSpec, parts)) {
        SparkShimLoader.shim.createAtomicReplaceTableExec(
          catalog.asInstanceOf[StagingTableCatalog],
          ident,
          columns,
          parts,
          qualifiedSpec,
          orCreate = orCreate) :: Nil
      } else {
        SparkShimLoader.shim.createReplaceTableExec(
          catalog,
          ident,
          columns,
          parts,
          qualifiedSpec,
          orCreate = orCreate) :: Nil
      }
    case _ => Nil
  }
}
