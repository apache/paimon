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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReplaceTable, ReplaceTableAsSelect, TableSpec}
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.execution.{PaimonTableAsSelectHelper, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.PaimonTableAsSelectHelper._
import org.apache.spark.sql.paimon.shims.SparkShimLoader

case class PaimonReplaceTableAsSelectStrategy(spark: SparkSession)
  extends SparkStrategy
  with PaimonTableAsSelectHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ReplaceTableAsSelect(
          ResolvedIdentifier(catalog: SparkBaseCatalog, ident),
          parts,
          query,
          tableSpec: TableSpec,
          options,
          orCreate,
          true) if supportsCatalog(catalog, tableSpec) =>
      // For V1 saveAsTable + overwrite on an existing table, rewrite to
      // OverwriteByExpression to preserve table definition.
      if (isV1SaveAsTableOverwrite) {
        val overwrite = rewriteToOverwrite(spark, catalog, ident, query, options)
        if (overwrite.isDefined) {
          val qe = spark.sessionState.executePlan(overwrite.get)
          return qe.sparkPlan :: Nil
        }
      }

      val (tableOptions, writeOptions) = splitTableAndWriteOptions(options)
      val qualifiedSpec = qualifyTableSpec(tableSpec, tableOptions)
      val operation =
        if (orCreate) Snapshot.Operation.CREATE_OR_REPLACE_TABLE_AS_SELECT
        else Snapshot.Operation.REPLACE_TABLE_AS_SELECT
      val finalWriteOptions =
        writeOptions + (PaimonWriteOptions.OPERATION_OPTION -> operation.name())
      // Pin snapshot in query to prevent self-referencing RTAS from reading truncated data
      val pinnedQuery = pinSnapshotInQuery(catalog, ident, query)
      if (canAtomicReplace(catalog, ident, qualifiedSpec, parts)) {
        SparkShimLoader.shim.createAtomicReplaceTableAsSelectExec(
          catalog.asInstanceOf[StagingTableCatalog],
          ident,
          parts,
          pinnedQuery,
          qualifiedSpec,
          finalWriteOptions,
          orCreate = orCreate) :: Nil
      } else {
        SparkShimLoader.shim.createReplaceTableAsSelectExec(
          catalog,
          ident,
          parts,
          pinnedQuery,
          qualifiedSpec,
          finalWriteOptions,
          orCreate = orCreate) :: Nil
      }
    case _ => Nil
  }
}

case class PaimonReplaceTableStrategy(spark: SparkSession)
  extends SparkStrategy
  with PaimonTableAsSelectHelper {

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
