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

import org.apache.paimon.CoreOptions.TYPE
import org.apache.paimon.options.Options
import org.apache.paimon.spark.{SparkCatalog, SparkGenericCatalog, SparkSource, SparkTable}
import org.apache.paimon.spark.catalog.SparkBaseCatalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, ResolvedIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReplaceTable, ReplaceTableAsSelect, TableSpec}
import org.apache.spark.sql.connector.catalog.{Identifier, StagingTableCatalog, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.{PaimonStrategyHelper, SparkPlan, SparkStrategy}
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import scala.collection.JavaConverters._

case class PaimonReplaceTableAsSelectStrategy(spark: SparkSession)
  extends SparkStrategy
  with PaimonStrategyHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ReplaceTableAsSelect(
          ResolvedIdentifier(catalog: SparkBaseCatalog, ident),
          parts,
          query,
          tableSpec: TableSpec,
          options,
          orCreate,
          true) if PaimonReplaceTableStrategyHelper.supportsCatalog(catalog, tableSpec) =>
      val (tableOptions, writeOptions) = PaimonStrategyHelper.splitTableAndWriteOptions(options)
      val qualifiedSpec = qualifyTableSpec(tableSpec, tableOptions)
      if (PaimonReplaceTableStrategyHelper.canAtomicReplace(catalog, ident, qualifiedSpec, parts)) {
        SparkShimLoader.shim.createAtomicReplaceTableAsSelectExec(
          catalog.asInstanceOf[StagingTableCatalog],
          ident,
          parts,
          query,
          qualifiedSpec,
          writeOptions,
          orCreate = orCreate) :: Nil
      } else {
        SparkShimLoader.shim.createReplaceTableAsSelectExec(
          catalog,
          ident,
          parts,
          query,
          qualifiedSpec,
          writeOptions,
          orCreate = orCreate) :: Nil
      }
    case _ => Nil
  }
}

case class PaimonReplaceTableStrategy(spark: SparkSession)
  extends SparkStrategy
  with PaimonStrategyHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case replace @ ReplaceTable(
          ResolvedIdentifier(catalog: SparkBaseCatalog, ident),
          schemaOrColumns,
          parts,
          tableSpec: TableSpec,
          orCreate) if PaimonReplaceTableStrategyHelper.supportsCatalog(catalog, tableSpec) =>
      val columns =
        SparkShimLoader.shim.toReplaceTableColumns(
          replace.tableSchema,
          schemaOrColumns,
          catalog,
          ident)
      val qualifiedSpec = qualifyTableSpec(tableSpec, Map.empty)
      if (PaimonReplaceTableStrategyHelper.canAtomicReplace(catalog, ident, qualifiedSpec, parts)) {
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

private[shim] object PaimonReplaceTableStrategyHelper {

  def supportsCatalog(catalog: SparkBaseCatalog, tableSpec: TableSpec): Boolean = catalog match {
    case _: SparkCatalog => true
    case _: SparkGenericCatalog =>
      tableSpec.provider.exists(_.equalsIgnoreCase(SparkSource.NAME))
    case _ => false
  }

  /**
   * Whether replace can use Spark's staged replace path. Paimon's replaceTable is not a
   * rollbackable atomic replace; it swaps the current schema and truncates current data while
   * preserving old snapshots. Return false for cases replaceTable would reject so Spark falls back
   * to drop+create.
   */
  def canAtomicReplace(
      catalog: SparkBaseCatalog,
      ident: Identifier,
      tableSpec: TableSpec,
      parts: Seq[Transform]): Boolean = {
    try {
      val existing = catalog.loadTable(ident)
      if (!existing.isInstanceOf[SparkTable]) return false
      val existingProvider =
        Option(existing.properties().get(TableCatalog.PROP_PROVIDER)).getOrElse(SparkSource.NAME)
      val targetProvider = tableSpec.provider.getOrElse(SparkSource.NAME)
      if (!existingProvider.equalsIgnoreCase(targetProvider)) return false
      val existingType = Options.fromMap(existing.properties()).get(TYPE)
      val targetType = Options.fromMap(tableSpec.properties.asJava).get(TYPE)
      if (existingType != targetType) return false
      val existingParts = existing.partitioning().toSeq
      existingParts.size == parts.size &&
      existingParts.zip(parts).forall { case (a, b) => a.toString == b.toString }
    } catch {
      case _: NoSuchTableException => true
    }
  }

}
