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

package org.apache.spark.sql.execution

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.TYPE
import org.apache.paimon.iceberg.IcebergOptions
import org.apache.paimon.options.Options
import org.apache.paimon.spark.{SparkCatalog, SparkGenericCatalog, SparkSource, SparkTable}
import org.apache.paimon.spark.catalog.SparkBaseCatalog
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.snapshot.TimeTravelUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, TableSpec}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._

/** Shared trait for CreateTableAsSelect and ReplaceTableAsSelect strategies. */
trait PaimonTableAsSelectHelper {

  def spark: SparkSession

  protected def makeQualifiedDBObjectPath(location: String): String = {
    CatalogUtils.makeQualifiedDBObjectPath(
      spark.sharedState.conf.get(WAREHOUSE_PATH),
      location,
      spark.sharedState.hadoopConf)
  }

  protected def qualifyTableSpec(
      tableSpec: TableSpec,
      tableOptions: Map[String, String]): TableSpec = {
    SparkShimLoader.shim.copyTableSpec(
      tableSpec,
      tableOptions,
      tableSpec.location.map(makeQualifiedDBObjectPath))
  }
}

object PaimonTableAsSelectHelper {

  private val tableOptionKeys: Set[String] =
    (CoreOptions.getOptions.asScala.map(_.key()) ++ IcebergOptions.getOptions.asScala.map(
      _.key())).toSet

  def splitTableAndWriteOptions(
      options: Map[String, String]): (Map[String, String], Map[String, String]) = {
    options.partition { case (key, _) => tableOptionKeys.contains(key) }
  }

  /** Whether the current call originates from V1 DataFrameWriter.saveAsTable(). */
  def isV1SaveAsTableOverwrite: Boolean = {
    Thread.currentThread().getStackTrace.exists {
      e =>
        val cls = e.getClassName
        cls.contains("DataFrameWriter") && !cls.contains("DataFrameWriterV2")
    }
  }

  /**
   * Pin snapshot for self-referencing RTAS queries. When the query reads from the same table being
   * replaced, this rewrites the DataSourceV2Relation to pin it to the pre-truncation snapshot.
   * Relations with user-specified time travel options are left unchanged.
   */
  def pinSnapshotInQuery(
      catalog: TableCatalog,
      ident: Identifier,
      query: LogicalPlan): LogicalPlan = {
    val snapshotId: java.lang.Long =
      try {
        val existing = catalog.loadTable(ident)
        if (!existing.isInstanceOf[SparkTable]) return query
        val paimonTable = existing.asInstanceOf[SparkTable].getTable
        if (!paimonTable.isInstanceOf[FileStoreTable]) return query
        paimonTable.asInstanceOf[FileStoreTable].snapshotManager().latestSnapshotId()
      } catch {
        case _: Exception => return query
      }
    if (snapshotId == null) return query

    query.transformDown {
      case r: DataSourceV2Relation
          if r.catalog.contains(catalog) && r.identifier.contains(ident) &&
            !TimeTravelUtil.hasTimeTravelOptions(Options.fromMap(r.options.asCaseSensitiveMap())) =>
        val pinned = new JHashMap[String, String](r.options.asCaseSensitiveMap())
        pinned.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), snapshotId.toString)
        r.copy(options = new CaseInsensitiveStringMap(pinned))
    }
  }

  /**
   * Rewrite to OverwriteByExpression or OverwritePartitionsDynamic for an existing table,
   * preserving table definition. Returns None if the table does not exist.
   */
  def rewriteToOverwrite(
      spark: SparkSession,
      catalog: TableCatalog,
      ident: Identifier,
      query: LogicalPlan,
      writeOptions: Map[String, String]): Option[LogicalPlan] = {
    try {
      val existing = catalog.loadTable(ident)
      if (!existing.isInstanceOf[SparkTable]) return None
      val relation =
        DataSourceV2Relation.create(existing, Some(catalog), Some(ident))
      val dynamicOverwrite = existing.partitioning().nonEmpty &&
        spark.sessionState.conf.partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC
      if (dynamicOverwrite) {
        Some(OverwritePartitionsDynamic.byName(relation, query, writeOptions))
      } else {
        Some(OverwriteByExpression.byName(relation, query, Literal(true), writeOptions))
      }
    } catch {
      case _: Exception => None
    }
  }

  def supportsCatalog(catalog: SparkBaseCatalog, tableSpec: TableSpec): Boolean = catalog match {
    case _: SparkCatalog => true
    case _: SparkGenericCatalog =>
      tableSpec.provider.exists(_.equalsIgnoreCase(SparkSource.NAME))
    case _ => false
  }

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
