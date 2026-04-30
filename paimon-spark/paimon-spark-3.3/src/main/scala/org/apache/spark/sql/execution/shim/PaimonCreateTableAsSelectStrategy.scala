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

import org.apache.paimon.CoreOptions
import org.apache.paimon.iceberg.IcebergOptions
import org.apache.paimon.spark.SparkCatalog

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.analysis.ResolvedDBObjectName
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LogicalPlan, ReplaceTableAsSelect, TableSpec}
import org.apache.spark.sql.connector.catalog.{Identifier, StagingTableCatalog, Table, TableCatalog}
import org.apache.spark.sql.execution.{PaimonStrategyHelper, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{CreateTableAsSelectExec, DataSourceV2Relation, ReplaceTableAsSelectExec}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

case class PaimonCreateTableAsSelectStrategy(spark: SparkSession)
  extends Strategy
  with PaimonStrategyHelper {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateTableAsSelect(
          ResolvedDBObjectName(catalog: SparkCatalog, ident),
          parts,
          query,
          tableSpec: TableSpec,
          options,
          ifNotExists) =>
      catalog match {
        case _: StagingTableCatalog =>
          throw new RuntimeException("Paimon can't extend StagingTableCatalog for now.")
        case _ =>
          val (newTableSpec, writeOptions) = tableSpecWithOptions(tableSpec, options)
          failIfPartitionedFormatTable(catalog, newTableSpec, parts)

          CreateTableAsSelectExec(
            catalog.asTableCatalog,
            ident.asIdentifier,
            parts,
            query,
            planLater(query),
            qualifyLocInTableSpec(newTableSpec),
            new CaseInsensitiveStringMap(writeOptions.asJava),
            ifNotExists
          ) :: Nil
      }
    case ReplaceTableAsSelect(
          ResolvedDBObjectName(catalog: SparkCatalog, ident),
          parts,
          query,
          tableSpec: TableSpec,
          options,
          orCreate) =>
      catalog match {
        case _: StagingTableCatalog =>
          throw new RuntimeException("Paimon can't extend StagingTableCatalog for now.")
        case _ =>
          val (newTableSpec, writeOptions) = tableSpecWithOptions(tableSpec, options)

          ReplaceTableAsSelectExec(
            catalog.asTableCatalog,
            ident.asIdentifier,
            parts,
            query,
            planLater(query),
            qualifyLocInTableSpec(newTableSpec),
            new CaseInsensitiveStringMap(writeOptions.asJava),
            orCreate,
            invalidateCache
          ) :: Nil
      }
    case _ => Nil
  }

  private lazy val tableOptionKeys: Seq[String] = {
    val coreOptionKeys = CoreOptions.getOptions.asScala.map(_.key()).toSeq

    // Include Iceberg compatibility options in table properties (fix for DataFrame writer options)
    val icebergOptionKeys = IcebergOptions.getOptions.asScala.map(_.key()).toSeq

    coreOptionKeys ++ icebergOptionKeys
  }

  private def tableSpecWithOptions(
      tableSpec: TableSpec,
      options: Map[String, String]): (TableSpec, Map[String, String]) = {
    val (tableOptions, writeOptions) = options.partition {
      case (key, _) => tableOptionKeys.contains(key)
    }
    (tableSpec.copy(properties = tableSpec.properties ++ tableOptions), writeOptions)
  }

  private def failIfPartitionedFormatTable(
      catalog: SparkCatalog,
      tableSpec: TableSpec,
      parts: Seq[_]): Unit = {
    if (catalog.isFormatTable(tableSpec.provider.orNull) && parts.nonEmpty) {
      throw new UnsupportedOperationException(
        "Using CTAS with partitioned format table is not supported yet.")
    }
  }

  private def invalidateCache(catalog: TableCatalog, table: Table, ident: Identifier): Unit = {
    val v2Relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
    spark.sharedState.cacheManager.uncacheQuery(spark, v2Relation, true, false)
  }
}
