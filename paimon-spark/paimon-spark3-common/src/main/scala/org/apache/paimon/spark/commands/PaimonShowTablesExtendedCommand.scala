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

package org.apache.paimon.spark.commands

import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{QuotingUtils, StringUtils}
import org.apache.spark.sql.connector.catalog.{Identifier, PaimonCatalogUtils, SupportsPartitionManagement, Table, TableCatalog}
import org.apache.spark.sql.connector.catalog.PaimonCatalogImplicits._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._

import scala.collection.JavaConverters._
import scala.collection.mutable

case class PaimonShowTablesExtendedCommand(
    catalog: TableCatalog,
    namespace: Seq[String],
    pattern: String,
    override val output: Seq[Attribute],
    isExtended: Boolean = false,
    partitionSpec: Option[TablePartitionSpec] = None)
  extends PaimonLeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    val rows = new mutable.ArrayBuffer[Row]()

    val tables = catalog.listTables(namespace.toArray)
    tables.map {
      tableIdent: Identifier =>
        if (StringUtils.filterPattern(Seq(tableIdent.name()), pattern).nonEmpty) {
          val table = catalog.loadTable(tableIdent)
          val information = getTableDetails(catalog.name, tableIdent, table)
          rows += Row(tableIdent.namespace().quoted, tableIdent.name(), false, s"$information\n")
        }
    }

    // TODO: view

    rows.toSeq
  }

  private def getTableDetails(catalogName: String, identifier: Identifier, table: Table): String = {
    val results = new mutable.LinkedHashMap[String, String]()

    results.put("Catalog", catalogName)
    results.put("Namespace", identifier.namespace().quoted)
    results.put("Table", identifier.name())
    val tableType = if (table.properties().containsKey(TableCatalog.PROP_EXTERNAL)) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }
    results.put("Type", tableType.name)

    PaimonCatalogUtils.TABLE_RESERVED_PROPERTIES
      .filterNot(_ == TableCatalog.PROP_EXTERNAL)
      .foreach(
        propKey => {
          if (table.properties.containsKey(propKey)) {
            results.put(propKey.capitalize, table.properties.get(propKey))
          }
        })

    val properties: Seq[String] =
      conf
        .redactOptions(table.properties.asScala.toMap)
        .toList
        .filter(kv => !PaimonCatalogUtils.TABLE_RESERVED_PROPERTIES.contains(kv._1))
        .sortBy(_._1)
        .map { case (key, value) => key + "=" + value }
    if (!table.properties().isEmpty) {
      results.put("Table Properties", properties.mkString("[", ", ", "]"))
    }

    // Partition Provider & Partition Columns
    if (supportsPartitions(table) && table.asPartitionable.partitionSchema().nonEmpty) {
      results.put("Partition Provider", "Catalog")
      results.put(
        "Partition Columns",
        table.asPartitionable
          .partitionSchema()
          .map(field => quoteIdentifier(field.name))
          .mkString("[", ", ", "]"))
    }

    if (table.schema().nonEmpty) {
      results.put("Schema", table.schema().treeString)
    }

    results
      .map {
        case (key, value) =>
          if (value.isEmpty) key else s"$key: $value"
      }
      .mkString("", "\n", "")
  }

  private def supportsPartitions(table: Table): Boolean = table match {
    case _: SupportsPartitionManagement => true
    case _ => false
  }

  // copy from spark for compatibility
  private def quoteIdentifier(name: String): String = {
    // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
    // identifier with back-ticks.
    "`" + name.replace("`", "``") + "`"
  }
}
