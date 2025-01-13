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

import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedPartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal, ToPrettyString}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsPartitionManagement, TableCatalog}
import org.apache.spark.sql.connector.catalog.PaimonCatalogImplicits._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters._
import scala.collection.mutable

case class PaimonShowTablePartitionCommand(
    override val output: Seq[Attribute],
    catalog: TableCatalog,
    tableIndent: Identifier,
    partSpec: ResolvedPartitionSpec)
  extends PaimonLeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val rows = new mutable.ArrayBuffer[Row]()
    val table = catalog.loadTable(tableIndent)
    val information = getTablePartitionDetails(tableIndent, table.asPartitionable, partSpec)
    rows += Row(tableIndent.namespace.quoted, tableIndent.name(), false, s"$information\n")

    rows.toSeq
  }

  private def getTablePartitionDetails(
      tableIdent: Identifier,
      partitionTable: SupportsPartitionManagement,
      partSpec: ResolvedPartitionSpec): String = {
    val results = new mutable.LinkedHashMap[String, String]()

    // "Partition Values"
    val partitionSchema = partitionTable.partitionSchema()
    val (names, ident) = (partSpec.names, partSpec.ident)
    val partitionIdentifiers = partitionTable.listPartitionIdentifiers(names.toArray, ident)
    if (partitionIdentifiers.isEmpty) {
      val part = ident
        .toSeq(partitionSchema)
        .zip(partitionSchema.map(_.name))
        .map(kv => s"${kv._2}" + s" = ${kv._1}")
        .mkString(", ")
      throw new RuntimeException(
        s"""
           |[PARTITIONS_NOT_FOUND] The partition(s) PARTITION ($part) cannot be found in table ${tableIdent.toString}.
           |Verify the partition specification and table name.
           |""".stripMargin)
    }
    assert(partitionIdentifiers.length == 1)
    val row = partitionIdentifiers.head
    val len = partitionSchema.length
    val partitions = new Array[String](len)
    val timeZoneId = conf.sessionLocalTimeZone
    for (i <- 0 until len) {
      val dataType = partitionSchema(i).dataType
      val partValueUTF8String =
        Compatibility
          .cast(Literal(row.get(i, dataType), dataType), StringType, Some(timeZoneId))
          .eval(null)
      val partValueStr = if (partValueUTF8String == null) "null" else partValueUTF8String.toString
      partitions(i) = escapePathName(partitionSchema(i).name) + "=" + escapePathName(partValueStr)
    }
    val partitionValues = partitions.mkString("[", ", ", "]")
    results.put("Partition Values", s"$partitionValues")

    // TODO "Partition Parameters", "Created Time", "Last Access", "Partition Statistics"

    results
      .map {
        case (key, value) =>
          if (value.isEmpty) key else s"$key: $value"
      }
      .mkString("", "\n", "")
  }
}
