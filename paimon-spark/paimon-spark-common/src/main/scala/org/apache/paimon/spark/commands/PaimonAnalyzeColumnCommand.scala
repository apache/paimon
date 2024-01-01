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

import org.apache.paimon.schema.SchemaChange
import org.apache.paimon.spark.{SparkCatalog, SparkTable}
import org.apache.paimon.spark.catalog.WithPaimonCatalog
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.DataFieldStats

import org.apache.spark.sql.{Row, SparkSession, Utils}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, TimestampType}

import java.util

case class PaimonAnalyzeColumnCommand(
    catalog: TableCatalog,
    identifier: Identifier,
    v2Table: SparkTable,
    columnNames: Option[Seq[String]],
    allColumns: Boolean)
  extends PaimonLeafRunnableCommand
  with WithFileStoreTable {

  override def table: FileStoreTable = v2Table.getTable.asInstanceOf[FileStoreTable]

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val relation = DataSourceV2Relation.create(v2Table, Some(catalog), Some(identifier))
    val attributes = getColumnsToAnalyze(relation, columnNames, allColumns)

    val totalSize = Utils.calculateTotalSize(
      sparkSession.sessionState,
      table.name(),
      Some(table.location().toUri))
    val (rowCount, colStats) = Utils.computeColumnStats(sparkSession, relation, attributes)

    val changes = new util.ArrayList[SchemaChange]()

    // todo: just for quick test, need to find a more suitable place to restore it
    changes.add(SchemaChange.setOption("totalSize", totalSize.toString))
    changes.add(SchemaChange.setOption("rowCount", rowCount.toString))

    colStats.foreach {
      case (attr, stats) =>
        changes.add(
          SchemaChange.updateColumnStats(
            attr.name,
            sparkColumnStatsToPaimonDataFieldStats(attr.dataType, stats)))
    }

    if (changes.size() > 0) {
      catalog
        .asInstanceOf[WithPaimonCatalog]
        .paimonCatalog()
        .alterTable(SparkCatalog.toIdentifier(identifier), changes, false)
    }

    Seq.empty[Row]
  }

  private def getColumnsToAnalyze(
      relation: DataSourceV2Relation,
      columnNames: Option[Seq[String]],
      allColumns: Boolean): Seq[Attribute] = {
    if (columnNames.isDefined && allColumns) {
      throw new UnsupportedOperationException(
        "Parameter `columnNames` and `allColumns` are " +
          "mutually exclusive. Only one of them should be specified.")
    }
    val columnsToAnalyze = if (allColumns) {
      relation.output
    } else {
      columnNames.get.map {
        col =>
          val exprOption = relation.output.find(attr => conf.resolver(attr.name, col))
          exprOption.getOrElse(
            throw new RuntimeException(s"Column $col not found in ${relation.table.name()}."))
      }
    }
    columnsToAnalyze.foreach {
      attr =>
        if (!Utils.analyzeSupportsType(attr.dataType)) {
          throw new UnsupportedOperationException(s"not supported")
        }
    }
    columnsToAnalyze
  }

  private def sparkColumnStatsToPaimonDataFieldStats(
      dataType: DataType,
      columnStat: ColumnStat): DataFieldStats = {
    new DataFieldStats(
      if (columnStat.distinctCount.isDefined) columnStat.distinctCount.get.longValue else null,
      if (columnStat.min.isDefined) sparkDataToPaimonData(columnStat.min.get, dataType) else null,
      if (columnStat.max.isDefined) sparkDataToPaimonData(columnStat.max.get, dataType) else null,
      if (columnStat.nullCount.isDefined) columnStat.nullCount.get.longValue else null,
      if (columnStat.avgLen.isDefined) columnStat.avgLen.get else null,
      if (columnStat.maxLen.isDefined) columnStat.maxLen.get else null)
  }

  private def sparkDataToPaimonData(o: Any, dataType: DataType): Any = {
    dataType match {
      case _: DecimalType =>
        val d = o.asInstanceOf[Decimal]
        org.apache.paimon.data.Decimal.fromBigDecimal(d.toJavaBigDecimal, d.precision, d.scale)
      case _: TimestampType =>
        val l = o.asInstanceOf[Long]
        org.apache.paimon.data.Timestamp.fromEpochMillis(l)
      case _ => o
    }
  }

}
