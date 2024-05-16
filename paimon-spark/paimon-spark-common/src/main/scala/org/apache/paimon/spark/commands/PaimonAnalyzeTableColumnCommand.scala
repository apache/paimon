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

import org.apache.paimon.schema.TableSchema
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.stats.{ColStats, Statistics}
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.BatchWriteBuilder

import org.apache.parquet.Preconditions
import org.apache.spark.sql.{PaimonStatsUtils, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, TimestampType}

import java.util
import java.util.UUID

import scala.collection.JavaConverters._

/** Command for analyze table and column. */
case class PaimonAnalyzeTableColumnCommand(
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
    val currentSnapshot = table.snapshotManager().latestSnapshot()
    if (currentSnapshot == null) {
      return Seq.empty[Row]
    }

    // compute stats
    val attributes = getColumnsToAnalyze(relation, columnNames, allColumns)
    val totalSize = PaimonStatsUtils.calculateTotalSize(
      sparkSession.sessionState,
      table.name(),
      Some(table.location().toUri))
    val (mergedRecordCount, colStats) =
      PaimonStatsUtils.computeColumnStats(sparkSession, relation, attributes)

    val totalRecordCount = currentSnapshot.totalRecordCount()
    Preconditions.checkState(
      totalRecordCount >= mergedRecordCount,
      s"totalRecordCount: $totalRecordCount should be greater or equal than mergedRecordCount: $mergedRecordCount.")
    val mergedRecordSize = totalSize * (mergedRecordCount.toDouble / totalRecordCount).toLong

    // convert to paimon stats
    val tableSchema = table.schema()

    val colStatsMap: util.Map[String, ColStats[_]] = new util.HashMap
    for ((attr, stats) <- colStats) {
      colStatsMap.put(attr.name, toPaimonColStats(attr, stats, tableSchema))
    }

    val stats = new Statistics(
      currentSnapshot.id(),
      currentSnapshot.schemaId(),
      mergedRecordCount,
      mergedRecordSize,
      colStatsMap)

    // commit stats
    val commit = table.store.newCommit(UUID.randomUUID.toString)
    commit.commitStatistics(stats, BatchWriteBuilder.COMMIT_IDENTIFIER)

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
        if (!PaimonStatsUtils.analyzeSupportsType(attr.dataType)) {
          throw new UnsupportedOperationException(
            s"Analyzing on col: ${attr.name}, data type: ${attr.dataType} is not supported.")
        }
    }
    columnsToAnalyze
  }

  private def toPaimonColStats(
      attribute: Attribute,
      columnStat: ColumnStat,
      paimonSchema: TableSchema): ColStats[_] = {
    paimonSchema.fields.asScala
      .find(_.name() == attribute.name)
      .map {
        field =>
          ColStats.newColStats(
            field.id(),
            if (columnStat.distinctCount.isDefined) columnStat.distinctCount.get.longValue
            else null,
            if (columnStat.min.isDefined)
              toPaimonData(columnStat.min.get, attribute.dataType).asInstanceOf[Comparable[Any]]
            else null,
            if (columnStat.max.isDefined)
              toPaimonData(columnStat.max.get, attribute.dataType).asInstanceOf[Comparable[Any]]
            else null,
            if (columnStat.nullCount.isDefined) columnStat.nullCount.get.longValue else null,
            if (columnStat.avgLen.isDefined) columnStat.avgLen.get else null,
            if (columnStat.maxLen.isDefined) columnStat.maxLen.get else null
          )
      }
      .getOrElse(throw new RuntimeException(s"Column ${attribute.name} is not found in schema."))
  }

  /**
   * Convert data from spark type to paimon, only cover datatype meet [[PaimonStatsUtils.hasMinMax]]
   * currently.
   */
  private def toPaimonData(o: Any, dataType: DataType): Any = {
    dataType match {
      case d if !PaimonStatsUtils.hasMinMax(d) =>
        // should not reach here
        throw new UnsupportedOperationException(s"Unsupported data type $d, value is $o.")
      case _: DecimalType =>
        val d = o.asInstanceOf[Decimal]
        org.apache.paimon.data.Decimal.fromBigDecimal(d.toJavaBigDecimal, d.precision, d.scale)
      case _: TimestampType =>
        val l = o.asInstanceOf[Long]
        org.apache.paimon.data.Timestamp.fromSQLTimestamp(DateTimeUtils.toJavaTimestamp(l))
      case _ => o
    }
  }
}
