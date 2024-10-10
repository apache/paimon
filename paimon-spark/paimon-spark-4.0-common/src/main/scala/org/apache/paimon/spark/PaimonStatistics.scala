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

package org.apache.paimon.spark

import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.stats.ColStats
import org.apache.paimon.types.{DataField, DataType, RowType}

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics

import java.util.{Optional, OptionalLong}

import scala.collection.JavaConverters._

case class PaimonStatistics[T <: PaimonBaseScan](scan: T) extends Statistics {

  private lazy val rowCount: Long = scan.lazyInputPartitions.map(_.rowCount()).sum

  private lazy val scannedTotalSize: Long = rowCount * scan.readSchema().defaultSize

  private lazy val paimonStats = if (scan.statistics.isPresent) scan.statistics.get() else null

  lazy val paimonStatsEnabled: Boolean = {
    paimonStats != null &&
    paimonStats.mergedRecordSize().isPresent &&
    paimonStats.mergedRecordCount().isPresent
  }

  private def getSizeForField(field: DataField): Long = {
    Option(paimonStats.colStats().get(field.name()))
      .map(_.avgLen())
      .filter(_.isPresent)
      .map(_.getAsLong)
      .getOrElse(field.`type`().defaultSize().toLong)
  }

  private def getSizeForRow(schema: RowType): Long = {
    schema.getFields.asScala.map(field => getSizeForField(field)).sum
  }

  override def sizeInBytes(): OptionalLong = {
    if (!paimonStatsEnabled) {
      return OptionalLong.of(scannedTotalSize)
    }

    val wholeSchemaSize = getSizeForRow(scan.tableRowType)

    val requiredDataSchemaSize = scan.requiredTableFields.map {
      field =>
        val dataField = scan.tableRowType.getField(field.name)
        getSizeForField(dataField)
    }.sum
    val requiredDataSizeInBytes =
      paimonStats.mergedRecordSize().getAsLong * (requiredDataSchemaSize.toDouble / wholeSchemaSize)

    val metadataSchemaSize = scan.metadataFields.map {
      field =>
        val dataField = PaimonMetadataColumn.get(field.name).toPaimonDataField
        getSizeForField(dataField)
    }.sum
    val metadataSizeInBytes = paimonStats.mergedRecordCount().getAsLong * metadataSchemaSize

    val sizeInBytes = (requiredDataSizeInBytes + metadataSizeInBytes).toLong
    // Avoid return 0 bytes if there are some valid rows.
    // Avoid return too small size in bytes which may less than row count,
    // note the compression ratio on disk is usually bigger than memory.
    val normalized = Math.max(sizeInBytes, paimonStats.mergedRecordCount().getAsLong)
    OptionalLong.of(normalized)
  }

  override def numRows(): OptionalLong =
    if (paimonStatsEnabled) paimonStats.mergedRecordCount() else OptionalLong.of(rowCount)

  override def columnStats(): java.util.Map[NamedReference, ColumnStatistics] = {
    val requiredFields = scan.requiredStatsSchema.fieldNames
    val resultMap = new java.util.HashMap[NamedReference, ColumnStatistics]()
    if (paimonStatsEnabled) {
      val paimonColStats = paimonStats.colStats()
      scan.tableRowType.getFields.asScala
        .filter {
          field => requiredFields.contains(field.name) && paimonColStats.containsKey(field.name())
        }
        .foreach {
          field =>
            resultMap.put(
              PaimonUtils.fieldReference(field.name()),
              PaimonColumnStats(field.`type`(), paimonColStats.get(field.name()))
            )
        }
    }
    resultMap
  }
}

case class PaimonColumnStats(
    override val nullCount: OptionalLong,
    override val min: Optional[Object],
    override val max: Optional[Object],
    override val distinctCount: OptionalLong,
    override val avgLen: OptionalLong,
    override val maxLen: OptionalLong)
  extends ColumnStatistics

object PaimonColumnStats {
  def apply(dateType: DataType, paimonColStats: ColStats[_]): PaimonColumnStats = {
    PaimonColumnStats(
      paimonColStats.nullCount,
      Optional.ofNullable(SparkInternalRow.fromPaimon(paimonColStats.min().orElse(null), dateType)),
      Optional.ofNullable(SparkInternalRow.fromPaimon(paimonColStats.max().orElse(null), dateType)),
      paimonColStats.distinctCount,
      paimonColStats.avgLen,
      paimonColStats.maxLen
    )
  }

  def apply(v1ColStats: ColumnStat): PaimonColumnStats = {
    import PaimonImplicits._
    PaimonColumnStats(
      if (v1ColStats.nullCount.isDefined) OptionalLong.of(v1ColStats.nullCount.get.longValue)
      else OptionalLong.empty(),
      v1ColStats.min,
      v1ColStats.max,
      if (v1ColStats.distinctCount.isDefined)
        OptionalLong.of(v1ColStats.distinctCount.get.longValue)
      else OptionalLong.empty(),
      if (v1ColStats.avgLen.isDefined) OptionalLong.of(v1ColStats.avgLen.get.longValue())
      else OptionalLong.empty(),
      if (v1ColStats.maxLen.isDefined) OptionalLong.of(v1ColStats.maxLen.get.longValue())
      else OptionalLong.empty()
    )
  }
}
