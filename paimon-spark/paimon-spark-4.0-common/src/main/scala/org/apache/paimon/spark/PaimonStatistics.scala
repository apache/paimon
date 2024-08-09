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

import org.apache.paimon.stats
import org.apache.paimon.stats.ColStats
import org.apache.paimon.types.DataType

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics

import java.util.{Optional, OptionalLong}

import scala.collection.JavaConverters._

case class PaimonStatistics[T <: PaimonBaseScan](scan: T) extends Statistics {

  private lazy val rowCount: Long = scan.getInputPartitions.map(_.rowCount()).sum

  private lazy val scannedTotalSize: Long = rowCount * scan.readSchema().defaultSize

  private lazy val paimonStats: Optional[stats.Statistics] = scan.statistics

  override def sizeInBytes(): OptionalLong = if (paimonStats.isPresent)
    paimonStats.get().mergedRecordSize()
  else OptionalLong.of(scannedTotalSize)

  override def numRows(): OptionalLong =
    if (paimonStats.isPresent) paimonStats.get().mergedRecordCount() else OptionalLong.of(rowCount)

  override def columnStats(): java.util.Map[NamedReference, ColumnStatistics] = {
    val requiredFields = scan.requiredStatsSchema.fieldNames
    val resultMap = new java.util.HashMap[NamedReference, ColumnStatistics]()
    if (paimonStats.isPresent) {
      val paimonColStats = paimonStats.get().colStats()
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
      if (v1ColStats.avgLen.isDefined) OptionalLong.of(v1ColStats.avgLen.get.longValue)
      else OptionalLong.empty(),
      if (v1ColStats.maxLen.isDefined) OptionalLong.of(v1ColStats.maxLen.get.longValue)
      else OptionalLong.empty()
    )
  }
}
