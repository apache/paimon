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

package org.apache.paimon.spark.scan

import org.apache.paimon.spark.DataConverter
import org.apache.paimon.spark.util.SplitUtils
import org.apache.paimon.stats
import org.apache.paimon.stats.ColStats
import org.apache.paimon.table.source.Split
import org.apache.paimon.types.{DataField, DataType, RowType}

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics

import java.util.{Optional, OptionalLong}

import scala.collection.JavaConverters._

case class PaimonStatistics(
    splits: Array[Split],
    readRowType: RowType,
    tableRowType: RowType,
    paimonStats: Optional[stats.Statistics]
) extends Statistics {

  lazy val numRows: OptionalLong = {
    if (splits.exists(_.rowCount() == -1)) {
      OptionalLong.empty()
    } else {
      OptionalLong.of(splits.map(_.rowCount()).sum)
    }
  }

  lazy val sizeInBytes: OptionalLong = {
    if (numRows.isPresent) {
      val sizeInBytes = numRows.getAsLong * estimateRowSize(readRowType)
      // Avoid return 0 bytes if there are some valid rows.
      // Avoid return too small size in bytes which may less than row count,
      // note the compression ratio on disk is usually bigger than memory.
      OptionalLong.of(Math.max(sizeInBytes, numRows.getAsLong))
    } else {
      val fileTotalSize = splits.map(SplitUtils.splitSize).sum
      if (fileTotalSize == 0) {
        OptionalLong.empty()
      } else {
        val size = (fileTotalSize * readRowSizeRatio).toLong
        OptionalLong.of(size)
      }
    }
  }

  lazy val readRowSizeRatio: Double =
    estimateRowSize(readRowType).toDouble / estimateRowSize(tableRowType)

  private def estimateRowSize(rowType: RowType): Long = {
    rowType.getFields.asScala.map(estimateFieldSize).sum
  }

  private def estimateFieldSize(field: DataField): Long = {
    if (paimonStats.isPresent) {
      val colStat = paimonStats.get.colStats().get(field.name())
      if (colStat != null && colStat.avgLen().isPresent) {
        colStat.avgLen().getAsLong
      } else {
        field.`type`().defaultSize().toLong
      }
    } else {
      field.`type`().defaultSize().toLong
    }
  }

  override lazy val columnStats: java.util.Map[NamedReference, ColumnStatistics] = {
    val requiredFields = readRowType.getFieldNames.asScala
    val resultMap = new java.util.HashMap[NamedReference, ColumnStatistics]()
    if (paimonStats.isPresent) {
      val paimonColStats = paimonStats.get.colStats()
      tableRowType.getFields.asScala
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
      Optional.ofNullable(
        DataConverter
          .fromPaimon(paimonColStats.min().orElse(null), dateType)),
      Optional.ofNullable(DataConverter.fromPaimon(paimonColStats.max().orElse(null), dateType)),
      paimonColStats.distinctCount,
      paimonColStats.avgLen,
      paimonColStats.maxLen
    )
  }

  def apply(v1ColStats: ColumnStat): PaimonColumnStats = {
    import org.apache.paimon.spark.PaimonImplicits._
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
