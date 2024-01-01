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

import org.apache.paimon.types.{DataFieldStats, DataType}
import org.apache.paimon.utils.OptionalUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Utils
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics

import java.util.{Optional, OptionalLong}

import scala.collection.JavaConverters._

case class PaimonStatistics[T <: PaimonBaseScan](scan: T) extends Statistics with Logging {

  private lazy val rowCount: Long = {
    // todo: add a config to control, e.g, read.use_analyze_stats
    if (scan.options.containsKey("rowCount")) {
      scan.options.get("rowCount").toLong
    } else {
      scan.getSplits.map(_.rowCount).sum
    }
  }

  private lazy val scannedTotalSize: Long = {
    // todo: add a config to control, e.g, read.use_analyze_stats
    if (scan.options.containsKey("totalSize")) {
      scan.options.get("totalSize").toLong
    } else {
      rowCount * scan.readSchema().defaultSize
    }
  }

  override def sizeInBytes(): OptionalLong = OptionalLong.of(scannedTotalSize)

  override def numRows(): OptionalLong = OptionalLong.of(rowCount)

  override def columnStats(): java.util.Map[NamedReference, ColumnStatistics] = {
    // todo: add a config to control, e.g, read.use_analyze_stats
    val requiredFields = scan.readSchema().fieldNames.toList.asJava
    val resultMap = new java.util.HashMap[NamedReference, ColumnStatistics]()
    scan.tableRowType.getFields
      .stream()
      .filter(field => requiredFields.contains(field.name) && field.stats() != null)
      .forEach(
        f =>
          resultMap.put(Utils.fieldReference(f.name()), PaimonColumnStats(f.`type`(), f.stats())))
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
  def apply(dateType: DataType, stats: DataFieldStats): PaimonColumnStats = {
    PaimonColumnStats(
      OptionalUtils.of(stats.nullCount),
      Optional.ofNullable(SparkInternalRow.fromPaimon(stats.min(), dateType)),
      Optional.ofNullable(SparkInternalRow.fromPaimon(stats.max(), dateType)),
      OptionalUtils.of(stats.distinctCount),
      OptionalUtils.of(stats.avgLen),
      OptionalUtils.of(stats.maxLen)
    )
  }
}
