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

import org.apache.paimon.{stats, CoreOptions}
import org.apache.paimon.predicate.{Predicate, PredicateBuilder}
import org.apache.paimon.spark.sources.PaimonMicroBatchStream
import org.apache.paimon.spark.statistics.StatisticsHelper
import org.apache.paimon.table.{DataTable, FileStoreTable, Table}
import org.apache.paimon.table.source.{ReadBuilder, Split}
import org.apache.paimon.types.RowType

import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.{Batch, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.util.Optional

import scala.collection.JavaConverters._

abstract class PaimonBaseScan(
    table: Table,
    requiredSchema: StructType,
    filters: Seq[Predicate],
    reservedFilters: Seq[Filter],
    pushDownLimit: Option[Int])
  extends Scan
  with SupportsReportStatistics
  with ScanHelper
  with StatisticsHelper {

  val tableRowType: RowType = table.rowType

  private lazy val tableSchema = SparkTypeUtils.fromPaimonRowType(tableRowType)

  protected var runtimeFilters: Array[Filter] = Array.empty

  protected var splits: Array[Split] = _

  override val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())

  lazy val statistics: Optional[stats.Statistics] = table.statistics()

  lazy val requiredStatsSchema: StructType = {
    val fieldNames = requiredSchema.fieldNames ++ reservedFilters.flatMap(_.references)
    StructType(tableSchema.filter(field => fieldNames.contains(field.name)))
  }

  lazy val readBuilder: ReadBuilder = {
    val _readBuilder = table.newReadBuilder()

    val projection = readSchema().fieldNames.map(field => tableRowType.getFieldNames.indexOf(field))
    _readBuilder.withProjection(projection)
    if (filters.nonEmpty) {
      val pushedPredicate = PredicateBuilder.and(filters: _*)
      _readBuilder.withFilter(pushedPredicate)
    }
    pushDownLimit.foreach(_readBuilder.withLimit)

    _readBuilder
  }

  def getOriginSplits: Array[Split] = {
    readBuilder.newScan().plan().splits().asScala.toArray
  }

  def getSplits: Array[Split] = {
    if (splits == null) {
      splits = reshuffleSplits(getOriginSplits)
    }
    splits
  }

  override def readSchema(): StructType = {
    val requiredFieldNames = requiredSchema.fieldNames
    StructType(tableSchema.filter(field => requiredFieldNames.contains(field.name)))
  }

  override def toBatch: Batch = {
    PaimonBatch(getSplits, readBuilder)
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new PaimonMicroBatchStream(table.asInstanceOf[DataTable], readBuilder, checkpointLocation)
  }

  override def estimateStatistics(): Statistics = {
    val stats = PaimonStatistics(this)
    if (reservedFilters.nonEmpty) {
      filterStatistics(stats, reservedFilters)
    } else {
      stats
    }
  }

  override def supportedCustomMetrics: Array[CustomMetric] = {
    val paimonMetrics: Array[CustomMetric] = table match {
      case _: FileStoreTable =>
        Array(
          PaimonNumSplitMetric(),
          PaimonSplitSizeMetric(),
          PaimonAvgSplitSizeMetric()
        )
      case _ =>
        Array.empty[CustomMetric]
    }
    super.supportedCustomMetrics() ++ paimonMetrics
  }

  override def description(): String = {
    val pushedFiltersStr = if (filters.nonEmpty) {
      ", PushedFilters: [" + filters.mkString(",") + "]"
    } else {
      ""
    }
    s"PaimonScan: [${table.name}]" + pushedFiltersStr +
      pushDownLimit.map(limit => s", Limit: [$limit]").getOrElse("")
  }
}
