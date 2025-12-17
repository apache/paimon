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

import org.apache.paimon.annotation.VisibleForTesting
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.spark.sources.PaimonMicroBatchStream
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.stats
import org.apache.paimon.table.{DataTable, FileStoreTable, InnerTable}
import org.apache.paimon.table.source.{InnerTableScan, Split}

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.types.StructType

import java.util.Optional

import scala.collection.JavaConverters._

abstract class PaimonBaseScan(table: InnerTable)
  extends Scan
  with SupportsReportStatistics
  with ScanHelper
  with ColumnPruningAndPushDown {

  protected var inputPartitions: Seq[PaimonInputPartition] = _

  protected var inputSplits: Array[Split] = _

  lazy val statistics: Optional[stats.Statistics] = table.statistics()

  private lazy val paimonMetricsRegistry: SparkMetricRegistry = SparkMetricRegistry()

  lazy val requiredStatsSchema: StructType = {
    val fieldNames = readTableRowType.getFields.asScala.map(_.name)
    StructType(tableSchema.filter(field => fieldNames.contains(field.name)))
  }

  @VisibleForTesting
  def getOriginSplits: Array[Split] = {
    if (inputSplits == null) {
      inputSplits = readBuilder
        .newScan()
        .asInstanceOf[InnerTableScan]
        .withMetricRegistry(paimonMetricsRegistry)
        .plan()
        .splits()
        .asScala
        .toArray
    }
    inputSplits
  }

  final def lazyInputPartitions: Seq[PaimonInputPartition] = {
    if (inputPartitions == null) {
      inputPartitions = getInputPartitions(getOriginSplits)
    }
    inputPartitions
  }

  override def toBatch: Batch = {
    ensureNoFullScan()
    PaimonBatch(lazyInputPartitions, readBuilder, coreOptions.blobAsDescriptor(), metadataColumns)
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new PaimonMicroBatchStream(table.asInstanceOf[DataTable], readBuilder, checkpointLocation)
  }

  override def estimateStatistics(): Statistics = {
    PaimonStatistics(this)
  }

  override def supportedCustomMetrics: Array[CustomMetric] = {
    Array(
      PaimonNumSplitMetric(),
      PaimonPartitionSizeMetric(),
      PaimonPlanningDurationMetric(),
      PaimonScannedSnapshotIdMetric(),
      PaimonScannedManifestsMetric(),
      PaimonSkippedTableFilesMetric(),
      PaimonResultedTableFilesMetric()
    )
  }

  override def reportDriverMetrics(): Array[CustomTaskMetric] = {
    paimonMetricsRegistry.buildSparkScanMetrics()
  }

  private def ensureNoFullScan(): Unit = {
    if (OptionUtils.readAllowFullScan()) {
      return
    }

    table match {
      case t: FileStoreTable if !t.partitionKeys().isEmpty =>
        val skippedFiles = paimonMetricsRegistry.buildSparkScanMetrics().collectFirst {
          case m: PaimonSkippedTableFilesTaskMetric => m.value
        }
        if (skippedFiles.contains(0)) {
          throw new RuntimeException("Full scan is not supported.")
        }
      case _ =>
    }
  }
}
