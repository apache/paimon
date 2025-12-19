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
import org.apache.paimon.spark.scan.BaseScan
import org.apache.paimon.spark.sources.PaimonMicroBatchStream
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.{DataTable, FileStoreTable, InnerTable}
import org.apache.paimon.table.source.{InnerTableScan, Split}

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream

import scala.collection.JavaConverters._

abstract class PaimonBaseScan(table: InnerTable) extends BaseScan with SQLConfHelper {

  private lazy val paimonMetricsRegistry: SparkMetricRegistry = SparkMetricRegistry()

  // May recalculate the splits after executing runtime filter push down.
  protected var _inputSplits: Array[Split] = _
  protected var _inputPartitions: Seq[PaimonInputPartition] = _

  @VisibleForTesting
  def inputSplits: Array[Split] = {
    if (_inputSplits == null) {
      _inputSplits = readBuilder
        .newScan()
        .asInstanceOf[InnerTableScan]
        .withMetricRegistry(paimonMetricsRegistry)
        .plan()
        .splits()
        .asScala
        .toArray
    }
    _inputSplits
  }

  final override def inputPartitions: Seq[PaimonInputPartition] = {
    if (_inputPartitions == null) {
      _inputPartitions = getInputPartitions(inputSplits)
    }
    _inputPartitions
  }

  override def toBatch: Batch = {
    ensureNoFullScan()
    super.toBatch
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new PaimonMicroBatchStream(table.asInstanceOf[DataTable], readBuilder, checkpointLocation)
  }

  override def supportedCustomMetrics: Array[CustomMetric] = {
    super.supportedCustomMetrics ++
      Array(
        PaimonPlanningDurationMetric(),
        PaimonScannedSnapshotIdMetric(),
        PaimonScannedManifestsMetric(),
        PaimonSkippedTableFilesMetric()
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
