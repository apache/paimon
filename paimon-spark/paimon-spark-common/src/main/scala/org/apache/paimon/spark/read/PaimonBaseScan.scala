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

package org.apache.paimon.spark.read

import org.apache.paimon.CoreOptions.BucketFunctionType
import org.apache.paimon.annotation.VisibleForTesting
import org.apache.paimon.spark._
import org.apache.paimon.spark.commands.BucketExpression.quote
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.spark.sources.PaimonMicroBatchStream
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.{BucketMode, DataTable, FileStoreTable, InnerTable}
import org.apache.paimon.table.source.{DataSplit, InnerTableScan, Split}

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, SupportsReportPartitioning}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream

import scala.collection.JavaConverters._

abstract class PaimonBaseScan(table: InnerTable)
  extends BaseScan
  with SupportsReportPartitioning
  with SQLConfHelper {

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

  def bucketedScanDisabled: Boolean

  @transient
  private lazy val extractBucketTransform: Option[Transform] = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val bucketSpec = fileStoreTable.bucketSpec()
        // todo introduce bucket transform for different bucket function type
        if (
          bucketSpec.getBucketMode != BucketMode.HASH_FIXED || coreOptions
            .bucketFunctionType() != BucketFunctionType.DEFAULT
        ) {
          None
        } else if (bucketSpec.getBucketKeys.size() > 1) {
          None
        } else {
          // Spark does not support bucket with several input attributes,
          // so we only support one bucket key case.
          assert(bucketSpec.getNumBuckets > 0)
          assert(bucketSpec.getBucketKeys.size() == 1)
          extractBucketNumber() match {
            case Some(num) =>
              val bucketKey = bucketSpec.getBucketKeys.get(0)
              if (requiredSchema.exists(f => conf.resolver(f.name, bucketKey))) {
                Some(Expressions.bucket(num, quote(bucketKey)))
              } else {
                None
              }

            case _ => None
          }
        }

      case _ => None
    }
  }

  /** Extract the bucket number from the splits only if all splits have the same totalBuckets number. */
  private def extractBucketNumber(): Option[Int] = {
    val splits = inputSplits
    if (splits.exists(!_.isInstanceOf[DataSplit])) {
      None
    } else {
      val deduplicated =
        splits.map(s => Option(s.asInstanceOf[DataSplit].totalBuckets())).toSeq.distinct

      deduplicated match {
        case Seq(Some(num)) => Some(num)
        case _ => None
      }
    }
  }

  protected def shouldDoBucketedScan: Boolean = {
    !bucketedScanDisabled && conf.v2BucketingEnabled && extractBucketTransform.isDefined
  }

  override def outputPartitioning: Partitioning = {
    extractBucketTransform
      .map(bucket => new KeyGroupedPartitioning(Array(bucket), inputPartitions.size))
      .getOrElse(new UnknownPartitioning(0))
  }

  override def getInputPartitions(splits: Array[Split]): Seq[PaimonInputPartition] = {
    if (!shouldDoBucketedScan || splits.exists(!_.isInstanceOf[DataSplit])) {
      return super.getInputPartitions(splits)
    }

    splits
      .map(_.asInstanceOf[DataSplit])
      .groupBy(_.bucket())
      .map {
        case (bucket, groupedSplits) =>
          PaimonBucketedInputPartition(groupedSplits, bucket)
      }
      .toSeq
  }
}
