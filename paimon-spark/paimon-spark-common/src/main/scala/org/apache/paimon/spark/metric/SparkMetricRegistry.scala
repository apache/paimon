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

package org.apache.paimon.spark.metric

import org.apache.paimon.metrics.{Gauge, Metric, MetricGroup, MetricRegistry}
import org.apache.paimon.operation.metrics.{CommitMetrics, ScanMetrics, WriterBufferMetric}
import org.apache.paimon.spark._

import org.apache.spark.metrics.source.PaimonMetricsSource
import org.apache.spark.sql.connector.metric.CustomTaskMetric

import java.util.{Map => JMap}

import scala.collection.mutable

case class SparkMetricRegistry() extends MetricRegistry {

  private val metricGroups = mutable.Map.empty[String, MetricGroup]

  override def createMetricGroup(
      groupName: String,
      variables: JMap[String, String]): MetricGroup = {
    val metricGroup = if (groupName == ScanMetrics.GROUP_NAME) {
      new ScanMetricGroup(variables)
    } else {
      new SparkMetricGroup(groupName, variables, PaimonMetricsSource.metricRegistry)
    }
    metricGroups.put(groupName, metricGroup)
    metricGroup
  }

  // Updates the current scan group with remaining files and the total runtime-pruned file count.
  def updateRuntimeFileCounts(remaining: Long, skipped: Long): Unit = {
    metricGroups.get(ScanMetrics.GROUP_NAME).foreach {
      case group: ScanMetricGroup => group.updateRuntimeFileCounts(remaining, skipped)
      case _ =>
    }
  }

  // Shares runtime-pruned file counts between Spark UI and JMX.
  private class ScanMetricGroup(variables: JMap[String, String])
    extends SparkMetricGroup(
      ScanMetrics.GROUP_NAME,
      variables,
      PaimonMetricsSource.metricRegistry) {

    // Publish one immutable pair to the JMX sampling thread. Each scan group owns its state,
    // so updating an older scan cannot overwrite the gauges registered by a newer scan.
    @volatile private var runtimeFileCounts: Option[(Long, Long)] = None

    // Replaces runtime totals; the original gauges still account for static pruning.
    def updateRuntimeFileCounts(remaining: Long, skipped: Long): Unit = {
      runtimeFileCounts = Some((remaining, skipped))
    }

    // Shares adjusted file counts with Spark UI and JMX, using core values before runtime pruning.
    override def gauge[T](name: String, original: Gauge[T]): Gauge[T] = {
      val effective: Gauge[T] = name match {
        case ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES =>
          () => runtimeFileCounts.map(_._1.asInstanceOf[T]).getOrElse(original.getValue)
        case ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES =>
          () =>
            (original.getValue.asInstanceOf[Long] + runtimeFileCounts.map(_._2).getOrElse(0L))
              .asInstanceOf[T]
        case _ => original
      }
      super.gauge(name, effective)
    }
  }

  def buildSparkScanMetrics(): Array[CustomTaskMetric] = {
    metricGroups.get(ScanMetrics.GROUP_NAME) match {
      case Some(group) =>
        val metrics = group.getMetrics
        Array(
          PaimonPlanningDurationTaskMetric(gauge[Long](metrics, ScanMetrics.LAST_SCAN_DURATION)),
          PaimonScannedSnapshotIdTaskMetric(
            gauge[Long](metrics, ScanMetrics.LAST_SCANNED_SNAPSHOT_ID)),
          PaimonScannedManifestsTaskMetric(
            gauge[Long](metrics, ScanMetrics.LAST_SCANNED_MANIFESTS)),
          PaimonSkippedTableFilesTaskMetric(
            gauge[Long](metrics, ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES)),
          PaimonResultedTableFilesTaskMetric(
            gauge[Long](metrics, ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES))
        )
      case None =>
        Array.empty
    }
  }

  def buildSparkWriteMetrics(): Array[CustomTaskMetric] = {
    metricGroups.get(WriterBufferMetric.GROUP_NAME) match {
      case Some(group) =>
        val metrics = group.getMetrics
        Array(
          PaimonNumWritersTaskMetric(gauge[Int](metrics, WriterBufferMetric.NUM_WRITERS))
        )
      case None =>
        Array.empty
    }
  }

  def buildSparkCommitMetrics(): Array[CustomTaskMetric] = {
    metricGroups.get(CommitMetrics.GROUP_NAME) match {
      case Some(group) =>
        val metrics = group.getMetrics
        Array(
          PaimonCommitDurationTaskMetric(gauge[Long](metrics, CommitMetrics.LAST_COMMIT_DURATION)),
          PaimonAddedTableFilesTaskMetric(
            gauge[Long](metrics, CommitMetrics.LAST_TABLE_FILES_ADDED)),
          PaimonDeletedTableFilesTaskMetric(
            gauge[Long](metrics, CommitMetrics.LAST_TABLE_FILES_DELETED)),
          PaimonInsertedRecordsTaskMetric(
            gauge[Long](metrics, CommitMetrics.LAST_DELTA_RECORDS_APPENDED)),
          PaimonAppendedChangelogFilesTaskMetric(
            gauge[Long](metrics, CommitMetrics.LAST_CHANGELOG_FILES_APPENDED)),
          PaimonPartitionsWrittenTaskMetric(
            gauge[Long](metrics, CommitMetrics.LAST_PARTITIONS_WRITTEN)),
          PaimonBucketsWrittenTaskMetric(gauge[Long](metrics, CommitMetrics.LAST_BUCKETS_WRITTEN))
        )
      case None =>
        Array.empty
    }
  }

  private def gauge[T](metrics: JMap[String, Metric], key: String): T = {
    metrics.get(key) match {
      case null =>
        throw new NoSuchElementException(s"Metric key '$key' not found")
      case g: Gauge[_] =>
        g.getValue.asInstanceOf[T]
      case m =>
        throw new ClassCastException(s"Expected Gauge, but got ${m.getClass.getName}")
    }
  }
}
