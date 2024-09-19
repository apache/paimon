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

import org.apache.paimon.metrics.{Gauge, MetricGroup, MetricGroupImpl, MetricRegistry}
import org.apache.paimon.operation.metrics.ScanMetrics
import org.apache.paimon.spark.{PaimonPlanningDurationTaskMetric, PaimonResultedTableFilesTaskMetric, PaimonScannedManifestsTaskMetric, PaimonSkippedTableFilesTaskMetric}

import org.apache.spark.sql.connector.metric.CustomTaskMetric

import java.util

import scala.collection.mutable

case class SparkMetricRegistry() extends MetricRegistry {

  private val metricGroups = mutable.Map.empty[String, MetricGroup]

  override protected def createMetricGroup(
      groupName: String,
      variables: util.Map[String, String]): MetricGroup = {
    val metricGroup = new MetricGroupImpl(groupName, variables)
    metricGroups.put(groupName, metricGroup)
    metricGroup
  }

  def buildSparkScanMetrics(): Array[CustomTaskMetric] = {
    metricGroups.get(ScanMetrics.GROUP_NAME) match {
      case Some(group) =>
        val metrics = group.getMetrics
        def gaugeLong(key: String): Long = metrics.get(key).asInstanceOf[Gauge[Long]].getValue
        Array(
          PaimonPlanningDurationTaskMetric(gaugeLong(ScanMetrics.LAST_SCAN_DURATION)),
          PaimonScannedManifestsTaskMetric(gaugeLong(ScanMetrics.LAST_SCANNED_MANIFESTS)),
          PaimonSkippedTableFilesTaskMetric(gaugeLong(ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES)),
          PaimonResultedTableFilesTaskMetric(gaugeLong(ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES))
        )
      case None =>
        Array.empty
    }
  }
}
