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

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}

object PaimonMetrics {
  // scan metrics
  val NUM_SPLITS = "numSplits"
  val PARTITION_SIZE = "partitionSize"
  val READ_BATCH_TIME = "readBatchTime"
  val PLANNING_DURATION = "planningDuration"
  val SCANNED_SNAPSHOT_ID = "scannedSnapshotId"
  val SCANNED_MANIFESTS = "scannedManifests"
  val SKIPPED_TABLE_FILES = "skippedTableFiles"
  val RESULTED_TABLE_FILES = "resultedTableFiles"

  // write metrics
  val NUM_WRITERS = "numWriters"

  // commit metrics
  val COMMIT_DURATION = "commitDuration"
  val APPENDED_TABLE_FILES = "appendedTableFiles"
  val APPENDED_RECORDS = "appendedRecords"
  val APPENDED_CHANGELOG_FILES = "appendedChangelogFiles"
  val PARTITIONS_WRITTEN = "partitionsWritten"
  val BUCKETS_WRITTEN = "bucketsWritten"
}

// Base custom task metrics
sealed trait PaimonTaskMetric extends CustomTaskMetric

// Base custom metrics
trait PaimonCustomMetric extends CustomMetric {
  def stringValue(l: Long): String = l.toString
}

trait PaimonSizeMetric extends PaimonCustomMetric {
  override def stringValue(l: Long): String = PaimonUtils.bytesToString(l)
}

trait PaimonTimingMetric extends PaimonCustomMetric {
  override def stringValue(l: Long): String = PaimonUtils.msDurationToString(l)
}

sealed trait PaimonSumMetric extends PaimonCustomMetric {
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    stringValue(taskMetrics.sum)
  }
}
sealed trait PaimonSizeSumMetric extends PaimonSumMetric with PaimonSizeMetric
sealed trait PaimonTimingSumMetric extends PaimonSumMetric with PaimonTimingMetric

sealed trait PaimonSummaryMetric extends PaimonCustomMetric {
  def description0(): String

  override def description(): String = s"${description0()} total (min, avg, med, max)"

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    if (taskMetrics.length == 0) {
      s"None"
    } else {
      val sorted = taskMetrics.sorted
      val total = sorted.sum
      val min = sorted.head
      val avg = total / sorted.length
      val med = sorted.apply(sorted.length / 2)
      val max = sorted.last
      s"\n${stringValue(total)} (${stringValue(min)}, ${stringValue(avg)}, ${stringValue(med)}, ${stringValue(max)})"
    }
  }
}
sealed trait PaimonSizeSummaryMetric extends PaimonSummaryMetric with PaimonSizeMetric
sealed trait PaimonTimingSummaryMetric extends PaimonSummaryMetric with PaimonTimingMetric

// Scan metrics
case class PaimonNumSplitMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.NUM_SPLITS
  override def description(): String = "number of splits read"
}

case class PaimonNumSplitsTaskMetric(override val value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.NUM_SPLITS
}

case class PaimonPartitionSizeMetric() extends PaimonSizeSummaryMetric {
  override def name(): String = PaimonMetrics.PARTITION_SIZE
  override def description0(): String = "partition size"
}

case class PaimonPartitionSizeTaskMetric(override val value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.PARTITION_SIZE
}

case class PaimonReadBatchTimeMetric() extends PaimonTimingSummaryMetric {
  override def name(): String = PaimonMetrics.READ_BATCH_TIME
  override def description0(): String = "read batch time"
}

case class PaimonReadBatchTimeTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.READ_BATCH_TIME
}

case class PaimonPlanningDurationMetric() extends PaimonTimingSumMetric {
  override def name(): String = PaimonMetrics.PLANNING_DURATION
  override def description(): String = "planing duration"
}

case class PaimonPlanningDurationTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.PLANNING_DURATION
}

case class PaimonScannedSnapshotIdMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.SCANNED_SNAPSHOT_ID
  override def description(): String = "scanned snapshot id"
}

case class PaimonScannedSnapshotIdTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.SCANNED_SNAPSHOT_ID
}

case class PaimonScannedManifestsMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.SCANNED_MANIFESTS
  override def description(): String = "number of scanned manifests"
}

case class PaimonScannedManifestsTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.SCANNED_MANIFESTS
}

case class PaimonSkippedTableFilesMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.SKIPPED_TABLE_FILES
  override def description(): String = "number of skipped table files"
}

case class PaimonSkippedTableFilesTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.SKIPPED_TABLE_FILES
}

case class PaimonResultedTableFilesMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.RESULTED_TABLE_FILES
  override def description(): String = "number of resulted table files"
}

case class PaimonResultedTableFilesTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.RESULTED_TABLE_FILES
}

// Write metrics
case class PaimonNumWritersMetric() extends PaimonSummaryMetric {
  override def name(): String = PaimonMetrics.NUM_WRITERS
  override def description0(): String = "number of writers"
}

case class PaimonNumWritersTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.NUM_WRITERS
}

// Commit metrics
case class PaimonCommitDurationMetric() extends PaimonTimingSumMetric {
  override def name(): String = PaimonMetrics.COMMIT_DURATION
  override def description(): String = "commit duration"
}

case class PaimonCommitDurationTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.COMMIT_DURATION
}

case class PaimonAppendedTableFilesMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.APPENDED_TABLE_FILES
  override def description(): String = "number of appended table files"
}

case class PaimonAppendedTableFilesTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.APPENDED_TABLE_FILES
}

case class PaimonAppendedRecordsMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.APPENDED_RECORDS
  override def description(): String = "number of appended records"
}

case class PaimonAppendedRecordsTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.APPENDED_RECORDS
}

case class PaimonAppendedChangelogFilesMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.APPENDED_CHANGELOG_FILES
  override def description(): String = "number of appended changelog files"
}

case class PaimonAppendedChangelogFilesTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.APPENDED_CHANGELOG_FILES
}

case class PaimonPartitionsWrittenMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.PARTITIONS_WRITTEN
  override def description(): String = "number of partitions written"
}

case class PaimonPartitionsWrittenTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.PARTITIONS_WRITTEN
}

case class PaimonBucketsWrittenMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.BUCKETS_WRITTEN
  override def description(): String = "number of buckets written"
}

case class PaimonBucketsWrittenTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.BUCKETS_WRITTEN
}
