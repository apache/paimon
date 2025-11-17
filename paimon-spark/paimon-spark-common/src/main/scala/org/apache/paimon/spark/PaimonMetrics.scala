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
import org.apache.spark.sql.connector.metric.{CustomAvgMetric, CustomSumMetric, CustomTaskMetric}

import java.text.DecimalFormat

object PaimonMetrics {
  // scan metrics
  val NUM_SPLITS = "numSplits"
  val SPLIT_SIZE = "splitSize"
  val AVG_SPLIT_SIZE = "avgSplitSize"
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
sealed trait PaimonSumMetric extends CustomSumMetric {
  protected def aggregateTaskMetrics0(taskMetrics: Array[Long]): Long = {
    var sum: Long = 0L
    for (taskMetric <- taskMetrics) {
      sum += taskMetric
    }
    sum
  }

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    String.valueOf(aggregateTaskMetrics0(taskMetrics))
  }
}

sealed trait PaimonAvgMetric extends CustomAvgMetric {
  protected def aggregateTaskMetrics0(taskMetrics: Array[Long]): Double = {
    if (taskMetrics.length > 0) {
      var sum = 0L
      for (taskMetric <- taskMetrics) {
        sum += taskMetric
      }
      sum.toDouble / taskMetrics.length
    } else {
      0d
    }
  }

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    val average = aggregateTaskMetrics0(taskMetrics)
    new DecimalFormat("#0.000").format(average)
  }
}

sealed trait PaimonMinMaxMetric extends CustomAvgMetric {
  def description0(): String

  override def description(): String = s"${description0()} total (min, med, max)"

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    val total = taskMetrics.sum
    val min = taskMetrics.min
    val med = taskMetrics.sorted.apply(taskMetrics.length / 2)
    val max = taskMetrics.max
    s"$total ($min, $med, $max)"
  }
}

// Scan metrics
case class PaimonNumSplitMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.NUM_SPLITS
  override def description(): String = "number of splits read"
}

case class PaimonNumSplitsTaskMetric(override val value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.NUM_SPLITS
}

case class PaimonSplitSizeMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.SPLIT_SIZE
  override def description(): String = "size of splits read"
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    PaimonUtils.bytesToString(aggregateTaskMetrics0(taskMetrics))
  }
}

case class PaimonSplitSizeTaskMetric(override val value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.SPLIT_SIZE
}

case class PaimonAvgSplitSizeMetric() extends PaimonAvgMetric {
  override def name(): String = PaimonMetrics.AVG_SPLIT_SIZE
  override def description(): String = "avg size of splits read"
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    val average = aggregateTaskMetrics0(taskMetrics).round
    PaimonUtils.bytesToString(average)
  }
}

case class PaimonAvgSplitSizeTaskMetric(override val value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.AVG_SPLIT_SIZE
}

case class PaimonPlanningDurationMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.PLANNING_DURATION
  override def description(): String = "planing duration (ms)"
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
case class PaimonNumWritersMetric() extends PaimonMinMaxMetric {
  override def name(): String = PaimonMetrics.NUM_WRITERS
  override def description0(): String = "number of writers"
}

case class PaimonNumWritersTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.NUM_WRITERS
}

// Commit metrics
case class PaimonCommitDurationMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.COMMIT_DURATION
  override def description(): String = "commit duration (ms)"
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
