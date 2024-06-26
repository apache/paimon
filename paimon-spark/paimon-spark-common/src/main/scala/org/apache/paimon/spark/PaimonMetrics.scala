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
import org.apache.spark.sql.connector.metric.{CustomAvgMetric, CustomMetric, CustomSumMetric, CustomTaskMetric}

import java.text.DecimalFormat

object PaimonMetrics {

  val NUM_SPLITS = "numSplits"

  val SPLIT_SIZE = "splitSize"

  val AVG_SPLIT_SIZE = "avgSplitSize"

  val PLANNING_DURATION = "planningDuration"

  val SCANNED_MANIFESTS = "scannedManifests"

  val SKIPPED_TABLE_FILES = "skippedTableFiles"

  val RESULTED_TABLE_FILES = "resultedTableFiles"
}

// paimon's task metric
sealed trait PaimonTaskMetric extends CustomTaskMetric

case class PaimonNumSplitsTaskMetric(override val value: Long) extends PaimonTaskMetric {

  override def name(): String = PaimonMetrics.NUM_SPLITS

}

case class PaimonSplitSizeTaskMetric(override val value: Long) extends PaimonTaskMetric {

  override def name(): String = PaimonMetrics.SPLIT_SIZE

}

case class PaimonAvgSplitSizeTaskMetric(override val value: Long) extends PaimonTaskMetric {

  override def name(): String = PaimonMetrics.AVG_SPLIT_SIZE

}

// paimon's sum metric
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

case class PaimonNumSplitMetric() extends PaimonSumMetric {

  override def name(): String = PaimonMetrics.NUM_SPLITS

  override def description(): String = "number of splits read"

}

case class PaimonSplitSizeMetric() extends PaimonSumMetric {

  override def name(): String = PaimonMetrics.SPLIT_SIZE

  override def description(): String = "size of splits read"

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    PaimonUtils.bytesToString(aggregateTaskMetrics0(taskMetrics))
  }
}

// paimon's avg metric
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

case class PaimonAvgSplitSizeMetric() extends PaimonAvgMetric {

  override def name(): String = PaimonMetrics.AVG_SPLIT_SIZE

  override def description(): String = "avg size of splits read"

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    val average = aggregateTaskMetrics0(taskMetrics).round
    PaimonUtils.bytesToString(average)
  }

}

case class PaimonPlanningDurationMetric() extends PaimonSumMetric {
  override def name(): String = PaimonMetrics.PLANNING_DURATION
  override def description(): String = "planing duration (ms)"
}

case class PaimonPlanningDurationTaskMetric(value: Long) extends PaimonTaskMetric {
  override def name(): String = PaimonMetrics.PLANNING_DURATION
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
