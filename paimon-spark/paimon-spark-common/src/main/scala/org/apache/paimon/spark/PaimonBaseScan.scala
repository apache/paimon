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

import org.apache.paimon.spark.sources.PaimonMicroBatchStream
import org.apache.paimon.table.{DataTable, Table}
import org.apache.paimon.table.source.{ReadBuilder, Split}

import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.{Batch, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

abstract class PaimonBaseScan(table: Table, readBuilder: ReadBuilder, desc: String)
  extends Scan
  with SupportsReportStatistics {

  protected var splits: Array[Split] = _

  override def description(): String = desc

  override def readSchema(): StructType = {
    SparkTypeUtils.fromPaimonRowType(readBuilder.readType())
  }

  override def toBatch: Batch = {
    PaimonBatch(getSplits, readBuilder)
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new PaimonMicroBatchStream(table.asInstanceOf[DataTable], readBuilder, checkpointLocation)
  }

  override def estimateStatistics(): Statistics = {
    PaimonStatistics(this)
  }

  def getSplits: Array[Split] = {
    if (splits == null) {
      splits = readBuilder.newScan().plan().splits().asScala.toArray
    }
    splits
  }

  override def supportedCustomMetrics: Array[CustomMetric] = {
    Array(
      PaimonNumSplitMetric(),
      PaimonSplitSizeMetric(),
      PaimonAvgSplitSizeMetric()
    )
  }
}
