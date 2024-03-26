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

import org.apache.paimon.data.{InternalRow => PaimonInternalRow}
import org.apache.paimon.reader.{RecordReader, RecordReaderIterator}
import org.apache.paimon.table.source.{DataSplit, Split}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader

import java.io.IOException

import scala.collection.JavaConverters._

case class PaimonPartitionReader(
    readFunc: Split => RecordReader[PaimonInternalRow],
    partition: SparkInputPartition,
    row: SparkInternalRow
) extends PartitionReader[InternalRow] {

  private lazy val split: Split = partition.split

  private lazy val iterator = {
    val reader = readFunc(split)
    PaimonRecordReaderIterator(reader)
  }

  override def next(): Boolean = {
    if (iterator.hasNext) {
      row.replace(iterator.next())
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    row
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val paimonMetricsValues: Array[CustomTaskMetric] = split match {
      case dataSplit: DataSplit =>
        val splitSize = dataSplit.dataFiles().asScala.map(_.fileSize).sum
        Array(
          PaimonNumSplitsTaskMetric(1L),
          PaimonSplitSizeTaskMetric(splitSize),
          PaimonAvgSplitSizeTaskMetric(splitSize)
        )

      case _ =>
        Array.empty[CustomTaskMetric]
    }
    super.currentMetricsValues() ++ paimonMetricsValues
  }

  override def close(): Unit = {
    try {
      iterator.close()
    } catch {
      case e: Exception =>
        throw new IOException(e)
    }
  }
}
