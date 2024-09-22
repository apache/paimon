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
import org.apache.paimon.reader.RecordReader
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.source.{DataSplit, Split}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader

import java.io.IOException

import scala.collection.JavaConverters._

case class PaimonPartitionReader(
    readFunc: Split => RecordReader[PaimonInternalRow],
    partition: PaimonInputPartition,
    row: SparkInternalRow,
    metadataColumns: Seq[PaimonMetadataColumn]
) extends PartitionReader[InternalRow] {

  private val splits: Iterator[Split] = partition.splits.toIterator
  private var currentRecordReader: PaimonRecordReaderIterator = readSplit()
  private var advanced = false
  private var currentRow: PaimonInternalRow = _

  override def next(): Boolean = {
    if (currentRecordReader == null) {
      false
    } else {
      advanceIfNeeded()
      currentRow != null
    }
  }

  override def get(): InternalRow = {
    if (!next) {
      null
    } else {
      advanced = false
      row.replace(currentRow)
    }
  }

  private def advanceIfNeeded(): Unit = {
    if (!advanced) {
      advanced = true
      var stop = false
      while (!stop) {
        if (currentRecordReader.hasNext) {
          currentRow = currentRecordReader.next()
        } else {
          currentRow = null
        }

        if (currentRow != null) {
          stop = true
        } else {
          currentRecordReader.close()
          currentRecordReader = readSplit()
          if (currentRecordReader == null) {
            stop = true
          }
        }
      }
    }
  }

  private def readSplit(): PaimonRecordReaderIterator = {
    if (splits.hasNext) {
      val split = splits.next();
      val reader = readFunc(split)
      PaimonRecordReaderIterator(reader, metadataColumns, split)
    } else {
      null
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val dataSplits = partition.splits.collect { case ds: DataSplit => ds }
    val numSplits = dataSplits.length
    val paimonMetricsValues: Array[CustomTaskMetric] = if (dataSplits.nonEmpty) {
      val splitSize = dataSplits.map(_.dataFiles().asScala.map(_.fileSize).sum).sum
      Array(
        PaimonNumSplitsTaskMetric(numSplits),
        PaimonSplitSizeTaskMetric(splitSize),
        PaimonAvgSplitSizeTaskMetric(splitSize / numSplits)
      )
    } else {
      Array.empty[CustomTaskMetric]
    }
    super.currentMetricsValues() ++ paimonMetricsValues
  }

  override def close(): Unit = {
    try {
      if (currentRecordReader != null) {
        currentRecordReader.close()
      }
    } catch {
      case e: Exception =>
        throw new IOException(e)
    }
  }
}
