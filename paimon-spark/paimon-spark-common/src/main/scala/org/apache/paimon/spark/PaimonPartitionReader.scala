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
import org.apache.paimon.disk.IOManager
import org.apache.paimon.spark.SparkUtils.createIOManager
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.source.{DataSplit, ReadBuilder, Split}
import org.apache.paimon.types.RowType

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader

import javax.annotation.Nullable

import java.util.{ArrayList => JList}

import scala.collection.JavaConverters._

case class PaimonPartitionReader(
    readBuilder: ReadBuilder,
    partition: PaimonInputPartition,
    metadataColumns: Seq[PaimonMetadataColumn]
) extends PartitionReader[InternalRow] {

  private val splits: Iterator[Split] = partition.splits.toIterator
  @Nullable private var currentRecordReader = readSplit()
  private var advanced = false
  private var currentRow: PaimonInternalRow = _
  private val ioManager: IOManager = createIOManager()
  private val sparkRow: SparkInternalRow = {
    val dataFields = new JList(readBuilder.readType().getFields)
    dataFields.addAll(metadataColumns.map(_.toPaimonDataField).asJava)
    val rowType = new RowType(dataFields)
    SparkInternalRow.create(rowType)
  }

  private lazy val read = readBuilder.newRead().withIOManager(ioManager)

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
      sparkRow.replace(currentRow)
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
      PaimonRecordReaderIterator(read.createReader(split), metadataColumns, split)
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
    } finally {
      ioManager.close()
    }
  }
}
