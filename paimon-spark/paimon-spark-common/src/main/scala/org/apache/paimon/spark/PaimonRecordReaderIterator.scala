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

import org.apache.paimon.data.{BinaryString, GenericRow, InternalRow => PaimonInternalRow, JoinedRow}
import org.apache.paimon.fs.Path
import org.apache.paimon.reader.{FileRecordIterator, RecordReader}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.spark.schema.PaimonMetadataColumn.{PARTITION_AND_BUCKET_META_COLUMNS, PATH_AND_INDEX_META_COLUMNS}
import org.apache.paimon.table.source.{DataSplit, Split}
import org.apache.paimon.utils.CloseableIterator

import org.apache.spark.sql.PaimonUtils

import java.io.IOException
import java.util.concurrent.TimeUnit.NANOSECONDS

case class PaimonRecordReaderIterator(
    reader: RecordReader[PaimonInternalRow],
    metadataColumns: Seq[PaimonMetadataColumn],
    split: Split)
  extends CloseableIterator[PaimonInternalRow] {

  if (
    metadataColumns.exists(c => PARTITION_AND_BUCKET_META_COLUMNS.contains(c.name)) && !split
      .isInstanceOf[DataSplit]
  ) {
    throw new RuntimeException(
      "There need be DataSplit when path and index metadata columns are required")
  }

  private val needMetadata = metadataColumns.nonEmpty
  private val needPathAndIndexMetadata =
    metadataColumns.exists(c => PATH_AND_INDEX_META_COLUMNS.contains(c.name))

  private val metadataRow: GenericRow =
    GenericRow.of(Array.fill(metadataColumns.size)(null.asInstanceOf[AnyRef]): _*)
  private val joinedRow: JoinedRow = JoinedRow.join(null, metadataRow)

  private var lastFilePath: Path = _
  private var currentIterator: RecordReader.RecordIterator[PaimonInternalRow] = readBatch()
  private var advanced = false
  private var currentResult: PaimonInternalRow = _
  private var readBatchTimeNs: Long = 0L

  override def hasNext: Boolean = {
    if (currentIterator == null) {
      false
    } else {
      advanceIfNeeded()
      currentResult != null
    }
  }

  override def next(): PaimonInternalRow = {
    if (!hasNext) {
      null
    } else {
      advanced = false
      currentResult
    }
  }

  override def close(): Unit = {
    try {
      if (currentIterator != null) {
        currentIterator.releaseBatch()
        currentResult == null
      }
    } finally {
      reader.close()
      PaimonUtils.unsetInputFileName()
    }
  }

  private def readBatch(): RecordReader.RecordIterator[PaimonInternalRow] = {
    val startTimeNs = System.nanoTime()

    val iter = reader.readBatch()
    iter match {
      case fileRecordIterator: FileRecordIterator[_] =>
        if (lastFilePath != fileRecordIterator.filePath()) {
          PaimonUtils.setInputFileName(fileRecordIterator.filePath().toUri.toString)
          lastFilePath = fileRecordIterator.filePath()
        }
      case i =>
        if (i != null && needPathAndIndexMetadata) {
          throw new RuntimeException(
            "There need be FileRecordIterator when metadata columns are required. " +
              "Only append table or deletion vector table support querying metadata columns.")
        }
    }
    readBatchTimeNs += System.nanoTime() - startTimeNs
    iter
  }

  def readBatchTimeMs: Long = {
    NANOSECONDS.toMillis(readBatchTimeNs)
  }

  private def advanceIfNeeded(): Unit = {
    if (!advanced) {
      advanced = true
      try {
        var stop = false
        while (!stop) {
          val dataRow = currentIterator.next()
          if (dataRow != null) {
            if (needMetadata) {
              updateMetadataRow(currentIterator.asInstanceOf[FileRecordIterator[PaimonInternalRow]])
              currentResult = joinedRow.replace(dataRow, metadataRow)
            } else {
              currentResult = dataRow
            }
          } else {
            currentResult = null
          }

          if (currentResult != null) {
            stop = true
          } else {
            currentIterator.releaseBatch()
            currentIterator = null
            currentIterator = readBatch()
            if (currentIterator == null) {
              stop = true
            }
          }
        }
      } catch {
        case e: IOException =>
          throw new RuntimeException(e)
      }
    }
  }

  private def updateMetadataRow(fileRecordIterator: FileRecordIterator[PaimonInternalRow]): Unit = {
    metadataColumns.zipWithIndex.foreach {
      case (metadataColumn, index) =>
        metadataColumn.name match {
          case PaimonMetadataColumn.ROW_INDEX_COLUMN =>
            metadataRow.setField(index, fileRecordIterator.returnedPosition())
          case PaimonMetadataColumn.FILE_PATH_COLUMN =>
            metadataRow.setField(index, BinaryString.fromString(lastFilePath.toUri.toString))
          case PaimonMetadataColumn.PARTITION_COLUMN =>
            metadataRow.setField(index, split.asInstanceOf[DataSplit].partition())
          case PaimonMetadataColumn.BUCKET_COLUMN =>
            metadataRow.setField(index, split.asInstanceOf[DataSplit].bucket())
        }
    }
  }
}
