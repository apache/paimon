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
import org.apache.paimon.utils.CloseableIterator

import org.apache.spark.sql.PaimonUtils

import java.io.IOException

case class PaimonRecordReaderIterator(
    reader: RecordReader[PaimonInternalRow],
    metadataColumns: Seq[PaimonMetadataColumn])
  extends CloseableIterator[PaimonInternalRow] {

  private var lastFilePath: Path = _
  private var isFileRecordIterator: Boolean = false
  private var currentIterator: RecordReader.RecordIterator[PaimonInternalRow] = readBatch()
  private var advanced = false
  private var currentResult: PaimonInternalRow = _

  private val needMetadata = metadataColumns.nonEmpty
  private val metadataRow: GenericRow =
    GenericRow.of(Array.fill(metadataColumns.size)(null.asInstanceOf[AnyRef]): _*)
  private val joinedRow: JoinedRow = JoinedRow.join(null, metadataRow)

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
    val iter = reader.readBatch()
    iter match {
      case fileRecordIterator: FileRecordIterator[_] =>
        isFileRecordIterator = true
        if (lastFilePath != fileRecordIterator.filePath()) {
          PaimonUtils.setInputFileName(fileRecordIterator.filePath().toUri.toString)
          lastFilePath = fileRecordIterator.filePath()
        }
      case _ =>
        isFileRecordIterator = false
    }
    iter
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
              if (!isFileRecordIterator) {
                throw new RuntimeException(
                  "There need be FileRecoredIterator when metadata columns are required.")
              }
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
          case PaimonMetadataColumn.ROW_INDEX.name =>
            metadataRow.setField(index, fileRecordIterator.returnedPosition())
          case PaimonMetadataColumn.FILE_PATH.name =>
            metadataRow.setField(index, BinaryString.fromString(lastFilePath.toUri.toString))
        }
    }
  }
}
