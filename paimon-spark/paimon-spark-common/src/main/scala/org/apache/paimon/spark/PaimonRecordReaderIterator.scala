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
import org.apache.paimon.reader.{FileRecordIterator, RecordReader, ScoreRecordIterator, ScoreRecordReader}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.spark.schema.PaimonMetadataColumn.{PATH_AND_INDEX_META_COLUMNS, VECTOR_SEARCH_META_COLUMN_NAMES}
import org.apache.paimon.table.source.{ChainSplit, DataSplit, Split, Splits}
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{CloseableIterator, Preconditions, ProjectedRow}

import org.apache.spark.sql.PaimonUtils

import java.io.IOException
import java.util.concurrent.TimeUnit.NANOSECONDS

case class PaimonRecordReaderIterator(
    reader: RecordReader[PaimonInternalRow],
    metadataColumns: Seq[PaimonMetadataColumn],
    split: Split,
    readType: RowType = null)
  extends CloseableIterator[PaimonInternalRow] {

  private val partitionRow = metadataColumns
    .find(_.name == PaimonMetadataColumn.PARTITION_COLUMN)
    .map {
      column =>
        require(readType != null, "Partition metadata requires a read type")
        val fields = column.toPaimonDataField.`type`().asInstanceOf[RowType].getFieldNames
        ProjectedRow.from(readType.getFieldIndices(fields))
    }

  private lazy val bucketMetadata =
    if (metadataColumns.exists(_.name == PaimonMetadataColumn.BUCKET_COLUMN)) {
      // Inspect the underlying split only for metadata. The reader still receives the original
      // split so fallback routing and query authorization are preserved.
      Some(Splits.underlying(split) match {
        case dataSplit: DataSplit => dataSplit.bucket()
        case chainSplit: ChainSplit => chainSplit.bucket()
        case _ =>
          throw new RuntimeException(
            "There need be DataSplit or ChainSplit when bucket metadata is required")
      })
    } else {
      None
    }

  private val needMetadata = metadataColumns.nonEmpty
  private val needPathAndIndexMetadata =
    metadataColumns.exists(c => PATH_AND_INDEX_META_COLUMNS.contains(c.name))
  private val needVectorSearchMetadata = reader.isInstanceOf[ScoreRecordReader[_]] &&
    metadataColumns.exists(c => VECTOR_SEARCH_META_COLUMN_NAMES.contains(c.name))

  Preconditions.checkArgument(
    !needVectorSearchMetadata ||
      metadataColumns.forall(c => VECTOR_SEARCH_META_COLUMN_NAMES.contains(c.name)))

  private val metadataRow: GenericRow =
    GenericRow.of(Array.fill(metadataColumns.size)(null.asInstanceOf[AnyRef]): _*)
  private val joinedRow: JoinedRow = JoinedRow.join(null, metadataRow)

  private var lastFilePath: Path = _
  // Initialize before readBatch() so the first batch's elapsed time is not reset to zero.
  private var readBatchTimeNs: Long = 0L
  private var currentIterator: RecordReader.RecordIterator[PaimonInternalRow] = readBatch()
  private var advanced = false
  private var currentResult: PaimonInternalRow = _

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
        currentResult = null
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
          PaimonUtils.setInputFileName(fileRecordIterator.filePath().toString)
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
            if (needVectorSearchMetadata) {
              updateScoreMetadata(
                currentIterator.asInstanceOf[ScoreRecordIterator[PaimonInternalRow]])
              currentResult = joinedRow.replace(dataRow, metadataRow)
            } else if (needMetadata) {
              updateMetadataRow(currentIterator, dataRow)
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

  private def updateMetadataRow(
      iterator: RecordReader.RecordIterator[PaimonInternalRow],
      dataRow: PaimonInternalRow): Unit = {
    metadataColumns.zipWithIndex.foreach {
      case (metadataColumn, index) =>
        metadataColumn.name match {
          case PaimonMetadataColumn.ROW_INDEX_COLUMN =>
            metadataRow.setField(
              index,
              iterator.asInstanceOf[FileRecordIterator[PaimonInternalRow]].returnedPosition())
          case PaimonMetadataColumn.FILE_PATH_COLUMN =>
            metadataRow.setField(index, BinaryString.fromString(lastFilePath.toString))
          case PaimonMetadataColumn.PARTITION_COLUMN =>
            // Core has already applied chain logical partitions, row filters and column masks.
            metadataRow.setField(index, partitionRow.get.replaceRow(dataRow))
          case PaimonMetadataColumn.BUCKET_COLUMN =>
            metadataRow.setField(index, bucketMetadata.get)
        }
    }
  }

  private def updateScoreMetadata(
      fileRecordIterator: ScoreRecordIterator[PaimonInternalRow]): Unit = {
    metadataColumns.zipWithIndex.foreach {
      case (metadataColumn, index) =>
        metadataColumn.name match {
          case PaimonMetadataColumn.ROW_ID_COLUMN =>
            metadataRow.setField(index, fileRecordIterator.returnedRowId())
          case PaimonMetadataColumn.SEARCH_SCORE_COLUMN =>
            metadataRow.setField(index, fileRecordIterator.returnedScore())
        }
    }
  }
}
