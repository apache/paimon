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

package org.apache.paimon.spark.write

import org.apache.paimon.casting.FallbackMappingRow
import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.data.{BinaryRow, BlobPlaceholder, GenericRow, InternalRow}
import org.apache.paimon.data.serializer.InternalSerializers
import org.apache.paimon.disk.IOManager
import org.apache.paimon.format.blob.BlobFileFormat.isBlobFile
import org.apache.paimon.io.{CompactIncrement, DataIncrement}
import org.apache.paimon.operation.AbstractFileStoreWrite
import org.apache.paimon.spark.SparkUtils
import org.apache.paimon.spark.util.SparkRowUtils
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage, CommitMessageImpl, TableWriteImpl}
import org.apache.paimon.types.RowType
import org.apache.paimon.types.VectorType.isVectorStoreFile
import org.apache.paimon.utils.RecordWriter
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class DataEvolutionTableDataWrite(
    writeBuilder: BatchWriteBuilder,
    writeType: RowType,
    firstRowIdToPartitionMap: mutable.HashMap[Long, (Array[Byte], Long)],
    catalogContext: CatalogContext,
    rawBlobPlaceholderMarkerIndexes: Map[Int, Int])
  extends InnerTableV1DataWrite {

  private var currentWriter: PerFileWriter = _
  private val ioManager: IOManager = SparkUtils.createIOManager
  private val rowIdIndex = writeType.getFieldCount
  private val firstRowIdIndex = rowIdIndex + 1
  private val commitMessages = ListBuffer[CommitMessageImpl]()

  private val toPaimonRow = {
    SparkRowUtils.toPaimonRow(writeType, -1, catalogContext)
  }
  private lazy val rowSerializer = InternalSerializers.create(writeType)
  private val rawBlobFallbackFields = rawBlobPlaceholderMarkerIndexes.toSeq.sortBy(_._1).toArray
  private val rawBlobFallbackFieldIndexes = rawBlobFallbackFields.map(_._1)
  private val rawBlobFallbackMappings = {
    val mappings = Array.fill(writeType.getFieldCount)(-1)
    rawBlobFallbackFields.zipWithIndex.foreach {
      case ((fieldIndex, _), fallbackIndex) =>
        mappings(fieldIndex) = fallbackIndex
    }
    mappings
  }
  private val rawBlobFallbackMarkerIndexes = rawBlobFallbackFields.map(_._2)
  private val rawBlobFallbackRow = new GenericRow(rawBlobFallbackMarkerIndexes.length)
  private val rawBlobFallbackMappingRow = new FallbackMappingRow(rawBlobFallbackMappings)

  def write(row: Row): Unit = {
    val firstRowId = row.getLong(firstRowIdIndex)
    val rowId = row.getLong(rowIdIndex)

    if (currentWriter == null || !currentWriter.matchFirstRowId(firstRowId)) {
      newCurrentWriter(firstRowId)
    }

    val paimonRow = toPaimonRow(row)
    currentWriter.write(
      if (rawBlobPlaceholderMarkerIndexes.isEmpty) {
        paimonRow
      } else {
        rawBlobFallbackMappingRow.replace(paimonRow, rawBlobPlaceholderFallbackRow(row))
      },
      rowId)
  }

  private def rawBlobPlaceholderFallbackRow(row: Row): InternalRow = {
    rawBlobFallbackMarkerIndexes.zipWithIndex.foreach {
      case (markerIndex, fallbackIndex) =>
        rawBlobFallbackRow.setField(
          fallbackIndex,
          if (row.getBoolean(markerIndex)) BlobPlaceholder.INSTANCE else null)
    }
    rawBlobFallbackRow
  }

  private def newCurrentWriter(firstRowId: Long): Unit = {
    finishCurrentWriter()
    val pair = firstRowIdToPartitionMap.getOrElse(firstRowId, null)
    if (pair == null) {
      throw new IllegalArgumentException(
        s"First row ID $firstRowId not found in partition map. " +
          s"Available first row IDs: ${firstRowIdToPartitionMap.keys.mkString(", ")}")
    }

    val (partitionBytes, numRecords) = pair
    val partition = SerializationUtils.deserializeBinaryRow(partitionBytes)

    val writer = writeBuilder
      .newWrite()
      .withWriteType(writeType)
      .asInstanceOf[TableWriteImpl[InternalRow]]
      .getWrite
      .asInstanceOf[AbstractFileStoreWrite[InternalRow]]
      .createWriter(partition, 0)
    currentWriter = PerFileWriter(partition, firstRowId, writer, numRecords)
  }

  private def finishCurrentWriter(): Unit = {
    if (currentWriter != null) {
      commitMessages ++= currentWriter.finish()
    }
    currentWriter = null
  }

  def write(row: Row, bucket: Int): Unit = {
    throw new UnsupportedOperationException(
      "DataEvolutionSparkTableWrite does not support writing with bucket.")
  }

  override def commitImpl(): Seq[CommitMessage] = {
    finishCurrentWriter()
    commitMessages.toSeq
  }

  override def close(): Unit = {
    ioManager.close()
  }

  private case class PerFileWriter(
      partition: BinaryRow,
      firstRowId: Long,
      recordWriter: RecordWriter[InternalRow],
      numRecords: Long) {

    private var numWritten = 0L
    private var fillerRows = 0L
    private var fillerRow: InternalRow = _

    def matchFirstRowId(firstRowId: Long): Boolean = {
      this.firstRowId == firstRowId
    }

    def write(row: InternalRow, rowId: Long): Unit = {
      assert(
        rowId >= firstRowId + numWritten,
        s"Row ID should be incremental. Expected at least ${firstRowId + numWritten}, but got $rowId.")
      assert(
        rowId < firstRowId + numRecords,
        s"Row ID $rowId is out of range [$firstRowId, ${firstRowId + numRecords}).")

      // For tables with existing deletion vectors, there may be some row id gaps.
      // We can simply pad any valid values since these rows will never be exposed.
      fillGapUntil(rowId, row)

      numWritten += 1
      recordWriter.write(row)
    }

    def finish(): Seq[CommitMessageImpl] = {
      try {
        fillGapUntil(firstRowId + numRecords)

        assert(
          numRecords == numWritten,
          s"Number of written records $numWritten does not match expected number $numRecords for first row ID $firstRowId.")
        if (fillerRows > 0) {
          DataEvolutionTableDataWrite.LOG.warn(
            s"Data evolution merge wrote $fillerRows filler rows out of $numRecords rows " +
              s"for row-id range [$firstRowId, ${firstRowId + numRecords}) to preserve " +
              "row-id continuity. Raw blob fields in filler rows are written as NULL.")
        }
        val result = recordWriter.prepareCommit(false)
        val dataFiles = result.newFilesIncrement().newFiles()
        val dataFileMetas = assignFirstRowIds(dataFiles.asScala.toSeq)
        Seq(
          new CommitMessageImpl(
            partition,
            0,
            null,
            new DataIncrement(
              dataFileMetas.asJava,
              Collections.emptyList(),
              Collections.emptyList()),
            CompactIncrement.emptyIncrement()
          ))
      } finally {
        recordWriter.close()
      }
    }

    private def fillGapUntil(rowId: Long, fillerSourceRow: InternalRow = null): Unit = {
      if (fillerRow == null && fillerSourceRow != null) {
        // Copy the first record this file writer met to minimize the influences on
        // file stats, but keep raw blob fields as NULLs so filler rows do not trigger
        // blob fallback.
        val copied = rowSerializer
          .copyRowData(fillerSourceRow, new GenericRow(writeType.getFieldCount))
          .asInstanceOf[GenericRow]
        rawBlobFallbackFieldIndexes.foreach(copied.setField(_, null))
        fillerRow = copied
      }

      assert(
        fillerRow != null || firstRowId + numWritten == rowId,
        s"Cannot fill row ID gaps before any real row for first row ID $firstRowId.")
      while (firstRowId + numWritten < rowId) {
        recordWriter.write(fillerRow)
        numWritten += 1
        fillerRows += 1
      }
    }

    private def assignFirstRowIds(dataFiles: Seq[org.apache.paimon.io.DataFileMeta])
        : Seq[org.apache.paimon.io.DataFileMeta] = {
      val assigned = ListBuffer[org.apache.paimon.io.DataFileMeta]()
      val blobFieldStarts = mutable.HashMap[String, Long]()
      var normalFileCount = 0
      var normalFileStart = firstRowId
      var vectorStoreStart = firstRowId

      dataFiles.foreach {
        file =>
          if (isBlobFile(file.fileName())) {
            val blobFieldName = file.writeCols().get(0)
            val blobStart = blobFieldStarts.getOrElse(blobFieldName, firstRowId)
            assigned += file.assignFirstRowId(blobStart)
            blobFieldStarts.update(blobFieldName, blobStart + file.rowCount())
          } else if (isVectorStoreFile(file.fileName())) {
            assigned += file.assignFirstRowId(vectorStoreStart)
            vectorStoreStart += file.rowCount()
          } else {
            normalFileCount += 1
            assigned += file.assignFirstRowId(normalFileStart)
            normalFileStart += file.rowCount()
          }
      }

      // Raw blob/vector-store only partial writes may produce no normal file. If a normal file is
      // produced, row-id assignment assumes there is at most one in this target row range.
      // DedicatedFormatRollingFileWriter validates dedicated file row counts when a normal file
      // exists.
      if (normalFileCount > 1) {
        throw new IllegalStateException(
          s"This is a bug: DataEvolution partial write should produce at most one normal file, " +
            s"but produced $normalFileCount files. Files: ${dataFiles.mkString(", ")}")
      }

      assigned.toSeq
    }
  }
}

object DataEvolutionTableDataWrite {
  private val LOG = LoggerFactory.getLogger(classOf[DataEvolutionTableDataWrite])
}
