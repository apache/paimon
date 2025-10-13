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

import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.data.{BinaryRow, InternalRow}
import org.apache.paimon.disk.IOManager
import org.apache.paimon.io.{CompactIncrement, DataIncrement}
import org.apache.paimon.operation.{AbstractFileStoreWrite, AppendFileStoreWrite}
import org.apache.paimon.spark.util.SparkRowUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessageImpl, CommitMessageSerializer, TableWriteImpl}
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.RecordWriter

import org.apache.spark.sql.Row

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class DataEvolutionSparkTableWrite(
    writeBuilder: BatchWriteBuilder,
    writeType: RowType,
    firstRowIdToPartitionMap: mutable.HashMap[Long, Tuple2[BinaryRow, Long]],
    blobAsDescriptor: Boolean,
    catalogContext: CatalogContext)
  extends SparkTableWriteTrait {

  private var currentWriter: PerFileWriter = _
  private val ioManager: IOManager = SparkUtils.createIOManager
  private val rowIdIndex = writeType.getFieldCount
  private val firstRowIdIndex = rowIdIndex + 1
  private val commitMessageImpls = ListBuffer[CommitMessageImpl]()

  private val toPaimonRow = {
    SparkRowUtils.toPaimonRow(writeType, -1, blobAsDescriptor, catalogContext)
  }

  def write(row: Row): Unit = {
    val firstRowId = row.getLong(firstRowIdIndex)
    val rowId = row.getLong(rowIdIndex)

    if (currentWriter == null || !currentWriter.matchFirstRowId(firstRowId)) {
      newCurrentWriter(firstRowId)
    }

    currentWriter.write(toPaimonRow(row), rowId)
  }

  def newCurrentWriter(firstRowId: Long): Unit = {
    finishCurrentWriter()
    val (partition, numRecords) = firstRowIdToPartitionMap.getOrElse(firstRowId, null)
    if (partition == null) {
      throw new IllegalArgumentException(
        s"First row ID $firstRowId not found in partition map. " +
          s"Available first row IDs: ${firstRowIdToPartitionMap.keys.mkString(", ")}")
    }

    val writer = writeBuilder
      .newWrite()
      .withWriteType(writeType)
      .asInstanceOf[TableWriteImpl[InternalRow]]
      .getWrite
      .asInstanceOf[AbstractFileStoreWrite[InternalRow]]
      .createWriter(partition, 0)
    currentWriter = PerFileWriter(partition, firstRowId, writer, numRecords)
  }

  def finishCurrentWriter(): Unit = {
    if (currentWriter != null) {
      commitMessageImpls.append(currentWriter.finish())
    }
    currentWriter = null
  }

  def write(row: Row, bucket: Int): Unit = {
    throw new UnsupportedOperationException(
      "DataEvolutionSparkTableWrite does not support writing with bucket.")
  }

  def finish(): Iterator[Array[Byte]] = {
    finishCurrentWriter()
    var bytesWritten = 0L
    var recordsWritten = 0L
    val commitMessages = new ListBuffer[Array[Byte]]()
    val serializer = new CommitMessageSerializer()
    commitMessageImpls.foreach {
      message: CommitMessageImpl =>
        message.newFilesIncrement().newFiles().asScala.foreach {
          dataFileMeta =>
            bytesWritten += dataFileMeta.fileSize()
            recordsWritten += dataFileMeta.rowCount()
        }
        commitMessages += serializer.serialize(message)
    }
    reportOutputMetrics(bytesWritten, recordsWritten)
    commitMessages.iterator
  }

  override def close(): Unit = {
    ioManager.close()
  }

  private case class PerFileWriter(
      partition: BinaryRow,
      firstRowId: Long,
      recordWriter: RecordWriter[InternalRow],
      numRecords: Long) {

    var numWritten = 0

    def matchFirstRowId(firstRowId: Long): Boolean = {
      this.firstRowId == firstRowId
    }

    def write(row: InternalRow, rowId: Long): Unit = {
      assert(rowId == firstRowId + numWritten, "Row ID does not match expected.")
      numWritten += 1
      recordWriter.write(row)
    }

    def finish(): CommitMessageImpl = {
      try {
        assert(
          numRecords == numWritten,
          s"Number of written records $numWritten does not match expected number $numRecords for first row ID $firstRowId.")
        val result = recordWriter.prepareCommit(false)
        val dataFiles = result.newFilesIncrement().newFiles()
        assert(dataFiles.size() == 1, "This is a bug, PerFileWriter could only produce one file")
        val dataFileMeta = dataFiles.get(0).assignFirstRowId(firstRowId)
        new CommitMessageImpl(
          partition,
          0,
          null,
          new DataIncrement(
            java.util.Arrays.asList(dataFileMeta),
            Collections.emptyList(),
            Collections.emptyList()),
          CompactIncrement.emptyIncrement())
      } finally {
        recordWriter.close()
      }
    }
  }
}
