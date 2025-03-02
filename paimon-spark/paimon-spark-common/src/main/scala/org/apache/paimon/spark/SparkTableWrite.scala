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

import org.apache.paimon.disk.IOManager
import org.apache.paimon.spark.util.SparkRowUtils
import org.apache.paimon.table.sink.{BatchTableWrite, BatchWriteBuilder, CommitMessageImpl, CommitMessageSerializer}
import org.apache.paimon.types.RowType

import org.apache.spark.TaskContext
import org.apache.spark.sql.{PaimonUtils, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class SparkTableWrite(
    writeBuilder: BatchWriteBuilder,
    rowType: RowType,
    inputSchema: StructType,
    rowKindColIdx: Int)
  extends AutoCloseable {

  val ioManager: IOManager = SparkUtils.createIOManager
  val write: BatchTableWrite =
    writeBuilder.newWrite().withIOManager(ioManager).asInstanceOf[BatchTableWrite]

  def write(row: InternalRow): Unit = {
    write.write(toPaimonRow(row))
  }

  def write(row: InternalRow, bucket: Int): Unit = {
    write.write(toPaimonRow(row), bucket)
  }

  def finish(): Iterator[Array[Byte]] = {
    var bytesWritten = 0L
    var recordsWritten = 0L
    val commitMessages = new ListBuffer[Array[Byte]]()
    val serializer = new CommitMessageSerializer()
    write.prepareCommit().asScala.foreach {
      case message: CommitMessageImpl =>
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
    write.close()
    ioManager.close()
  }

  private def toPaimonRow(row: InternalRow) =
    new SparkInternalRowWrapper(
      row,
      SparkRowUtils.getRowKind(row, rowKindColIdx),
      inputSchema,
      rowType.getFieldCount)

  private def reportOutputMetrics(bytesWritten: Long, recordsWritten: Long): Unit = {
    val taskContext = TaskContext.get
    if (taskContext != null) {
      PaimonUtils.updateOutputMetrics(
        taskContext.taskMetrics.outputMetrics,
        bytesWritten,
        recordsWritten)
    }
  }
}
