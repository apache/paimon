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

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.disk.IOManager
import org.apache.paimon.spark.SparkUtils
import org.apache.paimon.spark.util.SparkRowUtils
import org.apache.paimon.table.sink._
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.UriReaderFactory

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

/**
 * @param writeBuilder
 *   a batch write builder, or the stream write builder of a streaming query.
 * @param ignorePreviousFiles
 *   whether the write may ignore the files it would otherwise have to read first, because the
 *   commit overwrites them. A batch write builder decides this itself for an overwrite; a stream
 *   write builder does not know about overwrites.
 */
case class PaimonDataWrite(
    writeBuilder: WriteBuilder,
    writeType: RowType,
    rowKindColIdx: Int = -1,
    writeRowTracking: Boolean = false,
    fullCompactionDeltaCommits: Option[Int],
    commitIdentifier: Option[Long],
    uriReaderFactory: UriReaderFactory,
    postponePartitionBucketComputer: Option[BinaryRow => Integer],
    ignorePreviousFiles: Boolean = false)
  extends abstractInnerTableDataWrite[Row]
  with InnerTableV1DataWrite {

  private val ioManager: IOManager = SparkUtils.createIOManager

  val write: TableWriteImpl[Row] = {
    val _write = writeBuilder.newWrite().asInstanceOf[TableWriteImpl[Row]]
    _write.withIOManager(ioManager)
    if (writeRowTracking) {
      _write.withWriteType(writeType)
    }
    if (ignorePreviousFiles) {
      _write.withIgnorePreviousFiles(true)
    }
    _write
  }

  private val toPaimonRow = {
    SparkRowUtils.toPaimonRow(writeType, rowKindColIdx, uriReaderFactory)
  }

  def write(row: Row): Unit = {
    postWrite(write.writeAndReturn(toPaimonRow(row)))
  }

  def write(row: Row, bucket: Int): Unit = {
    val paimonRow = toPaimonRow(row)
    val sinkRecord = postponePartitionBucketComputer match {
      case Some(numBuckets) =>
        write.writeAndReturn(paimonRow, bucket, numBuckets(write.getPartition(paimonRow)))
      case None => write.writeAndReturn(paimonRow, bucket)
    }
    postWrite(sinkRecord)
  }

  override def commitImpl(): Seq[CommitMessage] = {
    val messages = commitIdentifier match {
      // A Spark task does not outlive its micro-batch, so unlike a Flink writer it cannot leave a
      // compaction running to be collected at the next commit.
      case Some(identifier) => write.prepareCommit(true, identifier)
      case None => write.prepareCommit()
    }
    messages.asScala.toSeq
  }

  override def close(): Unit = {
    write.close()
    ioManager.close()
  }
}
