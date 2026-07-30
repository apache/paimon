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

case class PaimonDataWrite(
    writeBuilder: BatchWriteBuilder,
    writeType: RowType,
    rowKindColIdx: Int = -1,
    writeRowTracking: Boolean = false,
    fullCompactionDeltaCommits: Option[Int],
    batchId: Option[Long],
    uriReaderFactory: UriReaderFactory,
    postponePartitionBucketComputer: Option[BinaryRow => Integer])
  extends abstractInnerTableDataWrite[Row]
  with InnerTableV1DataWrite {

  private val ioManager: IOManager = SparkUtils.createIOManager

  val write: TableWriteImpl[Row] = {
    val _write = writeBuilder.newWrite().asInstanceOf[TableWriteImpl[Row]]
    _write.withIOManager(ioManager)
    if (writeRowTracking) {
      _write.withWriteType(writeType)
    }
    _write
  }

  private val toPaimonRow = {
    SparkRowUtils.toPaimonRow(writeType, rowKindColIdx, uriReaderFactory)
  }

  private val cleanup = new SparkAttemptCleanup(
    writeBuilder.tableName(),
    SparkAttemptCleanup.commitUserOrUnknown(writeBuilder),
    writeBuilder,
    () => closeLocalResources())

  override protected def attemptCleanup: Option[SparkAttemptCleanup] = Some(cleanup)

  private def closeLocalResources(): Unit = {
    write.close()
    ioManager.close()
  }

  def write(row: Row): Unit = {
    cleanup.checkInterruptedPeriodically()
    postWrite(write.writeAndReturn(toPaimonRow(row)))
  }

  def write(row: Row, bucket: Int): Unit = {
    cleanup.checkInterruptedPeriodically()
    val paimonRow = toPaimonRow(row)
    val sinkRecord = postponePartitionBucketComputer match {
      case Some(numBuckets) =>
        write.writeAndReturn(paimonRow, bucket, numBuckets(write.getPartition(paimonRow)))
      case None => write.writeAndReturn(paimonRow, bucket)
    }
    postWrite(sinkRecord)
  }

  override def commitImpl(): Seq[CommitMessage] = {
    val messages = scala.collection.mutable.ListBuffer[CommitMessage]()
    write.prepareCommit(
      (msg: CommitMessage) => {
        val transformed = transformCommitMessage(msg)
        registerPrepared(Seq(transformed))
        messages += transformed
      })
    messages.toSeq
  }

  private def transformCommitMessage(message: CommitMessage): CommitMessage = {
    if (postponePartitionBucketComputer.isDefined) {
      message match {
        case m: CommitMessageImpl =>
          new CommitMessageImpl(
            m.partition(),
            m.bucket(),
            postponePartitionBucketComputer.get.apply(m.partition()),
            m.newFilesIncrement(),
            m.compactIncrement()
          )
        case _ => throw new RuntimeException()
      }
    } else {
      message
    }
  }

  override def close(): Unit = cleanup.close()
}
