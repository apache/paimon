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

import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.disk.IOManager
import org.apache.paimon.spark.SparkUtils
import org.apache.paimon.spark.util.SparkRowUtils
import org.apache.paimon.table.sink._
import org.apache.paimon.types.RowType

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

case class PaimonDataWrite(
    writeBuilder: BatchWriteBuilder,
    writeType: RowType,
    rowKindColIdx: Int = -1,
    writeRowTracking: Boolean = false,
    fullCompactionDeltaCommits: Option[Int],
    batchId: Option[Long],
    blobAsDescriptor: Boolean,
    catalogContext: CatalogContext,
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
    if (postponePartitionBucketComputer.isDefined) {
      _write.getWrite.withIgnoreNumBucketCheck(true)
    }
    _write
  }

  private val toPaimonRow = {
    SparkRowUtils.toPaimonRow(writeType, rowKindColIdx, blobAsDescriptor, catalogContext)
  }

  def write(row: Row): Unit = {
    postWrite(write.writeAndReturn(toPaimonRow(row)))
  }

  def write(row: Row, bucket: Int): Unit = {
    postWrite(write.writeAndReturn(toPaimonRow(row), bucket))
  }

  override def commitImpl(): Seq[CommitMessage] = {
    val commitMessages = write.prepareCommit().asScala.toSeq

    if (postponePartitionBucketComputer.isDefined) {
      commitMessages.map {
        case message: CommitMessageImpl =>
          new CommitMessageImpl(
            message.partition(),
            message.bucket(),
            postponePartitionBucketComputer.get.apply(message.partition()),
            message.newFilesIncrement(),
            message.compactIncrement()
          )
        case _ => throw new RuntimeException()
      }
    } else {
      commitMessages
    }
  }

  override def close(): Unit = {
    write.close()
    ioManager.close()
  }
}
