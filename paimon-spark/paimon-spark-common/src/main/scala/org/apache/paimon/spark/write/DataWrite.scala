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
import org.apache.paimon.table.sink._

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{PaimonUtils, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter => SparkDataWriter}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Write data in an input partition. */
trait DataWrite[T] extends AutoCloseable {

  def write(record: T): Unit

  def commit: WriteTaskResult = {
    try {
      preCommit()
      val commitMessages = commitImpl()
      postCommit(commitMessages)
      buildWriteTaskResult(commitMessages)
    } finally {
      close()
    }
  }

  def preCommit(): Unit = {}

  def commitImpl(): Seq[CommitMessage]

  def postCommit(commitMessages: Seq[CommitMessage]): Unit = {
    reportOutputMetrics(commitMessages)
  }

  /** Build serializable write commit messages which will be sent back to driver side. */
  def buildWriteTaskResult(commitMessages: Seq[CommitMessage]): WriteTaskResult

  private def reportOutputMetrics(commitMessages: Seq[CommitMessage]): Unit = {
    var bytesWritten = 0L
    var recordsWritten = 0L
    commitMessages.foreach {
      case message: CommitMessageImpl =>
        message.newFilesIncrement().newFiles().asScala.foreach {
          dataFileMeta =>
            bytesWritten += dataFileMeta.fileSize()
            recordsWritten += dataFileMeta.rowCount()
        }
      // todo: support format table.
      case _ =>
    }
    val taskContext = TaskContext.get
    if (taskContext != null) {
      PaimonUtils.updateOutputMetrics(
        taskContext.taskMetrics.outputMetrics,
        bytesWritten,
        recordsWritten)
    }
  }
}

/** DataWrite used for DSv1. */
trait V1DataWrite extends DataWrite[Row] {

  def write(row: Row): Unit

  def write(row: Row, bucket: Int): Unit
}

/** DataWrite used for DSv2. */
trait V2DataWrite extends DataWrite[InternalRow] with SparkDataWriter[InternalRow]

trait InnerTableDataWrite[T] extends DataWrite[T] {

  def buildWriteTaskResult(commitMessages: Seq[CommitMessage]): InnerTableWriteTaskResult = {
    InnerTableWriteTaskResult.fromCommitMessages(commitMessages)
  }

  override def commit: InnerTableWriteTaskResult = {
    super.commit.asInstanceOf[InnerTableWriteTaskResult]
  }
}

/** DataWrite used for DSv1 with paimon inner table. */
trait InnerTableV1DataWrite extends InnerTableDataWrite[Row] with V1DataWrite

/** DataWrite used for DSv2 with paimon inner table. */
trait InnerTableV2DataWrite extends InnerTableDataWrite[InternalRow] with V2DataWrite

abstract class abstractInnerTableDataWrite[T] extends InnerTableDataWrite[T] with Logging {

  val write: BatchTableWrite

  val fullCompactionDeltaCommits: Option[Int]

  /** For batch write, batchId is None, for streaming write, batchId is the current batch id (>= 0). */
  val batchId: Option[Long]

  private val needFullCompaction: Boolean = {
    fullCompactionDeltaCommits match {
      case Some(deltaCommits) if deltaCommits > 0 =>
        batchId match {
          case Some(id) => (id + 1) % deltaCommits == 0
          // When fullCompactionDeltaCommits is set, always trigger full compaction for batch write.
          case None => true
        }
      case _ => false
    }
  }

  private val writtenBuckets = mutable.Set[(BinaryRow, Integer)]()

  def postWrite(record: SinkRecord): Unit = {
    if (record == null) {
      return
    }

    if (needFullCompaction && !writtenBuckets.contains((record.partition(), record.bucket()))) {
      writtenBuckets.add((record.partition().copy(), record.bucket()))
    }
  }

  override def preCommit(): Unit = {
    if (needFullCompaction && writtenBuckets.nonEmpty) {
      logInfo("Start to compact buckets: " + writtenBuckets)
      writtenBuckets.foreach(
        (bucket: (BinaryRow, Integer)) => {
          write.compact(bucket._1, bucket._2, true)
        })
    }
  }
}
