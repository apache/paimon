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

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.{SparkInternalRowWrapper, SparkUtils}
import org.apache.paimon.table.{AppendOnlyFileStoreTable, FileStoreTable}
import org.apache.paimon.table.sink.{BatchTableWrite, BatchWriteBuilder, CommitMessage, CommitMessageSerializer}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import java.io.{IOException, UncheckedIOException}

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class SparkV2Write(
    storeTable: FileStoreTable,
    overwriteDynamic: Boolean,
    overwritePartitions: Option[Map[String, String]],
    writeSchema: StructType
) extends Write
  with RequiresDistributionAndOrdering
  with Logging {

  assert(
    !(overwriteDynamic && overwritePartitions.nonEmpty),
    "Cannot overwrite dynamically and by filter both")

  private val table =
    storeTable.copy(
      Map(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key -> overwriteDynamic.toString).asJava)

  private val batchWriteBuilder = {
    val builder = table.newBatchWriteBuilder()
    overwritePartitions.foreach(partitions => builder.withOverwrite(partitions.asJava))
    builder
  }

  private val writeRequirement = SparkWriteRequirement(table)

  override def requiredDistribution(): Distribution = {
    val distribution = writeRequirement.distribution
    logInfo(s"Requesting $distribution as write distribution for table ${table.name()}")
    distribution
  }

  override def requiredOrdering(): Array[SortOrder] = {
    val ordering = writeRequirement.ordering
    logInfo(s"Requesting ${ordering.mkString(",")} as write ordering for table ${table.name()}")
    ordering
  }

  override def toBatch: BatchWrite = new PaimonBatchWrite

  override def toString: String =
    if (overwriteDynamic)
      s"PaimonWrite(table=${table.fullName()}, overwriteDynamic=true)"
    else
      s"PaimonWrite(table=${table.fullName()}, overwritePartitions=$overwritePartitions)"

  private class PaimonBatchWrite extends BatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
      WriterFactory(writeSchema, batchWriteBuilder)

    override def useCommitCoordinator(): Boolean = false

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      logInfo(s"Committing to table ${table.name()}")
      val batchTableCommit = batchWriteBuilder.newCommit()

      val commitMessages = messages
        .collect {
          case taskCommit: TaskCommit => taskCommit.commitMessages
          case other =>
            throw new IllegalArgumentException(s"${other.getClass.getName} is not supported")
        }
        .flatten
        .toList

      try {
        val start = System.currentTimeMillis()
        batchTableCommit.commit(commitMessages.asJava)
        logInfo(s"Committed in ${System.currentTimeMillis() - start} ms")
      } finally {
        batchTableCommit.close()
      }
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {
      // TODO clean uncommitted files
    }
  }
}

private case class WriterFactory(writeSchema: StructType, batchWriteBuilder: BatchWriteBuilder)
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val batchTableWrite = batchWriteBuilder.newWrite()
    new PaimonDataWriter(batchTableWrite, writeSchema)
  }
}

private class PaimonDataWriter(batchTableWrite: BatchTableWrite, writeSchema: StructType)
  extends DataWriter[InternalRow] {

  private val ioManager = SparkUtils.createIOManager()
  batchTableWrite.withIOManager(ioManager)

  private val rowConverter: InternalRow => SparkInternalRowWrapper = {
    val numFields = writeSchema.fields.length
    val reusableWrapper = new SparkInternalRowWrapper(-1, writeSchema, numFields)
    record => reusableWrapper.replace(record)
  }

  override def write(record: InternalRow): Unit = {
    batchTableWrite.write(rowConverter.apply(record))
  }

  override def commit(): WriterCommitMessage = {
    try {
      val commitMessages = batchTableWrite.prepareCommit().asScala.toList
      TaskCommit(commitMessages)
    } finally {
      close()
    }
  }

  override def abort(): Unit = close()

  override def close(): Unit = {
    try {
      batchTableWrite.close()
      ioManager.close()
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }
}

private case class TaskCommit(commitMessagesList: List[CommitMessage]) extends WriterCommitMessage {
  // Although CommitMessage is serializable, the variables in CommitMessageImpl are all transient.
  private val serializedMessageList: List[Array[Byte]] = {
    Option(commitMessagesList).filter(_.nonEmpty) match {
      case Some(messages) =>
        val serializer = new CommitMessageSerializer()
        messages.map {
          msg =>
            Try(serializer.serialize(msg)) match {
              case Success(serializedBytes) => serializedBytes
              case Failure(e: IOException) => throw new UncheckedIOException(e)
              case Failure(e) => throw e
            }
        }
      case None => Nil
    }
  }

  def commitMessages(): List[CommitMessage] = {
    val deserializer = new CommitMessageSerializer()
    serializedMessageList.map {
      bytes =>
        Try(deserializer.deserialize(deserializer.getVersion, bytes)) match {
          case Success(msg) => msg
          case Failure(e: IOException) => throw new UncheckedIOException(e)
          case Failure(e) => throw e
        }
    }
  }
}
