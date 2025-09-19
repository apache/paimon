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
import org.apache.paimon.options.Options
import org.apache.paimon.spark.{SparkInternalRowWrapper, SparkUtils}
import org.apache.paimon.spark.commands.SchemaHelper
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchTableWrite, BatchWriteBuilder, CommitMessage, CommitMessageSerializer}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import java.io.{IOException, UncheckedIOException}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class PaimonV2Write(
    override val originTable: FileStoreTable,
    overwriteDynamic: Boolean,
    overwritePartitions: Option[Map[String, String]],
    rowSchema: StructType,
    options: Options
) extends Write
  with RequiresDistributionAndOrdering
  with SchemaHelper
  with Logging {

  assert(
    !(overwriteDynamic && overwritePartitions.exists(_.nonEmpty)),
    "Cannot overwrite dynamically and by filter both")

  private val writeSchema = mergeSchema(rowSchema, options)

  updateTableWithOptions(
    Map(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key -> overwriteDynamic.toString))

  private val writeRequirement = PaimonWriteRequirement(table)

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

  override def toBatch: BatchWrite =
    PaimonBatchWrite(table, writeSchema, rowSchema, overwritePartitions)

  override def toString: String = {
    val overwriteDynamicStr = if (overwriteDynamic) {
      ", overwriteDynamic=true"
    } else {
      ""
    }
    val overwritePartitionsStr = overwritePartitions match {
      case Some(partitions) if partitions.nonEmpty => s", overwritePartitions=$partitions"
      case Some(_) => ", overwriteTable=true"
      case None => ""
    }
    s"PaimonWrite(table=${table.fullName()}$overwriteDynamicStr$overwritePartitionsStr)"
  }
}

private case class PaimonBatchWrite(
    table: FileStoreTable,
    writeSchema: StructType,
    rowSchema: StructType,
    overwritePartitions: Option[Map[String, String]])
  extends BatchWrite
  with WriteHelper {

  private val batchWriteBuilder = {
    val builder = table.newBatchWriteBuilder()
    overwritePartitions.foreach(partitions => builder.withOverwrite(partitions.asJava))
    builder
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    WriterFactory(writeSchema, rowSchema, batchWriteBuilder)

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Committing to table ${table.name()}")
    val batchTableCommit = batchWriteBuilder.newCommit()

    val commitMessages = messages
      .collect {
        case taskCommit: TaskCommit => taskCommit.commitMessages()
        case other =>
          throw new IllegalArgumentException(s"${other.getClass.getName} is not supported")
      }
      .flatten
      .toSeq

    try {
      val start = System.currentTimeMillis()
      batchTableCommit.commit(commitMessages.asJava)
      logInfo(s"Committed in ${System.currentTimeMillis() - start} ms")
    } finally {
      batchTableCommit.close()
    }
    postCommit(commitMessages)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // TODO clean uncommitted files
  }
}

private case class WriterFactory(
    writeSchema: StructType,
    rowSchema: StructType,
    batchWriteBuilder: BatchWriteBuilder)
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val batchTableWrite = batchWriteBuilder.newWrite()
    new PaimonDataWriter(batchTableWrite, writeSchema, rowSchema)
  }
}

private class PaimonDataWriter(
    batchTableWrite: BatchTableWrite,
    writeSchema: StructType,
    rowSchema: StructType)
  extends DataWriter[InternalRow] {

  private val ioManager = SparkUtils.createIOManager()
  batchTableWrite.withIOManager(ioManager)

  private val rowConverter: InternalRow => SparkInternalRowWrapper = {
    val numFields = writeSchema.fields.length
    val reusableWrapper = new SparkInternalRowWrapper(-1, writeSchema, rowSchema, numFields)
    record => reusableWrapper.replace(record)
  }

  override def write(record: InternalRow): Unit = {
    batchTableWrite.write(rowConverter.apply(record))
  }

  override def commit(): WriterCommitMessage = {
    try {
      val commitMessages = batchTableWrite.prepareCommit().asScala.toSeq
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

class TaskCommit private (
    private val serializedMessageBytes: Seq[Array[Byte]]
) extends WriterCommitMessage {
  def commitMessages(): Seq[CommitMessage] = {
    val deserializer = new CommitMessageSerializer()
    serializedMessageBytes.map {
      bytes =>
        Try(deserializer.deserialize(deserializer.getVersion, bytes)) match {
          case Success(msg) => msg
          case Failure(e: IOException) => throw new UncheckedIOException(e)
          case Failure(e) => throw e
        }
    }
  }
}

object TaskCommit {
  def apply(commitMessages: Seq[CommitMessage]): TaskCommit = {
    val serializer = new CommitMessageSerializer()
    val serializedBytes: Seq[Array[Byte]] = Option(commitMessages)
      .filter(_.nonEmpty)
      .map(_.map {
        msg =>
          Try(serializer.serialize(msg)) match {
            case Success(serializedBytes) => serializedBytes
            case Failure(e: IOException) => throw new UncheckedIOException(e)
            case Failure(e) => throw e
          }
      })
      .getOrElse(Seq.empty)

    new TaskCommit(serializedBytes)
  }
}
