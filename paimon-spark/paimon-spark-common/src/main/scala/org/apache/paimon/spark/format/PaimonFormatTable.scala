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

package org.apache.paimon.spark.format

import org.apache.paimon.CoreOptions
import org.apache.paimon.format.csv.CsvOptions
import org.apache.paimon.spark.{BaseTable, FormatTableScanBuilder, SparkInternalRowWrapper}
import org.apache.paimon.spark.write.BaseV2WriteBuilder
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.format.{FormatTableCommit, TwoPhaseCommitMessage}
import org.apache.paimon.table.sink.{BatchTableWrite, BatchWriteBuilder, CommitMessage}
import org.apache.paimon.types.RowType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

import scala.collection.JavaConverters._

case class PaimonFormatTable(table: FormatTable)
  extends BaseTable
  with SupportsRead
  with SupportsWrite {

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, OVERWRITE_DYNAMIC, OVERWRITE_BY_FILTER)
  }

  override def properties: util.Map[String, String] = {
    val properties = new util.HashMap[String, String](table.options())
    if (table.comment.isPresent) {
      properties.put(TableCatalog.PROP_COMMENT, table.comment.get)
    }
    if (FormatTable.Format.CSV == table.format) {
      properties.put(
        "sep",
        properties.getOrDefault(
          CsvOptions.FIELD_DELIMITER.key(),
          CsvOptions.FIELD_DELIMITER.defaultValue()))
    }
    properties
  }

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    val scanBuilder = FormatTableScanBuilder(table.copy(caseInsensitiveStringMap))
    scanBuilder.pruneColumns(schema)
    scanBuilder
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    PaimonFormatTableWriterBuilder(table, info.schema)
  }
}

case class PaimonFormatTableWriterBuilder(table: FormatTable, writeSchema: StructType)
  extends BaseV2WriteBuilder(table) {

  override def partitionRowType(): RowType = table.partitionType

  override def build: Write = new Write() {
    override def toBatch: BatchWrite = {
      FormatTableBatchWrite(table, overwriteDynamic, overwritePartitions, writeSchema)
    }

    override def toStreaming: StreamingWrite = {
      throw new UnsupportedOperationException("FormatTable doesn't support streaming write")
    }
  }
}

private case class FormatTableBatchWrite(
    table: FormatTable,
    overwriteDynamic: Boolean,
    overwritePartitions: Option[Map[String, String]],
    writeSchema: StructType)
  extends BatchWrite
  with Logging {

  assert(
    !(overwriteDynamic && overwritePartitions.exists(_.nonEmpty)),
    "Cannot overwrite dynamically and by filter both")

  private val batchWriteBuilder = {
    val builder = table.newBatchWriteBuilder()
    if (overwriteDynamic) {
      builder.withOverwrite()
    } else {
      overwritePartitions.foreach(partitions => builder.withOverwrite(partitions.asJava))
    }
    builder
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    FormatTableWriterFactory(batchWriteBuilder, writeSchema)

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Committing to FormatTable ${table.name()}")
    val batchTableCommit = batchWriteBuilder.newCommit()
    val commitMessages = getPaimonCommitMessages(messages)
    try {
      val start = System.currentTimeMillis()
      batchTableCommit.commit(commitMessages)
      logInfo(s"Committed in ${System.currentTimeMillis() - start} ms")
    } catch {
      case e: Exception =>
        logError("Failed to commit FormatTable writes", e)
        throw e
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Aborting write to FormatTable ${table.name()}")
    val batchTableCommit = batchWriteBuilder.newCommit()
    val commitMessages = getPaimonCommitMessages(messages)
    batchTableCommit.abort(commitMessages)
  }

  private def getPaimonCommitMessages(
      messages: Array[WriterCommitMessage]): util.List[CommitMessage] = {
    messages
      .collect {
        case taskCommit: FormatTableTaskCommit => taskCommit.commitMessages()
        case other =>
          throw new IllegalArgumentException(s"${other.getClass.getName} is not supported")
      }
      .flatten
      .toList
      .asJava
  }
}

private case class FormatTableWriterFactory(
    batchWriteBuilder: BatchWriteBuilder,
    writeSchema: StructType)
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new FormatTableDataWriter(batchWriteBuilder, writeSchema)
  }
}

private class FormatTableDataWriter(batchWriteBuilder: BatchWriteBuilder, writeSchema: StructType)
  extends DataWriter[InternalRow]
  with Logging {

  private val rowConverter: InternalRow => org.apache.paimon.data.InternalRow = {
    val numFields = writeSchema.fields.length
    record => {
      new SparkInternalRowWrapper(-1, writeSchema, numFields).replace(record)
    }
  }

  private val write: BatchTableWrite = batchWriteBuilder.newWrite()

  override def write(record: InternalRow): Unit = {
    val paimonRow = rowConverter.apply(record)
    write.write(paimonRow)
  }

  override def commit(): WriterCommitMessage = {
    try {
      val commitMessages = write
        .prepareCommit()
        .asScala
        .map {
          case commitMessage: TwoPhaseCommitMessage => commitMessage
          case other =>
            throw new IllegalArgumentException(
              "Unsupported commit message type: " + other.getClass.getSimpleName)
        }
        .toSeq
      FormatTableTaskCommit(commitMessages)
    } finally {
      close()
    }
  }

  override def abort(): Unit = {
    logInfo("Aborting FormatTable data writer")
    close()
  }

  override def close(): Unit = {
    try {
      write.close()
    } catch {
      case e: Exception =>
        logError("Error closing FormatTableDataWriter", e)
        throw new RuntimeException(e)
    }
  }
}

/** Commit message container for FormatTable writes, holding committers that need to be executed. */
class FormatTableTaskCommit private (private val _commitMessages: Seq[CommitMessage])
  extends WriterCommitMessage {

  def commitMessages(): Seq[CommitMessage] = _commitMessages
}

object FormatTableTaskCommit {
  def apply(commitMessages: Seq[CommitMessage]): FormatTableTaskCommit = {
    new FormatTableTaskCommit(commitMessages)
  }
}
