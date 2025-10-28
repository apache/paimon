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

import org.apache.paimon.fs.TwoPhaseOutputStream
import org.apache.paimon.spark.{BaseTable, FormatTableScanBuilder, SparkInternalRowWrapper}
import org.apache.paimon.spark.write.BaseV2WriteBuilder
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.format.TwoPhaseCommitMessage
import org.apache.paimon.table.sink.BatchTableWrite
import org.apache.paimon.types.RowType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.io.FileNotFoundException
import java.util

import scala.collection.JavaConverters._

case class PaimonFormatTable(table: FormatTable)
  extends BaseTable
  with SupportsRead
  with SupportsWrite {

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, OVERWRITE_DYNAMIC, OVERWRITE_BY_FILTER)
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

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    FormatTableWriterFactory(table, writeSchema)

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Committing to FormatTable ${table.name()}")

    val committers = messages
      .collect {
        case taskCommit: FormatTableTaskCommit => taskCommit.committers()
        case other =>
          throw new IllegalArgumentException(s"${other.getClass.getName} is not supported")
      }
      .flatten
      .toSeq

    try {
      val start = System.currentTimeMillis()
      if (overwritePartitions.isDefined && overwritePartitions.get.nonEmpty) {
        val child = org.apache.paimon.partition.PartitionUtils
          .buildPartitionName(overwritePartitions.get.asJava)
        val partitionPath = new org.apache.paimon.fs.Path(table.location(), child)
        deletePreviousDataFile(partitionPath)
      } else if (overwritePartitions.isDefined && overwritePartitions.get.isEmpty) {
        committers
          .map(c => c.targetFilePath().getParent)
          .distinct
          .foreach(deletePreviousDataFile)
      }
      committers.foreach(c => c.commit(table.fileIO()))
      logInfo(s"Committed in ${System.currentTimeMillis() - start} ms")
    } catch {
      case e: Exception =>
        logError("Failed to commit FormatTable writes", e)
        throw e
    }
  }

  private def deletePreviousDataFile(partitionPath: org.apache.paimon.fs.Path): Unit = {
    if (table.fileIO().exists(partitionPath)) {
      val files = table.fileIO().listFiles(partitionPath, true)
      files
        .filter(f => !f.getPath.getName.startsWith(".") && !f.getPath.getName.startsWith("_"))
        .foreach(
          f => {
            try {
              table.fileIO().deleteQuietly(f.getPath)
            } catch {
              case _: FileNotFoundException => logInfo(s"File ${f.getPath} already deleted")
              case other => throw new RuntimeException(other)
            }
          })
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Aborting write to FormatTable ${table.name()}")
    val committers = messages.collect {
      case taskCommit: FormatTableTaskCommit => taskCommit.committers()
    }.flatten

    committers.foreach {
      committer =>
        try {
          committer.discard(table.fileIO())
        } catch {
          case e: Exception => logWarning(s"Failed to abort committer: ${e.getMessage}")
        }
    }
  }
}

private case class FormatTableWriterFactory(table: FormatTable, writeSchema: StructType)
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val formatTableWrite = table.newBatchWriteBuilder().newWrite()
    new FormatTableDataWriter(table, formatTableWrite, writeSchema)
  }
}

private class FormatTableDataWriter(
    table: FormatTable,
    formatTableWrite: BatchTableWrite,
    writeSchema: StructType)
  extends DataWriter[InternalRow]
  with Logging {

  private val rowConverter: InternalRow => org.apache.paimon.data.InternalRow = {
    val numFields = writeSchema.fields.length
    record => {
      new SparkInternalRowWrapper(-1, writeSchema, numFields).replace(record)
    }
  }

  override def write(record: InternalRow): Unit = {
    val paimonRow = rowConverter.apply(record)
    formatTableWrite.write(paimonRow)
  }

  override def commit(): WriterCommitMessage = {
    try {
      val committers = formatTableWrite
        .prepareCommit()
        .asScala
        .map {
          case committer: TwoPhaseCommitMessage => committer.getCommitter
          case other =>
            throw new IllegalArgumentException(
              "Unsupported commit message type: " + other.getClass.getSimpleName)
        }
        .toSeq
      FormatTableTaskCommit(committers)
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
      formatTableWrite.close()
    } catch {
      case e: Exception =>
        logError("Error closing FormatTableDataWriter", e)
        throw new RuntimeException(e)
    }
  }
}

/** Commit message container for FormatTable writes, holding committers that need to be executed. */
class FormatTableTaskCommit private (private val _committers: Seq[TwoPhaseOutputStream.Committer])
  extends WriterCommitMessage {

  def committers(): Seq[TwoPhaseOutputStream.Committer] = _committers
}

object FormatTableTaskCommit {
  def apply(committers: Seq[TwoPhaseOutputStream.Committer]): FormatTableTaskCommit = {
    new FormatTableTaskCommit(committers)
  }
}
