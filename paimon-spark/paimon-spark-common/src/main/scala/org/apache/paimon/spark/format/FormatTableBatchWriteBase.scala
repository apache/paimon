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

import org.apache.paimon.spark.SparkInternalRowWrapper
import org.apache.paimon.spark.write.{FormatTableWriteTaskResult, V2DataWrite, WriteTaskResult}
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.sink.{BatchTableWrite, BatchWriteBuilder, CommitMessage}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * Business logic for `FormatTable` batch writes, deliberately *not* extending
 * `org.apache.spark.sql.connector.write.BatchWrite`. See
 * [[org.apache.paimon.spark.write.PaimonBatchWriteBase]] for the full rationale: Spark 4.1 added a
 * default method `BatchWrite.commit(.., WriteSummary)` whose `WriteSummary` parameter type is
 * unavailable on Spark 4.0, so a class compiled against 4.1 that mixes in `BatchWrite` triggers
 * `ClassNotFoundException: WriteSummary` lazy-linking on 4.0 runtimes during Spark task
 * serialization. Keeping this base off `BatchWrite` lets common ship the implementation once;
 * per-version `paimon-spark{3,4}-common` modules supply a thin wrapper that mixes in `BatchWrite`,
 * and `paimon-spark-4.0/src/main` shadows that wrapper at the 4.0.2 compile target.
 */
abstract class FormatTableBatchWriteBase(
    table: FormatTable,
    overwriteDynamic: Option[Boolean],
    overwritePartitions: Option[Map[String, String]],
    writeSchema: StructType)
  extends Logging
  with Serializable {

  protected val batchWriteBuilder: BatchWriteBuilder = {
    val builder = table.newBatchWriteBuilder()
    // todo: add test for static overwrite the whole table
    if (overwriteDynamic.contains(true)) {
      builder.withOverwrite()
    } else {
      overwritePartitions.foreach(partitions => builder.withOverwrite(partitions.asJava))
    }
    builder
  }

  protected def createFormatTableDataWriterFactory(): DataWriterFactory = {
    (_: Int, _: Long) => new FormatTableDataWriter(batchWriteBuilder, writeSchema)
  }

  protected def commitMessages(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Committing to FormatTable ${table.name()}")
    val batchTableCommit = batchWriteBuilder.newCommit()
    val commitMessages = WriteTaskResult.merge(messages).asJava
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

  protected def abortMessages(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Aborting write to FormatTable ${table.name()}")
    val batchTableCommit = batchWriteBuilder.newCommit()
    val commitMessages = WriteTaskResult.merge(messages).asJava
    batchTableCommit.abort(commitMessages)
  }
}

private class FormatTableDataWriter(batchWriteBuilder: BatchWriteBuilder, writeSchema: StructType)
  extends V2DataWrite
  with Logging {

  private val rowConverter: InternalRow => org.apache.paimon.data.InternalRow = {
    val numFields = writeSchema.fields.length
    record => {
      new SparkInternalRowWrapper(writeSchema, numFields).replace(record)
    }
  }

  private val write: BatchTableWrite = batchWriteBuilder.newWrite()

  override def write(record: InternalRow): Unit = {
    val paimonRow = rowConverter.apply(record)
    write.write(paimonRow)
  }

  override def commitImpl(): Seq[CommitMessage] = {
    write.prepareCommit().asScala.toSeq
  }

  def buildWriteTaskResult(commitMessages: Seq[CommitMessage]): FormatTableWriteTaskResult = {
    FormatTableWriteTaskResult(commitMessages)
  }

  override def commit: FormatTableWriteTaskResult = {
    super.commit.asInstanceOf[FormatTableWriteTaskResult]
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
