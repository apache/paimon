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
import org.apache.paimon.deletionvectors.{Bitmap64DeletionVector, BitmapDeletionVector, DeletionVector}
import org.apache.paimon.spark.{PaimonDeletedRecordsTaskMetric, PaimonInsertedRecordsTaskMetric, PaimonUpdatedRecordsTaskMetric}
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage}
import org.apache.paimon.utils.UriReaderFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DeltaWriter, DeltaWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/** A deletion vector of one data file produced by a [[PaimonDeltaWriter]] task. */
case class SerializedDeletionVector(dataFilePath: String, deletionVector: Array[Byte])

/**
 * The [[WriterCommitMessage]] of a [[PaimonDeltaWriter]] task: the commit messages of the appended
 * data files (inserted rows and the new versions of updated rows) plus the per-file deletion
 * bitmaps of the deleted rows.
 */
case class PaimonDeltaWriteTaskResult(
    dataWriteResult: Option[InnerTableWriteTaskResult],
    deletionVectors: Array[SerializedDeletionVector])
  extends WriteTaskResult {

  override def commitMessages(): Seq[CommitMessage] =
    dataWriteResult.map(_.commitMessages()).getOrElse(Seq.empty)
}

case class PaimonDeltaWriterFactory(
    writeBuilder: BatchWriteBuilder,
    rowSchema: StructType,
    coreOptions: CoreOptions,
    uriReaderFactory: UriReaderFactory,
    filePathOrdinal: Int,
    rowIndexOrdinal: Int)
  extends DeltaWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DeltaWriter[InternalRow] =
    PaimonDeltaWriter(
      writeBuilder,
      rowSchema,
      coreOptions,
      uriReaderFactory,
      filePathOrdinal,
      rowIndexOrdinal)
}

/**
 * A [[DeltaWriter]] for deletion-vector enabled append tables. Deleted rows are accumulated as
 * per-file bitmaps keyed by the `__paimon_file_path` and `__paimon_row_index` row ID; the driver
 * merges the bitmaps across tasks and persists them as deletion-vector index files at commit time,
 * because Spark does not cluster the delta rows by file and several tasks may delete rows of the
 * same data file. Inserted rows and the new versions of updated rows go through the regular append
 * writer.
 */
case class PaimonDeltaWriter(
    writeBuilder: BatchWriteBuilder,
    rowSchema: StructType,
    coreOptions: CoreOptions,
    uriReaderFactory: UriReaderFactory,
    filePathOrdinal: Int,
    rowIndexOrdinal: Int)
  extends DeltaWriter[InternalRow] {

  private val deletionVectors = mutable.HashMap.empty[String, DeletionVector]

  private var deletedRecords = 0L
  private var updatedRecords = 0L
  private var insertedRecords = 0L

  // Only UPDATE and MERGE INTO write rows; DELETE never initializes the append writer.
  private var appendWriter: Option[PaimonV2DataWriter] = None

  private def getOrCreateAppendWriter: PaimonV2DataWriter = {
    appendWriter.getOrElse {
      val writer =
        PaimonV2DataWriter(writeBuilder, rowSchema, rowSchema, coreOptions, uriReaderFactory)
      appendWriter = Some(writer)
      writer
    }
  }

  private def markDeleted(id: InternalRow): Unit = {
    val filePath = id.getUTF8String(filePathOrdinal).toString
    val dv = deletionVectors.getOrElseUpdate(
      filePath,
      if (coreOptions.deletionVectorBitmap64()) new Bitmap64DeletionVector()
      else new BitmapDeletionVector())
    dv.delete(id.getLong(rowIndexOrdinal))
  }

  override def delete(metadata: InternalRow, id: InternalRow): Unit = {
    markDeleted(id)
    deletedRecords += 1
  }

  override def update(metadata: InternalRow, id: InternalRow, row: InternalRow): Unit = {
    markDeleted(id)
    getOrCreateAppendWriter.write(row)
    updatedRecords += 1
  }

  override def insert(row: InternalRow): Unit = {
    getOrCreateAppendWriter.write(row)
    insertedRecords += 1
  }

  override def commit(): WriterCommitMessage = {
    val dataWriteResult = appendWriter.map(_.commit)
    val serialized = deletionVectors.map {
      case (filePath, dv) =>
        SerializedDeletionVector(filePath, DeletionVector.serializeToBytes(dv))
    }.toArray
    PaimonDeltaWriteTaskResult(dataWriteResult, serialized)
  }

  override def abort(): Unit = appendWriter.foreach(_.abort())

  override def close(): Unit = appendWriter.foreach(_.close())

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    Array(
      PaimonDeletedRecordsTaskMetric(deletedRecords),
      PaimonUpdatedRecordsTaskMetric(updatedRecords),
      PaimonInsertedRecordsTaskMetric(insertedRecords)
    ) ++ appendWriter.map(_.currentMetricsValues()).getOrElse(Array.empty[CustomTaskMetric])
  }
}
