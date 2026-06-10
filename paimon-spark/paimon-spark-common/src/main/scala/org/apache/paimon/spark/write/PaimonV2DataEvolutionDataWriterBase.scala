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
import org.apache.paimon.data.{BinaryRow, InternalRow => PaimonInternalRow}
import org.apache.paimon.io.{CompactIncrement, DataIncrement}
import org.apache.paimon.operation.AbstractFileStoreWrite
import org.apache.paimon.schema.TableSchema
import org.apache.paimon.spark.{SparkInternalRowWrapper, SparkUtils}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage, CommitMessageImpl, RowPartitionKeyExtractor, TableWriteImpl}
import org.apache.paimon.utils.RecordWriter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * V2 [[DataWrite]] for copy-on-write UPDATE on data-evolution tables.
 *
 * Data-evolution rows derive `_ROW_ID` from the file's `firstRowId` plus the row position, so
 * rewritten files must hold contiguous row-id runs and carry a `firstRowId` themselves instead of
 * writing physical row-tracking columns (files with a physical `_ROW_ID` column are not assigned a
 * `firstRowId` at commit and cannot be grouped by the data-evolution read path). The surviving rows
 * arrive range-distributed and sorted by `_ROW_ID` (see
 * [[PaimonWriteRequirement.dataEvolutionCopyOnWrite]]); this writer starts a new file run whenever
 * the row id is not consecutive or the partition changes, and assigns each produced file the run's
 * row id offset, mirroring `DataEvolutionTableDataWrite.PerFileWriter` and the commit skip rule in
 * `RowTrackingCommitUtils` (files with a non-null `firstRowId` keep it). Sequence numbers are not
 * preserved: the new files keep `minSequenceNumber == 0` and the commit stamps them with the new
 * snapshot id, the same as any data-evolution rewrite.
 */
abstract class PaimonV2DataEvolutionDataWriterBase(
    writeBuilder: BatchWriteBuilder,
    tableSchema: TableSchema,
    writeSchema: org.apache.spark.sql.types.StructType,
    dataSchema: org.apache.spark.sql.types.StructType,
    metadataSchema: org.apache.spark.sql.types.StructType,
    catalogContext: CatalogContext)
  extends InnerTableV2DataWrite
  with Logging {

  private val rowIdIndex = metadataSchema.fieldIndex(PaimonMetadataColumn.ROW_ID_COLUMN)

  private val partitionExtractor = new RowPartitionKeyExtractor(tableSchema)

  private val reusableWrapper =
    new SparkInternalRowWrapper(writeSchema, writeSchema.fields.length, dataSchema, catalogContext)

  private val ioManager = SparkUtils.createIOManager()
  private var currentRun: RunWriter = _
  private val commitMessages = ListBuffer[CommitMessage]()
  private var closed = false

  protected def writeWithMetadata(metadata: InternalRow, data: InternalRow): Unit = {
    assert(
      metadata != null && !metadata.isNullAt(rowIdIndex),
      "Data-evolution copy-on-write requires a non-null _ROW_ID for every written row.")
    val rowId = metadata.getLong(rowIdIndex)
    val paimonRow = reusableWrapper.replace(data)
    val partition = partitionExtractor.partition(paimonRow)
    if (currentRun == null || !currentRun.accepts(rowId, partition)) {
      finishCurrentRun()
      currentRun = new RunWriter(rowId, partition.copy())
    }
    currentRun.write(paimonRow)
  }

  override def write(record: InternalRow): Unit = {
    throw new UnsupportedOperationException(
      "Data-evolution V2 copy-on-write only rewrites existing rows; " +
        "rows without row-tracking metadata are not supported.")
  }

  override def commitImpl(): Seq[CommitMessage] = {
    finishCurrentRun()
    commitMessages.toSeq
  }

  override def abort(): Unit = close()

  override def close(): Unit = {
    if (!closed) {
      closed = true
      if (currentRun != null) {
        currentRun.closeQuietly()
        currentRun = null
      }
      try {
        ioManager.close()
      } catch {
        case e: Exception => throw new RuntimeException(e)
      }
    }
  }

  private def finishCurrentRun(): Unit = {
    if (currentRun != null) {
      commitMessages ++= currentRun.finish()
      currentRun = null
    }
  }

  /** Writes one contiguous row-id run; may roll into several files, all stay range-adjacent. */
  private class RunWriter(startRowId: Long, partition: BinaryRow) {

    private var nextRowId = startRowId

    // The run writer must only append the rewritten rows and never restore the bucket's
    // existing files. The bucket-unaware append write already enforces this
    // (AppendFileStoreWrite pins ignorePreviousFiles=true and uses NoopCompactManager), but
    // declare it explicitly rather than depend on which FileStoreWrite subclass we got.
    private val recordWriter: RecordWriter[PaimonInternalRow] = writeBuilder
      .newWrite()
      .withIOManager(ioManager)
      .asInstanceOf[TableWriteImpl[PaimonInternalRow]]
      .withIgnorePreviousFiles(true)
      .getWrite
      .asInstanceOf[AbstractFileStoreWrite[PaimonInternalRow]]
      .createWriter(partition, 0)

    def accepts(rowId: Long, rowPartition: BinaryRow): Boolean = {
      rowId == nextRowId && partition == rowPartition
    }

    def write(row: PaimonInternalRow): Unit = {
      recordWriter.write(row)
      nextRowId += 1
    }

    def finish(): Seq[CommitMessage] = {
      try {
        val result = recordWriter.prepareCommit(false)
        // The run writer is a fresh append writer over no existing files, so it must only
        // produce new files; compaction output or deletions would bypass the firstRowId
        // re-assignment below and must not be dropped silently.
        assert(
          result.compactIncrement().isEmpty &&
            result.newFilesIncrement().deletedFiles().isEmpty &&
            result.newFilesIncrement().changelogFiles().isEmpty,
          s"Data-evolution copy-on-write run writer produced increments other than new files: " +
            s"${result.compactIncrement()}, ${result.newFilesIncrement()}, this is a bug."
        )
        val newFiles = result.newFilesIncrement().newFiles().asScala
        var fileFirstRowId = startRowId
        // `firstRowId` is assigned by walking `newFiles` in their returned order and accumulating
        // `rowCount`, which relies on the append writer returning files in write order (rows arrive
        // sorted by `_ROW_ID`). A zero-row file would hand the same `firstRowId` to the next file
        // and corrupt the run, so reject it explicitly rather than let the total-count assert pass.
        val assigned = newFiles.map {
          file =>
            assert(
              file.rowCount() > 0,
              s"Data-evolution copy-on-write run [$startRowId, $nextRowId) produced an empty file " +
                s"${file.fileName()}, this is a bug.")
            val assignedFile = file.assignFirstRowId(fileFirstRowId)
            fileFirstRowId += file.rowCount()
            assignedFile
        }
        assert(
          fileFirstRowId == nextRowId,
          s"Data-evolution copy-on-write run [$startRowId, $nextRowId) wrote " +
            s"${fileFirstRowId - startRowId} rows into files, this is a bug."
        )
        if (assigned.isEmpty) {
          Nil
        } else {
          Seq(
            new CommitMessageImpl(
              partition,
              0,
              null,
              new DataIncrement(assigned.asJava, Collections.emptyList(), Collections.emptyList()),
              CompactIncrement.emptyIncrement()))
        }
      } finally {
        recordWriter.close()
      }
    }

    def closeQuietly(): Unit = {
      try {
        recordWriter.close()
      } catch {
        case e: Exception => logWarning("Failed to close data-evolution run writer", e)
      }
    }
  }
}
