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

import org.apache.paimon.Snapshot
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.deletionvectors.DeletionVector
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer
import org.apache.paimon.fs.Path
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement}
import org.apache.paimon.manifest.FileKind
import org.apache.paimon.spark.PaimonMetrics
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.spark.schema.PaimonMetadataColumn.{FILE_PATH_COLUMN, ROW_INDEX_COLUMN}
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.connector.write.{DeltaWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Business logic for Paimon delta batch writes on deletion-vector enabled append tables,
 * deliberately *not* extending `org.apache.spark.sql.connector.write.DeltaBatchWrite` for the same
 * `BatchWrite.commit(.., WriteSummary)` cross-version reason documented on
 * [[PaimonBatchWriteBase]]: per-version `paimon-spark{3,4}-common` modules supply a thin
 * `PaimonDeltaBatchWrite` wrapper that mixes in `DeltaBatchWrite`, and `paimon-spark-4.0/src/main`
 * shadows that wrapper at the 4.0.2 compile target.
 *
 * Deleted rows arrive as per-task per-file bitmaps and appended rows as regular data commit
 * messages (see [[PaimonDeltaWriter]]); this class merges the bitmaps across tasks at commit time,
 * merges the result with the files' existing deletion vectors through
 * [[BaseAppendDeleteFileMaintainer]], and commits the data messages together with index-only
 * deletion-vector [[CommitMessage]]s.
 */
abstract class PaimonDeltaWriteBase(
    val table: FileStoreTable,
    val rowSchema: StructType,
    val rowIdSchema: StructType,
    operationType: Snapshot.Operation,
    readSnapshotId: Option[Long])
  extends WriteHelper
  with Serializable {

  protected val metricRegistry: SparkMetricRegistry = SparkMetricRegistry()

  @volatile protected var commitStarted: Boolean = false

  protected val batchWriteBuilder: BatchWriteBuilder = table.newBatchWriteBuilder()

  protected def createPaimonDeltaWriterFactory(info: PhysicalWriteInfo): DeltaWriterFactory = {
    PaimonDeltaWriterFactory(
      batchWriteBuilder,
      rowSchema,
      coreOptions,
      uriReaderFactoryForBlobDescriptor,
      rowIdSchema.fieldIndex(FILE_PATH_COLUMN),
      rowIdSchema.fieldIndex(ROW_INDEX_COLUMN)
    )
  }

  protected def commitMessages(messages: Array[WriterCommitMessage]): Unit = {
    commitStarted = true
    logInfo(s"Committing delta write to table ${table.name()}")
    val taskMessages = messages.filter(_ != null)
    val dataMessages = WriteTaskResult.merge(taskMessages)
    val mergedDvs = mergeTaskDeletionVectors(taskMessages)
    val dvMessages = if (mergedDvs.isEmpty) Seq.empty else buildDvCommitMessages(mergedDvs)
    val commitMessages = dataMessages ++ dvMessages

    val batchTableCommit = batchWriteBuilder.newCommit()
    batchTableCommit.withMetricRegistry(metricRegistry)
    batchTableCommit.withOperation(operationType)
    try {
      val start = System.currentTimeMillis()
      batchTableCommit.commit(commitMessages.asJava)
      logInfo(s"Committed in ${System.currentTimeMillis() - start} ms")
    } finally {
      batchTableCommit.close()
    }
    // Drop the commit-level insertedRecords gauge: it is derived from
    // `CommitStats.deltaRecordsAppended` which mixes ADD and DELETE row counts and would
    // overwrite the exact per-row record metrics reported by the delta writer tasks.
    postDriverMetrics(
      metricRegistry
        .buildSparkCommitMetrics()
        .filterNot(_.name() == PaimonMetrics.INSERTED_RECORDS))
    postCommit(commitMessages)
  }

  protected def abortMessages(messages: Array[WriterCommitMessage]): Unit = {
    if (commitStarted) {
      logWarning(s"Skip abort cleanup for table ${table.name()} because commit has already started")
      return
    }
    // Deletion-vector index files are only written inside `commitMessages`, so an abort before
    // commit only needs to clean up the appended data files. If `commitMessages` fails after the
    // maintainer persisted the index files, they are left behind like any data file of a failed
    // commit and reclaimed by orphan-file cleanup.
    logInfo(s"Aborting delta write to table ${table.name()}")
    val batchTableCommit = batchWriteBuilder.newCommit()
    try {
      val dataMessages = WriteTaskResult.merge(messages.filter(_ != null))
      batchTableCommit.abort(dataMessages.asJava)
    } finally {
      batchTableCommit.close()
    }
  }

  /** Merge the per-task per-file bitmaps into one deletion vector per data file. */
  private def mergeTaskDeletionVectors(
      messages: Array[WriterCommitMessage]): mutable.HashMap[String, DeletionVector] = {
    val merged = mutable.HashMap.empty[String, DeletionVector]
    messages.foreach {
      case result: PaimonDeltaWriteTaskResult =>
        result.deletionVectors.foreach {
          serialized =>
            val dv = DeletionVector.deserializeFromBytes(serialized.deletionVector)
            merged.get(serialized.dataFilePath) match {
              case Some(existing) => existing.merge(dv)
              case None => merged.put(serialized.dataFilePath, dv)
            }
        }
      case other =>
        throw new IllegalStateException(s"Unexpected delta writer commit message: $other")
    }
    merged
  }

  /**
   * Persist the merged deletion vectors as index files and build index-only commit messages. The
   * maintainer merges the new deletions with the files' deletion vectors of the snapshot captured
   * BEFORE the scan (same contract as the V1 commands' readSnapshot): a concurrent deletion-vector
   * change of the same file then surfaces as a commit conflict instead of being silently merged,
   * which could otherwise resurrect a concurrently deleted row as the new version of an UPDATE.
   */
  private def buildDvCommitMessages(
      mergedDvs: mutable.HashMap[String, DeletionVector]): Seq[CommitMessage] = {
    val snapshot = readSnapshotId
      .map {
        id =>
          try {
            table.snapshotManager().snapshot(id)
          } catch {
            case e: Exception =>
              throw new IllegalStateException(
                s"Cannot load the read snapshot $id of table ${table.name()}, it may have " +
                  "expired during the write, please retry.",
                e)
          }
      }
      .getOrElse(throw new IllegalStateException(
        s"Table ${table.name()} had no snapshot when the scan was planned but rows were deleted."))
    val fileNameToDv = mergedDvs.map { case (path, dv) => new Path(path).getName -> dv }

    val fileNameToPartition = mutable.HashMap.empty[String, BinaryRow]
    val snapshotReader = table.newSnapshotReader().withSnapshot(snapshot)
    if (coreOptions.manifestDeleteFileDropStats()) {
      snapshotReader.dropStats()
    }
    snapshotReader.withDataFileNameFilter(fileName => fileNameToDv.contains(fileName))
    snapshotReader.read().splits().asScala.foreach {
      case split: DataSplit =>
        split
          .dataFiles()
          .asScala
          .foreach(f => fileNameToPartition.put(f.fileName(), split.partition()))
      case _ =>
    }

    val missingFiles = fileNameToDv.keySet.diff(fileNameToPartition.keySet)
    if (missingFiles.nonEmpty) {
      throw new IllegalStateException(
        s"Data files $missingFiles of table ${table.name()} are missing in the read snapshot " +
          s"${snapshot.id()}, its manifests may have expired during the write, please retry.")
    }

    fileNameToDv
      .groupBy { case (fileName, _) => fileNameToPartition(fileName) }
      .map {
        case (partition, dvs) =>
          val indexHandler = table.store().newIndexFileHandler()
          val maintainer =
            BaseAppendDeleteFileMaintainer.forUnawareAppend(indexHandler, snapshot, partition)
          dvs.foreach { case (fileName, dv) => maintainer.notifyNewDeletionVector(fileName, dv) }
          val indexEntries = maintainer.persist()
          val (added, deleted) = indexEntries.asScala.partition(_.kind() == FileKind.ADD)
          new CommitMessageImpl(
            maintainer.getPartition,
            maintainer.getBucket,
            null,
            new DataIncrement(
              Collections.emptyList[DataFileMeta],
              Collections.emptyList[DataFileMeta],
              Collections.emptyList[DataFileMeta],
              added.map(_.indexFile).asJava,
              deleted.map(_.indexFile).asJava
            ),
            CompactIncrement.emptyIncrement()
          ): CommitMessage
      }
      .toSeq
  }
}
