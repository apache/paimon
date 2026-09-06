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
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement}
import org.apache.paimon.spark.{PaimonDeletedRecordsTaskMetric, SparkTypeUtils}
import org.apache.paimon.spark.commands.SparkDataFileMeta
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.spark.rowops.PaimonCopyOnWriteScan
import org.apache.paimon.spark.schema.PaimonMetadataColumn.{FILE_PATH, ROW_ID, SEQUENCE_NUMBER}
import org.apache.paimon.table.{FileStoreTable, SpecialFields}
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage, CommitMessageImpl}
import org.apache.paimon.utils.UriReaderFactory

import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Business logic for Paimon batch writes, deliberately *not* extending
 * `org.apache.spark.sql.connector.write.BatchWrite`. Spark 4.1 added a default method
 * `BatchWrite.commit(WriterCommitMessage[], WriteSummary)` whose `WriteSummary` parameter type does
 * not exist on Spark 4.0; a class compiled against 4.1 that declares `extends BatchWrite` carries
 * the inherited `commit(.., WriteSummary)` signature in its method table, which JVM
 * `ObjectStreamClass.getPrivateMethod` lazy-links during Spark task serialization, crashing on 4.0
 * with `ClassNotFoundException: WriteSummary`. Keeping this base off `BatchWrite` lets common ship
 * the implementation once; per-version `paimon-spark{3,4}-common` modules supply a thin wrapper
 * that mixes in `BatchWrite`, and `paimon-spark-4.0/src/main` shadows that wrapper at the 4.0.2
 * compile target.
 */
abstract class PaimonBatchWriteBase(
    val table: FileStoreTable,
    val writeSchema: StructType,
    val dataSchema: StructType,
    val overwritePartitions: Option[Map[String, String]],
    val copyOnWriteScan: Option[PaimonCopyOnWriteScan],
    operationType: Option[Snapshot.Operation] = None)
  extends WriteHelper
  with Serializable {

  protected val metricRegistry = SparkMetricRegistry()

  @volatile protected var commitStarted: Boolean = false

  protected val batchWriteBuilder: BatchWriteBuilder = {
    val builder = table.newBatchWriteBuilder()
    overwritePartitions.foreach(partitions => builder.withOverwrite(partitions.asJava))
    builder
  }

  private val writeRowTracking: Boolean =
    coreOptions.rowTrackingEnabled() && copyOnWriteScan.isDefined

  private lazy val rtPaimonWriteType =
    SpecialFields.rowTypeWithRowTracking(table.rowType(), false, true)

  private lazy val rtWriteSchema =
    SparkTypeUtils.fromPaimonRowType(rtPaimonWriteType)

  private lazy val rtMetadataSchema =
    StructType(Seq(FILE_PATH, ROW_ID, SEQUENCE_NUMBER).map(_.toStructField))

  protected def createPaimonDataWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    val uriReaderFactory = uriReaderFactoryForBlobDescriptor
    (_: Int, _: Long) => {
      if (writeRowTracking) {
        createPaimonMetadataAwareDataWriter(uriReaderFactory)
      } else {
        PaimonV2DataWriter(
          batchWriteBuilder,
          writeSchema,
          dataSchema,
          coreOptions,
          uriReaderFactory)
      }
    }
  }

  private def createPaimonMetadataAwareDataWriter(
      uriReaderFactory: UriReaderFactory): PaimonV2DataWriter = {
    new PaimonV2MetadataAwareDataWriter(
      batchWriteBuilder,
      writeSchema,
      rtWriteSchema,
      dataSchema,
      rtMetadataSchema,
      coreOptions,
      uriReaderFactory,
      rtPaimonWriteType)
  }

  protected def commitMessages(messages: Array[WriterCommitMessage]): Unit = {
    commitStarted = true
    logInfo(s"Committing to table ${table.name()}")
    val batchTableCommit = batchWriteBuilder.newCommit()
    batchTableCommit.withMetricRegistry(metricRegistry)
    val operation = operationType.getOrElse(
      if (overwritePartitions.isDefined) Snapshot.Operation.OVERWRITE else Snapshot.Operation.WRITE)
    batchTableCommit.withOperation(operation)
    val addCommitMessage = WriteTaskResult.merge(messages)
    val deletedCommitMessage = copyOnWriteScan match {
      case Some(scan) => buildDeletedCommitMessage(scan.scannedFiles)
      case None => Seq.empty
    }
    val commitMessages = addCommitMessage ++ deletedCommitMessage
    try {
      val start = System.currentTimeMillis()
      batchTableCommit.commit(commitMessages.asJava)
      logInfo(s"Committed in ${System.currentTimeMillis() - start} ms")
    } finally {
      batchTableCommit.close()
    }
    postDriverMetrics(
      metricRegistry.buildSparkCommitMetrics() ++
        deletedRecordsTaskMetric(operation, addCommitMessage, deletedCommitMessage))
    postCommit(commitMessages)
  }

  /**
   * For copy-on-write DELETE, the number of deleted records is exactly the row count of the removed
   * files minus the row count of the rewritten files. This does not hold for UPDATE and MERGE,
   * whose rewritten files mix copied rows with modified rows.
   */
  private def deletedRecordsTaskMetric(
      operation: Snapshot.Operation,
      addedMessages: Seq[CommitMessage],
      deletedMessages: Seq[CommitMessage]): Array[CustomTaskMetric] = {
    if (copyOnWriteScan.isEmpty || operation != Snapshot.Operation.DELETE) {
      return Array.empty
    }
    val addedRecords = addedMessages
      .collect { case m: CommitMessageImpl => m }
      .flatMap(_.newFilesIncrement().newFiles().asScala)
      .map(_.rowCount())
      .sum
    val deletedRecords = deletedMessages
      .collect { case m: CommitMessageImpl => m }
      .flatMap(_.newFilesIncrement().deletedFiles().asScala)
      .map(_.rowCount())
      .sum
    Array(PaimonDeletedRecordsTaskMetric(deletedRecords - addedRecords))
  }

  protected def abortMessages(messages: Array[WriterCommitMessage]): Unit = {
    if (commitStarted) {
      logWarning(s"Skip abort cleanup for table ${table.name()} because commit has already started")
      return
    }

    logInfo(s"Aborting write to table ${table.name()}")
    val batchTableCommit = batchWriteBuilder.newCommit()
    try {
      val commitMessages = WriteTaskResult.merge(messages.filter(_ != null))
      batchTableCommit.abort(commitMessages.asJava)
    } finally {
      batchTableCommit.close()
    }
  }

  private def buildDeletedCommitMessage(
      deletedFiles: Seq[SparkDataFileMeta]): Seq[CommitMessage] = {
    logInfo(s"[V2 Write] Building deleted commit message for ${deletedFiles.size} files")
    deletedFiles
      .groupBy(f => (f.partition, f.bucket))
      .map {
        case ((partition, bucket), files) =>
          val deletedDataFileMetas = files.map(_.dataFileMeta).toList.asJava

          new CommitMessageImpl(
            partition,
            bucket,
            files.head.totalBuckets,
            new DataIncrement(
              Collections.emptyList[DataFileMeta],
              deletedDataFileMetas,
              Collections.emptyList[DataFileMeta]),
            new CompactIncrement(
              Collections.emptyList[DataFileMeta],
              Collections.emptyList[DataFileMeta],
              Collections.emptyList[DataFileMeta])
          )
      }
      .toSeq
  }
}
