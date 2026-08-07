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

package org.apache.paimon.spark.sources

import org.apache.paimon.CoreOptions
import org.apache.paimon.options.Options
import org.apache.paimon.schema.TableSchema
import org.apache.paimon.spark.{PaimonImplicits, PaimonInputPartition, PaimonMicroBatchInputPartition, PaimonMicroBatchMetadata, PaimonPartitionReaderFactory, SparkConnectorOptions}
import org.apache.paimon.table.DataTable
import org.apache.paimon.table.source.{AllColumns, ReadBuilder}
import org.apache.paimon.utils.DataEvolutionUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SupportsTriggerAvailableNow}

import java.lang.{Long => JLong}
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

private[spark] case class PlannedMicroBatch(
    admittedSplits: Array[IndexedDataSplit],
    metadata: PaimonMicroBatchMetadata)

class PaimonMicroBatchStream(
    originTable: DataTable,
    readBuilder: ReadBuilder,
    checkpointLocation: String)
  extends MicroBatchStream
  with SupportsTriggerAvailableNow
  with StreamHelper
  with Logging {

  private val options = Options.fromMap(table.options())

  lazy val initOffset: PaimonSourceOffset = {
    val initSnapshotId = Math.max(
      table.snapshotManager().earliestSnapshotId(),
      streamScanStartingContext.getSnapshotId)
    val scanSnapshot = if (initSnapshotId == streamScanStartingContext.getSnapshotId) {
      streamScanStartingContext.getScanFullSnapshot.booleanValue()
    } else {
      false
    }
    PaimonSourceOffset(initSnapshotId, PaimonSourceOffset.INIT_OFFSET_INDEX, scanSnapshot)
  }

  // the committed offset this is used to detect the validity of subsequent offsets
  private var committedOffset: Option[PaimonSourceOffset] = None

  // the timestamp when the batch is triggered the last time.
  // It will be reset when there is non-empty PaimonSourceOffset returned by calling "latestOffset".
  var lastTriggerMillis = 0L

  // the latest offset when call "prepareForTriggerAvailableNow"
  // the query will be terminated when data is consumed to this offset in "TriggerAvailableNow" mode.
  private var offsetForTriggerAvailableNow: Option[PaimonSourceOffset] = None

  private lazy val defaultReadLimit: ReadLimit = {
    import PaimonImplicits._

    val readLimits = mutable.ArrayBuffer.empty[ReadLimit]
    options.getOptional(SparkConnectorOptions.MAX_BYTES_PER_TRIGGER).foreach {
      bytes => readLimits += ReadMaxBytes(bytes)
    }
    options.getOptional(SparkConnectorOptions.MAX_FILES_PER_TRIGGER).foreach {
      files => readLimits += ReadLimit.maxFiles(files)
    }
    options.getOptional(SparkConnectorOptions.MAX_ROWS_PER_TRIGGER).foreach {
      rows => readLimits += ReadLimit.maxRows(rows)
    }
    val minRowsOptional = options.getOptional(SparkConnectorOptions.MIN_ROWS_PER_TRIGGER)
    val maxDelayMSOptional = options.getOptional(SparkConnectorOptions.MAX_DELAY_MS_PER_TRIGGER)
    if (minRowsOptional.isPresent && maxDelayMSOptional.isPresent) {
      readLimits += ReadLimit.minRows(minRowsOptional.get(), maxDelayMSOptional.get())
    } else if (minRowsOptional.isPresent || maxDelayMSOptional.isPresent) {
      throw new IllegalArgumentException(
        "Can't provide only one of read.stream.minRowsPerTrigger and read.stream.maxTriggerDelayMs.")
    }

    PaimonReadLimits(ReadLimit.compositeLimit(readLimits.toArray), lastTriggerMillis)
      .map(_.toReadLimit)
      .getOrElse(ReadLimit.allAvailable())
  }

  private lazy val blobAsDescriptor: Boolean = options.get(CoreOptions.BLOB_AS_DESCRIPTOR)

  private lazy val batchWrittenColumnsEnabled: Boolean =
    options.get(SparkConnectorOptions.BATCH_WRITTEN_COLUMNS_ENABLED)

  private[spark] lazy val schemaLoader: Function[JLong, TableSchema] = {
    val schemaManager = table.schemaManager()
    val schemaCache = new ConcurrentHashMap[JLong, TableSchema]()
    val uncachedSchemaLoader = new Function[JLong, TableSchema] {
      override def apply(schemaId: JLong): TableSchema =
        schemaManager.schema(schemaId.longValue())
    }
    new Function[JLong, TableSchema] {
      override def apply(schemaId: JLong): TableSchema =
        schemaCache.computeIfAbsent(schemaId, uncachedSchemaLoader)
    }
  }

  override def getDefaultReadLimit: ReadLimit = defaultReadLimit

  override def prepareForTriggerAvailableNow(): Unit = {
    offsetForTriggerAvailableNow = getLatestOffset(initOffset, None, ReadLimit.allAvailable())
  }

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "That latestOffset(Offset, ReadLimit) method should be called instead of this method.")
  }

  override def latestOffset(start: Offset, limit: ReadLimit): Offset = {
    val startOffset = PaimonSourceOffset(start)
    getLatestOffset(startOffset, offsetForTriggerAvailableNow, limit).map {
      offset =>
        lastTriggerMillis = System.currentTimeMillis()
        offset
    }.orNull
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset = {
      val startOffset0 = PaimonSourceOffset(start)
      // Fall back to initOffset only when the checkpointed snapshot has expired.
      // initOffset is recomputed from the current table state on every (re)start,
      // so with scan modes like latest-full it points at the current snapshot with
      // scanSnapshot=true. Clamping a still-valid checkpointed offset up to it made
      // a restarted query silently skip the changelog gap and re-scan the whole
      // snapshot, re-emitting every row as +I.
      if (startOffset0.snapshotId < table.snapshotManager().earliestSnapshotId()) {
        logWarning(
          s"Checkpointed start offset $startOffset0 is no longer available " +
            s"(earliest snapshot: ${table.snapshotManager().earliestSnapshotId()}), " +
            s"falling back to $initOffset.")
        initOffset
      } else {
        startOffset0
      }
    }
    val endOffset = PaimonSourceOffset(end)

    val admittedSplits = getBatch(startOffset, Some(endOffset), None)
    if (!batchWrittenColumnsEnabled) {
      admittedSplits
        .map(ids => PaimonInputPartition(ids.entry))
        .toArray[InputPartition]
    } else {
      val plannedBatch = createPlannedMicroBatch(startOffset, endOffset, admittedSplits)
      plannedBatch.admittedSplits
        .map(ids => PaimonMicroBatchInputPartition(Seq(ids.entry), plannedBatch.metadata))
        .toArray[InputPartition]
    }
  }

  private def createPlannedMicroBatch(
      startOffset: PaimonSourceOffset,
      endOffset: PaimonSourceOffset,
      admittedSplits: Array[IndexedDataSplit]): PlannedMicroBatch = {
    val writtenColumns =
      try {
        DataEvolutionUtils.collectWrittenColumns(
          admittedSplits.map(_.entry).toSeq.asJava,
          schemaLoader
        )
      } catch {
        case NonFatal(e) =>
          logWarning("Failed to collect written columns for a micro-batch; using all columns.", e)
          AllColumns.INSTANCE
      }

    val metadata = PaimonMicroBatchMetadata(
      checkpointLocation,
      startOffset.json(),
      endOffset.json(),
      admittedSplits.length,
      writtenColumns)
    PlannedMicroBatch(admittedSplits, metadata)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    PaimonPartitionReaderFactory(readBuilder, blobAsDescriptor = blobAsDescriptor)
  }

  override def initialOffset(): Offset = {
    initOffset
  }

  override def deserializeOffset(json: String): Offset = {
    PaimonSourceOffset(json)
  }

  override def commit(end: Offset): Unit = {
    committedOffset = Some(PaimonSourceOffset(end))
    logInfo(s"$committedOffset is committed.")
  }

  override def stop(): Unit = {}

  override def table: DataTable = originTable

}
