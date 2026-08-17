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
import org.apache.paimon.spark.{PaimonImplicits, PaimonMicroBatchInputPartition, PaimonMicroBatchMetadata, PaimonPartitionReaderFactory, SparkConnectorOptions}
import org.apache.paimon.table.DataTable
import org.apache.paimon.table.source.{DataSplit, OutOfRangeException, ReadBuilder}
import org.apache.paimon.utils.DataEvolutionUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SupportsTriggerAvailableNow}

import java.lang.{Long => JLong}
import java.util.{ArrayList, Collections}
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import scala.collection.mutable

class PaimonMicroBatchStream(
    originTable: DataTable,
    readBuilder: ReadBuilder,
    checkpointLocation: String)
  extends MicroBatchStream
  with SupportsTriggerAvailableNow
  with StreamHelper
  with Logging {

  private val options = Options.fromMap(table.options())
  private val coreOptions = new CoreOptions(options)
  private val consumerId = Option(coreOptions.consumerId())
  private val warnedLegacyConsumerSnapshots = mutable.Set.empty[Long]

  override protected def includeSnapshotCompletionInOffset: Boolean = consumerId.isDefined

  lazy val initOffset: PaimonSourceOffset = {
    val startingSnapshotId = streamScanStartingContext.getSnapshotId
    val startingScanSnapshot = streamScanStartingContext.getScanFullSnapshot.booleanValue()
    val initSnapshotId = Math.max(earliestReadableId(startingScanSnapshot), startingSnapshotId)
    val scanSnapshot = if (initSnapshotId == startingSnapshotId) {
      startingScanSnapshot
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

  private[spark] lazy val schemaLoader: Function[JLong, TableSchema] = {
    val schemaManager = table.schemaManager()
    val schemaCache = new ConcurrentHashMap[JLong, TableSchema]()
    val uncachedSchemaLoader: Function[JLong, TableSchema] =
      schemaId => schemaManager.schema(schemaId.longValue())
    schemaId => schemaCache.computeIfAbsent(schemaId, uncachedSchemaLoader)
  }

  override def getDefaultReadLimit: ReadLimit = defaultReadLimit

  override def prepareForTriggerAvailableNow(): Unit = {
    offsetForTriggerAvailableNow = getLatestOffset(initOffset, None, ReadLimit.allAvailable())
  }

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "That latestOffset(Offset, ReadLimit) method should be called instead of this method.")
  }

  private def normalizeStartOffset(start: Offset): PaimonSourceOffset = {
    val startOffset = PaimonSourceOffset(start)
    val resumeSnapshotId = if (startOffset.snapshotCompleted) {
      startOffset.snapshotId + 1
    } else {
      startOffset.snapshotId
    }
    val resumeScanSnapshot = if (startOffset.snapshotCompleted) {
      false
    } else {
      startOffset.scanSnapshot
    }
    val earliestReadable = earliestReadableId(resumeScanSnapshot)
    // Fall back to initOffset only when the checkpointed resume position has expired.
    // initOffset is recomputed from the current table state on every (re)start,
    // so with scan modes like latest-full it points at the current snapshot with
    // scanSnapshot=true. Clamping a still-valid checkpointed offset up to it made
    // a restarted query silently skip the changelog gap and re-scan the whole
    // snapshot, re-emitting every row as +I.
    if (resumeSnapshotId < earliestReadable) {
      logWarning(
        s"Checkpointed start offset $startOffset is no longer available " +
          s"(earliest readable snapshot or changelog: $earliestReadable), " +
          s"attempting recovery from $initOffset.")
      initOffset
    } else {
      startOffset
    }
  }

  override def latestOffset(start: Offset, limit: ReadLimit): Offset = {
    val startOffset = normalizeStartOffset(start)
    getLatestOffset(startOffset, offsetForTriggerAvailableNow, limit).map {
      offset =>
        lastTriggerMillis = System.currentTimeMillis()
        offset
    }.orNull
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset = normalizeStartOffset(start)
    val endOffset = PaimonSourceOffset(end)
    if (
      startOffset.snapshotId == endOffset.snapshotId &&
      startOffset.scanSnapshot != endOffset.scanSnapshot
    ) {
      throw new OutOfRangeException(
        s"Cannot plan Paimon micro-batch because normalized start offset $startOffset and " +
          s"logged end offset $endOffset use different scan modes. The checkpointed range " +
          "is no longer readable without changing its data.")
    }
    if (startOffset.compareTo(endOffset) > 0) {
      throw new OutOfRangeException(
        s"Cannot plan Paimon micro-batch because normalized start offset $startOffset is " +
          s"newer than logged end offset $endOffset. The data needed to replay the logged " +
          "range is no longer readable.")
    }

    val admittedSplits = getBatch(startOffset, Some(endOffset), None)
    val metadata = createMicroBatchMetadata(startOffset, endOffset, admittedSplits)
    admittedSplits
      .map(ids => PaimonMicroBatchInputPartition(Seq(ids.entry), metadata))
      .toArray[InputPartition]
  }

  private def createMicroBatchMetadata(
      startOffset: PaimonSourceOffset,
      endOffset: PaimonSourceOffset,
      admittedSplits: Array[IndexedDataSplit]): PaimonMicroBatchMetadata = {
    val splits = new ArrayList[DataSplit](admittedSplits.length)
    admittedSplits.foreach(split => splits.add(split.entry))
    val admittedSplitSnapshot = Collections.unmodifiableList(splits)

    new PaimonMicroBatchMetadata(
      checkpointLocation,
      startOffset.json(),
      endOffset.json(),
      admittedSplits.length,
      () => DataEvolutionUtils.collectWrittenColumnIds(admittedSplitSnapshot, schemaLoader)
    )
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
    val offset = PaimonSourceOffset(end)
    consumerId.foreach {
      id =>
        offset.totalSplits match {
          case Some(totalSplits) if offset.index >= totalSplits =>
            throw new IllegalStateException(
              s"Invalid Paimon source offset $offset: split index must be smaller than " +
                s"totalSplits ($totalSplits).")
          case Some(_) if offset.snapshotCompleted =>
            notifyConsumerCheckpointComplete(offset.snapshotId + 1)
          case Some(_) =>
            // The snapshot has not been fully consumed yet.
            ()
          case None =>
            if (warnedLegacyConsumerSnapshots.add(offset.snapshotId)) {
              logWarning(
                s"Cannot advance Paimon consumer '$id' for snapshot " +
                  s"${offset.snapshotId} because the committed Spark offset does not contain " +
                  "totalSplits. This can happen when recovering a checkpoint written by an " +
                  "older Paimon version. The consumer remains unchanged and the snapshot may " +
                  "be replayed.")
            }
        }
    }

    committedOffset = Some(offset)
    logInfo(s"$committedOffset is committed.")
  }

  override def stop(): Unit = {}

  override def table: DataTable = originTable

  private def earliestReadableId(scanSnapshot: Boolean): Long = {
    if (!scanSnapshot && coreOptions.changelogLifecycleDecoupled()) {
      val earliestChangelogId = table.changelogManager().earliestLongLivedChangelogId()
      if (earliestChangelogId == null) {
        table.snapshotManager().earliestSnapshotId().longValue()
      } else {
        earliestChangelogId.longValue()
      }
    } else {
      table.snapshotManager().earliestSnapshotId().longValue()
    }
  }

}
