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

import org.apache.paimon.options.Options
import org.apache.paimon.spark.{PaimonImplicits, PaimonPartitionReaderFactory, SparkConnectorOptions}
import org.apache.paimon.spark.catalog.PaimonInputPartition
import org.apache.paimon.table.DataTable
import org.apache.paimon.table.source.ReadBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SupportsTriggerAvailableNow}

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
      if (startOffset0.compareTo(initOffset) < 0) {
        initOffset
      } else {
        startOffset0
      }
    }
    val endOffset = PaimonSourceOffset(end)

    getBatch(startOffset, Some(endOffset), None)
      .map(ids => PaimonInputPartition(Array(ids.entry)))
      .toArray[InputPartition]
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    PaimonPartitionReaderFactory(readBuilder)
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
