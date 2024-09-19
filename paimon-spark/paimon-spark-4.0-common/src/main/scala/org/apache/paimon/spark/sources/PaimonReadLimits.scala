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

import org.apache.spark.sql.connector.read.streaming.{CompositeReadLimit, ReadAllAvailable, ReadLimit, ReadMaxFiles, ReadMaxRows, ReadMinRows}

import scala.collection.JavaConverters._

case class ReadMaxBytes(bytes: Long) extends ReadLimit

object PaimonReadLimits {

  def apply(limit: ReadLimit, lastTriggerMillis: Long): Option[PaimonReadLimitGuard] = limit match {
    case _: ReadAllAvailable => None
    case composite: CompositeReadLimit if composite.getReadLimits.isEmpty => None
    case composite: CompositeReadLimit if composite.getReadLimits.forall(supportedReadLimit) =>
      Some(PaimonReadLimitGuard(composite.getReadLimits, lastTriggerMillis))
    case limit: ReadLimit if supportedReadLimit(limit) =>
      Some(PaimonReadLimitGuard(Array(limit), lastTriggerMillis))
    case other =>
      throw new UnsupportedOperationException(s"Not supported read limit: ${other.toString}")
  }

  private def supportedReadLimit(limit: ReadLimit): Boolean = {
    limit match {
      case _: ReadAllAvailable => true
      case _: ReadMaxFiles => true
      case _: ReadMinRows => true
      case _: ReadMaxRows => true
      case _: ReadMaxBytes => true
      case _ => false
    }
  }
}

case class PaimonReadLimitGuard(limits: Array[ReadLimit], lastTriggerMillis: Long) {

  private val (minRowsReadLimits, otherReadLimits) = limits.partition(_.isInstanceOf[ReadMinRows])

  assert(minRowsReadLimits.length <= 1, "Paimon supports one ReadMinRows Read Limit at most.")

  private var acceptedFiles: Int = 0

  private var acceptedBytes: Long = 0

  private var acceptedRows: Long = 0

  private val minRowsReadLimit = minRowsReadLimits.collectFirst { case limit: ReadMinRows => limit }

  // may skip this batch if it can't reach the time that must return batch
  // and the number of rows is less than the threshold.
  private var _maySkipBatch =
    minRowsReadLimit.exists(System.currentTimeMillis() - lastTriggerMillis <= _.maxTriggerDelayMs)

  private var _hasCapacity = true

  def toReadLimit: ReadLimit = {
    ReadLimit.compositeLimit(limits)
  }

  def admit(indexedDataSplit: IndexedDataSplit): Boolean = {
    if (!_hasCapacity) {
      return false
    }

    val (rows, bytes) = getBytesAndRows(indexedDataSplit)

    _hasCapacity = otherReadLimits.forall {
      case maxFiles: ReadMaxFiles =>
        acceptedFiles < maxFiles.maxFiles
      case maxRows: ReadMaxRows =>
        acceptedRows < maxRows.maxRows
      case maxBytes: ReadMaxBytes =>
        acceptedBytes < maxBytes.bytes
      case limit =>
        throw new UnsupportedOperationException(s"Not supported read limit: ${limit.toString}")
    }

    acceptedFiles += 1
    acceptedRows += rows
    acceptedBytes += bytes

    if (_maySkipBatch) {
      _maySkipBatch = minRowsReadLimit.exists(acceptedRows < _.minRows)
    }

    _hasCapacity
  }

  def skipBatch: Boolean = _maySkipBatch

  def hasCapacity: Boolean = _hasCapacity

  private def getBytesAndRows(indexedDataSplit: IndexedDataSplit): (Long, Long) = {
    var rows = 0L
    var bytes = 0L
    indexedDataSplit.entry.dataFiles().asScala.foreach {
      file =>
        rows += file.rowCount()
        bytes += file.fileSize()
    }
    (rows, bytes)
  }
}
