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
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.table.DataTable
import org.apache.paimon.table.source.{DataSplit, SnapshotNotExistPlan, StreamDataTableScan}
import org.apache.paimon.table.source.TableScan.Plan
import org.apache.paimon.table.source.snapshot.StartingContext
import org.apache.paimon.utils.{InternalRowPartitionComputer, TypeUtils}

import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable

case class IndexedDataSplit(snapshotId: Long, index: Long, entry: DataSplit) {

  // Keep the existing three-field case class API while carrying planning metadata internally.
  private var totalSplitsValue: Long = -1L

  private[spark] def totalSplits: Option[Long] =
    if (totalSplitsValue < 0) None else Some(totalSplitsValue)

  private[spark] def withTotalSplits(totalSplits: Long): IndexedDataSplit = {
    require(totalSplits > 0, s"Total splits must be positive, but was $totalSplits.")
    totalSplitsValue = totalSplits
    this
  }
}

private case class BatchResult(
    indexedDataSplits: Array[IndexedDataSplit],
    emptyFullSnapshotNextId: Option[Long])

private[spark] trait StreamHelper {

  def table: DataTable

  val initOffset: PaimonSourceOffset

  var lastTriggerMillis: Long

  protected def includeSnapshotCompletionInOffset: Boolean = false

  private lazy val streamScan: StreamDataTableScan =
    table.newStreamScan().dropStats().asInstanceOf[StreamDataTableScan]

  private lazy val partitionSchema: StructType =
    SparkTypeUtils.fromPaimonRowType(TypeUtils.project(table.rowType(), table.partitionKeys()))

  private lazy val partitionComputer: InternalRowPartitionComputer = {
    val options = new CoreOptions(table.options)
    new InternalRowPartitionComputer(
      options.partitionDefaultName,
      TypeUtils.project(table.rowType(), table.partitionKeys()),
      table.partitionKeys().asScala.toArray,
      options.legacyPartitionName()
    )
  }

  // Used to get the initial offset.
  lazy val streamScanStartingContext: StartingContext = streamScan.startingContext()

  protected def notifyConsumerCheckpointComplete(nextSnapshot: Long): Unit =
    streamScan.notifyCheckpointComplete(nextSnapshot)

  def getLatestOffset(
      startOffset: PaimonSourceOffset,
      endOffset: Option[PaimonSourceOffset],
      limit: ReadLimit): Option[PaimonSourceOffset] = {
    val batchResult = getBatchResult(startOffset, endOffset, Some(limit))
    batchResult.indexedDataSplits.lastOption
      .map {
        ids =>
          val scanSnapshot =
            startOffset.scanSnapshot && ids.snapshotId.equals(startOffset.snapshotId)
          if (includeSnapshotCompletionInOffset) {
            val totalSplits = ids.totalSplits.getOrElse(throw new IllegalStateException(
              s"Missing total splits for snapshot ${ids.snapshotId}."))
            PaimonSourceOffset.withTotalSplits(ids.snapshotId, ids.index, scanSnapshot, totalSplits)
          } else {
            PaimonSourceOffset(ids.snapshotId, ids.index, scanSnapshot)
          }
      }
      .orElse {
        batchResult.emptyFullSnapshotNextId.map {
          nextSnapshotId =>
            if (includeSnapshotCompletionInOffset) {
              PaimonSourceOffset.withTotalSplits(
                nextSnapshotId,
                PaimonSourceOffset.INIT_OFFSET_INDEX,
                scanSnapshot = false,
                totalSplits = 0L)
            } else {
              PaimonSourceOffset(
                nextSnapshotId,
                PaimonSourceOffset.INIT_OFFSET_INDEX,
                scanSnapshot = false)
            }
        }
      }
  }

  def getBatch(
      startOffset: PaimonSourceOffset,
      endOffset: Option[PaimonSourceOffset],
      limit: Option[ReadLimit]): Array[IndexedDataSplit] = {
    getBatchResult(startOffset, endOffset, limit).indexedDataSplits
  }

  private def getBatchResult(
      startOffset: PaimonSourceOffset,
      endOffset: Option[PaimonSourceOffset],
      limit: Option[ReadLimit]): BatchResult = {
    if (startOffset != null) {
      if (startOffset.emptySnapshotCompleted) {
        streamScan.restore(startOffset.snapshotId, false)
      } else if (startOffset.snapshotCompleted) {
        streamScan.restore(startOffset.snapshotId + 1, false)
      } else {
        streamScan.restore(startOffset.snapshotId, startOffset.scanSnapshot)
      }
    }

    val readLimitGuard = limit.flatMap(PaimonReadLimits(_, lastTriggerMillis))
    var hasSplits = true
    var hasNonEmptyPlan = false
    var firstPlan = true
    var emptyFullSnapshotNextId: Option[Long] = None
    def continue: Boolean = {
      hasSplits && readLimitGuard.forall(_.hasCapacity) && endOffset.forall(
        streamScan.checkpoint() <= _.snapshotId)
    }

    val indexedDataSplits = mutable.ArrayBuffer.empty[IndexedDataSplit]
    while (continue) {
      val plan = streamScan.plan()
      if (plan.splits.isEmpty) {
        val isCompletedEmptyFullSnapshot =
          firstPlan &&
            startOffset != null &&
            startOffset.scanSnapshot &&
            (plan ne SnapshotNotExistPlan.INSTANCE)
        if (isCompletedEmptyFullSnapshot) {
          Option(streamScan.checkpoint()).foreach {
            nextSnapshotId => emptyFullSnapshotNextId = Some(nextSnapshotId)
          }
        } else {
          hasSplits = false
        }
      } else {
        hasNonEmptyPlan = true
        indexedDataSplits ++= convertPlanToIndexedSplits(plan)
          // Filter by (start, end]
          .filter(ids => inRange(ids, startOffset, endOffset))
          // Filter splits by read limits other than ReadMinRows.
          .takeWhile(s => readLimitGuard.forall(_.admit(s)))
      }
      firstPlan = false
    }

    // Filter splits by ReadMinRows read limit if exists.
    // If this batch doesn't meet the condition of ReadMinRows, then nothing will be returned.
    if (readLimitGuard.exists(_.skipBatch) && hasNonEmptyPlan) {
      BatchResult(Array.empty, None)
    } else {
      BatchResult(indexedDataSplits.toArray, emptyFullSnapshotNextId)
    }
  }

  private def needToScanCurrentSnapshot(snapshotId: Long): Boolean = {
    snapshotId == initOffset.snapshotId && initOffset.scanSnapshot
  }

  /** Sort the [[DataSplit]] list and index them. */
  private def convertPlanToIndexedSplits(plan: Plan): Array[IndexedDataSplit] = {
    val dataSplits =
      plan.splits().asScala.collect { case dataSplit: DataSplit => dataSplit }.toArray
    val snapshotId = dataSplits.head.snapshotId()
    val totalSplits = dataSplits.length.toLong

    dataSplits
      .sortWith((ds1, ds2) => compareByPartitionAndBucket(ds1, ds2) < 0)
      .zipWithIndex
      .map {
        case (split, idx) =>
          val indexedSplit = IndexedDataSplit(snapshotId, idx, split)
          if (includeSnapshotCompletionInOffset) {
            indexedSplit.withTotalSplits(totalSplits)
          } else {
            indexedSplit
          }
      }
  }

  private def compareByPartitionAndBucket(dataSplit1: DataSplit, dataSplit2: DataSplit): Int = {
    val res = compareBinaryRow(dataSplit1.partition, dataSplit2.partition)
    if (res == 0) {
      dataSplit1.bucket - dataSplit2.bucket
    } else {
      res
    }
  }

  private def compareBinaryRow(row1: BinaryRow, ror2: BinaryRow): Int = {
    val partitionPath1 = PartitioningUtils.getPathFragment(
      partitionComputer.generatePartValues(row1).asScala.toMap,
      partitionSchema)
    val partitionPath2 = PartitioningUtils.getPathFragment(
      partitionComputer.generatePartValues(ror2).asScala.toMap,
      partitionSchema)
    partitionPath1.compareTo(partitionPath2)
  }

  private def inRange(
      indexedDataSplit: IndexedDataSplit,
      start: PaimonSourceOffset,
      end: Option[PaimonSourceOffset]): Boolean = {
    PaimonSourceOffset.gt(indexedDataSplit, start) && end.forall(
      PaimonSourceOffset.le(indexedDataSplit, _))
  }

}
