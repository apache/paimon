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
import org.apache.paimon.spark.commands.WithFileStoreTable
import org.apache.paimon.table.source.{DataSplit, InnerStreamTableScan, ScanMode}
import org.apache.paimon.table.source.TableScan.Plan
import org.apache.paimon.table.source.snapshot.StartingContext
import org.apache.paimon.utils.RowDataPartitionComputer

import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable

case class IndexedDataSplit(snapshotId: Long, index: Long, entry: DataSplit, isLast: Boolean)

trait StreamHelper extends WithFileStoreTable {

  val initOffset: PaimonSourceOffset

  private lazy val streamScan: InnerStreamTableScan = table.newStreamScan()

  private lazy val partitionSchema: StructType =
    SparkTypeUtils.fromPaimonRowType(table.schema().logicalPartitionType())

  private lazy val partitionComputer: RowDataPartitionComputer = new RowDataPartitionComputer(
    new CoreOptions(table.schema.options).partitionDefaultName,
    table.schema.logicalPartitionType,
    table.schema.partitionKeys.asScala.toArray
  )

  // Used to get the initial offset.
  def getStartingContext: StartingContext = streamScan.startingContext()

  def getLatestOffset: PaimonSourceOffset = {
    val latestSnapshotId = table.snapshotManager().latestSnapshotId()
    val plan = if (needToScanCurrentSnapshot(latestSnapshotId)) {
      table
        .newSnapshotReader()
        .withSnapshot(latestSnapshotId)
        .withMode(ScanMode.ALL)
        .read()
    } else {
      table
        .newSnapshotReader()
        .withSnapshot(latestSnapshotId)
        .withMode(ScanMode.DELTA)
        .read()
    }
    val indexedDataSplits = convertPlanToIndexedSplits(plan)
    indexedDataSplits.lastOption
      .map(ids => PaimonSourceOffset(ids.snapshotId, ids.index, scanSnapshot = false))
      .orNull
  }

  def getBatch(
      startOffset: PaimonSourceOffset,
      endOffset: PaimonSourceOffset): Array[IndexedDataSplit] = {
    val indexedDataSplits = mutable.ArrayBuffer.empty[IndexedDataSplit]
    if (startOffset != null) {
      streamScan.restore(startOffset.snapshotId, needToScanCurrentSnapshot(startOffset.snapshotId))
    }
    var hasSplits = true
    while (hasSplits && streamScan.checkpoint() <= endOffset.snapshotId) {
      val plan = streamScan.plan()
      if (plan.splits.isEmpty) {
        hasSplits = false
      } else {
        indexedDataSplits ++= convertPlanToIndexedSplits(plan)
      }
    }
    indexedDataSplits.filter(ids => inRange(ids, startOffset, endOffset)).toArray
  }

  private def needToScanCurrentSnapshot(snapshotId: Long): Boolean = {
    snapshotId == initOffset.snapshotId && initOffset.scanSnapshot
  }

  /** Sort the [[DataSplit]] list and index them. */
  private def convertPlanToIndexedSplits(plan: Plan): Array[IndexedDataSplit] = {
    val dataSplits =
      plan.splits().asScala.collect { case dataSplit: DataSplit => dataSplit }.toArray
    val snapshotId = dataSplits.head.snapshotId()
    val length = dataSplits.length

    dataSplits
      .sortWith((ds1, ds2) => compareByPartitionAndBucket(ds1, ds2) < 0)
      .zipWithIndex
      .map {
        case (split, idx) =>
          IndexedDataSplit(snapshotId, idx, split, idx == length - 1)
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
      end: PaimonSourceOffset): Boolean = {
    val startRange = indexedDataSplit.snapshotId > start.snapshotId ||
      (indexedDataSplit.snapshotId == start.snapshotId && indexedDataSplit.index > start.index)
    val endRange = indexedDataSplit.snapshotId < end.snapshotId ||
      (indexedDataSplit.snapshotId == end.snapshotId && indexedDataSplit.index <= end.index)

    startRange && endRange
  }

}
