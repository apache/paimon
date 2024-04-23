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

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.table.source.{DataSplit, DeletionFile, RawFile, Split}

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

trait ScanHelper {

  private val spark = SparkSession.active

  val coreOptions: CoreOptions

  private lazy val deletionVectors: Boolean = coreOptions.deletionVectorsEnabled()

  private lazy val openCostInBytes: Long = coreOptions.splitOpenFileCost()

  private lazy val leafNodeDefaultParallelism: Int = {
    spark.conf
      .get("spark.sql.leafNodeDefaultParallelism", spark.sparkContext.defaultParallelism.toString)
      .toInt
  }

  def reshuffleSplits(splits: Array[Split]): Array[Split] = {
    if (splits.length < leafNodeDefaultParallelism) {
      val (toReshuffle, reserved) = splits.partition {
        case split: DataSplit => split.beforeFiles().isEmpty && split.convertToRawFiles.isPresent
        case _ => false
      }
      val reshuffled = reshuffleSplits0(toReshuffle.collect { case ds: DataSplit => ds })
      reshuffled ++ reserved
    } else {
      splits
    }
  }

  private def reshuffleSplits0(splits: Array[DataSplit]): Array[DataSplit] = {
    val maxSplitBytes = computeMaxSplitBytes(splits)

    val newSplits = new ArrayBuffer[DataSplit]

    var currentSplit: Option[DataSplit] = None
    val currentDataFiles = new ArrayBuffer[DataFileMeta]
    val currentDeletionFiles = new ArrayBuffer[DeletionFile]
    val currentRawFiles = new ArrayBuffer[RawFile]
    var currentSize = 0L

    def closeDataSplit(): Unit = {
      if (currentSplit.nonEmpty && currentDataFiles.nonEmpty) {
        val newSplit =
          copyDataSplit(currentSplit.get, currentDataFiles, currentDeletionFiles, currentRawFiles)
        newSplits += newSplit
      }
      currentDataFiles.clear()
      currentDeletionFiles.clear()
      currentRawFiles.clear()
      currentSize = 0
    }

    splits.foreach {
      split =>
        currentSplit = Some(split)
        val hasRawFiles = split.convertToRawFiles().isPresent

        split.dataFiles().asScala.zipWithIndex.foreach {
          case (file, idx) =>
            if (currentSize + file.fileSize > maxSplitBytes) {
              closeDataSplit()
            }
            currentSize += file.fileSize + openCostInBytes
            currentDataFiles += file
            if (deletionVectors) {
              currentDeletionFiles += split.deletionFiles().get().get(idx)
            }
            if (hasRawFiles) {
              currentRawFiles += split.convertToRawFiles().get().get(idx)
            }
        }
        closeDataSplit()
    }

    newSplits.toArray
  }

  private def unpack(split: Split): Array[DataFileMeta] = {
    split match {
      case ds: DataSplit =>
        ds.dataFiles().asScala.toArray
      case _ => Array.empty
    }
  }

  private def copyDataSplit(
      split: DataSplit,
      dataFiles: Seq[DataFileMeta],
      deletionFiles: Seq[DeletionFile],
      rawFiles: Seq[RawFile]): DataSplit = {
    val builder = DataSplit
      .builder()
      .withSnapshot(split.snapshotId())
      .withPartition(split.partition())
      .withBucket(split.bucket())
      .withDataFiles(dataFiles.toList.asJava)
      .rawFiles(rawFiles.toList.asJava)
    if (deletionVectors) {
      builder.withDataDeletionFiles(deletionFiles.toList.asJava)
    }
    builder.build()
  }

  private def computeMaxSplitBytes(dataSplits: Seq[DataSplit]): Long = {
    val dataFiles = dataSplits.flatMap(unpack)
    val defaultMaxSplitBytes = spark.sessionState.conf.filesMaxPartitionBytes
    val minPartitionNum = spark.sessionState.conf.filesMinPartitionNum
      .getOrElse(leafNodeDefaultParallelism)
    val totalBytes = dataFiles.map(file => file.fileSize + openCostInBytes).sum
    val bytesPerCore = totalBytes / minPartitionNum

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }

}
