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
import org.apache.paimon.table.source.{DataSplit, DeletionFile, Split}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

trait ScanHelper extends Logging {

  private val spark = SparkSession.active

  val coreOptions: CoreOptions

  private lazy val deletionVectors: Boolean = coreOptions.deletionVectorsEnabled()

  private lazy val openCostInBytes: Long = coreOptions.splitOpenFileCost()

  private lazy val leafNodeDefaultParallelism: Int = {
    spark.conf
      .get("spark.sql.leafNodeDefaultParallelism", spark.sparkContext.defaultParallelism.toString)
      .toInt
  }

  def getInputPartitions(splits: Array[Split]): Seq[PaimonInputPartition] = {
    val (toReshuffle, reserved) = splits.partition {
      case split: DataSplit => split.beforeFiles().isEmpty && split.rawConvertible()
      case _ => false
    }
    if (toReshuffle.nonEmpty) {
      val startTS = System.currentTimeMillis()
      val reshuffled = getInputPartitions(toReshuffle.collect { case ds: DataSplit => ds })
      val all = reserved.map(PaimonInputPartition.apply) ++ reshuffled
      val duration = System.currentTimeMillis() - startTS
      logInfo(
        s"Reshuffle splits from ${toReshuffle.length} to ${reshuffled.length} in $duration ms. Total number of splits is ${all.length}")
      all
    } else {
      splits.map(PaimonInputPartition.apply)
    }
  }

  private def getInputPartitions(splits: Array[DataSplit]): Array[PaimonInputPartition] = {
    val maxSplitBytes = computeMaxSplitBytes(splits)

    var currentSize = 0L
    val currentSplits = new ArrayBuffer[DataSplit]
    val partitions = new ArrayBuffer[PaimonInputPartition]

    var currentSplit: Option[DataSplit] = None
    val currentDataFiles = new ArrayBuffer[DataFileMeta]
    val currentDeletionFiles = new ArrayBuffer[DeletionFile]

    def closeDataSplit(): Unit = {
      if (currentSplit.nonEmpty && currentDataFiles.nonEmpty) {
        val newSplit =
          copyDataSplit(currentSplit.get, currentDataFiles.toSeq, currentDeletionFiles.toSeq)
        currentSplits += newSplit
      }
      currentDataFiles.clear()
      currentDeletionFiles.clear()
    }

    def closeInputPartition(): Unit = {
      closeDataSplit()
      if (currentSplit.nonEmpty) {
        partitions += PaimonInputPartition(currentSplits.toArray)
      }
      currentSplits.clear()
      currentSize = 0
    }

    splits.foreach {
      split =>
        if (!currentSplit.exists(withSamePartitionAndBucket(_, split))) {
          // close and open another data split
          closeDataSplit()
          currentSplit = Some(split)
        }

        val ddFiles = dataFileAndDeletionFiles(split)
        ddFiles.foreach {
          case (dataFile, deletionFile) =>
            val size = dataFile
              .fileSize() + openCostInBytes + Option(deletionFile).map(_.length()).getOrElse(0L)
            if (currentSize + size > maxSplitBytes) {
              closeInputPartition()
            }
            currentDataFiles += dataFile
            if (deletionVectors) {
              currentDeletionFiles += deletionFile
            }
            currentSize += size
        }
    }
    closeInputPartition()

    partitions.toArray
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
      deletionFiles: Seq[DeletionFile]): DataSplit = {
    val builder = DataSplit
      .builder()
      .withSnapshot(split.snapshotId())
      .withPartition(split.partition())
      .withBucket(split.bucket())
      .withDataFiles(dataFiles.toList.asJava)
      .rawConvertible(split.rawConvertible)
      .withBucketPath(split.bucketPath)
    if (deletionVectors) {
      builder.withDataDeletionFiles(deletionFiles.toList.asJava)
    }
    builder.build()
  }

  private def withSamePartitionAndBucket(split1: DataSplit, split2: DataSplit): Boolean = {
    split1.partition().equals(split2.partition()) && split1.bucket() == split2.bucket()
  }

  private def dataFileAndDeletionFiles(split: DataSplit): Array[(DataFileMeta, DeletionFile)] = {
    if (deletionVectors && split.deletionFiles().isPresent) {
      val deletionFiles = split.deletionFiles().get().asScala
      split.dataFiles().asScala.zip(deletionFiles).toArray
    } else {
      split.dataFiles().asScala.map((_, null)).toArray
    }
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
