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

import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.fs.{FileIO, Path => PaimonPath}
import org.apache.paimon.options.{MemorySize, Options}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.spark.util.SplitUtils
import org.apache.paimon.table.source.ReadBuilder
import org.apache.paimon.table.source.Split
import org.apache.paimon.utils.UriReaderFactory

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.function.ToLongFunction

import scala.collection.JavaConverters._

/** A Spark [[Batch]] for paimon. */
case class PaimonBatch(
    inputPartitions: Seq[PaimonInputPartition],
    readBuilder: ReadBuilder,
    blobAsDescriptor: Boolean,
    metadataColumns: Seq[PaimonMetadataColumn] = Seq.empty)(
    uriReaderFactory: UriReaderFactory = null,
    blobDescriptorFieldIndices: Array[Int] = Array.empty[Int])
  extends Batch {

  private val log = LoggerFactory.getLogger(classOf[PaimonBatch])

  override def planInputPartitions(): Array[InputPartition] = {
    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    val pathOption =
      s"spark.paimon.${SparkConnectorOptions.SOURCE_SPLIT_METADATA_EXTERNALIZATION_PATH.key()}"
    val sharedPath = session
      .flatMap(_.conf.getOption(pathOption))
      .map(_.trim)
      .filter(_.nonEmpty)
    if (sharedPath.isEmpty) {
      return inputPartitions.map(_.asInstanceOf[InputPartition]).toArray
    }

    val spark = session.get
    val thresholdOption =
      s"spark.paimon.${SparkConnectorOptions.SOURCE_SPLIT_METADATA_INLINE_THRESHOLD.key()}"
    val threshold = spark.conf
      .getOption(thresholdOption)
      .map(MemorySize.parse(_))
      .getOrElse(SparkConnectorOptions.SOURCE_SPLIT_METADATA_INLINE_THRESHOLD.defaultValue())
      .getBytes
    require(threshold > 0, s"$thresholdOption must be positive")
    planSharedPartitions(spark, sharedPath.get, threshold)
  }

  private def planSharedPartitions(
      spark: SparkSession,
      basePath: String,
      threshold: Long): Array[InputPartition] = {
    val base = validateSharedPath(basePath, spark)
    val directory = new PaimonPath(
      base,
      s"paimon-split-stage-${System.currentTimeMillis()}-${UUID.randomUUID().toString}")
    val fileIO = sharedFileIO(spark, directory)
    val writer = new SharedSplitMetadataExternalizer.ScanWriter(fileIO, directory, threshold)
    var broadcastFileIO: Broadcast[FileIO] = null
    try {
      val planned = inputPartitions.map {
        case partition: SimplePaimonInputPartition =>
          externalize(
            partition,
            writer,
            spark,
            () => broadcastFileIO,
            value => broadcastFileIO = value)
        case partition: PaimonBucketedInputPartition =>
          externalize(
            partition,
            writer,
            spark,
            () => broadcastFileIO,
            value => broadcastFileIO = value)
        case partition => partition
      }
      writer.close()
      val result = planned.map(_.asInstanceOf[InputPartition]).toArray
      if (writer.hasExternalEntries()) {
        log.info(
          "Externalized {} Spark input partitions ({} bytes) to {}",
          Long.box(writer.externalEntryCount()),
          Long.box(writer.externalBytes()),
          directory)
        SplitMetadataCleanupManager.register(spark, fileIO, directory, broadcastFileIO)
      }
      result
    } catch {
      case error: Throwable =>
        closeAndCleanupShared(writer, fileIO, directory, error)
        if (broadcastFileIO != null) {
          try broadcastFileIO.destroy()
          catch {
            case cleanupError: Throwable => error.addSuppressed(cleanupError)
          }
        }
        throw error
    }
  }

  private def externalize(
      partition: PaimonInputPartition,
      writer: SharedSplitMetadataExternalizer.ScanWriter,
      spark: SparkSession,
      currentBroadcast: () => Broadcast[FileIO],
      setBroadcast: Broadcast[FileIO] => Unit): PaimonInputPartition = {
    val plannedSplits = partition.splits
    val encoded = writer.encode(
      plannedSplits.iterator.asJava,
      new ToLongFunction[Split] {
        override def applyAsLong(split: Split): Long = SplitUtils.splitSize(split)
      })
    if (!encoded.external()) {
      return partition
    }

    var broadcast = currentBroadcast()
    if (broadcast == null) {
      broadcast = spark.sparkContext.broadcast(writer.fileIO())
      setBroadcast(broadcast)
    }
    partition match {
      case bucketed: PaimonBucketedInputPartition =>
        new SharedExternalizedPaimonBucketedInputPartition(
          encoded.path(),
          encoded.offset(),
          encoded.length(),
          encoded.formatVersion(),
          encoded.splitCount(),
          encoded.rowCount(),
          encoded.estimatedDataBytes(),
          broadcast,
          plannedSplits,
          bucketed.bucket
        )
      case _ =>
        new SharedExternalizedPaimonInputPartition(
          encoded.path(),
          encoded.offset(),
          encoded.length(),
          encoded.formatVersion(),
          encoded.splitCount(),
          encoded.rowCount(),
          encoded.estimatedDataBytes(),
          broadcast,
          plannedSplits
        )
    }
  }

  private def sharedFileIO(spark: SparkSession, directory: PaimonPath): FileIO = {
    val prefix = "spark.paimon.source.split.metadata.externalization.fs."
    val options = new Options
    spark.conf.getAll.foreach {
      case (key, value) if key.startsWith(prefix) =>
        options.set(key.substring(prefix.length), value)
      case _ =>
    }
    FileIO.get(directory, CatalogContext.create(options, spark.sessionState.newHadoopConf()))
  }

  private def validateSharedPath(basePath: String, spark: SparkSession): PaimonPath = {
    val path = new PaimonPath(basePath)
    val scheme = path.toUri.getScheme
    val localSpark = spark.sparkContext.master.startsWith("local")
    require(
      localSpark || (scheme != null && !scheme.equalsIgnoreCase("file")),
      "spark.paimon.source.split.metadata.externalization.path must use a seekable file system " +
        "shared by every executor; local paths are only supported by local Spark masters"
    )
    path
  }

  private def closeAndCleanupShared(
      writer: SharedSplitMetadataExternalizer.ScanWriter,
      fileIO: FileIO,
      directory: PaimonPath,
      original: Throwable): Unit = {
    try writer.close()
    catch {
      case closeError: Throwable => original.addSuppressed(closeError)
    }
    try SharedSplitMetadataExternalizer.cleanup(fileIO, directory)
    catch {
      case cleanupError: Throwable => original.addSuppressed(cleanupError)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory =
    PaimonPartitionReaderFactory(
      readBuilder = readBuilder,
      metadataColumns = metadataColumns,
      blobAsDescriptor = blobAsDescriptor,
      uriReaderFactory = uriReaderFactory,
      blobDescriptorFieldIndices = blobDescriptorFieldIndices
    )
}
