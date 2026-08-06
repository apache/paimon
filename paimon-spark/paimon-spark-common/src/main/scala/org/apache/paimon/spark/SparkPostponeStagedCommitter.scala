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

import org.apache.paimon.{CoreOptions, KeyValue, Snapshot}
import org.apache.paimon.data.{BinaryRow, InternalRow}
import org.apache.paimon.data.serializer.InternalRowSerializer
import org.apache.paimon.operation.FileSystemWriteRestore
import org.apache.paimon.options.Options
import org.apache.paimon.postpone.BucketFiles
import org.apache.paimon.reader.RecordReaderIterator
import org.apache.paimon.table.{BucketMode, FileStoreTable, PostponeUtils}
import org.apache.paimon.table.PostponeUtils.PostponeBucketRouter
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl, CommitMessageSerializer, PostponeFixedBucketWriteBuilder, TableWriteImpl}
import org.apache.paimon.table.source.{DataSplit, PostponeMergePlan, PostponeMergeReadBuilder, SplitSerializer}
import org.apache.paimon.types.RowKind
import org.apache.paimon.utils.{IteratorRecordReader, SerializationUtils}

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{PaimonUtils, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Completes a fixed-bucket batch write from uncommitted postpone files.
 *
 * Job 1 has already materialized the source rows into bucket -2. This coordinator derives the
 * actual target partitions and bucket counts from those files. When necessary it first rescales
 * existing real-bucket data, then writes and commits the current batch to real buckets. Committed
 * postpone files are left to the regular postpone compaction path.
 */
private[spark] class SparkPostponeStagedCommitter(
    table: FileStoreTable,
    @transient spark: SparkSession,
    baseSnapshotId: Option[Long],
    overwritePartitionSpec: Option[Map[String, String]])
  extends Serializable {

  import SparkPostponeStagedCommitter._

  private val coreOptions = table.coreOptions()
  private val fixedWriteCommitUser = coreOptions.createCommitUser()
  private val fixedWriteTable = {
    val options = new java.util.HashMap[String, String]()
    options.put(
      CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key(),
      baseSnapshotId.getOrElse(0L).toString)
    // Every overwrite in this coordinator supplies its exact BinaryRow partitions. Disable the
    // table-level dynamic rewrite so an empty rewritten partition is still removed, and implement
    // INSERT OVERWRITE dynamic-partition semantics explicitly in commitCurrentBatch.
    options.put(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false")
    table.copy(options)
  }
  private val fixedWriteBuilder = fixedWriteTable.newPostponeFixedBucketWriteBuilder()
  private val rescaleWriteBuilder = {
    val options = new java.util.HashMap[String, String]()
    options.put(CoreOptions.CHANGELOG_PRODUCER.key(), CoreOptions.ChangelogProducer.NONE.toString)
    fixedWriteTable.copy(options).newPostponeFixedBucketWriteBuilder()
  }
  private val ignoreEmptyCommit = new Options(table.options())
    .getOptional(CoreOptions.SNAPSHOT_IGNORE_EMPTY_COMMIT)
    .orElse(true)

  def commit(
      stagedMessages: Seq[CommitMessage],
      operation: Snapshot.Operation): Seq[CommitMessage] = {
    val (stageMessages, passThroughMessages) = stagedMessages.partition(isStageMessage)
    var cleanupStageMessages = stageMessages
    var pendingRescaleMessages = Seq.empty[CommitMessage]
    var pendingFinalMessages = passThroughMessages
    var rescaleCommitStarted = false
    var finalCommitStarted = false

    try {
      val normalizedStageMessages = mergeBucketMessages(stageMessages)
      cleanupStageMessages = normalizedStageMessages
      val stagedSplits = splitsFromMessages(normalizedStageMessages)
      val stats = aggregateStats(stagedSplits)
      val touchedPartitions = stats.keysIterator.map(_.copy()).toSeq
      if (touchedPartitions.isEmpty) {
        if (
          overwritePartitionSpec.isDefined || passThroughMessages.nonEmpty || !ignoreEmptyCommit
        ) {
          finalCommitStarted = true
          commitCurrentBatch(passThroughMessages, Seq.empty, operation)
        }
        return passThroughMessages
      }

      // Overwrite removes the previous partition contents, so its bucket layout must not
      // constrain the replacement data. It is also unnecessary to scan the old layout here.
      val existingBuckets: Map[BinaryRow, Int] =
        if (overwritePartitionSpec.isDefined) {
          Map.empty
        } else {
          baseSnapshotId
            .map(
              id =>
                PostponeUtils
                  .getKnownNumBuckets(table, id, touchedPartitions.asJava)
                  .asScala
                  .iterator
                  .map { case (partition, buckets) => partition -> buckets.intValue() }
                  .toMap)
            .getOrElse(Map.empty[BinaryRow, Int])
        }
      val decisions = touchedPartitions.map {
        partition =>
          val stage = stats(partition)
          val decision = PostponeUtils.decideFixedBucketNum(
            stage.rowCount,
            stage.fileSize,
            existingBuckets.get(partition).map(Int.box).orNull,
            coreOptions
          )
          partition -> decision
      }.toMap

      val targetBuckets = decisions.map {
        case (partition, decision) => partition -> decision.targetBucketNum()
      }

      if (overwritePartitionSpec.isDefined) {
        val rewrittenMessages =
          writePostponeRecords(
            stagedSplits,
            targetBuckets,
            replacePreviousFiles = true,
            restoreSnapshotId = None)
        pendingFinalMessages = rewrittenMessages ++ passThroughMessages
        finalCommitStarted = true
        commitCurrentBatch(pendingFinalMessages, touchedPartitions, operation)
        return pendingFinalMessages
      }

      val rescaleBucketNums = decisions.collect {
        case (partition, decision) if decision.requiresRescale() =>
          partition -> decision.targetBucketNum()
      }
      if (rescaleBucketNums.nonEmpty) {
        pendingRescaleMessages =
          rewriteRealBuckets(readRealSplits(rescaleBucketNums.keys.toSeq), rescaleBucketNums)
        rescaleCommitStarted = true
        commitRescale(pendingRescaleMessages, rescaleBucketNums)
        pendingRescaleMessages = Seq.empty
      }

      // After rescale, the current batch must restore from the new snapshot. Otherwise it restores
      // from the snapshot captured before the staging job.
      val currentWriteSnapshotId =
        if (rescaleBucketNums.nonEmpty) {
          Option(table.snapshotManager().latestSnapshot()).map(_.id())
        } else {
          baseSnapshotId
        }
      val currentMessages =
        writePostponeRecords(
          stagedSplits,
          targetBuckets,
          replacePreviousFiles = false,
          restoreSnapshotId = currentWriteSnapshotId)
      pendingFinalMessages = currentMessages ++ passThroughMessages
      finalCommitStarted = true
      commitCurrentBatch(pendingFinalMessages, touchedPartitions, operation)
      pendingFinalMessages
    } catch {
      case error: Throwable =>
        if (!rescaleCommitStarted && pendingRescaleMessages.nonEmpty) {
          abortMessages(pendingRescaleMessages)
        }
        if (!finalCommitStarted && pendingFinalMessages.nonEmpty) {
          abortMessages(pendingFinalMessages)
        }
        throw error
    } finally {
      // Staged files were never visible, so they are always safe to delete, including when the
      // final commit returned an unknown result.
      abortMessages(cleanupStageMessages)
    }
  }

  private def aggregateStats(stagedSplits: Seq[DataSplit]): Map[BinaryRow, PartitionStats] = {
    val result = mutable.HashMap.empty[BinaryRow, PartitionStats]
    stagedSplits.foreach {
      split =>
        val previous = result.getOrElse(split.partition(), PartitionStats(0L, 0L))
        val splitFileSize = split
          .dataFiles()
          .asScala
          .iterator
          .map(_.fileSize())
          .foldLeft(0L)(Math.addExact)
        result.put(
          split.partition().copy(),
          PartitionStats(
            Math.addExact(previous.rowCount, split.rowCount()),
            Math.addExact(previous.fileSize, splitFileSize)))
    }
    result.toMap
  }

  private def splitsFromMessages(messages: Seq[CommitMessage]): Seq[DataSplit] = {
    messages.collect {
      case message: CommitMessageImpl if message.bucket() == BucketMode.POSTPONE_BUCKET =>
        val files = message.newFilesIncrement().newFiles().asScala ++
          message.compactIncrement().compactAfter().asScala
        if (files.isEmpty) {
          None
        } else {
          Some(
            DataSplit
              .builder()
              .withSnapshot(baseSnapshotId.getOrElse(0L))
              .withPartition(message.partition())
              .withBucket(BucketMode.POSTPONE_BUCKET)
              .withBucketPath(
                table
                  .store()
                  .pathFactory()
                  .bucketPath(message.partition(), BucketMode.POSTPONE_BUCKET)
                  .toString)
              .withTotalBuckets(message.totalBuckets())
              .withDataFiles(files.asJava)
              .isStreaming(false)
              .rawConvertible(false)
              .build())
        }
    }.flatten
  }

  private def isStageMessage(message: CommitMessage): Boolean = message match {
    case commit: CommitMessageImpl => commit.bucket() == BucketMode.POSTPONE_BUCKET
    case _ => false
  }

  private def readRealSplits(partitions: Seq[BinaryRow]): Seq[DataSplit] = {
    baseSnapshotId.toSeq.flatMap {
      snapshotId =>
        table
          .newSnapshotReader()
          .withSnapshot(snapshotId)
          .withPartitionFilter(partitions.asJava)
          .onlyReadRealBuckets()
          .read()
          .dataSplits()
          .asScala
    }
  }

  private def writePostponeRecords(
      postponeSplits: Seq[DataSplit],
      bucketNums: Map[BinaryRow, Int],
      replacePreviousFiles: Boolean,
      restoreSnapshotId: Option[Long]): Seq[CommitMessage] = {
    val router = createRouter(bucketNums)
    val readBuilder = PostponeMergeReadBuilder.createForSplits(table)
    val corePlan = readBuilder.plan(Seq.empty[DataSplit].asJava, postponeSplits.asJava, router)
    val records = routePostponeRecords(readBuilder, corePlan)
    val partitioned = records.repartitionAndSortWithinPartitions(
      new BucketGroupPartitioner(shuffleParallelism(bucketNums)))
    writeRoutedRecords(
      partitioned,
      bucketNums,
      replacePreviousFiles,
      restoreSnapshotId = restoreSnapshotId,
      writeBuilder = fixedWriteBuilder)
  }

  private def rewriteRealBuckets(
      realSplits: Seq[DataSplit],
      targetBucketNums: Map[BinaryRow, Int]): Seq[CommitMessage] = {
    val targetRouter = createRouter(targetBucketNums)
    val readBuilder = PostponeMergeReadBuilder.createForSplits(table)
    val corePlan = readBuilder.plan(realSplits.asJava, Seq.empty[DataSplit].asJava, targetRouter)
    val routedRecords = routeRealRecords(readBuilder, corePlan)
      .repartitionAndSortWithinPartitions(
        new BucketGroupPartitioner(shuffleParallelism(targetBucketNums)))
    writeRoutedRecords(
      routedRecords,
      targetBucketNums,
      replacePreviousFiles = true,
      restoreSnapshotId = None,
      writeBuilder = rescaleWriteBuilder)
  }

  private def writeRoutedRecords(
      records: RDD[(BucketOrderKey, PostponeRecord)],
      bucketNums: Map[BinaryRow, Int],
      replacePreviousFiles: Boolean,
      restoreSnapshotId: Option[Long],
      writeBuilder: PostponeFixedBucketWriteBuilder): Seq[CommitMessage] = {
    val written = records.mapPartitions {
      input =>
        if (!input.hasNext) {
          Iterator.empty
        } else {
          val ioManager = SparkUtils.createIOManager()
          val write = writeBuilder
            .newWrite(fixedWriteCommitUser, null)
            .withIOManager(ioManager)
            .asInstanceOf[TableWriteImpl[InternalRow]]
          if (replacePreviousFiles) {
            write.withIgnorePreviousFiles(true)
          } else {
            restoreSnapshotId.foreach {
              id =>
                write.withWriteRestore(
                  new FileSystemWriteRestore(
                    table.coreOptions(),
                    table.snapshotManager(),
                    table.store().newScan(),
                    table.store().newIndexFileHandler(),
                    id))
            }
          }
          try {
            val buffered = input.buffered
            while (buffered.hasNext) {
              val first = buffered.head._1
              val partition = deserializePartition(first.partition)
              val bucket = first.bucket
              while (buffered.hasNext && buffered.head._1.sameBucket(first)) {
                val record = buffered.next()._2
                val row = SerializationUtils.deserializeBinaryRow(record.value)
                row.setRowKind(RowKind.fromByteValue(record.rowKind))
                write.writeAndReturn(row, bucket, bucketNums(partition))
              }
            }
            val serializer = new CommitMessageSerializer()
            val commitMessages = write.prepareCommit().asScala.toVector
            reportOutputMetrics(commitMessages)
            Iterator.single(
              commitMessages
                .map(serializer.serialize))
          } finally {
            try {
              write.close()
            } finally {
              ioManager.close()
            }
          }
        }
    }
    deserializeCommitMessages(written.collect().iterator.flatten.toVector)
  }

  private def deserializeCommitMessages(
      serializedMessages: Iterable[Array[Byte]]): Seq[CommitMessage] = {
    val serializer = new CommitMessageSerializer()
    serializedMessages.iterator
      .map(serializer.deserialize(serializer.getVersion, _))
      .toVector
  }

  private def routeRealRecords(
      readBuilder: PostponeMergeReadBuilder,
      plan: PostponeMergePlan): RDD[(BucketOrderKey, PostponeRecord)] = {
    val splits = plan.realSplits().asScala.toSeq
    if (splits.isEmpty) {
      return spark.sparkContext.emptyRDD
    }
    val keyType = plan.keyType()
    val resultReadType = plan.resultReadType()
    val router = plan.bucketRouter()
    val indexedSplits = splits
      .groupBy(split => (split.partition(), split.bucket()))
      .values
      .map(bucketSplits => PostponeMergeInputScan.mergeRealSplits(bucketSplits.toSeq))
      .toSeq
      .map(serializeSplit)
      .zipWithIndex
    spark.sparkContext
      .parallelize(
        indexedSplits,
        Math.max(1, Math.min(indexedSplits.size, spark.sparkContext.defaultParallelism)))
      .mapPartitions {
        inputs =>
          val ioManager = SparkUtils.createIOManager()
          val read = readBuilder.newRead().withIOManager(ioManager)
          val openReaders = mutable.HashSet.empty[RecordReaderIterator[InternalRow]]
          Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit] {
            _ =>
              try {
                openReaders.foreach(_.close())
              } finally {
                ioManager.close()
              }
          })
          val keyExtractor = table.createRowKeyExtractor()
          val keySerializer = new InternalRowSerializer(keyType)
          val valueSerializer = new InternalRowSerializer(resultReadType)
          inputs.flatMap {
            case (serializedSplit, splitOrder) =>
              val split = deserializeSplit(serializedSplit)
              val splitPartition = split.partition()
              val partitionBytes = serializePartition(splitPartition).toIndexedSeq
              val emptyRecords =
                new IteratorRecordReader[KeyValue](
                  java.util.Collections.emptyList[KeyValue]().iterator())
              val reader = new RecordReaderIterator[InternalRow](
                read.createBucketMergeReader(split, emptyRecords))
              mapReader(reader, openReaders) {
                (row, localOrder) =>
                  keyExtractor.setRecord(row)
                  val key = keySerializer.toBinaryRow(keyExtractor.trimmedPrimaryKey())
                  val bucket = router.bucket(splitPartition, key)
                  BucketOrderKey(
                    partitionBytes,
                    bucket,
                    splitOrder.toLong,
                    localOrder) -> PostponeRecord(
                    row.getRowKind.toByteValue,
                    SerializationUtils.serializeBinaryRow(valueSerializer.toBinaryRow(row))
                  )
              }
          }
      }
  }

  private def routePostponeRecords(
      readBuilder: PostponeMergeReadBuilder,
      plan: PostponeMergePlan): RDD[(BucketOrderKey, PostponeRecord)] = {
    val splits = plan.postponeSplits().asScala.toSeq
    if (splits.isEmpty) {
      return spark.sparkContext.emptyRDD
    }
    val keyType = plan.keyType()
    val mergeReadType = plan.mergeReadType()
    val router = plan.bucketRouter()
    val indexedSplits = splits.map(serializeSplit).zipWithIndex
    spark.sparkContext
      .parallelize(
        indexedSplits,
        Math.max(1, Math.min(indexedSplits.size, spark.sparkContext.defaultParallelism)))
      .mapPartitions {
        inputs =>
          val ioManager = SparkUtils.createIOManager()
          val read = readBuilder.newRead().withIOManager(ioManager)
          val openReaders = mutable.HashSet.empty[RecordReaderIterator[KeyValue]]
          Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit] {
            _ =>
              try {
                openReaders.foreach(_.close())
              } finally {
                ioManager.close()
              }
          })
          val keySerializer = new InternalRowSerializer(keyType)
          val valueSerializer = new InternalRowSerializer(mergeReadType)
          inputs.flatMap {
            case (serializedSplit, writerOrder) =>
              val split = deserializeSplit(serializedSplit)
              val splitPartition = split.partition()
              val partitionBytes = serializePartition(splitPartition).toIndexedSeq
              val reader = new RecordReaderIterator[KeyValue](read.createPostponeReader(split))
              mapReader(reader, openReaders) {
                (keyValue, localOrder) =>
                  val key = keySerializer.toBinaryRow(keyValue.key())
                  val bucket = router.bucket(splitPartition, key)
                  BucketOrderKey(
                    partitionBytes,
                    bucket,
                    writerOrder.toLong,
                    localOrder) -> PostponeRecord(
                    keyValue.valueKind().toByteValue,
                    SerializationUtils.serializeBinaryRow(
                      valueSerializer.toBinaryRow(keyValue.value()))
                  )
              }
          }
      }
  }

  private def mergeBucketMessages(messages: Seq[CommitMessage]): Seq[CommitMessageImpl] = {
    val buckets = mutable.LinkedHashMap.empty[(BinaryRow, Int), BucketFiles]
    messages.foreach {
      case message: CommitMessageImpl =>
        val key = (message.partition(), message.bucket())
        val files = buckets.getOrElseUpdate(
          key,
          new BucketFiles(
            table.store().pathFactory().createDataFilePathFactory(key._1, key._2),
            table.fileIO()))
        files.update(message)
      case other => throw new IllegalArgumentException(s"Unsupported commit message $other")
    }
    buckets.map {
      case ((partition, bucket), files) =>
        files.makeMessage(partition, bucket)
    }.toSeq
  }

  private def commitRescale(
      messages: Seq[CommitMessage],
      rescaleBucketNums: Map[BinaryRow, Int]): Unit = {
    val snapshotId = baseSnapshotId.getOrElse {
      throw new IllegalStateException("Cannot rescale real buckets without a base snapshot.")
    }
    // A positive table bucket makes overwrite delete only real buckets. Commit messages carry the
    // per-partition totalBuckets values, so the copied table's uniform bucket number is only a
    // compatibility fallback for the existing commit API.
    val commitTable =
      PostponeUtils.tableForPostponeCompact(table, rescaleBucketNums.values.max, snapshotId)
    val commit = commitTable
      .newCommit(fixedWriteCommitUser)
      .appendCommitCheckConflict(true)
      .ignoreEmptyCommit(ignoreEmptyCommit)
      .withOperation(Snapshot.Operation.OVERWRITE)
      .withOverwriteStaticPartitions(rescaleBucketNums.keys.toSeq.asJava)
    try {
      commit.commit(messages.asJava)
    } finally {
      commit.close()
    }
  }

  private def commitCurrentBatch(
      messages: Seq[CommitMessage],
      touchedPartitions: Seq[BinaryRow],
      operation: Snapshot.Operation): Unit = {
    val dynamicPartitionOverwrite =
      overwritePartitionSpec.exists(_.isEmpty) && coreOptions.dynamicPartitionOverwrite()
    if (!dynamicPartitionOverwrite) {
      overwritePartitionSpec.foreach(spec => fixedWriteBuilder.withOverwrite(spec.asJava))
    }
    val commit = fixedWriteBuilder
      .newCommit(fixedWriteCommitUser, ignoreEmptyCommit)
      .withOperation(operation)
    if (dynamicPartitionOverwrite && touchedPartitions.nonEmpty) {
      commit.withOverwriteStaticPartitions(touchedPartitions.asJava)
    }
    try {
      commit.commit(messages.asJava)
    } finally {
      commit.close()
    }
  }

  private def abortMessages(messages: Seq[CommitMessage]): Unit = {
    if (messages.nonEmpty) {
      try {
        val commit = table.newBatchWriteBuilder().newCommit()
        try {
          commit.abort(messages.asJava)
        } finally {
          commit.close()
        }
      } catch {
        case error: Throwable =>
          // Cleanup failure must not hide the write or commit failure. Orphan-file cleanup remains
          // the final safety net for these never-committed files.
          SparkPostponeStagedCommitter.LOG.warn(
            s"Failed to clean uncommitted files for table ${table.name()}.",
            error)
      }
    }
  }

  private def createRouter(bucketNums: Map[BinaryRow, Int]): PostponeBucketRouter = {
    val javaBucketNums = new java.util.HashMap[BinaryRow, Integer](bucketNums.size)
    bucketNums.foreach {
      case (partition, buckets) => javaBucketNums.put(partition, Integer.valueOf(buckets))
    }
    PostponeUtils.createPostponeBucketRouter(table, javaBucketNums, 1)
  }

  private def shuffleParallelism(bucketNums: Map[BinaryRow, Int]): Int = {
    val useful = bucketNums.values.foldLeft(BigInt(0))(_ + _).max(BigInt(1))
    useful.min(BigInt(spark.sessionState.conf.numShufflePartitions)).toInt
  }
}

private[spark] object SparkPostponeStagedCommitter {

  private val LOG = org.slf4j.LoggerFactory.getLogger(classOf[SparkPostponeStagedCommitter])

  private case class PartitionStats(rowCount: Long, fileSize: Long)

  private def mapReader[T, R](
      reader: RecordReaderIterator[T],
      openReaders: mutable.Set[RecordReaderIterator[T]])(transform: (T, Long) => R): Iterator[R] = {
    openReaders += reader
    new Iterator[R] {
      private var nextLocalOrder = 0L
      private var readerClosed = false

      private def closeReader(): Unit = {
        if (!readerClosed) {
          readerClosed = true
          openReaders -= reader
          reader.close()
        }
      }

      override def hasNext: Boolean = {
        try {
          val hasNext = reader.hasNext
          if (!hasNext) {
            closeReader()
          }
          hasNext
        } catch {
          case error: Throwable =>
            closeReader()
            throw error
        }
      }

      override def next(): R = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        try {
          val result = transform(reader.next(), nextLocalOrder)
          nextLocalOrder = Math.addExact(nextLocalOrder, 1L)
          result
        } catch {
          case error: Throwable =>
            closeReader()
            throw error
        }
      }
    }
  }

  private case class BucketOrderKey(
      partition: IndexedSeq[Byte],
      bucket: Int,
      writerOrder: Long,
      localOrder: Long) {
    def sameBucket(other: BucketOrderKey): Boolean =
      partition == other.partition && bucket == other.bucket
  }

  implicit private val bucketOrder: Ordering[BucketOrderKey] = new Ordering[BucketOrderKey] {
    override def compare(left: BucketOrderKey, right: BucketOrderKey): Int = {
      val partitionComparison = compareBytes(left.partition, right.partition)
      if (partitionComparison != 0) {
        partitionComparison
      } else {
        val bucketComparison = Integer.compare(left.bucket, right.bucket)
        if (bucketComparison != 0) {
          bucketComparison
        } else {
          val writerComparison = java.lang.Long.compare(left.writerOrder, right.writerOrder)
          if (writerComparison != 0) {
            writerComparison
          } else {
            java.lang.Long.compare(left.localOrder, right.localOrder)
          }
        }
      }
    }
  }

  private def compareBytes(left: IndexedSeq[Byte], right: IndexedSeq[Byte]): Int = {
    val limit = Math.min(left.length, right.length)
    var index = 0
    while (index < limit) {
      val comparison = java.lang.Byte.compare(left(index), right(index))
      if (comparison != 0) {
        return comparison
      }
      index += 1
    }
    Integer.compare(left.length, right.length)
  }

  private case class BucketGroupPartitioner(override val numPartitions: Int) extends Partitioner {
    require(numPartitions > 0, "Shuffle partition number must be positive.")

    override def getPartition(key: Any): Int = {
      val bucketKey = key.asInstanceOf[BucketOrderKey]
      Math.floorMod(31 * bucketKey.partition.hashCode() + bucketKey.bucket, numPartitions)
    }
  }

  private case class PostponeRecord(rowKind: Byte, value: Array[Byte])

  private def reportOutputMetrics(messages: Seq[CommitMessage]): Unit = {
    Option(TaskContext.get()).foreach {
      taskContext =>
        val files = messages
          .collect { case message: CommitMessageImpl => message }
          .flatMap(_.newFilesIncrement().newFiles().asScala)
        val bytesWritten = files.iterator.map(_.fileSize()).foldLeft(0L)(Math.addExact)
        val recordsWritten = files.iterator.map(_.rowCount()).foldLeft(0L)(Math.addExact)
        PaimonUtils.updateOutputMetrics(
          taskContext.taskMetrics().outputMetrics,
          bytesWritten,
          recordsWritten)
    }
  }

  private def serializePartition(partition: BinaryRow): Array[Byte] =
    SerializationUtils.serializeBinaryRow(partition)

  private def deserializePartition(partition: IndexedSeq[Byte]): BinaryRow =
    SerializationUtils.deserializeBinaryRow(partition.toArray)

  private def serializeSplit(split: DataSplit): Array[Byte] = SplitSerializer.serialize(split)

  private def deserializeSplit(split: Array[Byte]): DataSplit =
    SplitSerializer.deserialize(split).asInstanceOf[DataSplit]

}
