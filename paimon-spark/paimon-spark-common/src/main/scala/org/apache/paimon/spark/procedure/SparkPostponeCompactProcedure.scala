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

package org.apache.paimon.spark.procedure

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement}
import org.apache.paimon.operation.FileSystemWriteRestore
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.postpone.BucketFiles
import org.apache.paimon.spark.PaimonImplicits._
import org.apache.paimon.spark.commands.{EncoderSerDeGroup, PostponeFixBucketProcessor}
import org.apache.paimon.spark.schema.SparkSystemColumns.{BUCKET_COL, ROW_KIND_COL}
import org.apache.paimon.spark.util.{ScanPlanHelper, SparkRowUtils}
import org.apache.paimon.spark.write.{PaimonDataWrite, WriteTaskResult}
import org.apache.paimon.table.{BlobDescriptorReaderFactory, BucketMode, FileStoreTable, PostponeUtils}
import org.apache.paimon.table.PostponeUtils.PostponeBucketAssigner
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.utils.{SerializationUtils, UriReaderFactory}

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory

import javax.annotation.Nullable

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Spark procedure helper for compacting postpone bucket tables. This provides the implementation
 * for Postpone Bucket mode compaction.
 */
case class SparkPostponeCompactProcedure(
    table: FileStoreTable,
    @transient spark: SparkSession,
    @Nullable partitionPredicate: PartitionPredicate,
    @transient relation: DataSourceV2Relation) {
  private val LOG = LoggerFactory.getLogger(getClass)

  private def createPostponePartitionBucketComputer(snapshotId: Long) = {
    PostponeUtils.createPostponeBucketAssigner(
      table,
      snapshotId,
      spark.sparkContext.defaultParallelism)
  }

  private def newDataWrite(
      realTable: FileStoreTable,
      rowKindColIdx: Int,
      postponePartitionBucketComputer: PostponeBucketAssigner,
      uriReaderFactoryForBlobDescriptor: UriReaderFactory): PaimonDataWrite = {
    val rowType = table.rowType()
    val coreOptions = table.coreOptions()

    val dataWrite = PaimonDataWrite(
      realTable.newBatchWriteBuilder,
      rowType,
      rowKindColIdx,
      writeRowTracking = coreOptions.dataEvolutionEnabled(),
      Option.apply(coreOptions.fullCompactionDeltaCommits()),
      None,
      uriReaderFactoryForBlobDescriptor,
      Some(partition => postponePartitionBucketComputer.assign(partition))
    )
    dataWrite
  }

  /** Creates a new BucketFiles instance for tracking file changes */
  private def createBucketFiles(partition: BinaryRow, bucket: Int): BucketFiles = {
    new BucketFiles(
      table.store.pathFactory.createDataFilePathFactory(partition, bucket),
      table.fileIO()
    )
  }

  def execute(): Unit = {
    LOG.info("Starting postpone bucket compaction for table: {}.", table.name())
    // Validate input parameters - no partition predicates supported yet, same behavior with flink
    assert(
      partitionPredicate == null,
      "Postpone bucket compaction currently does not support specifying partitions")

    val snapshot = table.latestSnapshot().orElse(null)
    if (snapshot == null) {
      LOG.info("Table has no snapshot, no compact job to execute.")
      return
    }
    val snapshotId = snapshot.id()
    val postponePartitionBucketComputer =
      createPostponePartitionBucketComputer(snapshotId)
    val realTable = PostponeUtils.tableForPostponeCompact(table, 1, snapshotId)

    // Read data splits from the POSTPONE_BUCKET (-2)
    val splits = PostponeUtils
      .splitAndOrderPostponeFiles(
        table.newSnapshotReader
          .withSnapshot(snapshotId)
          .withBucket(BucketMode.POSTPONE_BUCKET)
          .read
          .dataSplits
          .asScala
          .toList
          .asJava)
      .asScala
    val compactBuckets = PostponeUtils.getLevel0Buckets(table, snapshotId).asScala

    if (splits.isEmpty && compactBuckets.isEmpty) {
      LOG.info("Postpone bucket and real Level-0 buckets are empty, no compact job to execute.")
      return
    }
    val uriReaderFactory = BlobDescriptorReaderFactory.create(table)

    val rowWorkAndKind: (RDD[SparkPostponeCompactProcedure.PostponeCompactWork], Int) =
      if (splits.isEmpty) {
        (spark.sparkContext.emptyRDD, -1)
      } else {
        val datasetForWrite: Dataset[Row] = splits
          .groupBy(_.partition)
          .values
          .map(
            split => {
              PaimonUtils
                .createDataset(spark, ScanPlanHelper.createNewScanPlan(split.toArray, relation))
            })
          .reduce((a, b) => a.union(b))

        val withInitBucketCol = datasetForWrite.withColumn(BUCKET_COL, lit(-1))
        val bucketColIdx = SparkRowUtils.getFieldIndex(withInitBucketCol.schema, BUCKET_COL)
        val encoderGroupWithBucketCol = EncoderSerDeGroup(withInitBucketCol.schema)
        val processor = PostponeFixBucketProcessor(
          table,
          bucketColIdx,
          encoderGroupWithBucketCol,
          partition => postponePartitionBucketComputer.assign(partition)
        )
        val dataFrame = withInitBucketCol
          .mapPartitions(processor.processPartition)(encoderGroupWithBucketCol.encoder)
          .toDF()
        val rowKindColIdx = SparkRowUtils.getFieldIndex(withInitBucketCol.schema, ROW_KIND_COL)
        val rowType = table.rowType()
        val rowWorks = dataFrame.rdd.mapPartitions {
          rows =>
            val extractor = realTable.createRowKeyExtractor()
            val toPaimonRow = SparkRowUtils.toPaimonRow(rowType, rowKindColIdx, uriReaderFactory)
            rows.map {
              row =>
                extractor.setRecord(toPaimonRow(row))
                val partition = extractor.partition().copy()
                SparkPostponeCompactProcedure.PostponeCompactWork(
                  row.copy(),
                  SerializationUtils.serializeBinaryRow(partition),
                  row.getInt(bucketColIdx),
                  compactMarker = false)
            }
        }
        (rowWorks, rowKindColIdx)
      }

    val markerWorks = compactBuckets.map {
      bucket =>
        SparkPostponeCompactProcedure.PostponeCompactWork(
          null,
          SerializationUtils.serializeBinaryRow(bucket.partition()),
          bucket.bucket(),
          compactMarker = true)
    }
    val markerRdd =
      if (markerWorks.isEmpty) {
        spark.sparkContext.emptyRDD[SparkPostponeCompactProcedure.PostponeCompactWork]
      } else {
        spark.sparkContext.parallelize(
          markerWorks.toSeq,
          Math.min(markerWorks.size, Math.max(1, spark.sparkContext.defaultParallelism)))
      }

    val partitionedWorks = rowWorkAndKind._1
      .union(markerRdd)
      .map(
        work =>
          (
            SparkPostponeCompactProcedure
              .PostponeCompactKey(work.partition.toIndexedSeq, work.bucket),
            work))
      .partitionBy(new HashPartitioner(Math.max(1, spark.sparkContext.defaultParallelism)))

    val written = partitionedWorks.mapPartitions {
      works =>
        if (!works.hasNext) {
          Iterator.empty
        } else {
          val dataWrite =
            newDataWrite(
              realTable,
              rowWorkAndKind._2,
              postponePartitionBucketComputer,
              uriReaderFactory)
          dataWrite.write.withWriteRestore(
            new FileSystemWriteRestore(
              realTable.coreOptions(),
              realTable.snapshotManager(),
              realTable.store().newScan(),
              realTable.store().newIndexFileHandler(),
              snapshotId))
          var commitInvoked = false
          try {
            val pendingBuckets = mutable.LinkedHashMap
              .empty[SparkPostponeCompactProcedure.PostponeCompactKey, Array[Byte]]
            works.foreach {
              case (key, work) =>
                pendingBuckets.put(key, work.partition)
                if (!work.compactMarker) {
                  dataWrite.write(work.row, work.bucket)
                }
            }
            pendingBuckets.foreach {
              case (key, partition) =>
                dataWrite.write.compact(
                  SerializationUtils.deserializeBinaryRow(partition),
                  key.bucket,
                  false)
            }
            commitInvoked = true
            Iterator.single(dataWrite.commit)
          } finally {
            if (!commitInvoked) {
              dataWrite.close()
            }
          }
        }
    }

    // Create commit messages for removing old postpone bucket files
    val removeMessages = splits.map {
      split =>
        new CommitMessageImpl(
          split.partition(),
          split.bucket(),
          split.totalBuckets(),
          DataIncrement.emptyIncrement,
          new CompactIncrement(
            split.dataFiles(),
            Collections.emptyList[DataFileMeta],
            Collections.emptyList[DataFileMeta]
          )
        )
    }

    // Combine remove messages with new file write messages
    val commitMessages = removeMessages ++ WriteTaskResult.merge(written.collect().toSeq)

    // Process all commit messages using BucketFiles to track file changes
    val buckets = new mutable.HashMap[BinaryRow, mutable.HashMap[Int, BucketFiles]]()
    commitMessages.foreach {
      message =>
        val commitMessage = message.asInstanceOf[CommitMessageImpl]
        buckets
          .getOrElseUpdate(commitMessage.partition(), mutable.HashMap.empty[Int, BucketFiles])
          .getOrElseUpdate(
            commitMessage.bucket(),
            createBucketFiles(commitMessage.partition(), commitMessage.bucket()))
          .update(commitMessage)
    }

    val finalCommitMessages: Seq[CommitMessage] = buckets.iterator.flatMap {
      case (partition, bucketMap) =>
        bucketMap.iterator.map {
          case (bucket, bucketFiles) =>
            bucketFiles.makeMessage(partition, bucket).asInstanceOf[CommitMessage]
        }
    }.toSeq

    // Commit the final result
    try {
      assert(finalCommitMessages.nonEmpty, "No commit message to commit")
      val commitUser = table.coreOptions.createCommitUser
      val commit = realTable.newCommit(commitUser)
      commit.commit(finalCommitMessages.asJava)
    } catch {
      case e: Exception =>
        throw new RuntimeException("Failed to commit postpone bucket compaction result", e)
    }
    LOG.info("Successfully committed postpone bucket compaction for table: {}.", table.name())
  }
}

object SparkPostponeCompactProcedure {

  private[procedure] case class PostponeCompactWork(
      row: Row,
      partition: Array[Byte],
      bucket: Int,
      compactMarker: Boolean)
    extends Serializable

  private[procedure] case class PostponeCompactKey(partition: IndexedSeq[Byte], bucket: Int)
    extends Serializable

}
