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

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.BUCKET
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement}
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.postpone.BucketFiles
import org.apache.paimon.spark.commands.{EncoderSerDeGroup, PostponeFixBucketProcessor}
import org.apache.paimon.spark.schema.SparkSystemColumns.{BUCKET_COL, ROW_KIND_COL}
import org.apache.paimon.spark.util.{ScanPlanHelper, SparkRowUtils}
import org.apache.paimon.spark.write.{PaimonDataWrite, WriteTaskResult}
import org.apache.paimon.table.{BucketMode, FileStoreTable, PostponeUtils}
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}

import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
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

  // Unlike `PostponeUtils.tableForFixBucketWrite`, here explicitly set bucket to 1 without
  // WRITE_ONLY to enable proper compaction logic.
  private lazy val realTable = table.copy(Map(BUCKET.key -> "1").asJava)

  // Create bucket computer to determine bucket count for each partition
  private lazy val postponePartitionBucketComputer = {
    val knownNumBuckets = PostponeUtils.getKnownNumBuckets(table)
    val defaultBucketNum = Math.min(
      spark.sparkContext.defaultParallelism,
      table.coreOptions().postponeBatchWriteFixedBucketMaxParallelism)
    (p: BinaryRow) => knownNumBuckets.getOrDefault(p, defaultBucketNum)
  }

  private def partitionCols(df: DataFrame): Seq[Column] = {
    val inputSchema = df.schema
    val tableSchema = table.schema
    tableSchema
      .partitionKeys()
      .asScala
      .map(tableSchema.fieldNames().indexOf(_))
      .map(x => col(inputSchema.fieldNames(x)))
      .toSeq
  }

  private def repartitionByPartitionsAndBucket(df: DataFrame): DataFrame = {
    df.repartition(partitionCols(df) ++ Seq(col(BUCKET_COL)): _*)
  }

  // write data with the bucket processor
  private def writeWithBucketProcessor(
      repartitioned: DataFrame,
      bucketColIdx: Int,
      schema: StructType) = {
    import spark.implicits._
    val rowKindColIdx = SparkRowUtils.getFieldIndex(schema, ROW_KIND_COL)
    val writeBuilder = realTable.newBatchWriteBuilder
    val rowType = table.rowType()
    val coreOptions = table.coreOptions()

    def newWrite() = PaimonDataWrite(
      writeBuilder,
      rowType,
      rowKindColIdx,
      writeRowTracking = coreOptions.dataEvolutionEnabled(),
      Option.apply(coreOptions.fullCompactionDeltaCommits()),
      None,
      coreOptions.blobAsDescriptor(),
      table.catalogEnvironment().catalogContext(),
      Some(postponePartitionBucketComputer)
    )

    repartitioned.mapPartitions {
      iter =>
        {
          val write = newWrite()
          try {
            iter.foreach(row => write.write(row, row.getInt(bucketColIdx)))
            Iterator.apply(write.commit)
          } finally {
            write.close()
          }
        }
    }
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

    // Read data splits from the POSTPONE_BUCKET (-2)
    val splits =
      table.newSnapshotReader
        .withBucket(BucketMode.POSTPONE_BUCKET)
        .read
        .dataSplits
        .asScala

    if (splits.isEmpty) {
      LOG.info("Partition bucket is empty, no compact job to execute.")
      return
    }

    // Prepare dataset for writing by combining all partitions
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

    // Create processor to handle the actual bucket assignment
    val processor = PostponeFixBucketProcessor(
      table,
      bucketColIdx,
      encoderGroupWithBucketCol,
      postponePartitionBucketComputer
    )

    val dataFrame = withInitBucketCol
      .mapPartitions(processor.processPartition)(encoderGroupWithBucketCol.encoder)
      .toDF()
    val repartition = repartitionByPartitionsAndBucket(dataFrame)
    val written = writeWithBucketProcessor(repartition, bucketColIdx, withInitBucketCol.schema)

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
