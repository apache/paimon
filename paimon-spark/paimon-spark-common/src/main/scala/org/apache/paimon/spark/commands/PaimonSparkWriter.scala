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

package org.apache.paimon.spark.commands

import org.apache.paimon.index.BucketAssigner
import org.apache.paimon.spark.SparkRow
import org.apache.paimon.spark.SparkUtils.createIOManager
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.spark.schema.SparkSystemColumns.{BUCKET_COL, ROW_KIND_COL}
import org.apache.paimon.spark.util.SparkRowUtils
import org.apache.paimon.table.{BucketMode, FileStoreTable}
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage, CommitMessageSerializer, RowPartitionKeyExtractor}

import org.apache.spark.Partitioner
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.io.IOException

import scala.collection.JavaConverters._

case class PaimonSparkWriter(table: FileStoreTable) {

  private lazy val tableSchema = table.schema

  private lazy val rowType = table.rowType()

  private lazy val bucketMode = table match {
    case fileStoreTable: FileStoreTable =>
      fileStoreTable.bucketMode
    case _ =>
      BucketMode.FIXED
  }

  private lazy val primaryKeyCols = tableSchema.trimmedPrimaryKeys().asScala

  private lazy val serializer = new CommitMessageSerializer

  val writeBuilder: BatchWriteBuilder = table.newBatchWriteBuilder()

  def write(data: Dataset[_]): Seq[CommitMessage] = {
    val sparkSession = data.sparkSession
    import sparkSession.implicits._

    val dataSchema = SparkSystemColumns.filterSparkSystemColumns(data.schema)
    val rowkindColIdx = SparkRowUtils.getFieldIndex(data.schema, ROW_KIND_COL)

    // append _bucket_ column as placeholder
    val withInitBucketCol = data.withColumn(BUCKET_COL, lit(-1))
    val bucketColIdx = withInitBucketCol.schema.size - 1

    val originEncoderGroup = EncoderSerDeGroup(dataSchema)
    val encoderGroupWithBucketCol = EncoderSerDeGroup(withInitBucketCol.schema)

    val withBucketCol =
      assignBucketId(sparkSession, withInitBucketCol, bucketColIdx, encoderGroupWithBucketCol)

    val commitMessages = withBucketCol
      .mapPartitions {
        iter =>
          val ioManager = createIOManager
          val write = writeBuilder.newWrite()
          write.withIOManager(ioManager)
          try {
            iter.foreach {
              row =>
                val bucket = row.getInt(bucketColIdx)
                val bucketColDropped =
                  originEncoderGroup.internalToRow(encoderGroupWithBucketCol.rowToInternal(row))
                val sparkRow = new SparkRow(
                  rowType,
                  bucketColDropped,
                  SparkRowUtils.getRowKind(row, rowkindColIdx))
                write.write(sparkRow, bucket)
            }
            val serializer = new CommitMessageSerializer
            write.prepareCommit().asScala.map(serializer.serialize).toIterator

          } finally {
            write.close()
            ioManager.close()
          }
      }
      .collect()
      .map(deserializeCommitMessage(serializer, _))

    commitMessages.toSeq
  }

  def commit(commitMessages: Seq[CommitMessage]): Unit = {
    val tableCommit = writeBuilder.newCommit()
    try {
      tableCommit.commit(commitMessages.toList.asJava)
    } catch {
      case e: Throwable => throw new RuntimeException(e);
    } finally {
      tableCommit.close()
    }
  }

  /** assign a valid bucket id for each of record. */
  private def assignBucketId(
      sparkSession: SparkSession,
      withInitBucketCol: DataFrame,
      bucketColIdx: Int,
      encoderGroupWithBucketCol: EncoderSerDeGroup): Dataset[Row] = {

    val encoderWithBucketCOl = encoderGroupWithBucketCol.encoder

    bucketMode match {
      case BucketMode.DYNAMIC =>
        assert(primaryKeyCols.nonEmpty, "Only primary-key table can support dynamic bucket.")

        // Topology: input -> shuffle by special key & partition hash -> bucket-assigner -> shuffle by partition & bucket
        val numParallelism = Option(table.coreOptions.dynamicBucketAssignerParallelism)
          .map(_.toInt)
          .getOrElse {
            val defaultParallelism = sparkSession.sparkContext.defaultParallelism
            val numShufflePartitions = sparkSession.sessionState.conf.numShufflePartitions
            Math.max(defaultParallelism, numShufflePartitions)
          }
        val numAssigners = Option(table.coreOptions.dynamicBucketInitialBuckets)
          .map(initialBuckets => Math.min(initialBuckets.toInt, numParallelism))
          .getOrElse(numParallelism)

        val partitioned =
          repartitionByKeyPartitionHash(
            sparkSession,
            withInitBucketCol,
            numParallelism,
            numAssigners)
        val dynamicBucketProcessor =
          DynamicBucketProcessor(
            table,
            bucketColIdx,
            numParallelism,
            numAssigners,
            encoderGroupWithBucketCol)
        repartitionByPartitionsAndBucket(
          partitioned.mapPartitions(dynamicBucketProcessor.processPartition)(encoderWithBucketCOl))

      case BucketMode.UNAWARE =>
        assert(primaryKeyCols.isEmpty, "Only append table can support unaware bucket.")

        // Topology: input -> bucket-assigner
        val unawareBucketProcessor = UnawareBucketProcessor(bucketColIdx, encoderGroupWithBucketCol)
        withInitBucketCol
          .mapPartitions(unawareBucketProcessor.processPartition)(encoderWithBucketCOl)
          .toDF()

      case BucketMode.FIXED =>
        // Topology: input -> bucket-assigner -> shuffle by partition & bucket
        val commonBucketProcessor =
          CommonBucketProcessor(table, bucketColIdx, encoderGroupWithBucketCol)
        repartitionByPartitionsAndBucket(
          withInitBucketCol.mapPartitions(commonBucketProcessor.processPartition)(
            encoderWithBucketCOl))

      case _ =>
        throw new UnsupportedOperationException(s"Spark doesn't support $bucketMode mode.")
    }
  }

  /** Compute bucket id in dynamic bucket mode. */
  private def repartitionByKeyPartitionHash(
      sparkSession: SparkSession,
      data: DataFrame,
      numParallelism: Int,
      numAssigners: Int): DataFrame = {

    sparkSession.createDataFrame(
      data.rdd
        .mapPartitions(
          iterator => {
            val rowPartitionKeyExtractor = new RowPartitionKeyExtractor(tableSchema)
            iterator.map(
              row => {
                val sparkRow = new SparkRow(rowType, row)
                val partitionHash = rowPartitionKeyExtractor.partition(sparkRow).hashCode
                val keyHash = rowPartitionKeyExtractor.trimmedPrimaryKey(sparkRow).hashCode
                (
                  BucketAssigner
                    .computeHashKey(partitionHash, keyHash, numParallelism, numAssigners),
                  row)
              })
          },
          preservesPartitioning = true
        )
        .partitionBy(ModPartitioner(numParallelism))
        .map(_._2),
      data.schema
    )
  }

  private def repartitionByPartitionsAndBucket(ds: Dataset[Row]): Dataset[Row] = {
    val partitionCols = tableSchema.partitionKeys().asScala.map(col)
    ds.toDF().repartition(partitionCols ++ Seq(col(BUCKET_COL)): _*)
  }

  private def deserializeCommitMessage(
      serializer: CommitMessageSerializer,
      bytes: Array[Byte]): CommitMessage = {
    try {
      serializer.deserialize(serializer.getVersion, bytes)
    } catch {
      case e: IOException =>
        throw new RuntimeException("Failed to deserialize CommitMessage's object", e)
    }
  }

  case class ModPartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions
    override def getPartition(key: Any): Int = key.asInstanceOf[Int] % numPartitions
  }
}
