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

import org.apache.paimon.KeyValue
import org.apache.paimon.data.serializer.InternalRowSerializer
import org.apache.paimon.reader.{RecordReader, RecordReaderIterator}
import org.apache.paimon.spark.PostponeMergeInputScan._
import org.apache.paimon.spark.PostponeMergeOnRead.MergePlan
import org.apache.paimon.spark.util.SplitUtils
import org.apache.paimon.table.BucketMode
import org.apache.paimon.table.PostponeUtils.PostponeBucketRouter
import org.apache.paimon.table.source.{DataSplit, DeletionFile, PostponeMergePlan, PostponeMergeReadBuilder, SplitSerializer}
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.{BinaryType, ByteType, IntegerType, LongType, StructField, StructType}

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.JavaConverters._

/** Internal DSv2 scan which materializes real-split markers and routed postpone records. */
private[spark] case class PostponeMergeInputScan(mergePlan: MergePlan) extends Scan {

  override def readSchema(): StructType = CARRIER_SCHEMA

  override def toBatch: Batch =
    PostponeMergeInputBatch(mergePlan.readBuilder, mergePlan.corePlan)

  override def description(): String =
    "Paimon Postpone Scan: read postpone files and route records by target bucket"

  override def supportedCustomMetrics(): Array[CustomMetric] = {
    Array(
      PaimonPartitionSizeMetric(),
      PaimonReadBatchTimeMetric(),
      PaimonResultedPostponeFilesMetric(),
      PaimonNumPostponeRecordsMetric()
    )
  }

  override def reportDriverMetrics(): Array[CustomTaskMetric] = {
    val resultedPostponeFiles =
      mergePlan.corePlan.postponeSplits().asScala.map(_.dataFiles().size().toLong).sum
    Array(PaimonResultedPostponeFilesTaskMetric(resultedPostponeFiles))
  }
}

private[spark] object PostponeMergeInputScan {

  val PARTITION_COLUMN = "__paimon_postpone_partition"
  val BUCKET_COLUMN = "__paimon_postpone_bucket"
  val INPUT_KIND_COLUMN = "__paimon_postpone_input_kind"
  val REAL_SPLIT_COLUMN = "__paimon_postpone_real_split"
  val KEY_COLUMN = "__paimon_postpone_key"
  val WRITER_LOCAL_ORDER_COLUMN = "__paimon_postpone_writer_local_order"
  val ROW_KIND_COLUMN = "__paimon_postpone_row_kind"
  val VALUE_COLUMN = "__paimon_postpone_value"

  val REAL_SPLIT: Byte = 0
  val POSTPONE_RECORD: Byte = 1

  val CARRIER_SCHEMA: StructType = StructType(
    Seq(
      StructField(PARTITION_COLUMN, BinaryType, nullable = false),
      StructField(BUCKET_COLUMN, IntegerType, nullable = false),
      StructField(INPUT_KIND_COLUMN, ByteType, nullable = false),
      StructField(REAL_SPLIT_COLUMN, BinaryType, nullable = true),
      StructField(KEY_COLUMN, BinaryType, nullable = true),
      StructField(WRITER_LOCAL_ORDER_COLUMN, LongType, nullable = false),
      StructField(ROW_KIND_COLUMN, ByteType, nullable = false),
      StructField(VALUE_COLUMN, BinaryType, nullable = true)
    ))

  val PARTITION_ORDINAL: Int = CARRIER_SCHEMA.fieldIndex(PARTITION_COLUMN)
  val BUCKET_ORDINAL: Int = CARRIER_SCHEMA.fieldIndex(BUCKET_COLUMN)
  val INPUT_KIND_ORDINAL: Int = CARRIER_SCHEMA.fieldIndex(INPUT_KIND_COLUMN)
  val REAL_SPLIT_ORDINAL: Int = CARRIER_SCHEMA.fieldIndex(REAL_SPLIT_COLUMN)
  val KEY_ORDINAL: Int = CARRIER_SCHEMA.fieldIndex(KEY_COLUMN)
  val WRITER_LOCAL_ORDER_ORDINAL: Int = CARRIER_SCHEMA.fieldIndex(WRITER_LOCAL_ORDER_COLUMN)
  val ROW_KIND_ORDINAL: Int = CARRIER_SCHEMA.fieldIndex(ROW_KIND_COLUMN)
  val VALUE_ORDINAL: Int = CARRIER_SCHEMA.fieldIndex(VALUE_COLUMN)

  private case class PostponeMergeInputBatch(
      readBuilder: PostponeMergeReadBuilder,
      corePlan: PostponeMergePlan)
    extends Batch {

    override def planInputPartitions(): Array[InputPartition] = {
      val realPartitions = corePlan
        .realSplits()
        .asScala
        .groupBy(bucketKey)
        .map {
          case (_, splits) =>
            PaimonInputPartition(mergeRealSplits(splits.toSeq))
        }
      val postponePartitions = corePlan.postponeSplits().asScala.map(PaimonInputPartition(_))
      (realPartitions ++ postponePartitions).toArray[InputPartition]
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      PostponeMergeInputReaderFactory(
        readBuilder,
        corePlan.keyType(),
        corePlan.mergeReadType(),
        corePlan.bucketRouter())
    }
  }

  private case class PostponeMergeInputReaderFactory(
      readBuilder: PostponeMergeReadBuilder,
      keyType: RowType,
      mergeReadType: RowType,
      bucketRouter: PostponeBucketRouter)
    extends PartitionReaderFactory {

    override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
      partition match {
        case input: PaimonInputPartition =>
          val split = input.splits.head.asInstanceOf[DataSplit]
          if (split.bucket() == BucketMode.POSTPONE_BUCKET) {
            new PostponePartitionReader(split, readBuilder, keyType, mergeReadType, bucketRouter)
          } else {
            new RealBucketPartitionReader(split)
          }
        case other =>
          throw new IllegalArgumentException(
            "Unsupported postpone merge input partition: " + other.getClass.getName + ".")
      }
    }
  }

  private class RealBucketPartitionReader(split: DataSplit) extends PartitionReader[InternalRow] {

    private val row = new GenericInternalRow(
      Array[Any](
        SerializationUtils.serializeBinaryRow(split.partition()),
        split.bucket(),
        REAL_SPLIT,
        SplitSerializer.serialize(split),
        null,
        0L,
        0.toByte,
        null))
    private var emitted = false

    override def next(): Boolean = {
      if (!emitted) {
        emitted = true
        true
      } else {
        false
      }
    }

    override def get(): InternalRow = row

    override def close(): Unit = {}
  }

  /** Reads one writer and attaches an ordinal which preserves its order after the shuffle. */
  private class PostponePartitionReader(
      split: DataSplit,
      readBuilder: PostponeMergeReadBuilder,
      keyType: RowType,
      mergeReadType: RowType,
      bucketRouter: PostponeBucketRouter)
    extends PartitionReader[InternalRow] {

    private val keySerializer = new InternalRowSerializer(keyType)
    private val valueSerializer = new InternalRowSerializer(mergeReadType)
    private val partitionBytes = SerializationUtils.serializeBinaryRow(split.partition())
    private val timedReader =
      new TimedRecordReader[KeyValue](readBuilder.newRead().createPostponeReader(split))
    private val records =
      new RecordReaderIterator[KeyValue](timedReader)
    private val current = new GenericInternalRow(
      Array[Any](partitionBytes, 0, POSTPONE_RECORD, null, null, 0L, 0.toByte, null))
    private var nextWriterLocalOrder = 0L
    private var numPostponeRecords = 0L

    private lazy val partitionMetrics: Array[CustomTaskMetric] = {
      Array(PaimonPartitionSizeTaskMetric(SplitUtils.splitSize(split)))
    }

    override def next(): Boolean = {
      if (!records.hasNext) {
        false
      } else {
        val keyValue = records.next()
        val key = keySerializer.toBinaryRow(keyValue.key())
        val value = valueSerializer.toBinaryRow(keyValue.value())
        current.setInt(BUCKET_ORDINAL, bucketRouter.bucket(split.partition(), key))
        current.update(KEY_ORDINAL, SerializationUtils.serializeBinaryRow(key))
        current.setLong(WRITER_LOCAL_ORDER_ORDINAL, nextWriterLocalOrder)
        current.setByte(ROW_KIND_ORDINAL, keyValue.valueKind().toByteValue)
        current.update(VALUE_ORDINAL, SerializationUtils.serializeBinaryRow(value))
        nextWriterLocalOrder = Math.addExact(nextWriterLocalOrder, 1L)
        numPostponeRecords = Math.addExact(numPostponeRecords, 1L)
        true
      }
    }

    override def get(): InternalRow = current

    override def currentMetricsValues(): Array[CustomTaskMetric] = {
      partitionMetrics ++ Array(
        PaimonReadBatchTimeTaskMetric(timedReader.readBatchTimeMs),
        PaimonNumPostponeRecordsTaskMetric(numPostponeRecords)
      )
    }

    override def close(): Unit = records.close()
  }

  private[spark] class TimedRecordReader[T](delegate: RecordReader[T]) extends RecordReader[T] {

    private var readBatchTimeNs = 0L

    override def readBatch(): RecordReader.RecordIterator[T] = {
      val startTimeNs = System.nanoTime()
      try {
        delegate.readBatch()
      } finally {
        readBatchTimeNs += System.nanoTime() - startTimeNs
      }
    }

    def readBatchTimeMs: Long = NANOSECONDS.toMillis(readBatchTimeNs)

    override def close(): Unit = delegate.close()
  }

  private def bucketKey(split: DataSplit) = (split.partition(), split.bucket())

  private def mergeRealSplits(splits: Seq[DataSplit]): DataSplit = {
    if (splits.size == 1) {
      splits.head
    } else {
      val first = splits.head
      val builder = DataSplit
        .builder()
        .withSnapshot(first.snapshotId())
        .withPartition(first.partition())
        .withBucket(first.bucket())
        .withBucketPath(first.bucketPath())
        .withTotalBuckets(first.totalBuckets())
        .withDataFiles(splits.flatMap(_.dataFiles().asScala).asJava)
        .isStreaming(first.isStreaming())
        .rawConvertible(splits.forall(_.rawConvertible()))

      if (splits.exists(_.deletionFiles().isPresent)) {
        val deletionFiles = splits.flatMap {
          split =>
            if (split.deletionFiles().isPresent) {
              split.deletionFiles().get().asScala
            } else {
              Seq.fill(split.dataFiles().size())(null: DeletionFile)
            }
        }
        builder.withDataDeletionFiles(deletionFiles.asJava)
      }
      builder.build()
    }
  }
}
