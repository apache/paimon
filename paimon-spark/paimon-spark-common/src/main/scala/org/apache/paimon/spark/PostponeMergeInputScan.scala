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
import org.apache.paimon.disk.IOManager
import org.apache.paimon.io.DataOutputSerializer
import org.apache.paimon.reader.RecordReader
import org.apache.paimon.spark.PostponeMergeInputScan._
import org.apache.paimon.spark.PostponeMergeOnReadScan.MergePlan
import org.apache.paimon.table.PostponeUtils.PostponeBucketRouter
import org.apache.paimon.table.source.{DataSplit, PostponeFileReadTask, PostponeMergePlan, PostponeMergeReadBuilder}
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.{BinaryType, ByteType, IntegerType, LongType, StructField, StructType}

import scala.collection.JavaConverters._

/** Internal DSv2 scan which materializes real-split markers and routed postpone records. */
private[spark] case class PostponeMergeInputScan(mergePlan: MergePlan) extends Scan {

  override def readSchema(): StructType = CARRIER_SCHEMA

  override def toBatch: Batch =
    PostponeMergeInputBatch(mergePlan.readBuilder, mergePlan.corePlan)

  override def description(): String = "PaimonPostponeMergeInput"
}

private[spark] object PostponeMergeInputScan {

  val PARTITION_COLUMN = "__paimon_postpone_partition"
  val BUCKET_COLUMN = "__paimon_postpone_bucket"
  val KIND_COLUMN = "__paimon_postpone_kind"
  val REAL_SPLIT_COLUMN = "__paimon_postpone_real_split"
  val KEY_COLUMN = "__paimon_postpone_key"
  val SEQUENCE_COLUMN = "__paimon_postpone_sequence"
  val ROW_KIND_COLUMN = "__paimon_postpone_row_kind"
  val VALUE_COLUMN = "__paimon_postpone_value"

  val PARTITION_ORDINAL = 0
  val BUCKET_ORDINAL = 1
  val KIND_ORDINAL = 2
  val REAL_SPLIT_ORDINAL = 3
  val KEY_ORDINAL = 4
  val SEQUENCE_ORDINAL = 5
  val ROW_KIND_ORDINAL = 6
  val VALUE_ORDINAL = 7

  val REAL_SPLIT: Byte = 0
  val POSTPONE_RECORD: Byte = 1

  val CARRIER_SCHEMA: StructType = StructType(
    Seq(
      StructField(PARTITION_COLUMN, BinaryType, nullable = false),
      StructField(BUCKET_COLUMN, IntegerType, nullable = false),
      StructField(KIND_COLUMN, ByteType, nullable = false),
      StructField(REAL_SPLIT_COLUMN, BinaryType, nullable = true),
      StructField(KEY_COLUMN, BinaryType, nullable = true),
      StructField(SEQUENCE_COLUMN, LongType, nullable = false),
      StructField(ROW_KIND_COLUMN, ByteType, nullable = false),
      StructField(VALUE_COLUMN, BinaryType, nullable = true)
    ))

  private case class BucketKey(partition: IndexedSeq[Byte], bucket: Int) extends Serializable

  private case class RealBucketInputPartition(
      partition: Array[Byte],
      bucket: Int,
      splits: Seq[Array[Byte]])
    extends InputPartition

  private case class PostponeInputPartition(task: PostponeFileReadTask) extends InputPartition

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
          case (key, splits) =>
            RealBucketInputPartition(
              key.partition.toArray,
              key.bucket,
              splits.iterator.map(serializeSplit).toList)
        }
      val postponePartitions =
        corePlan.postponeFileTasks().asScala.map(PostponeInputPartition)
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
        case real: RealBucketInputPartition => new RealBucketPartitionReader(real)
        case postpone: PostponeInputPartition =>
          new PostponePartitionReader(
            postpone.task,
            readBuilder,
            keyType,
            mergeReadType,
            bucketRouter)
        case other =>
          throw new IllegalArgumentException(
            "Unsupported postpone merge input partition: " + other.getClass.getName + ".")
      }
    }
  }

  private class RealBucketPartitionReader(partition: RealBucketInputPartition)
    extends PartitionReader[InternalRow] {

    private val splits = partition.splits.iterator
    private var currentSplit: Array[Byte] = _

    override def next(): Boolean = {
      if (splits.hasNext) {
        currentSplit = splits.next()
        true
      } else {
        currentSplit = null
        false
      }
    }

    override def get(): InternalRow = {
      new GenericInternalRow(
        Array[Any](
          partition.partition,
          partition.bucket,
          REAL_SPLIT,
          currentSplit,
          null,
          0L,
          0.toByte,
          null))
    }

    override def close(): Unit = {}
  }

  private class PostponePartitionReader(
      task: PostponeFileReadTask,
      readBuilder: PostponeMergeReadBuilder,
      keyType: RowType,
      mergeReadType: RowType,
      bucketRouter: PostponeBucketRouter)
    extends PartitionReader[InternalRow] {

    private val split = task.split()
    private val keySerializer = new InternalRowSerializer(keyType)
    private val valueSerializer = new InternalRowSerializer(mergeReadType)
    private val partitionBytes = SerializationUtils.serializeBinaryRow(split.partition())
    private val bucketComputer = bucketRouter.createComputer()
    private val ioManager: IOManager = SparkUtils.createIOManager()
    private val reader =
      try {
        readBuilder.newRead().withIOManager(ioManager).createPostponeFileReader(task)
      } catch {
        case failure: Throwable =>
          try {
            ioManager.close()
          } catch {
            case closeFailure: Throwable => failure.addSuppressed(closeFailure)
          }
          throw failure
      }

    private var batch: RecordReader.RecordIterator[KeyValue] = _
    private var current: InternalRow = _
    private var closed = false

    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

    override def next(): Boolean = {
      current = null
      while (current == null && !closed) {
        if (batch == null) {
          batch = reader.readBatch()
          if (batch == null) {
            close()
            return false
          }
        }

        val keyValue = batch.next()
        if (keyValue == null) {
          batch.releaseBatch()
          batch = null
        } else {
          val key = keySerializer.toBinaryRow(keyValue.key())
          val value = valueSerializer.toBinaryRow(keyValue.value())
          current = new GenericInternalRow(
            Array[Any](
              partitionBytes,
              bucketComputer.bucket(split.partition(), keyValue.key()),
              POSTPONE_RECORD,
              null,
              SerializationUtils.serializeBinaryRow(key),
              keyValue.sequenceNumber(),
              keyValue.valueKind().toByteValue,
              SerializationUtils.serializeBinaryRow(value)
            ))
        }
      }
      current != null
    }

    override def get(): InternalRow = current

    override def close(): Unit = {
      if (!closed) {
        closed = true
        try {
          if (batch != null) {
            batch.releaseBatch()
            batch = null
          }
          reader.close()
        } finally {
          ioManager.close()
        }
      }
    }
  }

  private def bucketKey(split: DataSplit): BucketKey = {
    BucketKey(SerializationUtils.serializeBinaryRow(split.partition()).toIndexedSeq, split.bucket())
  }

  private def serializeSplit(split: DataSplit): Array[Byte] = {
    val output = new DataOutputSerializer(128)
    split.serialize(output)
    output.getCopyOfBuffer
  }
}
