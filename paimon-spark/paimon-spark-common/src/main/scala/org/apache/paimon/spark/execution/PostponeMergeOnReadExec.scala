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

package org.apache.paimon.spark.execution

import org.apache.paimon.KeyValue
import org.apache.paimon.data.{InternalRow => PaimonInternalRow}
import org.apache.paimon.io.DataInputDeserializer
import org.apache.paimon.reader.RecordReader
import org.apache.paimon.spark.PostponeMergeInputScan._
import org.apache.paimon.spark.PostponeMergeOnReadScan.MergePlan
import org.apache.paimon.spark.SparkUtils
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.table.source.{DataSplit, PostponeMergeRead, PostponeMergeReadBuilder}
import org.apache.paimon.types.{RowKind, RowType}
import org.apache.paimon.utils.{IteratorRecordReader, SerializationUtils}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import java.util.Arrays

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/** Merges a Spark-clustered stream of real splits and postpone records through Paimon Core. */
private[spark] case class PostponeMergeOnReadExec(
    override val output: Seq[Attribute],
    @transient mergePlan: MergePlan,
    shufflePartitions: Int,
    child: SparkPlan)
  extends UnaryExecNode {

  override def requiredChildDistribution: Seq[Distribution] = {
    Seq(
      SparkShimLoader.shim.createClusteredDistribution(
        Seq(child.output(PARTITION_ORDINAL), child.output(BUCKET_ORDINAL)),
        Math.max(1, shufflePartitions)))
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    Seq(
      Seq(
        SortOrder(child.output(PARTITION_ORDINAL), Ascending),
        SortOrder(child.output(BUCKET_ORDINAL), Ascending),
        SortOrder(child.output(KIND_ORDINAL), Ascending)
      ))
  }

  override def outputPartitioning: Partitioning = {
    UnknownPartitioning(child.outputPartitioning.numPartitions)
  }

  override def outputOrdering: Seq[SortOrder] = Nil

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // Extract execution values so executor closures never capture this SparkPlan.
    val readBuilder = mergePlan.readBuilder
    val resultRowType = mergePlan.corePlan.resultReadType()
    val blobAsDescriptor = mergePlan.blobAsDescriptor
    val outputAttributes = output

    child.execute().mapPartitions {
      rows =>
        val unsafeProjection = UnsafeProjection.create(outputAttributes, outputAttributes)
        new PostponeMergeOnReadExec.SortedBucketMergeIterator(
          rows,
          readBuilder,
          resultRowType,
          blobAsDescriptor)
          .map(row => unsafeProjection(row).copy(): InternalRow)
    }
  }
}

private[spark] object PostponeMergeOnReadExec {

  private case class BucketKey(partition: Array[Byte], bucket: Int)

  private def deserializeSplit(serialized: Array[Byte]): DataSplit = {
    DataSplit.deserialize(new DataInputDeserializer(serialized))
  }

  private def sameBucket(row: InternalRow, bucketKey: BucketKey): Boolean = {
    row.getInt(BUCKET_ORDINAL) == bucketKey.bucket &&
    Arrays.equals(row.getBinary(PARTITION_ORDINAL), bucketKey.partition)
  }

  /** Opens at most one Core merge reader at a time over the Spark-sorted carrier rows. */
  private class SortedBucketMergeIterator(
      rows: Iterator[InternalRow],
      readBuilder: PostponeMergeReadBuilder,
      resultRowType: RowType,
      blobAsDescriptor: Boolean)
    extends Iterator[InternalRow]
    with AutoCloseable {

    private val bufferedRows = rows.buffered
    private val ioManager = SparkUtils.createIOManager()
    private val read = readBuilder.newRead().withIOManager(ioManager)
    private var currentReader: BucketMergeIterator = _
    private var nextRow: InternalRow = _
    private var closed = false

    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

    override def hasNext: Boolean = {
      advanceIfNeeded()
      nextRow != null
    }

    override def next(): InternalRow = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val result = nextRow
      nextRow = null
      result
    }

    private def advanceIfNeeded(): Unit = {
      while (nextRow == null && !closed) {
        if (currentReader == null && !openNextReader()) {
          close()
          return
        }
        if (currentReader.hasNext) {
          nextRow = currentReader.next()
        } else {
          closeCurrentReader()
        }
      }
    }

    private def openNextReader(): Boolean = {
      if (!bufferedRows.hasNext) {
        false
      } else {
        val first = bufferedRows.head
        val bucketKey = BucketKey(first.getBinary(PARTITION_ORDINAL), first.getInt(BUCKET_ORDINAL))
        val realSplits = ArrayBuffer.empty[Array[Byte]]
        while (
          bufferedRows.hasNext &&
          sameBucket(bufferedRows.head, bucketKey) &&
          bufferedRows.head.getByte(KIND_ORDINAL) == REAL_SPLIT
        ) {
          realSplits += bufferedRows.next().getBinary(REAL_SPLIT_ORDINAL)
        }

        val postponeRecords = new Iterator[KeyValue] {
          override def hasNext: Boolean = {
            bufferedRows.hasNext &&
            sameBucket(bufferedRows.head, bucketKey) &&
            bufferedRows.head.getByte(KIND_ORDINAL) == POSTPONE_RECORD
          }

          override def next(): KeyValue = {
            if (!hasNext) {
              throw new NoSuchElementException
            }
            val row = bufferedRows.next()
            new KeyValue().replace(
              SerializationUtils.deserializeBinaryRow(row.getBinary(KEY_ORDINAL)),
              row.getLong(SEQUENCE_ORDINAL),
              RowKind.fromByteValue(row.getByte(ROW_KIND_ORDINAL)),
              SerializationUtils.deserializeBinaryRow(row.getBinary(VALUE_ORDINAL))
            )
          }
        }

        // Core drains postponeRecords into its spillable sort buffer before returning the reader.
        currentReader = new BucketMergeIterator(
          realSplits.iterator,
          postponeRecords,
          read,
          resultRowType,
          blobAsDescriptor)
        if (bufferedRows.hasNext && sameBucket(bufferedRows.head, bucketKey)) {
          throw new IllegalStateException(
            "Unexpected postpone merge carrier kind " +
              bufferedRows.head.getByte(KIND_ORDINAL) + " for one bucket.")
        }
        true
      }
    }

    private def closeCurrentReader(): Unit = {
      if (currentReader != null) {
        currentReader.close()
        currentReader = null
      }
    }

    override def close(): Unit = {
      if (!closed) {
        closed = true
        try {
          closeCurrentReader()
        } finally {
          ioManager.close()
        }
      }
    }
  }

  private class BucketMergeIterator(
      serializedRealSplits: Iterator[Array[Byte]],
      postponeRecords: Iterator[KeyValue],
      read: PostponeMergeRead,
      resultRowType: RowType,
      blobAsDescriptor: Boolean)
    extends Iterator[InternalRow]
    with AutoCloseable {

    private val sparkRow = SparkInternalRow.create(resultRowType, blobAsDescriptor)
    private val reader = read.createBucketMergeReader(
      serializedRealSplits.map(deserializeSplit).toList.asJava,
      new IteratorRecordReader[KeyValue](postponeRecords.asJava))

    private var currentBatch: RecordReader.RecordIterator[PaimonInternalRow] = _
    private var nextRow: InternalRow = _
    private var closed = false

    override def hasNext: Boolean = {
      advanceIfNeeded()
      nextRow != null
    }

    override def next(): InternalRow = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val result = nextRow
      nextRow = null
      result
    }

    private def advanceIfNeeded(): Unit = {
      while (nextRow == null && !closed) {
        if (currentBatch == null) {
          currentBatch = reader.readBatch()
          if (currentBatch == null) {
            close()
            return
          }
        }

        val row = currentBatch.next()
        if (row == null) {
          currentBatch.releaseBatch()
          currentBatch = null
        } else {
          nextRow = sparkRow.replace(row).copy()
        }
      }
    }

    override def close(): Unit = {
      if (!closed) {
        closed = true
        if (currentBatch != null) {
          currentBatch.releaseBatch()
          currentBatch = null
        }
        reader.close()
      }
    }
  }
}
