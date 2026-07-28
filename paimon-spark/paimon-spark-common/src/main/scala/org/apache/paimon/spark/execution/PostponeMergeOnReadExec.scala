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

import org.apache.paimon.{CoreOptions, KeyValue}
import org.apache.paimon.data.{InternalRow => PaimonInternalRow}
import org.apache.paimon.reader.RecordReaderIterator
import org.apache.paimon.spark.PaimonMetrics._
import org.apache.paimon.spark.PostponeMergeInputScan._
import org.apache.paimon.spark.PostponeMergeOnRead.MergePlan
import org.apache.paimon.spark.SparkUtils
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.spark.read.BinPackingSplits
import org.apache.paimon.spark.util.SplitUtils
import org.apache.paimon.table.source.{DataSplit, PostponeMergePlan, PostponeMergeReadBuilder, SplitSerializer}
import org.apache.paimon.types.{RowKind, RowType}
import org.apache.paimon.utils.{IteratorRecordReader, SerializationUtils}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{ExplainUtils, SparkPlan, SQLExecution, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import java.util.Arrays

import scala.collection.JavaConverters._

/** Merges a Spark-clustered stream of real splits and postpone records through Paimon Core. */
private[spark] case class PostponeMergeOnReadExec(
    override val output: Seq[Attribute],
    @transient mergePlan: MergePlan,
    numShufflePartitions: Int,
    child: SparkPlan)
  extends UnaryExecNode {

  private val NUM_OUTPUT_ROWS = "numOutputRows"

  override def nodeName: String = s"PaimonPostponeMergeScan ${mergePlan.realScanInfo.tableName}"

  override def simpleString(maxFields: Int): String = {
    s"$nodeName ${mergePlan.realScanInfo.description}"
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |Scan: ${mergePlan.realScanInfo.description}
       |""".stripMargin
  }

  override lazy val metrics: Map[String, SQLMetric] = {
    Map(
      NUM_OUTPUT_ROWS -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      NUM_SPLITS -> SQLMetrics.createMetric(sparkContext, "number of splits read"),
      PARTITION_SIZE -> SQLMetrics.createSizeMetric(sparkContext, "partition size"),
      READ_BATCH_TIME -> SQLMetrics.createTimingMetric(sparkContext, "read batch time"),
      PLANNING_DURATION -> SQLMetrics.createTimingMetric(sparkContext, "planing duration"),
      SCANNED_SNAPSHOT_ID -> SQLMetrics.createMetric(sparkContext, "scanned snapshot id"),
      SCANNED_MANIFESTS -> SQLMetrics.createMetric(sparkContext, "number of scanned manifests"),
      SKIPPED_TABLE_FILES -> SQLMetrics.createMetric(sparkContext, "number of skipped table files"),
      RESULTED_TABLE_FILES -> SQLMetrics.createMetric(
        sparkContext,
        "number of resulted table files")
    )
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    Seq(
      SparkShimLoader.shim.createClusteredDistribution(
        Seq(child.output(PARTITION_ORDINAL), child.output(BUCKET_ORDINAL)),
        Some(numShufflePartitions)))
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    Seq(
      Seq(
        SortOrder(child.output(PARTITION_ORDINAL), Ascending),
        SortOrder(child.output(BUCKET_ORDINAL), Ascending),
        SortOrder(child.output(INPUT_KIND_ORDINAL), Ascending),
        SortOrder(child.output(WRITER_LOCAL_ORDER_ORDINAL), Ascending)
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
    postDriverMetrics()

    val readBuilder = mergePlan.readBuilder
    val resultRowType = mergePlan.corePlan.resultReadType()
    val blobAsDescriptor = mergePlan.blobAsDescriptor
    val outputAttributes = output
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numSplits = longMetric(NUM_SPLITS)
    val partitionSize = longMetric(PARTITION_SIZE)
    val readBatchTime = longMetric(READ_BATCH_TIME)

    child.execute().mapPartitions {
      rows =>
        val unsafeProjection = UnsafeProjection.create(outputAttributes, outputAttributes)
        new PostponeMergeOnReadExec.SortedBucketMergeIterator(
          rows,
          readBuilder,
          resultRowType,
          blobAsDescriptor,
          numSplits,
          partitionSize,
          readBatchTime)
          .map {
            row =>
              numOutputRows += 1L
              unsafeProjection(row): InternalRow
          }
    }
  }

  private def postDriverMetrics(): Unit = {
    val updatedMetrics = mergePlan.realScanInfo.driverMetrics.flatMap {
      case (name, value) =>
        metrics.get(name).map {
          metric =>
            metric.set(value)
            metric
        }
    }.toSeq
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, updatedMetrics)
  }
}

private[spark] object PostponeMergeOnReadExec {

  private case class BucketKey(partition: Array[Byte], bucket: Int)

  private[spark] def computeShufflePartitions(
      plan: PostponeMergePlan,
      coreOptions: CoreOptions,
      conf: SQLConf): Int = {
    val maxShufflePartitions = conf.numShufflePartitions
    val openCostInBytes = BinPackingSplits.openCostInBytes(coreOptions, conf)
    val estimatedSize =
      BinPackingSplits.estimatedSize(plan.splits().asScala, openCostInBytes)
    val targetSize = BinPackingSplits.filesMaxPartitionBytes(coreOptions, conf)
    val sizeParallelism =
      if (estimatedSize <= 0) 1L else (estimatedSize - 1L) / targetSize + 1L
    val usefulParallelism = Math.max(
      1L,
      Math.min(maxShufflePartitions.toLong, Math.min(plan.numPotentialBuckets(), sizeParallelism)))
    // Leave room for different bucket groups which hash to the same reducer.
    val withHashHeadroom =
      if (usefulParallelism == 1L) 1L
      else usefulParallelism + (usefulParallelism + 1L) / 2L
    Math.min(maxShufflePartitions.toLong, withHashHeadroom).toInt
  }

  private def deserializeSplit(serialized: Array[Byte]): DataSplit = {
    SplitSerializer.deserialize(serialized).asInstanceOf[DataSplit]
  }

  private def sameBucket(row: InternalRow, bucketKey: BucketKey): Boolean = {
    row.getInt(BUCKET_ORDINAL) == bucketKey.bucket &&
    Arrays.equals(row.getBinary(PARTITION_ORDINAL), bucketKey.partition)
  }

  private class SortedBucketMergeIterator(
      rows: Iterator[InternalRow],
      readBuilder: PostponeMergeReadBuilder,
      resultRowType: RowType,
      blobAsDescriptor: Boolean,
      numSplits: SQLMetric,
      partitionSize: SQLMetric,
      readBatchTime: SQLMetric)
    extends Iterator[InternalRow]
    with AutoCloseable {

    private val bufferedRows = rows.buffered
    private val ioManager = SparkUtils.createIOManager()
    private val read = readBuilder.newRead().withIOManager(ioManager)
    private val sparkRow = SparkInternalRow.create(resultRowType, blobAsDescriptor)
    private var currentReader: RecordReaderIterator[PaimonInternalRow] = _
    private var currentTimedReader: TimedRecordReader[PaimonInternalRow] = _
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
          nextRow = sparkRow.replace(currentReader.next())
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
        val realSplit =
          if (
            sameBucket(bufferedRows.head, bucketKey) &&
            bufferedRows.head.getByte(INPUT_KIND_ORDINAL) == REAL_SPLIT
          ) {
            deserializeSplit(bufferedRows.next().getBinary(REAL_SPLIT_ORDINAL))
          } else {
            null
          }
        if (realSplit != null) {
          numSplits += 1L
          partitionSize += SplitUtils.splitSize(realSplit)
        }

        val postponeRecords = new Iterator[KeyValue] {
          override def hasNext: Boolean = {
            bufferedRows.hasNext &&
            sameBucket(bufferedRows.head, bucketKey) &&
            bufferedRows.head.getByte(INPUT_KIND_ORDINAL) == POSTPONE_RECORD
          }

          override def next(): KeyValue = {
            if (!hasNext) {
              throw new NoSuchElementException
            }
            val row = bufferedRows.next()
            new KeyValue().replace(
              SerializationUtils.deserializeBinaryRow(row.getBinary(KEY_ORDINAL)),
              RowKind.fromByteValue(row.getByte(ROW_KIND_ORDINAL)),
              SerializationUtils.deserializeBinaryRow(row.getBinary(VALUE_ORDINAL))
            )
          }
        }

        currentTimedReader = new TimedRecordReader[PaimonInternalRow](
          read.createBucketMergeReader(
            realSplit,
            new IteratorRecordReader[KeyValue](postponeRecords.asJava)))
        currentReader = new RecordReaderIterator[PaimonInternalRow](currentTimedReader)
        if (bufferedRows.hasNext && sameBucket(bufferedRows.head, bucketKey)) {
          throw new IllegalStateException(
            "Unexpected postpone merge carrier kind " +
              bufferedRows.head.getByte(INPUT_KIND_ORDINAL) + " for one bucket.")
        }
        true
      }
    }

    private def closeCurrentReader(): Unit = {
      if (currentReader != null) {
        try {
          currentReader.close()
        } finally {
          readBatchTime += currentTimedReader.readBatchTimeMs
          currentReader = null
          currentTimedReader = null
        }
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
}
