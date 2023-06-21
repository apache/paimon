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

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.index.PartitionIndex
import org.apache.paimon.spark.SparkRow
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessageSerializer, DynamicBucketRow, InnerTableCommit, RowPartitionKeyExtractor}
import org.apache.paimon.types.RowType

import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.functions._

import java.util.function.IntPredicate

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Used to write a [[DataFrame]] into a paimon table. */
case class WriteIntoPaimonTable(table: FileStoreTable, overwrite: Boolean, data: DataFrame)
  extends RunnableCommand
  with PaimonCommand {

  import WriteIntoPaimonTable._

  private lazy val tableSchema = table.schema()

  private lazy val rowType = table.rowType()

  private lazy val writeBuilder: BatchWriteBuilder = table.newBatchWriteBuilder()

  private lazy val serializer = new CommitMessageSerializer

  if (overwrite) {
    throw new UnsupportedOperationException("Overwrite is unsupported.");
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    import sparkSession.implicits._

    val dataEncoder = RowEncoder.apply(data.schema).resolveAndBind()
    val originFromRow = dataEncoder.createDeserializer()

    val partitionCols = tableSchema.partitionKeys().asScala.map(col)
    val primaryKeyCols = tableSchema.trimmedPrimaryKeys().asScala.map(col)
    val partitioned = if (isDynamicBucketTable && primaryKeyCols.nonEmpty) {
      // Make sure that the records with the same bucket values is within a task.
      data.repartition(primaryKeyCols: _*)
    } else {
      data
    }

    // append _bucket_ column as placeholder
    val withBucketCol = partitioned.withColumn(BUCKET_COL, lit(-1))
    val numSparkPartitions = withBucketCol.rdd.getNumPartitions
    val bucketColIdx = withBucketCol.schema.size - 1
    val withBucketDataEncoder = RowEncoder.apply(withBucketCol.schema).resolveAndBind()
    val toRow = withBucketDataEncoder.createSerializer()
    val fromRow = withBucketDataEncoder.createDeserializer()

    val bucketProcessor = if (isDynamicBucketTable) {
      DynamicBucketProcessor(table, rowType, bucketColIdx, numSparkPartitions, toRow, fromRow)
    } else {
      CommonBucketProcessor(writeBuilder, bucketColIdx, toRow, fromRow)
    }

    val committables =
      withBucketCol
        .mapPartitions(bucketProcessor.processPartition)(withBucketDataEncoder)
        .toDF()
        .repartition(partitionCols ++ Seq(col(BUCKET_COL)): _*)
        .mapPartitions {
          iter =>
            val write = writeBuilder.newWrite()
            try {
              iter.foreach {
                row =>
                  val bucket = row.getInt(bucketColIdx)
                  val bucketColDropped = originFromRow(toRow(row))
                  write.write(new DynamicBucketRow(new SparkRow(rowType, bucketColDropped), bucket))
              }
              val serializer = new CommitMessageSerializer
              write.prepareCommit().asScala.map(serializer.serialize).toIterator
            } finally {
              write.close()
            }
        }
        .collect()
        .map(deserializeCommitMessage(serializer, _))

    try {
      val tableCommit = writeBuilder.newCommit().asInstanceOf[InnerTableCommit]
      tableCommit.commit(committables.toList.asJava)
    } catch {
      case e: Throwable => throw new RuntimeException(e);
    }

    Seq.empty
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
    this.asInstanceOf[WriteIntoPaimonTable]
}

object WriteIntoPaimonTable {

  sealed trait BucketProcessor {
    def processPartition(rowIterator: Iterator[Row]): Iterator[Row]
  }

  case class CommonBucketProcessor(
      writeBuilder: BatchWriteBuilder,
      bucketColIndex: Int,
      toRow: ExpressionEncoder.Serializer[Row],
      fromRow: ExpressionEncoder.Deserializer[Row])
    extends BucketProcessor {

    private val rowType = writeBuilder.rowType

    def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {
      val batchTableWrite = writeBuilder.newWrite()
      new Iterator[Row] {
        override def hasNext: Boolean = rowIterator.hasNext

        override def next(): Row = {
          val row = rowIterator.next
          val sparkInternalRow = toRow(row)
          sparkInternalRow.setInt(
            bucketColIndex,
            batchTableWrite.getBucket(new SparkRow(rowType, row)))
          fromRow(sparkInternalRow)
        }
      }
    }
  }

  case class DynamicBucketProcessor(
      fileStoreTable: FileStoreTable,
      rowType: RowType,
      bucketColIndex: Int,
      numSparkPartitions: Long,
      toRow: ExpressionEncoder.Serializer[Row],
      fromRow: ExpressionEncoder.Deserializer[Row]
  ) extends BucketProcessor {

    private val targetBucketRowNumber = fileStoreTable.coreOptions.dynamicBucketTargetRowNum

    def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {
      val sparkPartitionId = TaskContext.getPartitionId()
      val indexFileHandler = fileStoreTable.store.newIndexFileHandler
      val partitionIndex = mutable.Map.empty[BinaryRow, PartitionIndex]
      val rowPartitionKeyExtractor = new RowPartitionKeyExtractor(fileStoreTable.schema)
      val buckFilter: IntPredicate = bucket =>
        math.abs(bucket % numSparkPartitions) == sparkPartitionId

      new Iterator[Row]() {
        override def hasNext: Boolean = rowIterator.hasNext

        override def next(): Row = {
          val row = rowIterator.next
          val sparkRow = new SparkRow(rowType, row)
          val hash = rowPartitionKeyExtractor.trimmedPrimaryKey(sparkRow).hashCode
          val partition = rowPartitionKeyExtractor.partition(sparkRow)
          val index = partitionIndex.getOrElseUpdate(
            partition,
            PartitionIndex.loadIndex(
              indexFileHandler,
              partition,
              targetBucketRowNumber,
              (_) => true))
          val bucket = index.assign(hash, buckFilter)
          val sparkInternalRow = toRow(row)
          sparkInternalRow.setInt(bucketColIndex, bucket)
          fromRow(sparkInternalRow)
        }
      }
    }
  }
}
