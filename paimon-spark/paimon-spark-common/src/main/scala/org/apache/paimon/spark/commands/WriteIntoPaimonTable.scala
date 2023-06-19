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
import org.apache.paimon.table.{FileStoreTable, Table}
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessageSerializer, DynamicBucketRow, InnerTableCommit, RowPartitionKeyExtractor}
import org.apache.paimon.types.RowType

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Used to write a [[DataFrame]] into a paimon table. */
case class WriteIntoPaimonTable(
    table: Table,
    overwrite: Boolean,
    data: DataFrame)
  extends RunnableCommand
  with PaimonCommand {

  import WriteIntoPaimonTable._

  if (overwrite) {
    throw new UnsupportedOperationException("Overwrite is unsupported.");
  }

  private val writeBuilder: BatchWriteBuilder = table.newBatchWriteBuilder()
  private val rowType = writeBuilder.rowType()
  private lazy val serializer = new CommitMessageSerializer

  override def run(sparkSession: SparkSession): Seq[Row] = {
    import sparkSession.implicits._

    val partitioned = if (table.partitionKeys().isEmpty) {
      data
    } else {
      // repartition data in order to reduce the number of PartitionIndex for per Spark Task.
      data.repartition(table.partitionKeys().asScala.map(col): _*)
    }

    // append _bucket_ column as placeholder
    val withBucketCol = partitioned.withColumn(BUCKET_COL, lit(-1))
    val bucketColIdx = withBucketCol.schema.size - 1

    val dataEncoder = RowEncoder.apply(withBucketCol.schema).resolveAndBind()
    val toRow = dataEncoder.createSerializer()
    val fromRow = dataEncoder.createDeserializer()

    val bucketProcessor = if (isDynamicBucketTable) {
      val fileStoreTable = table.asInstanceOf[FileStoreTable]
      DynamicBucketProcessor(fileStoreTable, rowType, bucketColIdx, toRow, fromRow)
    } else {
      CommonBucketProcessor(writeBuilder, bucketColIdx, toRow, fromRow)
    }
    val committables =
      withBucketCol
        .mapPartitions(bucketProcessor.processPartition)(dataEncoder)
        .groupByKey(row => row.getInt(bucketColIdx))
        .flatMapGroups {
          (bucket, iter) =>
            val write = writeBuilder.newWrite()
            try {
              val serializer = new CommitMessageSerializer
              iter.foreach {
                row =>
                  // Notice: `row` contains a _bucket_ column, but there's no impact and we don't have to remove it.
                  write.write(new DynamicBucketRow(new SparkRow(rowType, row), bucket))
              }
              write.prepareCommit().asScala.map(serializer.serialize)
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
      toRow: ExpressionEncoder.Serializer[Row],
      fromRow: ExpressionEncoder.Deserializer[Row]
  ) extends BucketProcessor {

    private val targetBucketRowNumber = fileStoreTable.coreOptions.dynamicBucketTargetRowNum

    def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {
      val indexFileHandler = fileStoreTable.store.newIndexFileHandler
      val partitionIndex = mutable.Map.empty[BinaryRow, PartitionIndex]
      val rowPartitionKeyExtractor = new RowPartitionKeyExtractor(fileStoreTable.schema)
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
          val bucket = index.assign(hash, (_) => true)
          val sparkInternalRow = toRow(row)
          sparkInternalRow.setInt(bucketColIndex, bucket)
          fromRow(sparkInternalRow)
        }
      }
    }
  }
}
