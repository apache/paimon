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

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.DYNAMIC_PARTITION_OVERWRITE
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.index.PartitionIndex
import org.apache.paimon.spark.{DynamicOverWrite, InsertInto, Overwrite, SaveMode, SparkRow}
import org.apache.paimon.spark.SparkUtils.createIOManager
import org.apache.paimon.table.{BucketMode, FileStoreTable}
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessageSerializer, DynamicBucketRow, RowPartitionKeyExtractor}
import org.apache.paimon.types.RowType

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import java.util.function.IntPredicate

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Used to write a [[DataFrame]] into a paimon table. */
case class WriteIntoPaimonTable(
    override val originTable: FileStoreTable,
    saveMode: SaveMode,
    data: DataFrame,
    options: CoreOptions)
  extends RunnableCommand
  with PaimonCommand
  with SchemaHelper
  with Logging {

  import WriteIntoPaimonTable._

  private lazy val serializer = new CommitMessageSerializer

  private lazy val mergeSchema = options.mergeSchema()

  /** \1. 2. */
  override def run(sparkSession: SparkSession): Seq[Row] = {
    import sparkSession.implicits._

    if (mergeSchema) {
      mergeAndCommitSchema(data.schema)
    }

    val (dynamicPartitionOverwriteMode, overwritePartition) = parseSaveMode()
    // use the extra options to rebuild the table object
    updateTableWithOptions(
      Map(DYNAMIC_PARTITION_OVERWRITE.key -> dynamicPartitionOverwriteMode.toString))

    val primaryKeyCols = tableSchema.trimmedPrimaryKeys().asScala.map(col)
    val partitionCols = tableSchema.partitionKeys().asScala.map(col)

    val dataEncoder = RowEncoder.apply(data.schema).resolveAndBind()
    val originFromRow = dataEncoder.createDeserializer()

    // append _bucket_ column as placeholder
    val withBucketCol = data.withColumn(BUCKET_COL, lit(-1))
    val bucketColIdx = withBucketCol.schema.size - 1
    val withBucketDataEncoder = RowEncoder.apply(withBucketCol.schema).resolveAndBind()
    val toRow = withBucketDataEncoder.createSerializer()
    val fromRow = withBucketDataEncoder.createDeserializer()

    def repartitionByBucket(ds: Dataset[Row]) = {
      ds.toDF().repartition(partitionCols ++ Seq(col(BUCKET_COL)): _*)
    }

    val rowType = table.rowType()
    val writeBuilder = table.newBatchWriteBuilder()

    val df =
      bucketMode match {
        case BucketMode.DYNAMIC =>
          val partitioned = if (primaryKeyCols.nonEmpty) {
            // Make sure that the records with the same bucket values is within a task.
            withBucketCol.repartition(primaryKeyCols: _*)
          } else {
            withBucketCol
          }
          val numSparkPartitions = partitioned.rdd.getNumPartitions
          val dynamicBucketProcessor =
            DynamicBucketProcessor(table, rowType, bucketColIdx, numSparkPartitions, toRow, fromRow)
          repartitionByBucket(
            partitioned.mapPartitions(dynamicBucketProcessor.processPartition)(
              withBucketDataEncoder))
        case BucketMode.UNAWARE =>
          val unawareBucketProcessor = UnawareBucketProcessor(bucketColIdx, toRow, fromRow)
          withBucketCol
            .mapPartitions(unawareBucketProcessor.processPartition)(withBucketDataEncoder)
            .toDF()
        case BucketMode.FIXED =>
          val commonBucketProcessor =
            CommonBucketProcessor(writeBuilder, bucketColIdx, toRow, fromRow)
          repartitionByBucket(
            withBucketCol.mapPartitions(commonBucketProcessor.processPartition)(
              withBucketDataEncoder))
      }

    val commitMessages = df
      .mapPartitions {
        iter =>
          val write = writeBuilder.newWrite()
          write.withIOManager(createIOManager)
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
      val tableCommit = if (overwritePartition == null) {
        writeBuilder.newCommit()
      } else {
        writeBuilder.withOverwrite(overwritePartition.asJava).newCommit()
      }
      tableCommit.commit(commitMessages.toList.asJava)
    } catch {
      case e: Throwable => throw new RuntimeException(e);
    }

    Seq.empty
  }

  private def parseSaveMode(): (Boolean, Map[String, String]) = {
    var dynamicPartitionOverwriteMode = false
    val overwritePartition = saveMode match {
      case InsertInto => null
      case Overwrite(filter) =>
        if (filter.isEmpty) {
          Map.empty[String, String]
        } else if (isTruncate(filter.get)) {
          Map.empty[String, String]
        } else {
          convertFilterToMap(filter.get, table.schema.logicalPartitionType())
        }
      case DynamicOverWrite =>
        dynamicPartitionOverwriteMode = true
        throw new UnsupportedOperationException("Dynamic Overwrite is unsupported for now.")
      case _ =>
        throw new UnsupportedOperationException(s" This mode is unsupported for now.")
    }
    (dynamicPartitionOverwriteMode, overwritePartition)
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

  case class UnawareBucketProcessor(
      bucketColIndex: Int,
      toRow: ExpressionEncoder.Serializer[Row],
      fromRow: ExpressionEncoder.Deserializer[Row])
    extends BucketProcessor {

    def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {
      new Iterator[Row] {
        override def hasNext: Boolean = rowIterator.hasNext

        override def next(): Row = {
          val row = rowIterator.next
          val sparkInternalRow = toRow(row)
          sparkInternalRow.setInt(bucketColIndex, 0)
          fromRow(sparkInternalRow)
        }
      }
    }
  }
}
