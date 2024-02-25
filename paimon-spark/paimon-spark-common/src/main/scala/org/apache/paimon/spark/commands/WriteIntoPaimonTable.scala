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

import org.apache.paimon.CoreOptions.DYNAMIC_PARTITION_OVERWRITE
import org.apache.paimon.index.{BucketAssigner, HashBucketAssigner}
import org.apache.paimon.options.Options
import org.apache.paimon.spark._
import org.apache.paimon.spark.SparkUtils.createIOManager
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.spark.schema.SparkSystemColumns.{BUCKET_COL, ROW_KIND_COL}
import org.apache.paimon.spark.util.{EncoderUtils, SparkRowUtils}
import org.apache.paimon.table.{BucketMode, FileStoreTable}
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessageSerializer, RowPartitionKeyExtractor}
import org.apache.paimon.types.RowType

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.functions._

import java.util.UUID

import scala.collection.JavaConverters._

/** Used to write a [[DataFrame]] into a paimon table. */
case class WriteIntoPaimonTable(
    override val originTable: FileStoreTable,
    saveMode: SaveMode,
    data: DataFrame,
    options: Options)
  extends RunnableCommand
  with PaimonCommand
  with SchemaHelper
  with Logging {

  import WriteIntoPaimonTable._

  private lazy val serializer = new CommitMessageSerializer

  private lazy val mergeSchema = options.get(SparkConnectorOptions.MERGE_SCHEMA)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    import sparkSession.implicits._

    val dataSchema = SparkSystemColumns.filterSparkSystemColumns(data.schema)

    if (mergeSchema) {
      val allowExplicitCast = options.get(SparkConnectorOptions.EXPLICIT_CAST)
      mergeAndCommitSchema(dataSchema, allowExplicitCast)
    }

    val (dynamicPartitionOverwriteMode, overwritePartition) = parseSaveMode()
    // use the extra options to rebuild the table object
    updateTableWithOptions(
      Map(DYNAMIC_PARTITION_OVERWRITE.key -> dynamicPartitionOverwriteMode.toString))

    val primaryKeyCols = tableSchema.trimmedPrimaryKeys().asScala.map(col)
    val partitionCols = tableSchema.partitionKeys().asScala.map(col)

    val dataEncoder = EncoderUtils.encode(dataSchema).resolveAndBind()
    val originFromRow = dataEncoder.createDeserializer()

    val rowkindColIdx = SparkRowUtils.getFieldIndex(data.schema, ROW_KIND_COL)

    // append _bucket_ column as placeholder
    val withBucketCol = data.withColumn(BUCKET_COL, lit(-1))
    val bucketColIdx = withBucketCol.schema.size - 1
    val withBucketDataEncoder = EncoderUtils.encode(withBucketCol.schema).resolveAndBind()
    val toRow = withBucketDataEncoder.createSerializer()
    val fromRow = withBucketDataEncoder.createDeserializer()

    def repartitionByKeyPartitionHash(
        input: DataFrame,
        sparkRowType: RowType,
        numParallelism: Int,
        numAssigners: Int) = {
      sparkSession.createDataFrame(
        input.rdd
          .mapPartitions(
            iterator => {
              val rowPartitionKeyExtractor = new RowPartitionKeyExtractor(table.schema)
              iterator.map(
                row => {
                  val sparkRow = new SparkRow(sparkRowType, row)
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
        withBucketCol.schema
      )
    }

    def repartitionByBucket(ds: Dataset[Row]) = {
      ds.toDF().repartition(partitionCols ++ Seq(col(BUCKET_COL)): _*)
    }

    val rowType = table.rowType()
    val writeBuilder = table.newBatchWriteBuilder()

    if (overwritePartition != null) {
      writeBuilder.withOverwrite(overwritePartition.asJava)
    }

    val df =
      bucketMode match {
        case BucketMode.DYNAMIC =>
          // Topology: input -> shuffle by special key & partition hash -> bucket-assigner -> shuffle by partition & bucket
          val numParallelism =
            if (table.coreOptions.dynamicBucketAssignerParallelism.!=(null))
              table.coreOptions.dynamicBucketAssignerParallelism.toInt
            else sparkSession.sparkContext.defaultParallelism
          val numAssigners =
            if (table.coreOptions().dynamicBucketInitialBuckets.!=(null))
              Math.min(table.coreOptions().dynamicBucketInitialBuckets.toInt, numParallelism)
            else numParallelism

          val partitioned = if (primaryKeyCols.nonEmpty) {
            repartitionByKeyPartitionHash(withBucketCol, rowType, numParallelism, numAssigners)
          } else {
            withBucketCol
          }
          val dynamicBucketProcessor =
            DynamicBucketProcessor(
              table,
              rowType,
              bucketColIdx,
              numParallelism,
              numAssigners,
              toRow,
              fromRow)
          repartitionByBucket(
            partitioned.mapPartitions(dynamicBucketProcessor.processPartition)(
              withBucketDataEncoder))
        case BucketMode.UNAWARE =>
          // Topology: input -> bucket-assigner
          val unawareBucketProcessor = UnawareBucketProcessor(bucketColIdx, toRow, fromRow)
          withBucketCol
            .mapPartitions(unawareBucketProcessor.processPartition)(withBucketDataEncoder)
            .toDF()
        case BucketMode.FIXED =>
          // Topology: input -> bucket-assigner -> shuffle by partition & bucket
          val commonBucketProcessor =
            CommonBucketProcessor(writeBuilder, bucketColIdx, toRow, fromRow)
          repartitionByBucket(
            withBucketCol.mapPartitions(commonBucketProcessor.processPartition)(
              withBucketDataEncoder))
        case _ =>
          throw new UnsupportedOperationException(
            s"Write with bucket mode $bucketMode is not supported")
      }

    val commitMessages = df
      .mapPartitions {
        iter =>
          val ioManager = createIOManager
          val write = writeBuilder.newWrite()
          write.withIOManager(ioManager)
          try {
            iter.foreach {
              row =>
                val bucket = row.getInt(bucketColIdx)
                val bucketColDropped = originFromRow(toRow(row))
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

    val tableCommit = writeBuilder.newCommit()
    try {
      tableCommit.commit(commitMessages.toList.asJava)
    } catch {
      case e: Throwable => throw new RuntimeException(e);
    } finally {
      tableCommit.close()
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
        Map.empty[String, String]
      case _ =>
        throw new UnsupportedOperationException(s" This mode is unsupported for now.")
    }
    (dynamicPartitionOverwriteMode, overwritePartition)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
    this.asInstanceOf[WriteIntoPaimonTable]

}

object WriteIntoPaimonTable {

  sealed private trait BucketProcessor {
    def processPartition(rowIterator: Iterator[Row]): Iterator[Row]
  }

  private case class CommonBucketProcessor(
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

  private case class DynamicBucketProcessor(
      fileStoreTable: FileStoreTable,
      rowType: RowType,
      bucketColIndex: Int,
      numSparkPartitions: Int,
      numAssigners: Int,
      toRow: ExpressionEncoder.Serializer[Row],
      fromRow: ExpressionEncoder.Deserializer[Row]
  ) extends BucketProcessor {

    private val targetBucketRowNumber = fileStoreTable.coreOptions.dynamicBucketTargetRowNum
    private val commitUser = UUID.randomUUID.toString

    def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {
      val rowPartitionKeyExtractor = new RowPartitionKeyExtractor(fileStoreTable.schema)
      val assigner = new HashBucketAssigner(
        fileStoreTable.snapshotManager(),
        commitUser,
        fileStoreTable.store.newIndexFileHandler,
        numSparkPartitions,
        numAssigners,
        TaskContext.getPartitionId(),
        targetBucketRowNumber
      )

      new Iterator[Row]() {
        override def hasNext: Boolean = rowIterator.hasNext

        override def next(): Row = {
          val row = rowIterator.next
          val sparkRow = new SparkRow(rowType, row)
          val hash = rowPartitionKeyExtractor.trimmedPrimaryKey(sparkRow).hashCode
          val partition = rowPartitionKeyExtractor.partition(sparkRow)
          val bucket = assigner.assign(partition, hash)
          val sparkInternalRow = toRow(row)
          sparkInternalRow.setInt(bucketColIndex, bucket)
          fromRow(sparkInternalRow)
        }
      }
    }
  }

  private case class UnawareBucketProcessor(
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

  private case class ModPartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = key.asInstanceOf[Int] % numPartitions
  }
}
