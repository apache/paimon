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
import org.apache.paimon.codegen.{CodeGenUtils, Projection}
import org.apache.paimon.crosspartition.{GlobalIndexAssigner, IndexBootstrap, KeyPartOrRow}
import org.apache.paimon.data.{BinaryRow, GenericRow, JoinedRow}
import org.apache.paimon.data.serializer.InternalSerializers
import org.apache.paimon.index.PartitionIndex
import org.apache.paimon.options.Options
import org.apache.paimon.spark._
import org.apache.paimon.spark.SparkUtils.createIOManager
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.spark.schema.SparkSystemColumns.{BUCKET_COL, ROW_KIND_COL}
import org.apache.paimon.spark.util.{EncoderUtils, SparkRowUtils}
import org.apache.paimon.table.{BucketMode, FileStoreTable}
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessageSerializer, DynamicBucketRow, RowPartitionKeyExtractor}
import org.apache.paimon.types.{RowKind, RowType}
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.functions._

import java.util.function.IntPredicate

import scala.collection.JavaConverters._
import scala.collection.mutable

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

    val (_, _, originFromRow) = EncoderUtils.getEncoderAndSerDe(dataSchema)

    var newData = data

    if (
      bucketMode.equals(BucketMode.GLOBAL_DYNAMIC) && !newData.schema.fieldNames.contains(
        ROW_KIND_COL)
    ) {
      newData = data.withColumn(ROW_KIND_COL, lit(RowKind.INSERT.toByteValue))
    }
    val rowkindColIdx = SparkRowUtils.getFieldIndex(newData.schema, ROW_KIND_COL)

    // append bucket column as placeholder
    newData = newData.withColumn(BUCKET_COL, lit(-1))
    val bucketColIdx = SparkRowUtils.getFieldIndex(newData.schema, BUCKET_COL)

    val (newDataEncoder, toRow, fromRow) = EncoderUtils.getEncoderAndSerDe(newData.schema)

    def repartitionByBucket(df: DataFrame) = {
      df.repartition(partitionCols ++ Seq(col(BUCKET_COL)): _*)
    }

    val rowType = table.rowType()
    val writeBuilder = table.newBatchWriteBuilder()

    val df: Dataset[Row] =
      bucketMode match {
        case BucketMode.DYNAMIC =>
          // Topology: input -> shuffle by key hash -> bucket-assigner -> shuffle by partition & bucket
          val partitioned = if (primaryKeyCols.nonEmpty) {
            // Make sure that the records with the same bucket values is within a task.
            newData.repartition(primaryKeyCols: _*)
          } else {
            newData
          }
          val numSparkPartitions = partitioned.rdd.getNumPartitions
          val dynamicBucketProcessor =
            DynamicBucketProcessor(table, rowType, bucketColIdx, numSparkPartitions, toRow, fromRow)

          repartitionByBucket(
            partitioned
              .mapPartitions(dynamicBucketProcessor.processPartition)(newDataEncoder)
              .toDF())
        case BucketMode.GLOBAL_DYNAMIC =>
          // Topology: input -> bootstrap -> shuffle by key hash -> bucket-assigner -> shuffle by partition & bucket
          val numSparkPartitions = newData.rdd.getNumPartitions
          val primaryKeys: java.util.List[String] = table.schema().primaryKeys()
          val bootstrapType: RowType = IndexBootstrap.bootstrapType(table.schema())
          val rowType: RowType = SparkTypeUtils.toPaimonType(newData.schema).asInstanceOf[RowType]

          // row: (keyHash, (kind, internalRow))
          val bootstrapRow: RDD[(Int, (KeyPartOrRow, Array[Byte]))] = newData.rdd.mapPartitions {
            iter =>
              {
                val sparkPartitionId = TaskContext.getPartitionId()

                val keyPartProject: Projection =
                  CodeGenUtils.newProjection(bootstrapType, primaryKeys)
                val rowProject: Projection = CodeGenUtils.newProjection(rowType, primaryKeys)
                val bootstrapSer = InternalSerializers.create(bootstrapType)
                val rowSer = InternalSerializers.create(rowType)

                val lst = scala.collection.mutable.ListBuffer[(Int, (KeyPartOrRow, Array[Byte]))]()

                val bootstrap = new IndexBootstrap(table)
                bootstrap.bootstrap(
                  numSparkPartitions,
                  sparkPartitionId,
                  row => {
                    val bytes: Array[Byte] =
                      SerializationUtils.serializeBinaryRow(bootstrapSer.toBinaryRow(row))
                    lst.append((keyPartProject(row).hashCode(), (KeyPartOrRow.KEY_PART, bytes)))
                  }
                )
                lst.iterator ++ iter.map(
                  r => {
                    val sparkRow =
                      new SparkRow(rowType, r, SparkRowUtils.getRowKind(r, rowkindColIdx))
                    val bytes: Array[Byte] =
                      SerializationUtils.serializeBinaryRow(rowSer.toBinaryRow(sparkRow))
                    (rowProject(sparkRow).hashCode(), (KeyPartOrRow.ROW, bytes))
                  })
              }
          }

          var assignerParallelism: Integer = table.coreOptions.dynamicBucketAssignerParallelism
          if (assignerParallelism == null) {
            assignerParallelism = numSparkPartitions
          }

          val rowRDD: RDD[Row] =
            bootstrapRow.partitionBy(new HashPartitioner(assignerParallelism)).mapPartitions {
              iter =>
                {
                  val sparkPartitionId = TaskContext.getPartitionId()
                  val lst = scala.collection.mutable.ListBuffer[Row]()
                  val ioManager = createIOManager
                  val assigner = new GlobalIndexAssigner(table)
                  try {
                    assigner.open(
                      ioManager,
                      assignerParallelism,
                      sparkPartitionId,
                      (row, bucket) => {
                        val extraRow: GenericRow = new GenericRow(2)
                        extraRow.setField(0, row.getRowKind.toByteValue)
                        extraRow.setField(1, bucket)
                        lst.append(
                          fromRow(
                            SparkInternalRow.fromPaimon(new JoinedRow(row, extraRow), rowType)))
                      }
                    )
                    iter.foreach(
                      row => {
                        val tuple: (KeyPartOrRow, Array[Byte]) = row._2
                        val binaryRow = SerializationUtils.deserializeBinaryRow(tuple._2)
                        tuple._1 match {
                          case KeyPartOrRow.KEY_PART => assigner.bootstrapKey(binaryRow)
                          case KeyPartOrRow.ROW => assigner.processInput(binaryRow)
                          case _ =>
                            throw new UnsupportedOperationException(s"unknown kind ${tuple._1}")
                        }
                      })
                    assigner.endBoostrap(true)
                    lst.iterator
                  } finally {
                    assigner.close()
                    if (ioManager != null) {
                      ioManager.close()
                    }
                  }
                }
            }
          repartitionByBucket(sparkSession.createDataFrame(rowRDD, newData.schema))
        case BucketMode.UNAWARE =>
          // Topology: input -> bucket-assigner
          val unawareBucketProcessor = UnawareBucketProcessor(bucketColIdx, toRow, fromRow)
          newData
            .mapPartitions(unawareBucketProcessor.processPartition)(newDataEncoder)
            .toDF()
        case BucketMode.FIXED =>
          // Topology: input -> bucket-assigner -> shuffle by partition & bucket
          val commonBucketProcessor =
            CommonBucketProcessor(writeBuilder, bucketColIdx, toRow, fromRow)
          repartitionByBucket(
            newData.mapPartitions(commonBucketProcessor.processPartition)(newDataEncoder).toDF())
        case _ =>
          throw new UnsupportedOperationException(s"unsupported bucket mode $bucketMode")
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
                write.write(new DynamicBucketRow(sparkRow, bucket))
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

    val tableCommit = if (overwritePartition == null) {
      writeBuilder.newCommit()
    } else {
      writeBuilder.withOverwrite(overwritePartition.asJava).newCommit()
    }
    try {
      tableCommit.commit(commitMessages.toList.asJava)
    } catch {
      case e: Throwable => throw new RuntimeException(e);
    } finally {
      tableCommit.close();
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
              (_) => true,
              buckFilter))
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
