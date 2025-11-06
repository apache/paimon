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

import org.apache.paimon.{CoreOptions, Snapshot}
import org.apache.paimon.CoreOptions.{PartitionSinkStrategy, WRITE_ONLY}
import org.apache.paimon.codegen.CodeGenUtils
import org.apache.paimon.crosspartition.{IndexBootstrap, KeyPartOrRow}
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.data.serializer.InternalSerializers
import org.apache.paimon.deletionvectors.DeletionVector
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer
import org.apache.paimon.fs.Path
import org.apache.paimon.index.{BucketAssigner, SimpleHashBucketAssigner}
import org.apache.paimon.io.{CompactIncrement, DataIncrement}
import org.apache.paimon.manifest.FileKind
import org.apache.paimon.spark.{SparkRow, SparkTableWrite, SparkTypeUtils}
import org.apache.paimon.spark.catalog.functions.BucketFunction
import org.apache.paimon.spark.schema.SparkSystemColumns.{BUCKET_COL, ROW_KIND_COL}
import org.apache.paimon.spark.sort.TableSorter
import org.apache.paimon.spark.util.OptionUtils.paimonExtensionEnabled
import org.apache.paimon.spark.util.SparkRowUtils
import org.apache.paimon.spark.write.WriteHelper
import org.apache.paimon.table.{FileStoreTable, PostponeUtils, SpecialFields}
import org.apache.paimon.table.BucketMode._
import org.apache.paimon.table.sink._
import org.apache.paimon.types.{RowKind, RowType}
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.io.IOException
import java.util.Collections.singletonMap

import scala.collection.JavaConverters._

case class PaimonSparkWriter(
    table: FileStoreTable,
    writeRowTracking: Boolean = false,
    batchId: Long = -1)
  extends WriteHelper {

  private lazy val tableSchema = table.schema

  private lazy val bucketMode = table.bucketMode

  private val fullCompactionDeltaCommits: Option[Int] =
    Option.apply(coreOptions.fullCompactionDeltaCommits())

  @transient private lazy val serializer = new CommitMessageSerializer

  private val writeType = {
    if (writeRowTracking) {
      SpecialFields.rowTypeWithRowTracking(table.rowType(), true)
    } else {
      table.rowType()
    }
  }

  val postponeBatchWriteFixedBucket: Boolean =
    table.bucketMode() == POSTPONE_MODE && coreOptions.postponeBatchWriteFixedBucket()

  val writeBuilder: BatchWriteBuilder = {
    val tableForWrite = if (postponeBatchWriteFixedBucket) {
      PostponeUtils.tableForFixBucketWrite(table)
    } else {
      table
    }
    tableForWrite.newBatchWriteBuilder()
  }

  def writeOnly(): PaimonSparkWriter = {
    PaimonSparkWriter(table.copy(singletonMap(WRITE_ONLY.key(), "true")))
  }

  def withRowTracking(): PaimonSparkWriter = {
    if (coreOptions.rowTrackingEnabled()) {
      PaimonSparkWriter(table, writeRowTracking = true)
    } else {
      this
    }
  }

  def write(data: DataFrame): Seq[CommitMessage] = {
    val sparkSession = data.sparkSession
    import sparkSession.implicits._

    val withInitBucketCol = bucketMode match {
      case BUCKET_UNAWARE => data
      case KEY_DYNAMIC if !data.schema.fieldNames.contains(ROW_KIND_COL) =>
        data
          .withColumn(ROW_KIND_COL, lit(RowKind.INSERT.toByteValue))
          .withColumn(BUCKET_COL, lit(-1))
      case _ => data.withColumn(BUCKET_COL, lit(-1))
    }
    val rowKindColIdx = SparkRowUtils.getFieldIndex(withInitBucketCol.schema, ROW_KIND_COL)
    val bucketColIdx = SparkRowUtils.getFieldIndex(withInitBucketCol.schema, BUCKET_COL)
    val encoderGroupWithBucketCol = EncoderSerDeGroup(withInitBucketCol.schema)
    val postponePartitionBucketComputer: Option[BinaryRow => Integer] =
      if (postponeBatchWriteFixedBucket) {
        val knownNumBuckets = PostponeUtils.getKnownNumBuckets(table)
        val defaultPostponeNumBuckets = withInitBucketCol.rdd.getNumPartitions
        Some((p: BinaryRow) => knownNumBuckets.getOrDefault(p, defaultPostponeNumBuckets))
      } else {
        None
      }

    def newWrite() = SparkTableWrite(
      writeBuilder,
      writeType,
      rowKindColIdx,
      writeRowTracking,
      fullCompactionDeltaCommits,
      batchId,
      coreOptions.blobAsDescriptor(),
      table.catalogEnvironment().catalogContext(),
      postponePartitionBucketComputer
    )

    def sparkParallelism = {
      val defaultParallelism = sparkSession.sparkContext.defaultParallelism
      val numShufflePartitions = sparkSession.sessionState.conf.numShufflePartitions
      Math.max(defaultParallelism, numShufflePartitions)
    }

    def writeWithoutBucket(dataFrame: DataFrame): Dataset[Array[Byte]] = {
      dataFrame.mapPartitions {
        iter =>
          {
            val write = newWrite()
            try {
              iter.foreach(row => write.write(row))
              write.finish()
            } finally {
              write.close()
            }
          }
      }
    }

    def writeWithBucket(dataFrame: DataFrame): Dataset[Array[Byte]] = {
      dataFrame.mapPartitions {
        iter =>
          {
            val write = newWrite()
            try {
              iter.foreach(row => write.write(row, row.getInt(bucketColIdx)))
              write.finish()
            } finally {
              write.close()
            }
          }
      }
    }

    def writeWithBucketProcessor(
        dataFrame: DataFrame,
        processor: BucketProcessor[Row]): Dataset[Array[Byte]] = {
      val repartitioned = repartitionByPartitionsAndBucket(
        dataFrame
          .mapPartitions(processor.processPartition)(encoderGroupWithBucketCol.encoder)
          .toDF())
      writeWithBucket(repartitioned)
    }

    def writeWithBucketAssigner(
        dataFrame: DataFrame,
        funcFactory: () => Row => Int): Dataset[Array[Byte]] = {
      dataFrame.mapPartitions {
        iter =>
          {
            val assigner = funcFactory.apply()
            val write = newWrite()
            try {
              iter.foreach(row => write.write(row, assigner.apply(row)))
              write.finish()
            } finally {
              write.close()
            }
          }
      }
    }

    val written: Dataset[Array[Byte]] = bucketMode match {
      case KEY_DYNAMIC =>
        // Topology: input -> bootstrap -> shuffle by key hash -> bucket-assigner -> shuffle by partition & bucket
        val rowType = SparkTypeUtils.toPaimonType(withInitBucketCol.schema).asInstanceOf[RowType]
        val assignerParallelism = Option(coreOptions.dynamicBucketAssignerParallelism)
          .map(_.toInt)
          .getOrElse(sparkParallelism)
        val bootstrapped = bootstrapAndRepartitionByKeyHash(
          withInitBucketCol,
          assignerParallelism,
          rowKindColIdx,
          rowType)

        val globalDynamicBucketProcessor =
          GlobalDynamicBucketProcessor(
            table,
            rowType,
            assignerParallelism,
            encoderGroupWithBucketCol)
        val repartitioned = repartitionByPartitionsAndBucket(
          sparkSession.createDataFrame(
            bootstrapped.mapPartitions(globalDynamicBucketProcessor.processPartition),
            withInitBucketCol.schema))

        writeWithBucket(repartitioned)

      case HASH_DYNAMIC =>
        val assignerParallelism = {
          val parallelism = Option(coreOptions.dynamicBucketAssignerParallelism)
            .map(_.toInt)
            .getOrElse(sparkParallelism)
          if (coreOptions.dynamicBucketMaxBuckets() != -1) {
            Math.min(coreOptions.dynamicBucketMaxBuckets().toInt, parallelism)
          } else {
            parallelism
          }
        }
        val numAssigners = Option(coreOptions.dynamicBucketInitialBuckets)
          .map(initialBuckets => Math.min(initialBuckets.toInt, assignerParallelism))
          .getOrElse(assignerParallelism)

        def partitionByKey(): DataFrame = {
          repartitionByKeyPartitionHash(
            sparkSession,
            withInitBucketCol,
            assignerParallelism,
            numAssigners)
        }

        if (table.snapshotManager().latestSnapshotFromFileSystem() == null) {
          // bootstrap mode
          // Topology: input -> shuffle by special key & partition hash -> bucket-assigner
          writeWithBucketAssigner(
            partitionByKey(),
            () => {
              val extractor = new RowPartitionKeyExtractor(table.schema)
              val assigner =
                new SimpleHashBucketAssigner(
                  numAssigners,
                  TaskContext.getPartitionId(),
                  coreOptions.dynamicBucketTargetRowNum,
                  coreOptions.dynamicBucketMaxBuckets
                )
              row => {
                val sparkRow = new SparkRow(writeType, row)
                assigner.assign(
                  extractor.partition(sparkRow),
                  extractor.trimmedPrimaryKey(sparkRow).hashCode)
              }
            }
          )
        } else {
          // Topology: input -> shuffle by special key & partition hash -> bucket-assigner -> shuffle by partition & bucket
          writeWithBucketProcessor(
            partitionByKey(),
            DynamicBucketProcessor(
              table,
              bucketColIdx,
              assignerParallelism,
              numAssigners,
              encoderGroupWithBucketCol)
          )
        }

      case POSTPONE_MODE if coreOptions.postponeBatchWriteFixedBucket() =>
        // Topology: input -> bucket-assigner -> shuffle by partition & bucket
        writeWithBucketProcessor(
          withInitBucketCol,
          PostponeFixBucketProcessor(
            table,
            bucketColIdx,
            encoderGroupWithBucketCol,
            postponePartitionBucketComputer.get))

      case BUCKET_UNAWARE | POSTPONE_MODE =>
        var input = data
        if (tableSchema.partitionKeys().size() > 0) {
          coreOptions.partitionSinkStrategy match {
            case PartitionSinkStrategy.HASH =>
              input = data.repartition(partitionCols(data): _*)
            case _ =>
          }
        }
        val clusteringColumns = coreOptions.clusteringColumns()
        if ((!coreOptions.clusteringIncrementalEnabled()) && (!clusteringColumns.isEmpty)) {
          val strategy = coreOptions.clusteringStrategy(tableSchema.fields().size())
          val sorter = TableSorter.getSorter(table, strategy, clusteringColumns)
          input = sorter.sort(data)
        }
        writeWithoutBucket(input)

      case HASH_FIXED =>
        if (paimonExtensionEnabled && BucketFunction.supportsTable(table)) {
          // Topology: input -> shuffle by partition & bucket
          val bucketNumber = coreOptions.bucket()
          val bucketKeyCol = tableSchema
            .bucketKeys()
            .asScala
            .map(tableSchema.fieldNames().indexOf(_))
            .map(x => col(data.schema.fieldNames(x)))
            .toSeq
          val args = Seq(
            lit(new CoreOptions(tableSchema.options()).bucketFunctionType().toString),
            lit(bucketNumber)) ++ bucketKeyCol
          val repartitioned =
            repartitionByPartitionsAndBucket(
              data.withColumn(BUCKET_COL, call_udf(BucketExpression.FIXED_BUCKET, args: _*)))
          writeWithBucket(repartitioned)
        } else {
          // Topology: input -> bucket-assigner -> shuffle by partition & bucket
          writeWithBucketProcessor(
            withInitBucketCol,
            CommonBucketProcessor(table, bucketColIdx, encoderGroupWithBucketCol))
        }

      case _ =>
        throw new UnsupportedOperationException(s"Spark doesn't support $bucketMode mode.")
    }

    written
      .collect()
      .map(deserializeCommitMessage(serializer, _))
      .toSeq
  }

  /**
   * Write all the deletion vectors to the index files. If it's in unaware mode, one index file maps
   * deletion vectors; else, one index file will contain all deletion vector with the same partition
   * and bucket.
   */
  def persistDeletionVectors(
      deletionVectors: Dataset[SparkDeletionVector],
      snapshot: Snapshot): Seq[CommitMessage] = {
    val sparkSession = deletionVectors.sparkSession
    import sparkSession.implicits._
    val serializedCommits = deletionVectors
      .groupByKey(_.bucketPath)
      .mapGroups {
        (_, iter: Iterator[SparkDeletionVector]) =>
          val indexHandler = table.store().newIndexFileHandler()
          var dvIndexFileMaintainer: BaseAppendDeleteFileMaintainer = null
          while (iter.hasNext) {
            val sdv: SparkDeletionVector = iter.next()
            if (dvIndexFileMaintainer == null) {
              val partition = SerializationUtils.deserializeBinaryRow(sdv.partition)
              dvIndexFileMaintainer = if (bucketMode == BUCKET_UNAWARE) {
                BaseAppendDeleteFileMaintainer.forUnawareAppend(indexHandler, snapshot, partition)
              } else {
                BaseAppendDeleteFileMaintainer.forBucketedAppend(
                  indexHandler,
                  snapshot,
                  partition,
                  sdv.bucket)
              }
            }
            if (dvIndexFileMaintainer == null) {
              throw new RuntimeException("can't create the dv maintainer.")
            }

            dvIndexFileMaintainer.notifyNewDeletionVector(
              new Path(sdv.dataFilePath).getName,
              DeletionVector.deserializeFromBytes(sdv.deletionVector))
          }
          val indexEntries = dvIndexFileMaintainer.persist()

          val (added, deleted) = indexEntries.asScala.partition(_.kind() == FileKind.ADD)

          val commitMessage = new CommitMessageImpl(
            dvIndexFileMaintainer.getPartition,
            dvIndexFileMaintainer.getBucket,
            null,
            new DataIncrement(
              java.util.Collections.emptyList(),
              java.util.Collections.emptyList(),
              java.util.Collections.emptyList(),
              added.map(_.indexFile).asJava,
              deleted.map(_.indexFile).asJava),
            CompactIncrement.emptyIncrement()
          )
          val serializer = new CommitMessageSerializer
          serializer.serialize(commitMessage)
      }
    serializedCommits
      .collect()
      .map(deserializeCommitMessage(serializer, _))
  }

  def commit(commitMessages: Seq[CommitMessage]): Unit = {
    val finalWriteBuilder = if (postponeBatchWriteFixedBucket) {
      writeBuilder
        .asInstanceOf[BatchWriteBuilderImpl]
        .copyWithNewTable(PostponeUtils.tableForCommit(table))
    } else {
      writeBuilder
    }
    val tableCommit = finalWriteBuilder.newCommit()
    try {
      tableCommit.commit(commitMessages.toList.asJava)
    } catch {
      case e: Throwable => throw new RuntimeException(e);
    } finally {
      tableCommit.close()
    }
    postCommit(commitMessages)
  }

  /** Boostrap and repartition for cross partition mode. */
  private def bootstrapAndRepartitionByKeyHash(
      data: DataFrame,
      parallelism: Int,
      rowKindColIdx: Int,
      rowType: RowType): RDD[(KeyPartOrRow, Array[Byte])] = {
    val numSparkPartitions = data.rdd.getNumPartitions
    val primaryKeys = table.schema().primaryKeys()
    val bootstrapType = IndexBootstrap.bootstrapType(table.schema())
    data.rdd
      .mapPartitions {
        iter =>
          {
            val sparkPartitionId = TaskContext.getPartitionId()
            val keyPartProject = CodeGenUtils.newProjection(bootstrapType, primaryKeys)
            val rowProject = CodeGenUtils.newProjection(rowType, primaryKeys)
            val bootstrapSer = InternalSerializers.create(bootstrapType)
            val rowSer = InternalSerializers.create(rowType)
            val bootstrapIterator = new IndexBootstrap(table)
              .bootstrap(numSparkPartitions, sparkPartitionId)
              .toCloseableIterator
            TaskContext.get().addTaskCompletionListener[Unit](_ => bootstrapIterator.close())
            val toPaimonRow = SparkRowUtils.toPaimonRow(
              rowType,
              rowKindColIdx,
              table.coreOptions().blobAsDescriptor(),
              table.catalogEnvironment().catalogContext())

            bootstrapIterator.asScala
              .map(
                row => {
                  val bytes: Array[Byte] =
                    SerializationUtils.serializeBinaryRow(bootstrapSer.toBinaryRow(row))
                  (keyPartProject(row).hashCode(), (KeyPartOrRow.KEY_PART, bytes))
                }) ++ iter.map(
              r => {
                val sparkRow = toPaimonRow(r)
                val bytes: Array[Byte] =
                  SerializationUtils.serializeBinaryRow(rowSer.toBinaryRow(sparkRow))
                (rowProject(sparkRow).hashCode(), (KeyPartOrRow.ROW, bytes))
              })
          }
      }
      .partitionBy(ModPartitioner(parallelism))
      .map(_._2)
  }

  /** Repartition for dynamic bucket mode. */
  private def repartitionByKeyPartitionHash(
      sparkSession: SparkSession,
      data: DataFrame,
      parallelism: Int,
      numAssigners: Int): DataFrame = {
    sparkSession.createDataFrame(
      data.rdd
        .mapPartitions(
          iterator => {
            val rowPartitionKeyExtractor = new RowPartitionKeyExtractor(tableSchema)
            iterator.map(
              row => {
                val sparkRow = new SparkRow(writeType, row)
                val partitionHash = rowPartitionKeyExtractor.partition(sparkRow).hashCode
                val keyHash = rowPartitionKeyExtractor.trimmedPrimaryKey(sparkRow).hashCode
                (
                  BucketAssigner
                    .computeHashKey(partitionHash, keyHash, parallelism, numAssigners),
                  row)
              })
          })
        .partitionBy(ModPartitioner(parallelism))
        .map(_._2),
      data.schema
    )
  }

  private def repartitionByPartitionsAndBucket(df: DataFrame): DataFrame = {
    df.repartition(partitionCols(df) ++ Seq(col(BUCKET_COL)): _*)
  }

  def partitionCols(df: DataFrame): Seq[Column] = {
    val inputSchema = df.schema
    tableSchema
      .partitionKeys()
      .asScala
      .map(tableSchema.fieldNames().indexOf(_))
      .map(x => col(inputSchema.fieldNames(x)))
      .toSeq
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

  private case class ModPartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions
    override def getPartition(key: Any): Int = Math.abs(key.asInstanceOf[Int] % numPartitions)
  }
}

object PaimonSparkWriter {

  def apply(table: FileStoreTable): PaimonSparkWriter = {
    new PaimonSparkWriter(table)
  }
}
