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

import org.apache.paimon.CoreOptions.WRITE_ONLY
import org.apache.paimon.codegen.CodeGenUtils
import org.apache.paimon.crosspartition.{IndexBootstrap, KeyPartOrRow}
import org.apache.paimon.data.serializer.InternalSerializers
import org.apache.paimon.deletionvectors.DeletionVector
import org.apache.paimon.deletionvectors.append.AppendDeletionFileMaintainer
import org.apache.paimon.index.{BucketAssigner, SimpleHashBucketAssigner}
import org.apache.paimon.io.{CompactIncrement, DataIncrement, IndexIncrement}
import org.apache.paimon.manifest.{FileKind, IndexManifestEntry}
import org.apache.paimon.spark.{SparkRow, SparkTableWrite, SparkTypeUtils}
import org.apache.paimon.spark.schema.SparkSystemColumns.{BUCKET_COL, ROW_KIND_COL}
import org.apache.paimon.spark.util.SparkRowUtils
import org.apache.paimon.table.BucketMode._
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink._
import org.apache.paimon.types.{RowKind, RowType}
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.io.IOException
import java.util.Collections.singletonMap

import scala.collection.JavaConverters._

case class PaimonSparkWriter(table: FileStoreTable) {

  private lazy val tableSchema = table.schema

  private lazy val rowType = table.rowType()

  private lazy val bucketMode = table.bucketMode

  @transient private lazy val serializer = new CommitMessageSerializer

  val writeBuilder: BatchWriteBuilder = table.newBatchWriteBuilder()

  def writeOnly(): PaimonSparkWriter = {
    PaimonSparkWriter(table.copy(singletonMap(WRITE_ONLY.key(), "true")))
  }

  def write(data: DataFrame): Seq[CommitMessage] = {
    val sparkSession = data.sparkSession
    import sparkSession.implicits._

    val withInitBucketCol = bucketMode match {
      case CROSS_PARTITION if !data.schema.fieldNames.contains(ROW_KIND_COL) =>
        data
          .withColumn(ROW_KIND_COL, lit(RowKind.INSERT.toByteValue))
          .withColumn(BUCKET_COL, lit(-1))
      case _ => data.withColumn(BUCKET_COL, lit(-1))
    }
    val rowKindColIdx = SparkRowUtils.getFieldIndex(withInitBucketCol.schema, ROW_KIND_COL)
    val bucketColIdx = SparkRowUtils.getFieldIndex(withInitBucketCol.schema, BUCKET_COL)
    val encoderGroupWithBucketCol = EncoderSerDeGroup(withInitBucketCol.schema)

    def newWrite(): SparkTableWrite = new SparkTableWrite(writeBuilder, rowType, rowKindColIdx)

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
      case CROSS_PARTITION =>
        // Topology: input -> bootstrap -> shuffle by key hash -> bucket-assigner -> shuffle by partition & bucket
        val rowType = SparkTypeUtils.toPaimonType(withInitBucketCol.schema).asInstanceOf[RowType]
        val assignerParallelism = Option(table.coreOptions.dynamicBucketAssignerParallelism)
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
        val assignerParallelism = Option(table.coreOptions.dynamicBucketAssignerParallelism)
          .map(_.toInt)
          .getOrElse(sparkParallelism)
        val numAssigners = Option(table.coreOptions.dynamicBucketInitialBuckets)
          .map(initialBuckets => Math.min(initialBuckets.toInt, assignerParallelism))
          .getOrElse(assignerParallelism)

        def partitionByKey(): DataFrame = {
          repartitionByKeyPartitionHash(
            sparkSession,
            withInitBucketCol,
            assignerParallelism,
            numAssigners)
        }

        if (table.snapshotManager().latestSnapshot() == null) {
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
                  table.coreOptions.dynamicBucketTargetRowNum)
              row => {
                val sparkRow = new SparkRow(rowType, row)
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

      case BUCKET_UNAWARE =>
        // Topology: input ->
        writeWithoutBucket(data)

      case HASH_FIXED =>
        // Topology: input -> bucket-assigner -> shuffle by partition & bucket
        writeWithBucketProcessor(
          withInitBucketCol,
          CommonBucketProcessor(table, bucketColIdx, encoderGroupWithBucketCol))

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
   * deletion vectors; else, one index file will contains all deletion vector with the same
   * partition and bucket.
   */
  def persistDeletionVectors(deletionVectors: Dataset[SparkDeletionVectors]): Seq[CommitMessage] = {
    val sparkSession = deletionVectors.sparkSession
    import sparkSession.implicits._
    val snapshotId = table.snapshotManager().latestSnapshotId();
    val serializedCommits = deletionVectors
      .groupByKey(_.partitionAndBucket)
      .mapGroups {
        case (_, iter: Iterator[SparkDeletionVectors]) =>
          val indexHandler = table.store().newIndexFileHandler()
          var dvIndexFileMaintainer: AppendDeletionFileMaintainer = null
          while (iter.hasNext) {
            val sdv: SparkDeletionVectors = iter.next()
            if (dvIndexFileMaintainer == null) {
              val partition = SerializationUtils.deserializeBinaryRow(sdv.partition)
              dvIndexFileMaintainer = if (bucketMode == BUCKET_UNAWARE) {
                AppendDeletionFileMaintainer.forUnawareAppend(indexHandler, snapshotId, partition)
              } else {
                AppendDeletionFileMaintainer.forBucketedAppend(
                  indexHandler,
                  snapshotId,
                  partition,
                  sdv.bucket)
              }
            }
            if (dvIndexFileMaintainer == null) {
              throw new RuntimeException("can't create the dv maintainer.")
            }

            sdv.dataFileAndDeletionVector.foreach {
              case (dataFileName, dv) =>
                dvIndexFileMaintainer.notifyNewDeletionVector(
                  dataFileName,
                  DeletionVector.deserializeFromBytes(dv))
            }
          }
          val indexEntries = dvIndexFileMaintainer.persist()

          val (added, deleted) = indexEntries.asScala.partition(_.kind() == FileKind.ADD)

          val commitMessage = new CommitMessageImpl(
            dvIndexFileMaintainer.getPartition,
            dvIndexFileMaintainer.getBucket,
            DataIncrement.emptyIncrement(),
            CompactIncrement.emptyIncrement(),
            new IndexIncrement(added.map(_.indexFile).asJava, deleted.map(_.indexFile).asJava)
          )
          val serializer = new CommitMessageSerializer
          serializer.serialize(commitMessage)
      }
    serializedCommits
      .collect()
      .map(deserializeCommitMessage(serializer, _))
  }

  def buildCommitMessageFromIndexManifestEntry(
      indexManifestEntries: Seq[IndexManifestEntry]): Seq[CommitMessage] = {
    indexManifestEntries
      .groupBy(entry => (entry.partition(), entry.bucket()))
      .map {
        case ((partition, bucket), entries) =>
          val (added, removed) = entries.partition(_.kind() == FileKind.ADD)
          new CommitMessageImpl(
            partition,
            bucket,
            DataIncrement.emptyIncrement(),
            CompactIncrement.emptyIncrement(),
            new IndexIncrement(added.map(_.indexFile()).asJava, removed.map(_.indexFile()).asJava))
      }
      .toSeq
  }

  def commit(commitMessages: Seq[CommitMessage]): Unit = {
    val tableCommit = writeBuilder.newCommit()
    try {
      tableCommit.commit(commitMessages.toList.asJava)
    } catch {
      case e: Throwable => throw new RuntimeException(e);
    } finally {
      tableCommit.close()
    }
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
            new IndexBootstrap(table)
              .bootstrap(numSparkPartitions, sparkPartitionId)
              .toCloseableIterator
              .asScala
              .map(
                row => {
                  val bytes: Array[Byte] =
                    SerializationUtils.serializeBinaryRow(bootstrapSer.toBinaryRow(row))
                  (Math.abs(keyPartProject(row).hashCode()), (KeyPartOrRow.KEY_PART, bytes))
                }) ++ iter.map(
              r => {
                val sparkRow =
                  new SparkRow(rowType, r, SparkRowUtils.getRowKind(r, rowKindColIdx))
                val bytes: Array[Byte] =
                  SerializationUtils.serializeBinaryRow(rowSer.toBinaryRow(sparkRow))
                (Math.abs(rowProject(sparkRow).hashCode()), (KeyPartOrRow.ROW, bytes))
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
                val sparkRow = new SparkRow(rowType, row)
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
    val partitionCols = tableSchema.partitionKeys().asScala.map(col)
    df.repartition(partitionCols ++ Seq(col(BUCKET_COL)): _*)
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
    override def getPartition(key: Any): Int = key.asInstanceOf[Int] % numPartitions
  }
}
