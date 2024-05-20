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
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.deletionvectors.{DeletionVector, DeletionVectorsMaintainer}
import org.apache.paimon.index.{BucketAssigner, SimpleHashBucketAssigner}
import org.apache.paimon.io.{CompactIncrement, DataIncrement, IndexIncrement}
import org.apache.paimon.manifest.{FileKind, IndexManifestEntry}
import org.apache.paimon.spark.{SparkRow, SparkTableWrite}
import org.apache.paimon.spark.SparkUtils.createIOManager
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.spark.schema.SparkSystemColumns.{BUCKET_COL, ROW_KIND_COL}
import org.apache.paimon.spark.util.SparkRowUtils
import org.apache.paimon.table.{BucketMode, FileStoreTable}
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage, CommitMessageImpl, CommitMessageSerializer, RowPartitionKeyExtractor}
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.io.IOException
import java.util.Collections.singletonMap

import scala.collection.JavaConverters._

case class PaimonSparkWriter(table: FileStoreTable) {

  private lazy val tableSchema = table.schema

  private lazy val rowType = table.rowType()

  private lazy val bucketMode = table.bucketMode

  private lazy val primaryKeyCols = tableSchema.trimmedPrimaryKeys().asScala

  private lazy val serializer = new CommitMessageSerializer

  val writeBuilder: BatchWriteBuilder = table.newBatchWriteBuilder()

  def writeOnly(): PaimonSparkWriter = {
    PaimonSparkWriter(table.copy(singletonMap(WRITE_ONLY.key(), "true")))
  }

  def write(data: Dataset[Row]): Seq[CommitMessage] = {
    val sparkSession = data.sparkSession
    import sparkSession.implicits._

    val rowKindColIdx = SparkRowUtils.getFieldIndex(data.schema, ROW_KIND_COL)
    assert(
      rowKindColIdx == -1 || rowKindColIdx == data.schema.length - 1,
      "Row kind column should be the last field.")

    // append _bucket_ column as placeholder
    val withInitBucketCol = data.withColumn(BUCKET_COL, lit(-1))
    val bucketColIdx = withInitBucketCol.schema.size - 1
    val encoderGroupWithBucketCol = EncoderSerDeGroup(withInitBucketCol.schema)

    def newWrite(): SparkTableWrite = new SparkTableWrite(writeBuilder, rowType, rowKindColIdx)

    def writeWithoutBucket(): Dataset[Array[Byte]] = {
      data.mapPartitions {
        iter =>
          {
            val write = newWrite()
            try {
              iter.foreach(row => write.write(row))
              write.finish().asScala
            } finally {
              write.close()
            }
          }
      }
    }

    def writeWithBucketProcessor(
        dataFrame: DataFrame,
        processor: BucketProcessor): Dataset[Array[Byte]] = {
      val repartitioned = repartitionByPartitionsAndBucket(
        dataFrame.mapPartitions(processor.processPartition)(encoderGroupWithBucketCol.encoder))
      repartitioned.mapPartitions {
        iter =>
          {
            val write = newWrite()
            try {
              iter.foreach(row => write.write(row, row.getInt(bucketColIdx)))
              write.finish().asScala
            } finally {
              write.close()
            }
          }
      }
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
              write.finish().asScala
            } finally {
              write.close()
            }
          }
      }
    }

    val written: Dataset[Array[Byte]] = bucketMode match {
      case BucketMode.HASH_DYNAMIC =>
        assert(primaryKeyCols.nonEmpty, "Only primary-key table can support dynamic bucket.")

        val numParallelism = Option(table.coreOptions.dynamicBucketAssignerParallelism)
          .map(_.toInt)
          .getOrElse {
            val defaultParallelism = sparkSession.sparkContext.defaultParallelism
            val numShufflePartitions = sparkSession.sessionState.conf.numShufflePartitions
            Math.max(defaultParallelism, numShufflePartitions)
          }
        val numAssigners = Option(table.coreOptions.dynamicBucketInitialBuckets)
          .map(initialBuckets => Math.min(initialBuckets.toInt, numParallelism))
          .getOrElse(numParallelism)

        def partitionByKey(): DataFrame = {
          repartitionByKeyPartitionHash(
            sparkSession,
            withInitBucketCol,
            numParallelism,
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
              numParallelism,
              numAssigners,
              encoderGroupWithBucketCol))
        }
      case BucketMode.BUCKET_UNAWARE =>
        // Topology: input ->
        writeWithoutBucket()
      case BucketMode.HASH_FIXED =>
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

  def persistDeletionVectors(deletionVectors: Dataset[SparkDeletionVector]): Seq[CommitMessage] = {
    val sparkSession = deletionVectors.sparkSession
    import sparkSession.implicits._

    val store = table.store()
    deletionVectors
      .map {
        sdv =>
          val deletionVectorsMaintainerFactory =
            new DeletionVectorsMaintainer.Factory(store.newIndexFileHandler())
          val maintainer = deletionVectorsMaintainerFactory.create()
          maintainer.notifyNewDeletion(
            sdv.file,
            DeletionVector.deserializeFromBytes(sdv.deletionVector))

          val commitMessage = new CommitMessageImpl(
            SerializationUtils.deserializeBinaryRow(sdv.partition),
            sdv.bucket,
            DataIncrement.emptyIncrement(),
            CompactIncrement.emptyIncrement(),
            new IndexIncrement(maintainer.prepareCommit()))

          val serializer = new CommitMessageSerializer
          serializer.serialize(commitMessage)
      }
      .collect()
      .map(deserializeCommitMessage(serializer, _))
  }

  def persistDeletionVectors0(deletionVectors: Dataset[SparkDeletionVector]): Seq[CommitMessage] = {
    val sparkSession = deletionVectors.sparkSession
    import sparkSession.implicits._

    val store = table.store()
    val lastSnapshotId = table.snapshotManager().latestSnapshotId()

    deletionVectors
      .groupByKey {
        sdv =>
          val pathFactory = table.store().pathFactory()
          pathFactory
            .relativePartitionAndBucketPath(
              SerializationUtils.deserializeBinaryRow(sdv.partition),
              sdv.bucket)
            .toString
      }
      .mapGroups {
        case (partitionAndBucket, iter: Iterator[SparkDeletionVector]) =>
          val grouped: SparkDeletionVectors = iter
            .map {
              sdv =>
                SparkDeletionVectors(
                  partitionAndBucket,
                  sdv.partition,
                  sdv.bucket,
                  List((sdv.file, sdv.deletionVector)))
            }
            .reduce((sdv1, sdv2) => sdv1.copy(fileAndDVs = sdv1.fileAndDVs ++ sdv2.fileAndDVs))
          val partition = SerializationUtils.deserializeBinaryRow(grouped.partition)
          val deletionVectorsMaintainerFactory =
            new DeletionVectorsMaintainer.Factory(store.newIndexFileHandler())
          val maintainer = deletionVectorsMaintainerFactory.createOrRestore(
            lastSnapshotId,
            partition,
            grouped.bucket)
          grouped.fileAndDVs.foreach {
            case (file, dv) =>
              maintainer.notifyNewDeletion(file, DeletionVector.deserializeFromBytes(dv))
          }

          val commitMessage = new CommitMessageImpl(
            partition,
            grouped.bucket,
            DataIncrement.emptyIncrement(),
            CompactIncrement.emptyIncrement(),
            new IndexIncrement(maintainer.prepareCommit()))

          val serializer = new CommitMessageSerializer
          serializer.serialize(commitMessage)
      }
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

  /** Compute bucket id in dynamic bucket mode. */
  private def repartitionByKeyPartitionHash(
      sparkSession: SparkSession,
      data: DataFrame,
      numParallelism: Int,
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
                    .computeHashKey(partitionHash, keyHash, numParallelism, numAssigners),
                  row)
              })
          },
          preservesPartitioning = true
        )
        .partitionBy(ModPartitioner(numParallelism))
        .map(_._2),
      data.schema
    )
  }

  private def repartitionByPartitionsAndBucket(ds: Dataset[Row]): Dataset[Row] = {
    val partitionCols = tableSchema.partitionKeys().asScala.map(col)
    ds.toDF().repartition(partitionCols ++ Seq(col(BUCKET_COL)): _*)
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
