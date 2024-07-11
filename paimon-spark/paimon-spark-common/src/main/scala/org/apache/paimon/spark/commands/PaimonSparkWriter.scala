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
import org.apache.paimon.index.{BucketAssigner, HashBucketAssigner, SimpleHashBucketAssigner}
import org.apache.paimon.io.{CompactIncrement, DataIncrement, IndexIncrement}
import org.apache.paimon.manifest.{FileKind, IndexManifestEntry}
import org.apache.paimon.spark.{SparkRow, SparkTableWrite}
import org.apache.paimon.spark.schema.SparkSystemColumns.ROW_KIND_COL
import org.apache.paimon.spark.util.SparkRowUtils
import org.apache.paimon.table.{BucketMode, FileStoreTable}
import org.apache.paimon.table.sink._
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

import java.io.IOException
import java.util.Collections.singletonMap
import java.util.UUID

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

case class PaimonSparkWriter(table: FileStoreTable) {

  private lazy val tableSchema = table.schema

  private lazy val rowType = table.rowType()

  private lazy val bucketMode = table.bucketMode

  private lazy val primaryKeyCols = tableSchema.trimmedPrimaryKeys().asScala

  private lazy val coreOptions = table.coreOptions()

  @transient private lazy val serializer = new CommitMessageSerializer

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

    def newWrite(): SparkTableWrite = new SparkTableWrite(writeBuilder, rowType, rowKindColIdx)

    def writeWithBucket(data: RDD[(Row, Int)]): RDD[Array[Byte]] = {
      data.mapPartitions {
        iter =>
          {
            val write = newWrite()
            try {
              iter.foreach(pair => write.write(pair._1, pair._2))
              write.finish().asScala
            } finally {
              write.close()
            }
          }
      }
    }

    def writeWithoutBucket(data: Dataset[Row]): RDD[Array[Byte]] = {
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
      }.rdd
    }

    def sparkParallelism = {
      val defaultParallelism = sparkSession.sparkContext.defaultParallelism
      val numShufflePartitions = sparkSession.sessionState.conf.numShufflePartitions
      Math.max(defaultParallelism, numShufflePartitions)
    }

    val written: RDD[Array[Byte]] = bucketMode match {
      case BucketMode.HASH_DYNAMIC =>
        assert(primaryKeyCols.nonEmpty, "Only primary-key table can support dynamic bucket.")

        val parallelism = Option(coreOptions.dynamicBucketAssignerParallelism)
          .map(_.toInt)
          .getOrElse(sparkParallelism)

        val numAssigners = Option(coreOptions.dynamicBucketInitialBuckets)
          .map(initialBuckets => Math.min(initialBuckets.toInt, parallelism))
          .getOrElse(parallelism)

        val partitioned = repartitionForDynamicBucketAssign(data, parallelism, numAssigners)

        if (table.snapshotManager().latestSnapshot() == null) {
          // bootstrap mode
          // Topology: input -> shuffle by key & partition -> bucket-assigner
          writeWithBucket(assignDynamicBucket(partitioned, numAssigners))
        } else {
          // Topology: input -> shuffle by key & partition -> bucket-assigner -> shuffle by partition & bucket
          writeWithBucket(
            assignDynamicBucketAndRepartition(table, partitioned, parallelism, numAssigners))
        }
      case BucketMode.BUCKET_UNAWARE =>
        // Topology: input ->
        writeWithoutBucket(data)
      case BucketMode.HASH_FIXED =>
        // Topology: input -> bucket-assigner -> shuffle by partition & bucket
        writeWithBucket(assignFixedBucketAndRepartition(data, sparkParallelism))
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
   * one deletion vector; else, one index file will contains all deletion vector with the same
   * partition and bucket.
   */
  def persistDeletionVectors(deletionVectors: Dataset[SparkDeletionVectors]): Seq[CommitMessage] = {
    val sparkSession = deletionVectors.sparkSession
    import sparkSession.implicits._

    val fileStore = table.store()
    val lastSnapshotId = table.snapshotManager().latestSnapshotId()

    def createOrRestoreDVMaintainer(
        partition: BinaryRow,
        bucket: Int): DeletionVectorsMaintainer = {
      val deletionVectorsMaintainerFactory =
        new DeletionVectorsMaintainer.Factory(fileStore.newIndexFileHandler())
      fileStore.bucketMode() match {
        case BucketMode.BUCKET_UNAWARE =>
          deletionVectorsMaintainerFactory.create()
        case _ =>
          deletionVectorsMaintainerFactory.createOrRestore(lastSnapshotId, partition, bucket)
      }
    }

    def commitDeletionVector(
        sdv: SparkDeletionVectors,
        serializer: CommitMessageSerializer): Array[Byte] = {
      val partition = SerializationUtils.deserializeBinaryRow(sdv.partition)
      val maintainer = createOrRestoreDVMaintainer(partition, sdv.bucket)
      sdv.dataFileAndDeletionVector.foreach {
        case (dataFileName, dv) =>
          maintainer.notifyNewDeletion(dataFileName, DeletionVector.deserializeFromBytes(dv))
      }

      val commitMessage = new CommitMessageImpl(
        partition,
        sdv.bucket,
        DataIncrement.emptyIncrement(),
        CompactIncrement.emptyIncrement(),
        new IndexIncrement(maintainer.writeDeletionVectorsIndex()))

      serializer.serialize(commitMessage)
    }

    val serializedCommits = deletionVectors
      .groupByKey(_.partitionAndBucket)
      .mapGroups {
        case (_, iter: Iterator[SparkDeletionVectors]) =>
          val serializer = new CommitMessageSerializer
          val grouped = iter.reduce {
            (sdv1, sdv2) =>
              sdv1.copy(dataFileAndDeletionVector =
                sdv1.dataFileAndDeletionVector ++ sdv2.dataFileAndDeletionVector)
          }
          commitDeletionVector(grouped, serializer)
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

  private def repartitionForDynamicBucketAssign(
      data: Dataset[Row],
      parallelism: Int,
      numAssigners: Int): RDD[Row] = {
    hashRepartition(
      data.rdd
        .mapPartitions(
          iterator => {
            val rowPartitionKeyExtractor = new RowPartitionKeyExtractor(tableSchema)
            iterator.map(
              row => {
                val sparkRow = new SparkRow(rowType, row)
                val partition = rowPartitionKeyExtractor.partition(sparkRow)
                val key = rowPartitionKeyExtractor.trimmedPrimaryKey(sparkRow)
                (hash(partition, key, parallelism, numAssigners), row)
              })
          }),
      parallelism
    )
  }

  private def assignDynamicBucket(data: RDD[Row], numAssigners: Int): RDD[(Row, Int)] = {
    data
      .mapPartitions(
        iterator => {
          val extractor = new RowPartitionKeyExtractor(tableSchema)
          val assigner = new SimpleHashBucketAssigner(
            numAssigners,
            TaskContext.getPartitionId(),
            coreOptions.dynamicBucketTargetRowNum()
          )
          iterator.map(
            row => {
              val sparkRow = new SparkRow(rowType, row)
              val partition = extractor.partition(sparkRow)
              val key = extractor.trimmedPrimaryKey(sparkRow)
              val bucket = assigner.assign(partition, key.hashCode)
              (row, bucket)
            })
        })
  }

  private def assignDynamicBucketAndRepartition(
      fileStoreTable: FileStoreTable,
      data: RDD[Row],
      parallelism: Int,
      numAssigners: Int): RDD[(Row, Int)] = {
    val targetBucketRowNumber = coreOptions.dynamicBucketTargetRowNum
    val commitUser = UUID.randomUUID.toString
    hashRepartition(
      data
        .mapPartitions(
          iterator => {
            val extractor = new RowPartitionKeyExtractor(tableSchema)
            val assigner = new HashBucketAssigner(
              fileStoreTable.snapshotManager(),
              commitUser,
              fileStoreTable.store.newIndexFileHandler,
              parallelism,
              numAssigners,
              TaskContext.getPartitionId(),
              targetBucketRowNumber
            )
            iterator.map(
              row => {
                val sparkRow = new SparkRow(rowType, row)
                val partition = extractor.partition(sparkRow)
                val key = extractor.trimmedPrimaryKey(sparkRow)
                val bucket = assigner.assign(partition, key.hashCode)
                (hash(partition, bucket, parallelism), (row, bucket))
              })
          }),
      parallelism
    )
  }

  private def assignFixedBucketAndRepartition(
      data: Dataset[Row],
      parallelism: Int): RDD[(Row, Int)] = {
    hashRepartition(
      data.rdd
        .mapPartitions(
          iterator => {
            val extractor = new FixedBucketRowKeyExtractor(tableSchema)
            iterator.map(
              row => {
                val sparkRow = new SparkRow(rowType, row)
                extractor.setRecord(sparkRow)
                val partition = extractor.partition()
                val bucket = extractor.bucket()
                (hash(partition, bucket, parallelism), (row, bucket))
              })
          }),
      parallelism
    )
  }

  private def hash(
      partition: BinaryRow,
      key: BinaryRow,
      parallelism: Int,
      numAssigners: Int): Int = {
    BucketAssigner.computeHashKey(partition.hashCode(), key.hashCode(), parallelism, numAssigners)
  }

  private def hash(partition: BinaryRow, bucket: Int, parallelism: Int): Int = {
    ChannelComputer.hash(partition, bucket, parallelism)
  }

  private def hashRepartition[T: ClassTag](rdd: RDD[(Int, T)], parallelism: Int): RDD[T] = {
    rdd.partitionBy(ModPartitioner(parallelism)).map(_._2)
  }

  private case class ModPartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions
    override def getPartition(key: Any): Int = key.asInstanceOf[Int] % numPartitions
  }
}
