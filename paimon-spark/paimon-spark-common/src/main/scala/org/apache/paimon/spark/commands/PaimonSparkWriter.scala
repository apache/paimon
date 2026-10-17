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
import org.apache.paimon.CoreOptions.{COMMIT_LAST_SAFE_SNAPSHOT, PartitionSinkStrategy, WRITE_ONLY}
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
import org.apache.paimon.spark.{SparkPostponeStagedCommitter, SparkRow}
import org.apache.paimon.spark.catalog.functions.BucketFunction
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.spark.schema.SparkSystemColumns.{BUCKET_COL, ROW_KIND_COL}
import org.apache.paimon.spark.sort.TableSorter
import org.apache.paimon.spark.util.OptionUtils.paimonExtensionEnabled
import org.apache.paimon.spark.util.SparkRowUtils
import org.apache.paimon.spark.write.{PaimonDataWrite, StreamingWrite, WriteHelper, WriteTaskResult}
import org.apache.paimon.table.{FileStoreTable, SpecialFields}
import org.apache.paimon.table.BucketMode._
import org.apache.paimon.table.sink._
import org.apache.paimon.types.RowKind
import org.apache.paimon.utils.{SerializationUtils, UriReaderFactory}

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.io.IOException
import java.util.{Map => JMap}
import java.util.Collections.singletonMap

import scala.collection.JavaConverters._

/**
 * @param streaming
 *   the micro-batch this writer writes, when it writes for a streaming query. Such a write goes
 *   through the stream write API, the way a Flink streaming job does: it is committed under an
 *   identifier by the committer of the query, which recognises a replayed micro-batch.
 */
case class PaimonSparkWriter(
    table: FileStoreTable,
    writeRowTracking: Boolean = false,
    streaming: Option[StreamingWrite] = None)
  extends WriteHelper
  with Logging {

  private val commitIdentifier: Option[Long] = streaming.map(_.commitIdentifier)

  private lazy val tableSchema = table.schema

  private lazy val bucketMode = table.bucketMode

  private val fullCompactionDeltaCommits: Option[Int] =
    Option.apply(coreOptions.fullCompactionDeltaCommits())

  @transient private lazy val serializer = new CommitMessageSerializer

  @transient private var stagedSparkSession: SparkSession = _
  @transient private var directPostponeWriteBuilder: PostponeFixedBucketWriteBuilder = _
  private var overwritePartitionSpec: Option[Map[String, String]] = None

  private val writeType = {
    if (writeRowTracking) {
      // The historical data and new data are processed separately.
      // 1. The historical data contains the non-null RowId, but its sequenceNumber may not have been generated yet,
      //    will be generated according to the Snapshot id when committing. (But the previously updated data included
      //    the sequenceNumber value).
      // 2. The new data will be written to the branch without writeRowTracking.
      SpecialFields.rowTypeWithRowTracking(table.rowType(), false, true)
    } else {
      table.rowType()
    }
  }

  @transient private lazy val metricRegistry = SparkMetricRegistry()

  /**
   * A streaming write to a postpone bucket table always goes to bucket -2, to be bucketed by a
   * later compaction, the same as a Flink streaming write: the fixed-bucket paths are for a batch
   * job that ends with its commit.
   */
  val postponeBatchWriteFixedBucket: Boolean =
    streaming.isEmpty && table.bucketMode() == POSTPONE_MODE &&
      coreOptions.postponeBatchWriteFixedBucket()

  private val postponeBaseSnapshotId =
    if (postponeBatchWriteFixedBucket)
      Option(table.snapshotManager().latestSnapshot()).map(_.id())
    else None

  private val configuredPostponeDefaultBucketNum: Option[Int] = {
    val bucketNum = coreOptions.postponeDefaultBucketNum()
    if (bucketNum.isPresent) Some(bucketNum.get().intValue()) else None
  }

  val writeBuilder: WriteBuilder = streaming match {
    case Some(streamingWrite) =>
      // Every micro-batch is committed under the commit user of the query, which is stable across
      // its runs, so that a replayed micro-batch can be recognised as already committed.
      table.newStreamWriteBuilder().withCommitUser(streamingWrite.context.commitUser)
    case None => table.newBatchWriteBuilder()
  }

  def withOverwrite(): PaimonSparkWriter = withOverwrite(java.util.Collections.emptyMap())

  def withOverwrite(partition: JMap[String, String]): PaimonSparkWriter = {
    overwritePartitionSpec = Some(partition.asScala.toMap)
    writeBuilder match {
      case batchWriteBuilder: BatchWriteBuilder if !postponeBatchWriteFixedBucket =>
        batchWriteBuilder.withOverwrite(partition)
      // A stream write builder is told at commit time; see [[commit]].
      case _ =>
    }
    this
  }

  def writeOnly(): PaimonSparkWriter = {
    PaimonSparkWriter(table.copy(singletonMap(WRITE_ONLY.key(), "true")), streaming = streaming)
  }

  def withRowTracking(): PaimonSparkWriter = {
    if (coreOptions.rowTrackingEnabled()) {
      PaimonSparkWriter(table, writeRowTracking = true, streaming = streaming)
    } else {
      this
    }
  }

  def write(data: DataFrame): Seq[CommitMessage] = {
    val sparkSession = data.sparkSession
    val uriReaderFactory = uriReaderFactoryForBlobDescriptor
    import sparkSession.implicits._

    val directPostponeBucketNum =
      if (
        postponeBatchWriteFixedBucket && configuredPostponeDefaultBucketNum.isDefined &&
        (overwritePartitionSpec.isDefined || baseSnapshotHasNoRealBuckets)
      ) {
        configuredPostponeDefaultBucketNum
      } else {
        None
      }
    val activeWriteBuilder: WriteBuilder = directPostponeBucketNum match {
      case Some(_) =>
        val directWriteOptions = new java.util.HashMap[String, String]()
        directWriteOptions.put(
          COMMIT_LAST_SAFE_SNAPSHOT.key(),
          postponeBaseSnapshotId.getOrElse(0L).toString)
        val builder = table.copy(directWriteOptions).newPostponeFixedBucketWriteBuilder()
        overwritePartitionSpec.foreach(spec => builder.withOverwrite(spec.asJava))
        directPostponeWriteBuilder = builder
        builder
      case None =>
        directPostponeWriteBuilder = null
        writeBuilder
    }
    stagedSparkSession = null

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
      directPostponeBucketNum.map(bucketNum => (_: BinaryRow) => Integer.valueOf(bucketNum))
    def newWrite() =
      PaimonDataWrite(
        activeWriteBuilder,
        writeType,
        rowKindColIdx,
        writeRowTracking,
        fullCompactionDeltaCommits,
        commitIdentifier,
        uriReaderFactory,
        postponePartitionBucketComputer,
        // The stream write builder does not know that the commit overwrites, see [[commit]].
        ignorePreviousFiles = streaming.isDefined && overwritePartitionSpec.isDefined
      )

    def sparkParallelism = {
      val defaultParallelism = sparkSession.sparkContext.defaultParallelism
      val numShufflePartitions = sparkSession.sessionState.conf.numShufflePartitions
      Math.max(defaultParallelism, numShufflePartitions)
    }

    def writeWithoutBucket(dataFrame: DataFrame) = {
      dataFrame.mapPartitions {
        iter =>
          {
            val write = newWrite()
            try {
              iter.foreach(row => write.write(row))
              Iterator.apply(write.commit)
            } finally {
              write.close()
            }
          }
      }
    }

    def writeWithBucket(dataFrame: DataFrame) = {
      dataFrame.mapPartitions {
        iter =>
          {
            val write = newWrite()
            try {
              iter.foreach(row => write.write(row, row.getInt(bucketColIdx)))
              Iterator.apply(write.commit)
            } finally {
              write.close()
            }
          }
      }
    }

    def writeWithBucketProcessor(dataFrame: DataFrame, processor: BucketProcessor[Row]) = {
      val repartitioned = repartitionByPartitionsAndBucket(
        dataFrame
          .mapPartitions(processor.processPartition)(encoderGroupWithBucketCol.encoder)
          .toDF())
      writeWithBucket(repartitioned)
    }

    def writeWithBucketAssigner(dataFrame: DataFrame, funcFactory: () => Row => Int) = {
      dataFrame.mapPartitions {
        iter =>
          {
            val assigner = funcFactory.apply()
            val write = newWrite()
            try {
              iter.foreach(row => write.write(row, assigner.apply(row)))
              Iterator.apply(write.commit)
            } finally {
              write.close()
            }
          }
      }
    }

    val written: Dataset[_ <: WriteTaskResult] = bucketMode match {
      case KEY_DYNAMIC =>
        // Topology: input -> bootstrap -> shuffle by key hash -> bucket-assigner -> shuffle by partition & bucket
        val assignerParallelism = Option(coreOptions.dynamicBucketAssignerParallelism)
          .map(_.toInt)
          .getOrElse(sparkParallelism)
        val bootstrapped =
          bootstrapAndRepartitionByKeyHash(
            withInitBucketCol,
            assignerParallelism,
            rowKindColIdx,
            uriReaderFactory)

        val globalDynamicBucketProcessor =
          GlobalDynamicBucketProcessor(table, assignerParallelism, encoderGroupWithBucketCol)
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
            numAssigners,
            uriReaderFactory)
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
                val sparkRow =
                  SparkRow.fromUriReaderFactory(writeType, row, RowKind.INSERT, uriReaderFactory)
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

      case POSTPONE_MODE if directPostponeBucketNum.isDefined =>
        // The configured bucket number is final for overwrite and for a table without real
        // buckets, so route the input directly without first materializing bucket -2 files.
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
        if (
          table.bucketMode() != POSTPONE_MODE &&
          (!coreOptions.clusteringIncrementalEnabled() || coreOptions
            .clusteringIncrementalOptimizeWrite()) && (!clusteringColumns.isEmpty)
        ) {
          val strategy = coreOptions.clusteringStrategy(clusteringColumns.size())
          val sorter = TableSorter.getSorter(table, strategy, clusteringColumns)
          input = sorter.sort(data)
        }
        writeWithoutBucket(input)

      case HASH_FIXED =>
        if (paimonExtensionEnabled(sparkSession) && BucketFunction.supportsTable(table)) {
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

    val taskResults = written.collect().toSeq
    if (postponeBatchWriteFixedBucket && directPostponeWriteBuilder == null) {
      stagedSparkSession = sparkSession
    }
    WriteTaskResult.merge(taskResults)
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
              deleted.map(_.indexFile).asJava
            ),
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
    commit(commitMessages, null)
  }

  def commit(commitMessages: Seq[CommitMessage], operation: Snapshot.Operation): Unit = {
    streaming match {
      case Some(streamingWrite) =>
        commitMicroBatch(streamingWrite, commitMessages, operation)
        postCommit(commitMessages)
      case None =>
        commitBatch(commitMessages, operation)
    }
  }

  private def commitBatch(
      commitMessages: Seq[CommitMessage],
      operation: Snapshot.Operation): Unit = {
    if (postponeBatchWriteFixedBucket && directPostponeWriteBuilder == null) {
      if (stagedSparkSession == null) {
        throw new IllegalStateException("Postpone staged write has no SparkSession.")
      }
      val finalOperation = Option(operation).getOrElse(Snapshot.Operation.WRITE)
      val finalMessages = new SparkPostponeStagedCommitter(
        table,
        stagedSparkSession,
        postponeBaseSnapshotId,
        overwritePartitionSpec).commit(commitMessages, finalOperation)
      postCommit(finalMessages)
      return
    }
    val activeWriteBuilder: BatchWriteBuilder =
      Option(directPostponeWriteBuilder).getOrElse(writeBuilder.asInstanceOf[BatchWriteBuilder])
    val tableCommit = activeWriteBuilder.newCommit()
    tableCommit.withMetricRegistry(metricRegistry)
    if (operation != null) {
      tableCommit.withOperation(operation)
    }
    try {
      tableCommit.commit(commitMessages.toList.asJava)
    } catch {
      case e: Throwable => throw new RuntimeException(e);
    } finally {
      tableCommit.close()
    }
    postCommit(commitMessages)
  }

  /**
   * Commits a micro-batch through the committer of the query, which the first micro-batch of a run
   * creates and every later one reuses.
   */
  private def commitMicroBatch(
      streamingWrite: StreamingWrite,
      commitMessages: Seq[CommitMessage],
      operation: Snapshot.Operation): Unit = {
    streamingWrite.context.openCommitter {
      val committer = writeBuilder
        .asInstanceOf[StreamWriteBuilder]
        .newCommit()
        .asInstanceOf[InnerTableCommit]
      committer.withMetricRegistry(metricRegistry)
      // Both are the same for every micro-batch of a query: its output mode does not change, and
      // a batch that wrote nothing is no reason to publish a snapshot, as for a batch write.
      overwritePartitionSpec.foreach(spec => committer.withOverwrite(spec.asJava))
      committer.ignoreEmptyCommit(
        Option(coreOptions.toConfiguration.get(CoreOptions.SNAPSHOT_IGNORE_EMPTY_COMMIT))
          .forall(_.booleanValue()))
      if (operation != null) {
        committer.withOperation(operation)
      }
      committer
    }
    streamingWrite.context.commit(streamingWrite, commitMessages, table)
  }

  private def baseSnapshotHasNoRealBuckets: Boolean = {
    postponeBaseSnapshotId.forall {
      snapshotId =>
        !table
          .newSnapshotReader()
          .withSnapshot(snapshotId)
          .onlyReadRealBuckets()
          .readFileIterator()
          .hasNext
    }
  }

  /** Bootstrap and repartition for cross partition mode. */
  private def bootstrapAndRepartitionByKeyHash(
      data: DataFrame,
      parallelism: Int,
      rowKindColIdx: Int,
      uriReaderFactory: UriReaderFactory): RDD[(KeyPartOrRow, Array[Byte])] = {
    val numSparkPartitions = data.rdd.getNumPartitions
    val primaryKeys = table.schema().primaryKeys()
    val bootstrapType = IndexBootstrap.bootstrapType(table.schema())
    val rowType = table.rowType()
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
            val toPaimonRow =
              SparkRowUtils.toPaimonRow(rowType, rowKindColIdx, uriReaderFactory)

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
      numAssigners: Int,
      uriReaderFactory: UriReaderFactory): DataFrame = {
    sparkSession.createDataFrame(
      data.rdd
        .mapPartitions(
          iterator => {
            val rowPartitionKeyExtractor = new RowPartitionKeyExtractor(tableSchema)
            iterator.map(
              row => {
                val sparkRow =
                  SparkRow.fromUriReaderFactory(writeType, row, RowKind.INSERT, uriReaderFactory)
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
