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

package org.apache.paimon.spark.write

import org.apache.paimon.CoreOptions
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement}
import org.apache.paimon.options.Options
import org.apache.paimon.spark._
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.commands.{SchemaHelper, SparkDataFileMeta}
import org.apache.paimon.spark.commands.SparkDataFileMeta.convertToSparkDataFileMeta
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage, CommitMessageImpl, TableWriteImpl}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.internal.Logging
import org.apache.spark.sql.PaimonSparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType

import java.util.Collections

import scala.collection.JavaConverters._

class PaimonV2Write(
    override val originTable: FileStoreTable,
    overwriteDynamic: Boolean,
    overwritePartitions: Option[Map[String, String]],
    dataSchema: StructType,
    options: Options
) extends Write
  with RequiresDistributionAndOrdering
  with SchemaHelper
  with Logging {

  assert(
    !(overwriteDynamic && overwritePartitions.exists(_.nonEmpty)),
    "Cannot overwrite dynamically and by filter both")

  private var isOverwriteFiles = false

  private var copyOnWriteScan: Option[PaimonCopyOnWriteScan] = None

  private val writeSchema = mergeSchema(dataSchema, options)

  def overwriteFiles(scan: Option[PaimonCopyOnWriteScan]): PaimonV2Write = {
    this.isOverwriteFiles = true
    this.copyOnWriteScan = scan
    this
  }

  updateTableWithOptions(
    Map(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key -> overwriteDynamic.toString))

  private val writeRequirement = PaimonWriteRequirement(table)

  override def requiredDistribution(): Distribution = {
    val distribution = writeRequirement.distribution
    logInfo(s"Requesting $distribution as write distribution for table ${table.name()}")
    distribution
  }

  override def requiredOrdering(): Array[SortOrder] = {
    val ordering = writeRequirement.ordering
    logInfo(s"Requesting ${ordering.mkString(",")} as write ordering for table ${table.name()}")
    ordering
  }

  override def toBatch: BatchWrite = {
    if (isOverwriteFiles) {
      CopyOnWriteBatchWrite(table, writeSchema, dataSchema, overwritePartitions, copyOnWriteScan)
    } else {
      PaimonBatchWrite(table, writeSchema, dataSchema, overwritePartitions)
    }
  }

  override def supportedCustomMetrics(): Array[CustomMetric] = {
    Array(
      // write metrics
      PaimonNumWritersMetric(),
      // commit metrics
      PaimonCommitDurationMetric(),
      PaimonAppendedTableFilesMetric(),
      PaimonAppendedRecordsMetric(),
      PaimonAppendedChangelogFilesMetric(),
      PaimonPartitionsWrittenMetric(),
      PaimonBucketsWrittenMetric()
    )
  }

  override def toString: String = {
    val overwriteDynamicStr = if (overwriteDynamic) {
      ", overwriteDynamic=true"
    } else {
      ""
    }
    val overwritePartitionsStr = overwritePartitions match {
      case Some(partitions) if partitions.nonEmpty => s", overwritePartitions=$partitions"
      case Some(_) => ", overwriteTable=true"
      case None => ""
    }
    s"PaimonWrite(table=${table.fullName()}$overwriteDynamicStr$overwritePartitionsStr)"
  }

  override def description(): String = toString
}

private case class PaimonBatchWrite(
    table: FileStoreTable,
    writeSchema: StructType,
    dataSchema: StructType,
    overwritePartitions: Option[Map[String, String]])
  extends PaimonBatchWriteBase(table, writeSchema, dataSchema, overwritePartitions) {}

abstract class PaimonBatchWriteBase(
    table: FileStoreTable,
    writeSchema: StructType,
    dataSchema: StructType,
    overwritePartitions: Option[Map[String, String]])
  extends BatchWrite
  with WriteHelper {

  protected val metricRegistry = SparkMetricRegistry()

  protected val batchWriteBuilder = {
    val builder = table.newBatchWriteBuilder()
    overwritePartitions.foreach(partitions => builder.withOverwrite(partitions.asJava))
    builder
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    val fullCompactionDeltaCommits: Option[Int] =
      Option.apply(coreOptions.fullCompactionDeltaCommits())
    WriterFactory(writeSchema, dataSchema, batchWriteBuilder, fullCompactionDeltaCommits)
  }

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Committing to table ${table.name()}")
    val batchTableCommit = batchWriteBuilder.newCommit()
    batchTableCommit.withMetricRegistry(metricRegistry)
    val commitMessages = WriteTaskResult.merge(messages)
    try {
      val start = System.currentTimeMillis()
      batchTableCommit.commit(commitMessages.asJava)
      logInfo(s"Committed in ${System.currentTimeMillis() - start} ms")
    } finally {
      batchTableCommit.close()
    }
    postDriverMetrics()
    postCommit(commitMessages)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // TODO clean uncommitted files
  }

  // Spark support v2 write driver metrics since 4.0, see https://github.com/apache/spark/pull/48573
  // To ensure compatibility with 3.x, manually post driver metrics here instead of using Spark's API.
  protected def postDriverMetrics(): Unit = {
    val spark = PaimonSparkSession.active
    // todo: find a more suitable way to get metrics.
    val commitMetrics = metricRegistry.buildSparkCommitMetrics()
    val executionId = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val executionMetrics = Compatibility.getExecutionMetrics(spark, executionId.toLong).distinct
    val metricUpdates = executionMetrics.flatMap {
      m =>
        commitMetrics.find(x => m.metricType.toLowerCase.contains(x.name.toLowerCase)) match {
          case Some(customTaskMetric) => Some((m.accumulatorId, customTaskMetric.value()))
          case None => None
        }
    }
    SQLMetrics.postDriverMetricsUpdatedByValue(spark.sparkContext, executionId, metricUpdates)
  }
}

private case class CopyOnWriteBatchWrite(
    table: FileStoreTable,
    writeSchema: StructType,
    dataSchema: StructType,
    overwritePartitions: Option[Map[String, String]],
    scan: Option[PaimonCopyOnWriteScan])
  extends PaimonBatchWriteBase(table, writeSchema, dataSchema, overwritePartitions) {

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"CopyOnWrite committing to table ${table.name()}")

    val batchTableCommit = batchWriteBuilder.newCommit()

    try {
      if (scan.isEmpty) {
        batchTableCommit.truncateTable()
      } else {
        val touchedFiles = candidateFiles(scan.get.dataSplits)

        val deletedCommitMessage = buildDeletedCommitMessage(touchedFiles)

        val addCommitMessages = WriteTaskResult.merge(messages)

        val commitMessages = addCommitMessages ++ deletedCommitMessage

        batchTableCommit.withMetricRegistry(metricRegistry)
        val start = System.currentTimeMillis()
        batchTableCommit.commit(commitMessages.asJava)
        logInfo(s"Committed in ${System.currentTimeMillis() - start} ms")
        postCommit(commitMessages)
      }
    } finally {
      batchTableCommit.close()
      postDriverMetrics()
    }
  }

  private def candidateFiles(candidateDataSplits: Seq[DataSplit]): Array[SparkDataFileMeta] = {
    val totalBuckets = coreOptions.bucket()
    candidateDataSplits
      .flatMap(dataSplit => convertToSparkDataFileMeta(dataSplit, totalBuckets))
      .toArray
  }

  private def buildDeletedCommitMessage(
      deletedFiles: Array[SparkDataFileMeta]): Seq[CommitMessage] = {
    deletedFiles
      .groupBy(f => (f.partition, f.bucket))
      .map {
        case ((partition, bucket), files) =>
          val deletedDataFileMetas = files.map(_.dataFileMeta).toList.asJava

          new CommitMessageImpl(
            partition,
            bucket,
            files.head.totalBuckets,
            new DataIncrement(
              Collections.emptyList[DataFileMeta],
              deletedDataFileMetas,
              Collections.emptyList[DataFileMeta]),
            new CompactIncrement(
              Collections.emptyList[DataFileMeta],
              Collections.emptyList[DataFileMeta],
              Collections.emptyList[DataFileMeta])
          )
      }
      .toSeq
  }
}

private case class WriterFactory(
    writeSchema: StructType,
    dataSchema: StructType,
    batchWriteBuilder: BatchWriteBuilder,
    fullCompactionDeltaCommits: Option[Int])
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    PaimonV2DataWriter(batchWriteBuilder, writeSchema, dataSchema, fullCompactionDeltaCommits)
  }
}

private case class PaimonV2DataWriter(
    writeBuilder: BatchWriteBuilder,
    writeSchema: StructType,
    dataSchema: StructType,
    fullCompactionDeltaCommits: Option[Int],
    batchId: Option[Long] = None)
  extends abstractInnerTableDataWrite[InternalRow]
  with InnerTableV2DataWrite {

  private val ioManager = SparkUtils.createIOManager()

  private val metricRegistry = SparkMetricRegistry()

  val write: TableWriteImpl[InternalRow] = {
    writeBuilder
      .newWrite()
      .withIOManager(ioManager)
      .withMetricRegistry(metricRegistry)
      .asInstanceOf[TableWriteImpl[InternalRow]]
  }

  private val rowConverter: InternalRow => SparkInternalRowWrapper = {
    val numFields = writeSchema.fields.length
    val reusableWrapper = new SparkInternalRowWrapper(-1, writeSchema, dataSchema, numFields)
    record => reusableWrapper.replace(record)
  }

  override def write(record: InternalRow): Unit = {
    postWrite(write.writeAndReturn(rowConverter.apply(record)))
  }

  override def commitImpl(): Seq[CommitMessage] = {
    write.prepareCommit().asScala.toSeq
  }

  override def abort(): Unit = close()

  override def close(): Unit = {
    try {
      write.close()
      ioManager.close()
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    metricRegistry.buildSparkWriteMetrics()
  }
}
