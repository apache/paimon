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
import org.apache.paimon.options.Options
import org.apache.paimon.spark.{PaimonAppendedChangelogFilesMetric, PaimonAppendedRecordsMetric, PaimonAppendedTableFilesMetric, PaimonBucketsWrittenMetric, PaimonCommitDurationMetric, PaimonNumWritersMetric, PaimonPartitionsWrittenMetric, SparkInternalRowWrapper, SparkUtils}
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.commands.SchemaHelper
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage, CommitMessageSerializer, TableWriteImpl}

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

import java.io.{IOException, UncheckedIOException}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

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

  private val writeSchema = mergeSchema(dataSchema, options)

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

  override def toBatch: BatchWrite =
    PaimonBatchWrite(table, writeSchema, dataSchema, overwritePartitions)

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
  extends BatchWrite
  with WriteHelper {

  private val metricRegistry = SparkMetricRegistry()

  private val batchWriteBuilder = {
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

    val commitMessages = messages
      .collect {
        case taskCommit: TaskCommit => taskCommit.commitMessages()
        case other =>
          throw new IllegalArgumentException(s"${other.getClass.getName} is not supported")
      }
      .flatten
      .toSeq

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
  private def postDriverMetrics(): Unit = {
    val spark = PaimonSparkSession.active
    // todo: find a more suitable way to get metrics.
    val commitMetrics = metricRegistry.buildSparkCommitMetrics()
    val executionId = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val executionMetrics = Compatibility.getExecutionMetrics(spark, executionId.toLong)
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

private case class WriterFactory(
    writeSchema: StructType,
    dataSchema: StructType,
    batchWriteBuilder: BatchWriteBuilder,
    fullCompactionDeltaCommits: Option[Int])
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    PaimonDataWriter(batchWriteBuilder, writeSchema, dataSchema, fullCompactionDeltaCommits)
  }
}

private case class PaimonDataWriter(
    writeBuilder: BatchWriteBuilder,
    writeSchema: StructType,
    dataSchema: StructType,
    fullCompactionDeltaCommits: Option[Int],
    batchId: Long = -1)
  extends DataWriter[InternalRow]
  with DataWriteHelper {

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

  override def commit(): WriterCommitMessage = {
    try {
      preFinish()
      val commitMessages = write.prepareCommit().asScala
      TaskCommit(commitMessages)
    } finally {
      close()
    }
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

class TaskCommit private (
    private val serializedMessageBytes: Seq[Array[Byte]]
) extends WriterCommitMessage {
  def commitMessages(): Seq[CommitMessage] = {
    val deserializer = new CommitMessageSerializer()
    serializedMessageBytes.map {
      bytes =>
        Try(deserializer.deserialize(deserializer.getVersion, bytes)) match {
          case Success(msg) => msg
          case Failure(e: IOException) => throw new UncheckedIOException(e)
          case Failure(e) => throw e
        }
    }
  }
}

object TaskCommit {
  def apply(commitMessages: Seq[CommitMessage]): TaskCommit = {
    val serializer = new CommitMessageSerializer()
    val serializedBytes: Seq[Array[Byte]] = Option(commitMessages)
      .filter(_.nonEmpty)
      .map(_.map {
        msg =>
          Try(serializer.serialize(msg)) match {
            case Success(serializedBytes) => serializedBytes
            case Failure(e: IOException) => throw new UncheckedIOException(e)
            case Failure(e) => throw e
          }
      })
      .getOrElse(Seq.empty)

    new TaskCommit(serializedBytes)
  }
}
