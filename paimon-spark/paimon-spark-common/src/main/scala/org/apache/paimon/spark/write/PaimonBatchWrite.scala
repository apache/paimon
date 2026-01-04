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

import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement}
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.commands.SparkDataFileMeta
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.spark.rowops.PaimonCopyOnWriteScan
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage, CommitMessageImpl}

import org.apache.spark.sql.PaimonSparkSession
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType

import java.util.Collections

import scala.collection.JavaConverters._

case class PaimonBatchWrite(
    table: FileStoreTable,
    writeSchema: StructType,
    dataSchema: StructType,
    overwritePartitions: Option[Map[String, String]],
    copyOnWriteScan: Option[PaimonCopyOnWriteScan])
  extends BatchWrite
  with WriteHelper {

  protected val metricRegistry = SparkMetricRegistry()

  protected val batchWriteBuilder: BatchWriteBuilder = {
    val builder = table.newBatchWriteBuilder()
    overwritePartitions.foreach(partitions => builder.withOverwrite(partitions.asJava))
    builder
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    val fullCompactionDeltaCommits: Option[Int] =
      Option.apply(coreOptions.fullCompactionDeltaCommits())
    (_: Int, _: Long) => {
      PaimonV2DataWriter(batchWriteBuilder, writeSchema, dataSchema, fullCompactionDeltaCommits)
    }
  }

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Committing to table ${table.name()}")
    val batchTableCommit = batchWriteBuilder.newCommit()
    batchTableCommit.withMetricRegistry(metricRegistry)
    val addCommitMessage = WriteTaskResult.merge(messages)
    val deletedCommitMessage = copyOnWriteScan match {
      case Some(scan) => buildDeletedCommitMessage(scan.scannedFiles)
      case None => Seq.empty
    }
    val commitMessages = addCommitMessage ++ deletedCommitMessage
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

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // TODO clean uncommitted files
  }

  private def buildDeletedCommitMessage(
      deletedFiles: Seq[SparkDataFileMeta]): Seq[CommitMessage] = {
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
