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
import org.apache.paimon.spark.{SparkInternalRowWrapper, SparkUtils}
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage, TableWriteImpl}
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{IOUtils, UriReaderFactory}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.types.StructType

import java.util.function.Consumer

import scala.collection.JavaConverters._

case class PaimonV2DataWriter(
    writeBuilder: BatchWriteBuilder,
    writeSchema: StructType,
    dataSchema: StructType,
    coreOptions: CoreOptions,
    uriReaderFactory: UriReaderFactory,
    batchId: Option[Long] = None,
    paimonWriteType: Option[RowType] = None,
    metadataSchema: Option[StructType] = None,
    plainWriteSchema: Option[StructType] = None)
  extends abstractInnerTableDataWrite[InternalRow]
  with InnerTableV2DataWrite {

  private val ioManager = SparkUtils.createIOManager()
  private val metricRegistry = SparkMetricRegistry()

  val fullCompactionDeltaCommits: Option[Int] =
    Option.apply(coreOptions.fullCompactionDeltaCommits())

  private def createTableWrite(writeType: Option[RowType]): TableWriteImpl[InternalRow] = {
    val w = writeBuilder
      .newWrite()
      .withIOManager(ioManager)
      .withMetricRegistry(metricRegistry)
      .asInstanceOf[TableWriteImpl[InternalRow]]
    writeType.foreach(w.withWriteType)
    w
  }

  val write: TableWriteImpl[InternalRow] = createTableWrite(paimonWriteType)

  private var plainWrite: Option[TableWriteImpl[InternalRow]] = None

  private def getPlainWrite: TableWriteImpl[InternalRow] = {
    plainWrite.getOrElse {
      val w = createTableWrite(None)
      plainWrite = Some(w)
      w
    }
  }

  private val cleanup = new SparkAttemptCleanup(
    writeBuilder.tableName(),
    SparkAttemptCleanup.commitUserOrUnknown(writeBuilder),
    writeBuilder,
    () => closeLocalResources())

  override protected def attemptCleanup: Option[SparkAttemptCleanup] = Some(cleanup)

  private def closeLocalResources(): Unit = {
    try {
      val closeables = Seq[AutoCloseable](write) ++ plainWrite.toSeq ++ Seq(ioManager)
      IOUtils.closeAll(closeables.asJava)
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }

  private def createRowConverter(
      writeSchema: StructType,
      schema: StructType): InternalRow => SparkInternalRowWrapper = {
    val numFields = writeSchema.fields.length
    val reusableWrapper =
      SparkInternalRowWrapper.fromUriReaderFactory(writeSchema, numFields, schema, uriReaderFactory)
    record => reusableWrapper.replace(record)
  }

  private val rowConverter: InternalRow => SparkInternalRowWrapper =
    createRowConverter(writeSchema, dataSchema)

  private val plainRowConverter: Option[InternalRow => SparkInternalRowWrapper] =
    plainWriteSchema.map(schema => createRowConverter(schema, dataSchema))

  private val metadataAwareRowConverter: Option[InternalRow => SparkInternalRowWrapper] =
    metadataSchema.map(
      schema => createRowConverter(writeSchema, StructType(dataSchema.fields ++ schema.fields)))

  private val joinedRow = new JoinedRow()

  override def write(record: InternalRow): Unit = {
    cleanup.checkInterruptedPeriodically()
    plainRowConverter match {
      case Some(converter) =>
        postWrite(getPlainWrite.writeAndReturn(converter.apply(record)))
      case _ =>
        postWrite(write.writeAndReturn(rowConverter.apply(record)))
    }
  }

  def writeWithMetadata(metadata: InternalRow, record: InternalRow): Unit = {
    metadataAwareRowConverter match {
      case Some(converter) =>
        postWrite(write.writeAndReturn(converter.apply(joinedRow(record, metadata))))
      case None =>
        write(record)
    }
  }

  override def commitImpl(): Seq[CommitMessage] = {
    val result = scala.collection.mutable.ListBuffer[CommitMessage]()
    val onPrepared = new Consumer[CommitMessage] {
      override def accept(msg: CommitMessage): Unit = {
        registerPrepared(Seq(msg))
        result += msg
      }
    }
    write.prepareCommit(onPrepared)
    plainWrite.foreach(_.prepareCommit(onPrepared))
    result.toSeq
  }

  override def abort(): Unit = {
    // abortPrepared deletes prepared files (or closes the writer if still writing). Delegate
    // residual writer/ioManager close to cleanup.close() so failures are swallowed consistently
    // and we do not double-close outside the cleanup state machine.
    cleanup.abortPrepared()
    cleanup.close()
  }

  override def close(): Unit = cleanup.close()

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    metricRegistry.buildSparkWriteMetrics()
  }
}
