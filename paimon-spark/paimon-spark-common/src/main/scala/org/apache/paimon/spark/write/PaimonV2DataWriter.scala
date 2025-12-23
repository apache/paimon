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

import org.apache.paimon.spark.{SparkInternalRowWrapper, SparkUtils}
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage, TableWriteImpl}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class PaimonV2DataWriter(
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
