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

import org.apache.paimon.data.{InternalRow => PaimonInternalRow}
import org.apache.paimon.index.HashBucketAssigner
import org.apache.paimon.spark.SparkRow
import org.apache.paimon.spark.util.EncoderUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.RowPartitionKeyExtractor

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{InternalRow => SparkInternalRow}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.{Deserializer, Serializer}
import org.apache.spark.sql.types.StructType

import java.util.UUID

case class EncoderSerDeGroup(schema: StructType) {

  val encoder: ExpressionEncoder[Row] = EncoderUtils.encode(schema).resolveAndBind()

  private val serializer: Serializer[Row] = encoder.createSerializer()

  private val deserializer: Deserializer[Row] = encoder.createDeserializer()

  def rowToInternal(row: Row): SparkInternalRow = {
    serializer(row)
  }

  def internalToRow(internalRow: SparkInternalRow): Row = {
    deserializer(internalRow)
  }
}

sealed trait BucketProcessor {
  def processPartition(rowIterator: Iterator[Row]): Iterator[Row]
}

case class CommonBucketProcessor(
    table: FileStoreTable,
    bucketColIndex: Int,
    encoderGroup: EncoderSerDeGroup)
  extends BucketProcessor {

  def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {
    val rowType = table.rowType()
    val rowKeyExtractor = table.createRowKeyExtractor()

    def getBucketId(row: PaimonInternalRow): Int = {
      rowKeyExtractor.setRecord(row)
      rowKeyExtractor.bucket()
    }

    new Iterator[Row] {
      override def hasNext: Boolean = rowIterator.hasNext

      override def next(): Row = {
        val row = rowIterator.next
        val sparkInternalRow = encoderGroup.rowToInternal(row)
        sparkInternalRow.setInt(bucketColIndex, getBucketId(new SparkRow(rowType, row)))
        encoderGroup.internalToRow(sparkInternalRow)
      }
    }
  }
}

case class DynamicBucketProcessor(
    fileStoreTable: FileStoreTable,
    bucketColIndex: Int,
    numSparkPartitions: Int,
    numAssigners: Int,
    encoderGroup: EncoderSerDeGroup
) extends BucketProcessor {

  private val targetBucketRowNumber = fileStoreTable.coreOptions.dynamicBucketTargetRowNum
  private val rowType = fileStoreTable.rowType
  private val commitUser = UUID.randomUUID.toString

  def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {
    val rowPartitionKeyExtractor = new RowPartitionKeyExtractor(fileStoreTable.schema)
    val assigner =
      new HashBucketAssigner(
        fileStoreTable.snapshotManager(),
        commitUser,
        fileStoreTable.store.newIndexFileHandler,
        numSparkPartitions,
        numAssigners,
        TaskContext.getPartitionId(),
        targetBucketRowNumber
      )

    new Iterator[Row]() {
      override def hasNext: Boolean = rowIterator.hasNext

      override def next(): Row = {
        val row = rowIterator.next
        val sparkRow = new SparkRow(rowType, row)
        val hash = rowPartitionKeyExtractor.trimmedPrimaryKey(sparkRow).hashCode
        val partition = rowPartitionKeyExtractor.partition(sparkRow)
        val bucket = assigner.assign(partition, hash)
        val sparkInternalRow = encoderGroup.rowToInternal(row)
        sparkInternalRow.setInt(bucketColIndex, bucket)
        encoderGroup.internalToRow(sparkInternalRow)
      }
    }
  }
}
