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

import org.apache.paimon.crosspartition.{GlobalIndexAssigner, KeyPartOrRow}
import org.apache.paimon.data.{BinaryRow, GenericRow, InternalRow => PaimonInternalRow, JoinedRow}
import org.apache.paimon.disk.IOManager
import org.apache.paimon.index.HashBucketAssigner
import org.apache.paimon.spark.{SparkInternalRow, SparkRow}
import org.apache.paimon.spark.SparkUtils.createIOManager
import org.apache.paimon.spark.util.EncoderUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.RowPartitionKeyExtractor
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{CloseableIterator, SerializationUtils}

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.{Deserializer, Serializer}
import org.apache.spark.sql.types.StructType

import java.util.UUID

import scala.collection.mutable

case class EncoderSerDeGroup(schema: StructType) {

  val encoder: ExpressionEncoder[Row] = EncoderUtils.encode(schema).resolveAndBind()

  private val serializer: Serializer[Row] = encoder.createSerializer()

  private val deserializer: Deserializer[Row] = encoder.createDeserializer()

  def rowToInternal(row: Row): InternalRow = {
    serializer(row)
  }

  def internalToRow(internalRow: InternalRow): Row = {
    deserializer(internalRow)
  }
}

sealed trait BucketProcessor[In] {
  def processPartition(rowIterator: Iterator[In]): Iterator[Row]
}

case class CommonBucketProcessor(
    table: FileStoreTable,
    bucketColIndex: Int,
    encoderGroup: EncoderSerDeGroup)
  extends BucketProcessor[Row] {

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
) extends BucketProcessor[Row] {

  private val targetBucketRowNumber = fileStoreTable.coreOptions.dynamicBucketTargetRowNum
  private val rowType = fileStoreTable.rowType
  private val commitUser = UUID.randomUUID.toString

  def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {
    val rowPartitionKeyExtractor = new RowPartitionKeyExtractor(fileStoreTable.schema)
    val assigner = new HashBucketAssigner(
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

case class GlobalDynamicBucketProcessor(
    fileStoreTable: FileStoreTable,
    rowType: RowType,
    numAssigners: Integer,
    encoderGroup: EncoderSerDeGroup)
  extends BucketProcessor[(KeyPartOrRow, Array[Byte])] {

  override def processPartition(
      rowIterator: Iterator[(KeyPartOrRow, Array[Byte])]): Iterator[Row] = {
    new GlobalIndexAssignerIterator(
      rowIterator,
      fileStoreTable,
      rowType,
      numAssigners,
      encoderGroup)
  }
}

class GlobalIndexAssignerIterator(
    rowIterator: Iterator[(KeyPartOrRow, Array[Byte])],
    fileStoreTable: FileStoreTable,
    rowType: RowType,
    numAssigners: Integer,
    encoderGroup: EncoderSerDeGroup)
  extends Iterator[Row]
  with AutoCloseable {

  private val queue = mutable.Queue[Row]()

  val ioManager: IOManager = createIOManager

  var currentResult: Row = _

  var advanced = false

  val assigner: GlobalIndexAssigner = {
    val _assigner = new GlobalIndexAssigner(fileStoreTable)
    _assigner.open(
      0,
      ioManager,
      numAssigners,
      TaskContext.getPartitionId(),
      (row, bucket) => {
        val extraRow: GenericRow = new GenericRow(2)
        extraRow.setField(0, row.getRowKind.toByteValue)
        extraRow.setField(1, bucket)
        queue.enqueue(
          encoderGroup.internalToRow(
            SparkInternalRow.fromPaimon(new JoinedRow(row, extraRow), rowType)))
      }
    )
    rowIterator.foreach {
      row =>
        {
          val internalRow = SerializationUtils.deserializeBinaryRow(row._2)
          row._1 match {
            case KeyPartOrRow.KEY_PART => _assigner.bootstrapKey(internalRow)
            case KeyPartOrRow.ROW => _assigner.processInput(internalRow)
            case _ =>
              throw new UnsupportedOperationException(s"unknown kind ${row._1}")
          }
        }
    }
    _assigner
  }

  private val emitIterator: CloseableIterator[BinaryRow] = assigner.endBoostrapWithoutEmit(true)

  override def hasNext: Boolean = {
    advanceIfNeeded()
    currentResult != null
  }

  override def next(): Row = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    advanced = false
    currentResult
  }

  def advanceIfNeeded(): Unit = {
    if (!advanced) {
      advanced = true
      currentResult = null
      var stop = false
      while (!stop) {
        if (queue.nonEmpty) {
          currentResult = queue.dequeue()
          stop = true
        } else if (emitIterator.hasNext) {
          assigner.processInput(emitIterator.next())
        } else {
          stop = true
        }
      }
    }
  }

  override def close(): Unit = {
    emitIterator.close()
    assigner.close()
    if (ioManager != null) {
      ioManager.close()
    }
  }
}
