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

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.operation.FileStoreCommit
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.BatchWriteBuilder
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{FileStorePathFactory, RowDataPartitionComputer}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement
import org.apache.spark.sql.types.StructType

import java.util.{Collections, Map => JMap, UUID}

import scala.collection.JavaConverters._

trait PaimonPartitionManagement extends SupportsPartitionManagement {
  self: SparkTable =>

  def partitionKeys() = getTable.partitionKeys

  def tableRowType() = new RowType(
    getTable.rowType.getFields.asScala
      .filter(dataFiled => partitionKeys.contains(dataFiled.name()))
      .asJava)

  override def partitionSchema(): StructType = {
    SparkTypeUtils.fromPaimonRowType(tableRowType)
  }

  override def dropPartition(internalRow: InternalRow): Boolean = {
    // convert internalRow to row
    val row: Row = CatalystTypeConverters
      .createToScalaConverter(partitionSchema())
      .apply(internalRow)
      .asInstanceOf[Row]
    val rowDataPartitionComputer = new RowDataPartitionComputer(
      CoreOptions.PARTITION_DEFAULT_NAME.defaultValue,
      tableRowType,
      partitionKeys.asScala.toArray)
    val partitionMap = rowDataPartitionComputer.generatePartValues(new SparkRow(tableRowType, row))
    getTable match {
      case table: FileStoreTable =>
        val commit: FileStoreCommit = table.store.newCommit(UUID.randomUUID.toString)
        commit.dropPartitions(
          Collections.singletonList(partitionMap),
          BatchWriteBuilder.COMMIT_IDENTIFIER)
      case _ =>
        throw new UnsupportedOperationException(
          "Only AbstractFileStoreTable supports drop partitions.")
    }
    true
  }

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: JMap[String, String]): Unit = {
    throw new UnsupportedOperationException("Replace partition is not supported")
  }

  override def loadPartitionMetadata(ident: InternalRow): JMap[String, String] = {
    throw new UnsupportedOperationException("Load partition is not supported")
  }

  override def listPartitionIdentifiers(
      partitionCols: Array[String],
      internalRow: InternalRow): Array[InternalRow] = {
    assert(
      partitionCols.length == internalRow.numFields,
      s"Number of partition names (${partitionCols.length}) must be equal to " +
        s"the number of partition values (${internalRow.numFields})."
    )
    val schema: StructType = partitionSchema()
    assert(
      partitionCols.forall(fieldName => schema.fieldNames.contains(fieldName)),
      s"Some partition names ${partitionCols.mkString("[", ", ", "]")} don't belong to " +
        s"the partition schema '${schema.sql}'."
    )

    val tableScan = getTable.newReadBuilder.newScan
    val binaryRows = tableScan.listPartitions.asScala.toList
    binaryRows
      .map(
        binaryRow => {
          val sparkInternalRow: SparkInternalRow =
            new SparkInternalRow(SparkTypeUtils.toPaimonType(schema).asInstanceOf[RowType])
          sparkInternalRow.replace(binaryRow)
        })
      .filter(
        sparkInternalRow => {
          partitionCols.zipWithIndex
            .map {
              case (partitionName, index) =>
                val internalRowIndex = schema.fieldIndex(partitionName)
                val structField = schema.fields(internalRowIndex)
                sparkInternalRow
                  .get(internalRowIndex, structField.dataType)
                  .equals(internalRow.get(index, structField.dataType))
            }
            .forall(identity)
        })
      .toArray
  }

  override def createPartition(ident: InternalRow, properties: JMap[String, String]): Unit = {
    throw new UnsupportedOperationException("Create partition is not supported")
  }
}
