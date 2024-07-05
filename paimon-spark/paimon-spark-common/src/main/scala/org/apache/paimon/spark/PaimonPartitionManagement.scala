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

import org.apache.paimon.operation.FileStoreCommit
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.BatchWriteBuilder
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{InternalRowPartitionComputer, TypeUtils}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement
import org.apache.spark.sql.types.StructType

import java.util.{Collections, Map => JMap, Objects, UUID}

import scala.collection.JavaConverters._

trait PaimonPartitionManagement extends SupportsPartitionManagement {
  self: SparkTable =>

  private lazy val partitionRowType: RowType = TypeUtils.project(table.rowType, table.partitionKeys)

  override lazy val partitionSchema: StructType = SparkTypeUtils.fromPaimonRowType(partitionRowType)

  override def dropPartition(internalRow: InternalRow): Boolean = {
    table match {
      case fileStoreTable: FileStoreTable =>
        // convert internalRow to row
        val row: Row = CatalystTypeConverters
          .createToScalaConverter(CharVarcharUtils.replaceCharVarcharWithString(partitionSchema))
          .apply(internalRow)
          .asInstanceOf[Row]
        val rowDataPartitionComputer = new InternalRowPartitionComputer(
          fileStoreTable.coreOptions().partitionDefaultName(),
          partitionRowType,
          table.partitionKeys().asScala.toArray)
        val partitionMap =
          rowDataPartitionComputer.generatePartValues(new SparkRow(partitionRowType, row))
        val commit: FileStoreCommit = fileStoreTable.store.newCommit(UUID.randomUUID.toString)
        commit.dropPartitions(
          Collections.singletonList(partitionMap),
          BatchWriteBuilder.COMMIT_IDENTIFIER)
        true
      case _ =>
        throw new UnsupportedOperationException("Only FileStoreTable supports drop partitions.")
    }
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
    assert(
      partitionCols.forall(fieldName => partitionSchema.fieldNames.contains(fieldName)),
      s"Some partition names ${partitionCols.mkString("[", ", ", "]")} don't belong to " +
        s"the partition schema '${partitionSchema.sql}'."
    )
    table.newReadBuilder.newScan.listPartitions.asScala
      .map(binaryRow => SparkInternalRow.fromPaimon(binaryRow, partitionRowType))
      .filter(
        sparkInternalRow => {
          partitionCols.zipWithIndex
            .map {
              case (partitionName, index) =>
                val internalRowIndex = partitionSchema.fieldIndex(partitionName)
                val structField = partitionSchema.fields(internalRowIndex)
                Objects.equals(
                  sparkInternalRow.get(internalRowIndex, structField.dataType),
                  internalRow.get(index, structField.dataType))
            }
            .forall(identity)
        })
      .toArray
  }

  override def createPartition(ident: InternalRow, properties: JMap[String, String]): Unit = {
    throw new UnsupportedOperationException("Create partition is not supported")
  }
}
